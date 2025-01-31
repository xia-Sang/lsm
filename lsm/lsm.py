import os
import threading
from typing import Optional, List, Iterator, Tuple

from .memtable.table import MemTable
from .sstable.table import SSTable
from .wal.wal import WAL
from .filter.bloom import BloomFilter
from .file_manager.manager import FileManager

class LSMTree:
    """LSM 树实现"""
    
    def __init__(self, data_dir: str, memtable_size: int = 1024 * 1024):
        """初始化 LSM 树
        
        Args:
            data_dir: 数据目录，用于存储 WAL 和 SSTable 文件
            memtable_size: MemTable 的最大大小（字节），默认 1MB
        """
        self.data_dir = data_dir
        self.memtable_size = memtable_size
        self.sequence = 0  # SSTable 序列号
        
        # 创建数据目录
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, "wal"), exist_ok=True)
        os.makedirs(os.path.join(data_dir, "sstable"), exist_ok=True)
        
        # 初始化组件
        self.memtable = MemTable()
        self.wal = WAL(os.path.join(data_dir, "wal"))
        self.sstables: List[SSTable] = []
        
        # 从磁盘恢复数据
        self._recover()
        
        # 用于并发控制
        self._lock = threading.Lock()
    
    def put(self, key: str, value: str):
        """写入键值对
        
        Args:
            key: 键
            value: 值
        """
        with self._lock:
            # 先写 WAL
            self.wal.append(key, value)
            
            # 再写 MemTable
            self.memtable.put(key, value)
            
            # 如果 MemTable 太大，触发合并
            if self.memtable.size >= self.memtable_size:
                self._compact_memtable()
    
    def get(self, key: str) -> Optional[str]:
        """获取键对应的值
        
        Args:
            key: 键
            
        Returns:
            如果键存在返回对应的值，否则返回 None
        """
        with self._lock:
            # 先查 MemTable
            value = self.memtable.get(key)
            if value is not None:
                return None if value == "\0" else value
            
            # 再查 SSTable，从新到旧查找
            for sstable in reversed(self.sstables):
                value = sstable.get(key)
                if value is not None:
                    return None if value == "\0" else value
            
            return None
    
    def delete(self, key: str):
        """删除键值对
        
        Args:
            key: 要删除的键
        """
        # 写入 \0 表示删除
        self.put(key, "\0")
    
    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, str]]:
        """范围查询
        
        Args:
            start_key: 起始键（包含）
            end_key: 结束键（包含）
            
        Returns:
            键值对迭代器
        """
        with self._lock:
            # 收集所有键值对
            result = {}
            
            # 从旧到新遍历 SSTable
            for sstable in self.sstables:
                for key, value in sstable.range_scan(start_key, end_key):
                    if key not in result:  # 只保留最新的值
                        result[key] = value
            
            # 最后查 MemTable
            for key, value in self.memtable:
                if start_key <= key <= end_key:
                    result[key] = value
            
            # 按键排序并过滤删除标记
            for key in sorted(result.keys()):
                value = result[key]
                if value != "\0":  # 跳过删除标记
                    yield key, value
    
    def compact(self):
        """手动触发合并操作
        
        这个方法会：
        1. 如果 MemTable 不为空，将其转换为 SSTable
        2. 如果有多个 SSTable，将它们合并成一个
        """
        with self._lock:
            # 先将 MemTable 转换为 SSTable
            if len(self.memtable) > 0:
                self._compact_memtable()
            
            # 然后合并所有 SSTable
            if len(self.sstables) > 1:
                self._compact_sstables()
    
    def _compact_memtable(self):
        """将 MemTable 转换为 SSTable"""
        # 确保目录存在
        os.makedirs(os.path.join(self.data_dir, "sstable"), exist_ok=True)
        
        # 获取 MemTable 中的数据
        data = list(self.memtable)  # 转换为列表以获取大小
        if not data:  # 如果 MemTable 为空，直接返回
            return
            
        # 创建文件管理器
        file_manager = FileManager(os.path.join(self.data_dir, "sstable"))
        
        # 创建新的 SSTable
        sstable = SSTable.create_from_memtable(
            file_manager=file_manager,
            base_name=f"sst_{self.sequence}",
            level=0,  # 新创建的 SSTable 总是在 level 0
            sequence=self.sequence,
            entries=iter(sorted(data, key=lambda x: x[0])),  # 确保数据按键排序
            expected_entries=len(data)
        )
        
        if sstable:
            self.sequence += 1
            # 添加到 SSTable 列表
            self.sstables.append(sstable)
            
            # 清空 MemTable 和 WAL
            self.memtable = MemTable()
            self.wal.delete()
            self.wal = WAL(os.path.join(self.data_dir, "wal"))
            
            # 如果 SSTable 太多，触发合并
            if len(self.sstables) > 3:  # 可以根据需要调整阈值
                self._compact_sstables()
        else:
            print("Failed to create SSTable")
            
    def _compact_sstables(self):
        """合并多个 SSTable"""
        if len(self.sstables) <= 1:
            return
        
        # 收集所有数据
        all_data = {}  # 使用字典来保存最新的值
        
        # 从新到旧遍历所有 SSTable，确保保留最新的值
        for sstable in reversed(self.sstables):
            try:
                # 使用范围查询获取所有数据
                min_key = sstable.metadata.min_key
                max_key = sstable.metadata.max_key
                for key, value in sstable.range_scan(min_key, max_key):
                    if key not in all_data:  # 只保留最新的值
                        all_data[key] = value
            except Exception as e:
                print(f"Error during SSTable scan: {e}")
                continue
        
        # 过滤掉已删除的键
        valid_data = [(k, v) for k, v in sorted(all_data.items()) if v != "\0"]
        
        if valid_data:  # 只在有数据时创建新的 SSTable
            # 创建文件管理器
            file_manager = FileManager(os.path.join(self.data_dir, "sstable"))
            
            # 创建新的 SSTable
            sstable = SSTable.create_from_memtable(
                file_manager=file_manager,
                base_name=f"sst_{self.sequence}",
                level=1,  # 合并后的 SSTable 在 level 1
                sequence=self.sequence,
                entries=iter(valid_data),
                expected_entries=len(valid_data)
            )
            
            if sstable:
                self.sequence += 1
                
                # 保存旧的 SSTable 列表
                old_sstables = self.sstables[:]
                
                # 更新 SSTable 列表
                self.sstables = [sstable]
                
                # 删除旧的 SSTable 文件
                for old_sstable in old_sstables:
                    try:
                        old_sstable.close()
                        old_sstable.delete()
                    except Exception as e:
                        print(f"Error deleting old SSTable: {e}")
            else:
                print("Failed to create compacted SSTable")
    
    def _recover(self):
        """从磁盘恢复数据"""
        # 恢复 SSTable
        sstable_dir = os.path.join(self.data_dir, "sstable")
        if os.path.exists(sstable_dir):
            # 查找并排序所有 SSTable 文件
            meta_files = []
            max_sequence = -1
            for name in os.listdir(sstable_dir):
                if name.endswith(".sst"):
                    try:
                        # 从文件名中提取序列号 (格式: sst_<sequence>.sst)
                        sequence = int(name[4:-4])  # 去掉 "sst_" 和 ".sst"
                        meta_files.append((sequence, name))
                        max_sequence = max(max_sequence, sequence)
                    except (ValueError, IndexError):
                        print(f"Invalid SSTable file name: {name}")
                        continue
            
            # 更新序列号为最大序列号 + 1
            self.sequence = max_sequence + 1 if max_sequence >= 0 else 0
            
            # 按序列号排序，确保按正确顺序加载（降序，最新的在前面）
            meta_files.sort(key=lambda x: x[0], reverse=True)  # 按序列号降序排序
            
            # 加载每个 SSTable
            for sequence, _ in meta_files:
                try:
                    sstable = SSTable(sstable_dir, 0, sequence)  # 先假设是 level 0
                    if sstable.load():
                        self.sstables.append(sstable)
                    else:
                        print(f"Failed to load SSTable {sequence}")
                except Exception as e:
                    print(f"Error loading SSTable {sequence}: {e}")
        
        # 然后恢复 WAL，因为它包含最新的数据
        wal_path = os.path.join(self.data_dir, "wal")
        if os.path.exists(wal_path):
            self.wal = WAL(wal_path)
            try:
                # 从 WAL 恢复数据到 MemTable
                recovered_data = list(self.wal.recover())  # 先收集所有数据
                for key, value in recovered_data:
                    self.memtable.put(key, value)
                    
                # 如果 MemTable 太大，立即进行压缩
                if self.memtable.size >= self.memtable_size:
                    self._compact_memtable()
            except Exception as e:
                print(f"Error during WAL recovery: {e}")
                # 如果 WAL 恢复失败，创建新的
                self.wal = WAL(wal_path)
                self.memtable = MemTable()

    def close(self):
        """关闭 LSM 树，确保数据持久化"""
        with self._lock:
            try:
                # 如果 MemTable 不为空，将其转换为 SSTable
                if len(self.memtable) > 0:
                    self._compact_memtable()
                
                # 如果有多个 SSTable，进行合并
                if len(self.sstables) > 1:
                    self._compact_sstables()
                
                # 确保所有 SSTable 都被正确关闭
                for sstable in self.sstables:
                    sstable.close()
                
                # 最后关闭 WAL
                self.wal.close()
            except Exception as e:
                print(f"Error during LSM tree closure: {e}")
                raise