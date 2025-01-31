import os
import json
import struct
from typing import Dict, Optional, List, Tuple, Iterator
from ..file_manager.manager import FileManager
from ..filter.bloom import BloomFilter
from ..config import default_config

class SSTableMetadata:
    """SSTable元数据"""
    
    def __init__(self, level: int, sequence: int, data_size: int, min_key: str, max_key: str,
                 index_offset: int, bloom_offset: int):
        """初始化SSTable元数据
        
        Args:
            level: 层级
            sequence: 序列号
            data_size: 数据大小
            min_key: 最小键
            max_key: 最大键
            index_offset: 索引区域的偏移量
            bloom_offset: 布隆过滤器的偏移量
        """
        self.level = level
        self.sequence = sequence
        self.data_size = data_size
        self.min_key = min_key
        self.max_key = max_key
        self.index_offset = index_offset
        self.bloom_offset = bloom_offset
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'level': self.level,
            'sequence': self.sequence,
            'data_size': self.data_size,
            'min_key': self.min_key,
            'max_key': self.max_key,
            'index_offset': self.index_offset,
            'bloom_offset': self.bloom_offset
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'SSTableMetadata':
        """从字典创建元数据"""
        return cls(
            level=data['level'],
            sequence=data['sequence'],
            data_size=data['data_size'],
            min_key=data['min_key'],
            max_key=data['max_key'],
            index_offset=data['index_offset'],
            bloom_offset=data['bloom_offset']
        )

class SSTable:
    """
    SSTable实现 - 单文件格式
    
    文件布局：
    +----------------+  <- 0
    |    Header     |  文件头(4KB)：魔数(4B) + 版本(4B) + 元数据
    +----------------+  <- 4KB
    |     Data      |  数据区域：key_size(4B) + key + value_size(4B) + value
    |      ...      |
    +----------------+  <- index_offset
    |     Index     |  索引区域：稀疏索引，每N条记录一个索引项
    |      ...      |
    +----------------+  <- bloom_offset
    |    Bloom      |  布隆过滤器
    |    Filter     |
    +----------------+
    """
    
    # 常量定义
    INDEX_INTERVAL = 128  # 每128条记录创建一个索引项
    
    def __init__(self, 
                 base_dir: str,
                 level: int,
                 sequence: int):
        """初始化SSTable
        
        Args:
            base_dir: 基础目录
            level: 层级
            sequence: 序列号
        """
        self.base_dir = base_dir
        self.level = level
        self.sequence = sequence
        
        # 文件路径
        self.file_path = os.path.join(base_dir, f"sst_{sequence}.sst")
        
        # 布隆过滤器
        self.filter = None
        
        # 元数据
        self.metadata = None
        
        # 索引
        self.index = {}
    
    @classmethod
    def _compare_keys(cls, key1: str, key2: str) -> int:
        """比较两个键的大小
        
        先比较长度，长度相同时按字符串比较
        
        Args:
            key1: 第一个键
            key2: 第二个键
            
        Returns:
            如果key1 < key2返回-1
            如果key1 == key2返回0
            如果key1 > key2返回1
        """
        # 先比较长度
        len1, len2 = len(key1), len(key2)
        if len1 < len2:
            return -1
        if len1 > len2:
            return 1
        # 长度相同时按字符串比较
        if key1 < key2:
            return -1
        if key1 > key2:
            return 1
        return 0
    
    @classmethod
    def create_from_memtable(cls,
                            file_manager: FileManager,
                            base_name: str,
                            level: int,
                            sequence: int,
                            entries: Iterator[Tuple[str, str]],
                            expected_entries: int) -> Optional['SSTable']:
        """
        从MemTable创建SSTable
        
        Args:
            file_manager: 文件管理器
            base_name: SSTable文件名前缀
            level: SSTable所在层级
            sequence: 序列号
            entries: 键值对迭代器，必须按键排序
            expected_entries: 预期的条目数量
            
        Returns:
            创建的SSTable实例，如果创建失败返回None
        """
        try:
            table = cls(file_manager.base_dir, level, sequence)
            
            # 创建布隆过滤器，使用合适的大小和哈希函数个数
            optimal_size = max(expected_entries * 10, 1000)  # 每个元素使用10个比特
            optimal_hash_count = 7  # 使用固定的哈希函数个数
            table.filter = BloomFilter(optimal_size, optimal_hash_count)
            
            # 写入文件头
            with open(table.file_path, 'wb') as f:
                # 写入魔数和版本号
                f.write(default_config.SST_MAGIC_NUMBER)
                f.write(struct.pack('>I', default_config.SST_VERSION))
                
                # 预留文件头空间
                f.write(b'\0' * (default_config.SST_HEADER_SIZE - 8))
            
            # 写入数据并创建索引
            data_offset = default_config.SST_HEADER_SIZE
            index_entries = []
            min_key = None
            max_key = None
            entry_count = 0
            
            with open(table.file_path, 'ab') as f:
                for key, value in entries:
                    # 更新键范围
                    if min_key is None:
                        min_key = key
                    max_key = key
                    
                    # 添加到布隆过滤器
                    table.filter.add(key)
                    
                    # 写入键值对
                    key_bytes = key.encode('utf-8')
                    value_bytes = value.encode('utf-8')
                    key_size = len(key_bytes)
                    value_size = len(value_bytes)
                    
                    record = struct.pack('>I', key_size) + key_bytes + struct.pack('>I', value_size) + value_bytes
                    f.write(record)
                    
                    # 创建索引项
                    if entry_count % cls.INDEX_INTERVAL == 0:
                        index_entries.append({
                            'key': key,
                            'offset': data_offset,
                            'size': len(record)
                        })
                    
                    data_offset += len(record)
                    entry_count += 1
                
                if entry_count == 0:  # 没有数据，创建失败
                    os.remove(table.file_path)
                    return None
                
                # 写入索引
                index_offset = data_offset
                for entry in index_entries:
                    index_line = f"{entry['key']}\t{entry['offset']}\t{entry['size']}\n"
                    f.write(index_line.encode('utf-8'))
                
                # 写入布隆过滤器
                bloom_offset = f.tell()
                # 写入布隆过滤器的大小和哈希函数个数
                f.write(struct.pack('>II', table.filter.size, table.filter.hash_count))
                # 写入位数组
                f.write(table.filter.to_bytes())
                
                # 创建元数据
                metadata = SSTableMetadata(
                    level=level,
                    sequence=sequence,
                    data_size=index_offset - default_config.SST_HEADER_SIZE,
                    min_key=min_key,
                    max_key=max_key,
                    index_offset=index_offset,
                    bloom_offset=bloom_offset
                )
                
                # 写入元数据到文件头
                metadata_json = json.dumps(metadata.to_dict())
                metadata_bytes = metadata_json.encode('utf-8')
                if len(metadata_bytes) > default_config.SST_HEADER_SIZE - 8:
                    raise ValueError("Metadata too large for header")
                
                # 使用 'r+b' 模式重写文件头
                with open(table.file_path, 'r+b') as header_file:
                    header_file.seek(8)  # 跳过魔数和版本号
                    header_file.write(metadata_bytes)
                    header_file.write(b'\0' * (default_config.SST_HEADER_SIZE - 8 - len(metadata_bytes)))
            
            table.metadata = metadata
            # 加载索引到内存
            table.index = {entry['key']: (entry['offset'], entry['size']) for entry in index_entries}
            return table
            
        except Exception as e:
            print(f"Error creating SSTable: {e}")
            if os.path.exists(table.file_path):
                try:
                    os.remove(table.file_path)
                except:
                    pass
            return None

    def load(self) -> bool:
        """加载SSTable
        
        Returns:
            是否成功加载
        """
        try:
            with open(self.file_path, 'rb') as f:
                # 验证魔数
                magic = f.read(4)
                if magic != default_config.SST_MAGIC_NUMBER:
                    print("Invalid magic number")
                    return False
                
                # 验证版本号
                version = struct.unpack('>I', f.read(4))[0]
                if version != default_config.SST_VERSION:
                    print("Invalid version")
                    return False
                
                # 读取元数据
                metadata_bytes = f.read(default_config.SST_HEADER_SIZE - 8)
                try:
                    # 找到第一个 null 字节的位置
                    null_pos = metadata_bytes.find(b'\0')
                    if null_pos == -1:
                        null_pos = len(metadata_bytes)
                    metadata_json = metadata_bytes[:null_pos].decode('utf-8')
                    if not metadata_json:
                        print("Empty metadata")
                        return False
                    self.metadata = SSTableMetadata.from_dict(json.loads(metadata_json))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    print(f"Failed to parse metadata: {e}")
                    return False
                
                # 加载索引
                f.seek(self.metadata.index_offset)
                index_data = f.read(self.metadata.bloom_offset - self.metadata.index_offset)
                self.index = {}
                for line in index_data.decode('utf-8').splitlines():
                    if line.strip():
                        key, offset, size = line.split('\t')
                        self.index[key] = (int(offset), int(size))
                
                # 加载布隆过滤器
                f.seek(self.metadata.bloom_offset)
                # 读取布隆过滤器的大小和哈希函数个数
                size, hash_count = struct.unpack('>II', f.read(8))
                # 读取布隆过滤器的位数组数据
                filter_data = f.read()
                # 使用读取的参数创建布隆过滤器
                self.filter = BloomFilter.from_bytes(filter_data, size, hash_count)
                
                return True
        except Exception as e:
            print(f"Failed to load SSTable: {e}")
            return False
    
    def get(self, key: str) -> Optional[str]:
        """从SSTable中获取值"""
        if not self.metadata or not self.filter:
            return None
        
        # 检查布隆过滤器
        if not self.filter.contains(key):
            return None
        
        # 二分查找最近的索引项
        index_keys = sorted(self.index.keys())
        if not index_keys:
            return None
        
        # 找到小于等于目标key的最大索引项
        left, right = 0, len(index_keys) - 1
        while left <= right:
            mid = (left + right) // 2
            if self._compare_keys(index_keys[mid], key) <= 0:
                left = mid + 1
            else:
                right = mid - 1
        
        if right < 0:
            return None
        
        # 获取索引项信息
        index_key = index_keys[right]
        start_offset = self.index[index_key][0]
        
        # 从数据区域顺序查找
        with open(self.file_path, 'rb') as f:
            f.seek(start_offset)
            while f.tell() < self.metadata.index_offset:
                try:
                    # 读取键
                    key_size_bytes = f.read(4)
                    if not key_size_bytes:
                        break
                    key_size = struct.unpack('>I', key_size_bytes)[0]
                    curr_key = f.read(key_size).decode('utf-8')
                    
                    # 读取值
                    value_size_bytes = f.read(4)
                    if not value_size_bytes:
                        break
                    value_size = struct.unpack('>I', value_size_bytes)[0]
                    
                    if curr_key == key:
                        value_bytes = f.read(value_size)
                        if len(value_bytes) != value_size:
                            return None
                        return value_bytes.decode('utf-8')
                    elif self._compare_keys(curr_key, key) > 0:
                        break
                    else:
                        f.seek(value_size, 1)  # 跳过值
                except (struct.error, UnicodeDecodeError):
                    break  # 处理文件末尾或损坏的情况
        
        return None
    
    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, str]]:
        """范围查询"""
        if not self.metadata:
            return
        
        # 检查是否与查询范围有交集
        if (self._compare_keys(start_key, self.metadata.max_key) > 0 or
            self._compare_keys(end_key, self.metadata.min_key) < 0):
            return
        
        # 从数据区域开始扫描
        with open(self.file_path, 'rb') as f:
            f.seek(default_config.SST_HEADER_SIZE)  # 从数据区域开始
            while f.tell() < self.metadata.index_offset:
                try:
                    # 读取键
                    key_size_bytes = f.read(4)
                    if not key_size_bytes:
                        break
                    key_size = struct.unpack('>I', key_size_bytes)[0]
                    key = f.read(key_size).decode('utf-8')
                    
                    # 读取值大小
                    value_size_bytes = f.read(4)
                    if not value_size_bytes:
                        break
                    value_size = struct.unpack('>I', value_size_bytes)[0]
                    
                    # 检查键是否在范围内
                    if self._compare_keys(key, start_key) >= 0 and self._compare_keys(key, end_key) <= 0:
                        # 读取值
                        value = f.read(value_size).decode('utf-8')
                        yield key, value
                    elif self._compare_keys(key, end_key) > 0:
                        # 如果超出范围，提前结束
                        break
                    else:
                        # 跳过值
                        f.seek(value_size, 1)
                except (struct.error, UnicodeDecodeError) as e:
                    print(f"Error during range scan: {e}")
                    break
    
    def close(self):
        """关闭SSTable，释放资源"""
        self.filter = None
        self.metadata = None
        self.index = {}
    
    def delete(self):
        """删除SSTable文件"""
        try:
            if os.path.exists(self.file_path):
                os.remove(self.file_path)
        except Exception as e:
            print(f"Failed to delete SSTable: {e}")
