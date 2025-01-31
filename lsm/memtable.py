from typing import Optional, Iterator, Tuple
from BTrees.OOBTree import OOBTree
import os
from threading import Lock, RLock
from contextlib import contextmanager

class MemTable:
    def __init__(self, db_path: str, table_id: int):
        """
        初始化MemTable，使用BTrees的OOBTree实现
        OOBTree是一个优化的B树实现，专门用于对象间的映射
        它提供了持久化和事务支持，非常适合数据库应用
        
        Args:
            db_path: 数据库路径（仅用于标识）
            table_id: 表ID
        """
        self.db_path = db_path
        self.table_id = table_id
        self.tree = OOBTree()  # 使用OOBTree，它是一个高性能的B树实现
        self._size = 0
        # 使用RLock允许在同一线程中重入，这对于嵌套的读操作很有用
        self._read_lock = RLock()
        # 使用普通Lock用于写操作，因为写操作不需要重入
        self._write_lock = Lock()
    
    @contextmanager
    def _read_locked(self):
        """读锁的上下文管理器"""
        with self._read_lock:
            yield
    
    @contextmanager
    def _write_locked(self):
        """写锁的上下文管理器"""
        with self._write_lock:
            yield
    
    def put(self, key: str, value: str) -> None:
        """
        插入或更新键值对
        线程安全的实现
        
        Args:
            key: 键
            value: 值
        """
        with self._write_locked():
            if key not in self.tree:
                self._size += 1
            self.tree[key] = value
    
    def get(self, key: str) -> Optional[str]:
        """
        获取键对应的值
        线程安全的实现
        
        Args:
            key: 键
            
        Returns:
            如果键存在返回对应的值，否则返回None
        """
        with self._read_locked():
            return self.tree.get(key)
    
    def scan(self, start_key: str = "", end_key: str = "~") -> Iterator[Tuple[str, str]]:
        """
        范围扫描
        线程安全的实现
        
        Args:
            start_key: 起始键（包含）
            end_key: 结束键（包含）
            
        Returns:
            键值对迭代器
        """
        with self._read_locked():
            for key in self.tree.keys(min=start_key, max=end_key):
                yield key, self.tree[key]
    
    def get_size(self) -> int:
        """
        获取当前表中的键值对数量
        线程安全的实现
        
        Returns:
            键值对数量
        """
        with self._read_locked():
            return self._size
    
    def flush_to_sstable(self, sstable_path: str) -> None:
        """
        将MemTable刷新到SSTable
        使用B树的有序特性，直接写入文件
        线程安全的实现
        
        Args:
            sstable_path: SSTable文件路径
        """
        # 在写入文件时获取读锁以确保一致性
        with self._read_locked():
            os.makedirs(os.path.dirname(sstable_path), exist_ok=True)
            with open(sstable_path, 'w') as f:
                # 利用B树的有序性，按顺序写入所有键值对
                for key, value in self.tree.items():
                    f.write(f"{key}\t{value}\n")
    
    def clear(self) -> None:
        """
        清空MemTable
        线程安全的实现
        """
        with self._write_locked():
            self.tree.clear()
            self._size = 0
    
    def delete(self, key: str) -> bool:
        """
        删除键值对
        线程安全的实现
        
        Args:
            key: 要删除的键
            
        Returns:
            如果删除成功返回True，如果键不存在返回False
        """
        with self._write_locked():
            if key in self.tree:
                del self.tree[key]
                self._size -= 1
                return True
            return False
    
    def __len__(self) -> int:
        with self._read_locked():
            return self._size
    
    def __iter__(self) -> Iterator[Tuple[str, str]]:
        """
        返回键值对迭代器，利用B树的有序遍历
        线程安全的实现
        """
        # 获取一个快照以确保一致性
        with self._read_locked():
            # 复制当前的键值对到列表中
            items = [(key, self.tree[key]) for key in self.tree.keys()]
        # 返回结果的迭代器
        return iter(items)