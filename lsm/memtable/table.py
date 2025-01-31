from typing import Dict, Optional, Iterator, Tuple
import sys

class MemTable:
    """内存表实现，使用跳表或有序字典存储键值对"""
    
    def __init__(self):
        """初始化内存表"""
        self._data: Dict[str, str] = {}  # 使用字典存储键值对
        self._size = 0  # 当前使用的内存大小（字节）
    
    def put(self, key: str, value: str):
        """插入或更新键值对
        
        Args:
            key: 键
            value: 值
        """
        # 计算新数据的大小
        new_size = sys.getsizeof(key) + sys.getsizeof(value)
        
        # 如果键已存在，减去旧数据的大小
        if key in self._data:
            old_value = self._data[key]
            self._size -= (sys.getsizeof(key) + sys.getsizeof(old_value))
        
        # 更新数据和大小
        self._data[key] = value
        self._size += new_size
    
    def get(self, key: str) -> Optional[str]:
        """获取键对应的值
        
        Args:
            key: 键
            
        Returns:
            如果键存在返回对应的值，否则返回None
        """
        return self._data.get(key)
    
    def delete(self, key: str):
        """删除键值对
        
        Args:
            key: 要删除的键
        """
        if key in self._data:
            # 更新大小
            value = self._data[key]
            self._size -= (sys.getsizeof(key) + sys.getsizeof(value))
            # 删除键值对
            del self._data[key]
    
    @staticmethod
    def _compare_keys(key1: str, key2: str) -> int:
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
    
    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, str]]:
        """范围查询
        
        Args:
            start_key: 起始键（包含）
            end_key: 结束键（包含）
            
        Returns:
            范围内的键值对迭代器
        """
        for key in sorted(self._data.keys(), key=lambda k: (len(k), k)):
            if self._compare_keys(start_key, key) <= 0 and self._compare_keys(key, end_key) <= 0:
                yield key, self._data[key]
    
    @property
    def size(self) -> int:
        """获取当前使用的内存大小（字节）"""
        return self._size
    
    def __iter__(self) -> Iterator[Tuple[str, str]]:
        """返回按键排序的键值对迭代器"""
        for key in sorted(self._data.keys(), key=lambda k: (len(k), k)):
            yield key, self._data[key]
    
    def __len__(self) -> int:
        """返回键值对数量"""
        return len(self._data)
