import math
import mmh3  # MurmurHash3，一个快速的非加密哈希函数
from typing import List

class BloomFilter:
    """布隆过滤器"""
    
    def __init__(self, size: int = 1000, hash_count: int = 7):
        """初始化布隆过滤器
        
        Args:
            size: 位数组大小
            hash_count: 哈希函数个数
        """
        # 根据预期条目数和期望的假阳性率优化大小
        self.size = size
        self.hash_count = hash_count
        self.bit_array = [False] * self.size
    
    def _get_hash_values(self, item: str) -> List[int]:
        """获取一个项的所有哈希值
        
        Args:
            item: 待哈希的项
        
        Returns:
            哈希值列表
        """
        hash_values = []
        for i in range(self.hash_count):
            # 使用不同的种子生成哈希值，确保更好的分布
            hash1 = mmh3.hash(item, i) % self.size
            hash2 = mmh3.hash(item, i + self.hash_count) % self.size
            # 使用双重哈希来生成更均匀的哈希值
            combined_hash = (hash1 + i * hash2) % self.size
            hash_values.append(abs(combined_hash))
        return hash_values
    
    def add(self, item: str):
        """添加一个项到过滤器
        
        Args:
            item: 待添加的项
        """
        for index in self._get_hash_values(item):
            self.bit_array[index] = True
    
    def contains(self, item: str) -> bool:
        """检查一个项是否可能在过滤器中
        
        Args:
            item: 待检查的项
            
        Returns:
            如果项可能在过滤器中返回True，否则返回False
        """
        return all(self.bit_array[index] for index in self._get_hash_values(item))
    
    def to_bytes(self) -> bytes:
        """将过滤器转换为字节序列
        
        Returns:
            字节序列
        """
        # 将布尔列表转换为字节序列
        result = bytearray()
        for i in range(0, len(self.bit_array), 8):
            byte = 0
            for j in range(8):
                if i + j < len(self.bit_array) and self.bit_array[i + j]:
                    byte |= (1 << j)
            result.append(byte)
        return bytes(result)
    
    @classmethod
    def from_bytes(cls, data: bytes, size: int, hash_count: int) -> 'BloomFilter':
        """从字节序列恢复过滤器
        
        Args:
            data: 字节序列
            size: 位数组大小
            hash_count: 哈希函数个数
            
        Returns:
            恢复的过滤器
        """
        filter = cls(size, hash_count)
        
        # 从字节序列恢复布尔列表
        for i in range(len(data)):
            byte = data[i]
            for j in range(8):
                if i * 8 + j < size:
                    filter.bit_array[i * 8 + j] = bool(byte & (1 << j))
        
        return filter