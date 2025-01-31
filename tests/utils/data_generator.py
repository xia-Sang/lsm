import random
import string
import uuid
from typing import Dict, List, Tuple, Generator
from datetime import datetime, timedelta

class DataGenerator:
    """测试数据生成器"""
    
    @staticmethod
    def generate_random_string(length: int) -> str:
        """生成指定长度的随机字符串"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    @staticmethod
    def generate_timestamp_key() -> str:
        """生成基于时间戳的键"""
        return datetime.now().strftime("%Y%m%d%H%M%S%f")
    
    @staticmethod
    def generate_uuid_key() -> str:
        """生成UUID键"""
        return str(uuid.uuid4())
    
    @staticmethod
    def generate_sequential_key(prefix: str, index: int, padding: int = 10) -> str:
        """生成序列键"""
        return f"{prefix}_{str(index).zfill(padding)}"
    
    @staticmethod
    def generate_value(min_length: int = 10, max_length: int = 100) -> str:
        """生成随机长度的值"""
        length = random.randint(min_length, max_length)
        return DataGenerator.generate_random_string(length)
    
    @classmethod
    def generate_sequential_pairs(cls, 
                                count: int, 
                                prefix: str = "key",
                                min_value_length: int = 10,
                                max_value_length: int = 100) -> Dict[str, str]:
        """生成序列键值对"""
        return {
            cls.generate_sequential_key(prefix, i): 
            cls.generate_value(min_value_length, max_value_length)
            for i in range(count)
        }
    
    @classmethod
    def generate_timestamp_pairs(cls,
                               count: int,
                               start_time: datetime = None,
                               interval_seconds: int = 1,
                               min_value_length: int = 10,
                               max_value_length: int = 100) -> Dict[str, str]:
        """生成基于时间戳的键值对"""
        if start_time is None:
            start_time = datetime.now()
            
        return {
            (start_time + timedelta(seconds=i*interval_seconds)).strftime("%Y%m%d%H%M%S%f"):
            cls.generate_value(min_value_length, max_value_length)
            for i in range(count)
        }
    
    @classmethod
    def generate_uuid_pairs(cls,
                          count: int,
                          min_value_length: int = 10,
                          max_value_length: int = 100) -> Dict[str, str]:
        """生成基于UUID的键值对"""
        return {
            str(uuid.uuid4()): 
            cls.generate_value(min_value_length, max_value_length)
            for _ in range(count)
        }
    
    @classmethod
    def generate_random_pairs(cls,
                            count: int,
                            key_length: int = 16,
                            min_value_length: int = 10,
                            max_value_length: int = 100) -> Dict[str, str]:
        """生成随机键值对"""
        return {
            cls.generate_random_string(key_length):
            cls.generate_value(min_value_length, max_value_length)
            for _ in range(count)
        }
    
    @classmethod
    def generate_sorted_pairs(cls,
                            count: int,
                            min_value_length: int = 10,
                            max_value_length: int = 100) -> List[Tuple[str, str]]:
        """生成有序的键值对列表"""
        pairs = cls.generate_random_pairs(count, 16, min_value_length, max_value_length)
        return sorted(pairs.items())
    
    @classmethod
    def generate_key_stream(cls, 
                          count: int, 
                          key_type: str = "sequential",
                          prefix: str = "key") -> Generator[str, None, None]:
        """
        生成键流
        
        Args:
            count: 生成的键数量
            key_type: 键类型，可选值：sequential, timestamp, uuid, random
            prefix: 序列键的前缀
        """
        if key_type == "sequential":
            for i in range(count):
                yield cls.generate_sequential_key(prefix, i)
        elif key_type == "timestamp":
            start_time = datetime.now()
            for i in range(count):
                yield (start_time + timedelta(seconds=i)).strftime("%Y%m%d%H%M%S%f")
        elif key_type == "uuid":
            for _ in range(count):
                yield str(uuid.uuid4())
        else:  # random
            for _ in range(count):
                yield cls.generate_random_string(16)
    
    @classmethod
    def generate_value_stream(cls,
                            count: int,
                            min_length: int = 10,
                            max_length: int = 100) -> Generator[str, None, None]:
        """生成值流"""
        for _ in range(count):
            yield cls.generate_value(min_length, max_length)
    
    @classmethod
    def generate_pair_stream(cls,
                           count: int,
                           key_type: str = "sequential",
                           prefix: str = "key",
                           min_value_length: int = 10,
                           max_value_length: int = 100) -> Generator[Tuple[str, str], None, None]:
        """生成键值对流"""
        key_stream = cls.generate_key_stream(count, key_type, prefix)
        value_stream = cls.generate_value_stream(count, min_value_length, max_value_length)
        for key, value in zip(key_stream, value_stream):
            yield key, value
