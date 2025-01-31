import random
import string
from typing import List, Tuple

def generate_random_string(length: int) -> str:
    """生成指定长度的随机字符串
    
    Args:
        length: 字符串长度
        
    Returns:
        随机字符串
    """
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_sequential_kv_pairs(count: int) -> List[Tuple[str, str]]:
    """生成顺序的键值对
    
    Args:
        count: 键值对数量
        
    Returns:
        键值对列表，键为 "key_00000001" 格式
    """
    pairs = []
    for i in range(count):
        key = f"key_{i:08d}"
        value = f"value_{i:08d}"
        pairs.append((key, value))
    return pairs

def generate_random_kv_pairs(count: int, key_length: int = 16, value_length: int = 100) -> List[Tuple[str, str]]:
    """生成随机的键值对
    
    Args:
        count: 键值对数量
        key_length: 键长度
        value_length: 值长度
        
    Returns:
        随机键值对列表
    """
    pairs = []
    for _ in range(count):
        key = generate_random_string(key_length)
        value = generate_random_string(value_length)
        pairs.append((key, value))
    return pairs
