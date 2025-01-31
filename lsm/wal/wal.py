import os
import json
from typing import Dict, Optional, Iterator, Tuple, List
from ..file_manager.manager import FileManager

import os
import struct
from typing import Iterator, Tuple, Dict

class WAL:
    """预写日志（Write-Ahead Log）实现"""
    
    def __init__(self, directory: str):
        """初始化WAL
        
        Args:
            directory: WAL文件所在目录
        """
        self.directory = directory
        self.file_path = os.path.join(directory, "wal")
        self.file = None
        self._open()
    
    def _open(self):
        """打开WAL文件"""
        # 确保目录存在
        os.makedirs(self.directory, exist_ok=True)
        # 以追加模式打开文件
        self.file = open(self.file_path, 'ab')
    
    def append(self, key: str, value: str):
        """追加一条记录
        
        Args:
            key: 键
            value: 值。空字符串表示删除操作
        """
        # 将键值对编码为字节
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')
        
        # 写入格式：key_size(4字节) + key + value_size(4字节) + value
        self.file.write(struct.pack('>I', len(key_bytes)))
        self.file.write(key_bytes)
        self.file.write(struct.pack('>I', len(value_bytes)))
        self.file.write(value_bytes)
        
        # 确保写入磁盘
        self.file.flush()
        os.fsync(self.file.fileno())
    
    def recover(self) -> Iterator[Tuple[str, str]]:
        """从WAL文件中恢复数据。
        对于每个键，只返回最新的值。
        如果最新的值是空字符串，表示该键已被删除。
        
        Returns:
            恢复的键值对迭代器
        """
        # 关闭当前文件
        self.close()
        
        # 用字典存储最新的值
        latest_values: Dict[str, str] = {}
        
        # 以二进制读取模式打开文件
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'rb') as f:
                    while True:
                        try:
                            # 读取键长度
                            key_size_bytes = f.read(4)
                            if not key_size_bytes or len(key_size_bytes) < 4:  # 文件结束或损坏
                                break
                            
                            # 读取键
                            key_size = struct.unpack('>I', key_size_bytes)[0]
                            if key_size <= 0 or key_size > 1024 * 1024:  # 键的大小不合理
                                print(f"Invalid key size: {key_size}")
                                break
                                
                            key_bytes = f.read(key_size)
                            if len(key_bytes) < key_size:  # 文件损坏
                                print("Incomplete key data")
                                break
                            
                            try:
                                key = key_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                print("Invalid key encoding")
                                break
                            
                            # 读取值长度
                            value_size_bytes = f.read(4)
                            if len(value_size_bytes) < 4:  # 文件损坏
                                print("Incomplete value size")
                                break
                            value_size = struct.unpack('>I', value_size_bytes)[0]
                            
                            if value_size < 0 or value_size > 1024 * 1024 * 10:  # 值的大小不合理
                                print(f"Invalid value size: {value_size}")
                                break
                            
                            # 读取值
                            value_bytes = f.read(value_size)
                            if len(value_bytes) < value_size:  # 文件损坏
                                print("Incomplete value data")
                                break
                            
                            try:
                                value = value_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                print("Invalid value encoding")
                                break
                            
                            # 更新最新值
                            latest_values[key] = value
                            
                        except (struct.error, UnicodeDecodeError) as e:
                            print(f"Error during WAL recovery: {e}")
                            break
            except IOError as e:
                print(f"Error during WAL recovery: {e}")
        
        # 返回所有键值对
        result = [(key, value) for key, value in sorted(latest_values.items())]
        
        # 重新打开文件以供写入
        self._open()
        
        return iter(result)
    
    def close(self):
        """关闭WAL文件"""
        if self.file:
            self.file.close()
            self.file = None
    
    def delete(self):
        """删除WAL文件"""
        self.close()
        try:
            os.remove(self.file_path)
        except FileNotFoundError:
            pass
