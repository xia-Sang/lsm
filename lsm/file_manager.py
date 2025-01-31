import os
import json
import struct
from typing import Dict, Any, BinaryIO, Optional, Iterator, List
from threading import Lock
from contextlib import contextmanager

class FileManager:
    """文件管理器，处理所有文件操作的基类"""
    
    def __init__(self, base_dir: str):
        """
        初始化文件管理器
        
        Args:
            base_dir: 基础目录路径
        """
        self.base_dir = os.path.abspath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)
        self._file_locks: Dict[str, Lock] = {}
        self._global_lock = Lock()
    
    def _get_file_lock(self, file_path: str) -> Lock:
        """获取文件锁"""
        with self._global_lock:
            if file_path not in self._file_locks:
                self._file_locks[file_path] = Lock()
            return self._file_locks[file_path]
    
    @contextmanager
    def _file_locked(self, file_path: str):
        """文件锁的上下文管理器"""
        lock = self._get_file_lock(file_path)
        with lock:
            yield
    
    def _ensure_dir(self, file_path: str) -> None:
        """确保目录存在"""
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
    
    def write_bytes(self, file_path: str, data: bytes, append: bool = False) -> None:
        """
        写入字节数据
        
        Args:
            file_path: 文件路径
            data: 字节数据
            append: 是否追加模式
        """
        abs_path = os.path.join(self.base_dir, file_path)
        self._ensure_dir(abs_path)
        
        with self._file_locked(abs_path):
            mode = 'ab' if append else 'wb'
            with open(abs_path, mode) as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())  # 确保数据写入磁盘
    
    def read_bytes(self, file_path: str, offset: int = 0, size: int = -1) -> bytes:
        """
        读取字节数据
        
        Args:
            file_path: 文件路径
            offset: 起始位置
            size: 读取大小，-1表示读取到文件末尾
            
        Returns:
            读取的字节数据
        """
        abs_path = os.path.join(self.base_dir, file_path)
        with self._file_locked(abs_path):
            with open(abs_path, 'rb') as f:
                f.seek(offset)
                return f.read(size)
    
    def append_record(self, file_path: str, record: bytes) -> int:
        """
        追加记录，使用长度前缀格式
        
        Args:
            file_path: 文件路径
            record: 记录数据
            
        Returns:
            记录的偏移位置
        """
        abs_path = os.path.join(self.base_dir, file_path)
        self._ensure_dir(abs_path)
        
        with self._file_locked(abs_path):
            with open(abs_path, 'ab') as f:
                offset = f.tell()
                # 写入4字节的记录长度和记录内容
                data = struct.pack('>I', len(record)) + record
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
                return offset
    
    def read_record(self, file_path: str, offset: int) -> Optional[bytes]:
        """
        从指定位置读取一条记录
        
        Args:
            file_path: 文件路径
            offset: 记录的起始位置
            
        Returns:
            记录数据，如果到达文件末尾则返回None
        """
        abs_path = os.path.join(self.base_dir, file_path)
        with self._file_locked(abs_path):
            with open(abs_path, 'rb') as f:
                f.seek(offset)
                # 读取记录长度
                length_data = f.read(4)
                if not length_data:
                    return None
                length = struct.unpack('>I', length_data)[0]
                # 读取记录内容
                return f.read(length)
    
    def iterate_records(self, file_path: str) -> Iterator[bytes]:
        """
        遍历文件中的所有记录
        
        Args:
            file_path: 文件路径
            
        Yields:
            每条记录的数据
        """
        abs_path = os.path.join(self.base_dir, file_path)
        with self._file_locked(abs_path):
            with open(abs_path, 'rb') as f:
                while True:
                    # 读取记录长度
                    length_data = f.read(4)
                    if not length_data:
                        break
                    length = struct.unpack('>I', length_data)[0]
                    # 读取记录内容
                    record = f.read(length)
                    if not record:
                        break
                    yield record
    
    def file_size(self, file_path: str) -> int:
        """获取文件大小"""
        abs_path = os.path.join(self.base_dir, file_path)
        with self._file_locked(abs_path):
            return os.path.getsize(abs_path)
    
    def list_files(self, dir_path: str = "", pattern: str = "") -> List[str]:
        """列出目录下的文件"""
        abs_path = os.path.join(self.base_dir, dir_path)
        files = []
        if os.path.exists(abs_path):
            for f in os.listdir(abs_path):
                if pattern and not f.endswith(pattern):
                    continue
                files.append(os.path.join(dir_path, f))
        return sorted(files)
    
    def remove_file(self, file_path: str) -> None:
        """删除文件"""
        abs_path = os.path.join(self.base_dir, file_path)
        with self._file_locked(abs_path):
            if os.path.exists(abs_path):
                os.remove(abs_path)
    
    def rename_file(self, old_path: str, new_path: str) -> None:
        """重命名文件"""
        abs_old_path = os.path.join(self.base_dir, old_path)
        abs_new_path = os.path.join(self.base_dir, new_path)
        self._ensure_dir(abs_new_path)
        with self._file_locked(abs_old_path), self._file_locked(abs_new_path):
            os.rename(abs_old_path, abs_new_path)
