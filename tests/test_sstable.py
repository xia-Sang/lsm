import unittest
import tempfile
import os
import shutil
import struct
from typing import Dict, List, Tuple
from lsm.sstable.table import SSTable
from lsm.config import default_config
from lsm.file_manager.manager import FileManager

class TestSSTable(unittest.TestCase):
    def setUp(self):
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        self.file_manager = FileManager(self.temp_dir)
    
    def tearDown(self):
        # 清理临时目录
        shutil.rmtree(self.temp_dir)
    
    def test_file_format(self):
        """测试SSTable文件格式"""
        # 准备数据
        data = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        ]
        
        # 创建SSTable
        table = SSTable.create_from_memtable(
            self.file_manager,
            "sst",
            level=0,
            sequence=1,
            entries=iter(data),
            expected_entries=len(data)
        )
        
        # 验证文件存在
        sst_file = os.path.join(self.temp_dir, "sst_1.sst")
        self.assertTrue(os.path.exists(sst_file))
        
        # 验证文件格式
        with open(sst_file, 'rb') as f:
            # 验证魔数
            magic = f.read(4)
            self.assertEqual(magic, default_config.SST_MAGIC_NUMBER)
            
            # 验证版本号
            version = struct.unpack('>I', f.read(4))[0]
            self.assertEqual(version, default_config.SST_VERSION)
            
            # 验证文件头大小
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            self.assertGreater(file_size, default_config.SST_HEADER_SIZE)
    
    def test_create_and_load(self):
        """测试创建和加载SSTable"""
        # 准备数据
        data = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        ]
        
        # 创建SSTable
        table = SSTable.create_from_memtable(
            self.file_manager,
            "sst",
            level=0,
            sequence=1,
            entries=iter(data),
            expected_entries=len(data)
        )
        
        # 验证创建的表
        self.assertIsNotNone(table)
        self.assertEqual(table.level, 0)
        self.assertEqual(table.sequence, 1)
        
        # 验证元数据
        self.assertIsNotNone(table.metadata)
        self.assertEqual(table.metadata.min_key, "key1")
        self.assertEqual(table.metadata.max_key, "key3")
        self.assertGreater(table.metadata.data_size, 0)
        self.assertGreater(table.metadata.index_offset, default_config.SST_HEADER_SIZE)
        self.assertGreater(table.metadata.bloom_offset, table.metadata.index_offset)
        
        # 关闭表
        table.close()
        
        # 重新加载表
        loaded_table = SSTable(self.temp_dir, 0, 1)
        self.assertTrue(loaded_table.load())
        
        # 验证数据
        for key, value in data:
            self.assertEqual(loaded_table.get(key), value)
        
        # 验证不存在的键
        self.assertIsNone(loaded_table.get("nonexistent"))
    
    def test_bloom_filter(self):
        """测试布隆过滤器"""
        # 准备数据
        data = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        ]
        
        # 创建SSTable
        table = SSTable.create_from_memtable(
            self.file_manager,
            "sst",
            level=0,
            sequence=1,
            entries=iter(data),
            expected_entries=len(data)
        )
        
        # 验证布隆过滤器
        self.assertIsNotNone(table.filter)
        
        # 验证存在的键
        for key, _ in data:
            self.assertTrue(table.filter.contains(key))
        
        # 验证不存在的键
        self.assertFalse(table.filter.contains("nonexistent"))
    
    def test_range_scan(self):
        """测试范围查询"""
        # 准备数据
        data = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4"),
            ("key5", "value5")
        ]
        
        # 创建SSTable
        table = SSTable.create_from_memtable(
            self.file_manager,
            "sst",
            level=0,
            sequence=1,
            entries=iter(data),
            expected_entries=len(data)
        )
        
        # 测试范围查询
        results = list(table.range_scan("key2", "key4"))
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], ("key2", "value2"))
        self.assertEqual(results[1], ("key3", "value3"))
        self.assertEqual(results[2], ("key4", "value4"))
        
        # 测试边界情况
        results = list(table.range_scan("key1", "key1"))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], ("key1", "value1"))
        
        # 测试空范围
        results = list(table.range_scan("key6", "key7"))
        self.assertEqual(len(results), 0)
    
    def test_large_dataset(self):
        """测试大数据集"""
        # 准备大量数据
        data = []
        for i in range(1000):
            key = f"key{i:04d}"
            value = f"value{i:04d}"
            data.append((key, value))
        
        # 创建SSTable
        table = SSTable.create_from_memtable(
            self.file_manager,
            "sst",
            level=0,
            sequence=1,
            entries=iter(data),
            expected_entries=len(data)
        )
        
        # 验证文件大小合理
        sst_file = os.path.join(self.temp_dir, "sst_1.sst")
        file_size = os.path.getsize(sst_file)
        self.assertGreater(file_size, default_config.SST_HEADER_SIZE)
        self.assertLess(file_size, 10 * 1024 * 1024)  # 不应超过10MB
        
        # 验证随机访问性能
        import random
        import time
        start_time = time.time()
        for _ in range(100):
            idx = random.randint(0, len(data) - 1)
            key, value = data[idx]
            self.assertEqual(table.get(key), value)
        query_time = time.time() - start_time
        self.assertLess(query_time, 1.0)  # 100次随机查询应在1秒内完成
        
        # 验证范围查询性能
        start_time = time.time()
        results = list(table.range_scan("key0100", "key0199"))
        scan_time = time.time() - start_time
        self.assertEqual(len(results), 100)
        self.assertLess(scan_time, 0.5)  # 范围查询应在0.5秒内完成
    
    def test_corrupted_file(self):
        """测试损坏的文件处理"""
        # 准备数据
        data = [("key1", "value1")]
        
        # 创建SSTable
        table = SSTable.create_from_memtable(
            self.file_manager,
            "sst",
            level=0,
            sequence=1,
            entries=iter(data),
            expected_entries=len(data)
        )
        
        # 损坏文件
        sst_file = os.path.join(self.temp_dir, "sst_1.sst")
        with open(sst_file, 'r+b') as f:
            f.seek(0)
            f.write(b'XXXX')  # 破坏魔数
        
        # 尝试加载损坏的文件
        corrupted_table = SSTable(self.temp_dir, 0, 1)
        self.assertFalse(corrupted_table.load())
    
    def test_concurrent_reads(self):
        """测试并发读取"""
        # 准备数据
        data = [(f"key{i}", f"value{i}") for i in range(100)]
        
        # 创建SSTable
        table = SSTable.create_from_memtable(
            self.file_manager,
            "sst",
            level=0,
            sequence=1,
            entries=iter(data),
            expected_entries=len(data)
        )
        
        # 并发读取测试
        import threading
        errors = []
        
        def reader_thread():
            try:
                for key, expected_value in data:
                    actual_value = table.get(key)
                    if actual_value != expected_value:
                        errors.append(f"Key {key}: expected {expected_value}, got {actual_value}")
            except Exception as e:
                errors.append(str(e))
        
        threads = [threading.Thread(target=reader_thread) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        self.assertEqual(len(errors), 0, f"Concurrent read errors: {errors}")

if __name__ == '__main__':
    unittest.main()
