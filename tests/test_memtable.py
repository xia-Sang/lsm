import unittest
import random
import string
from typing import Dict, List, Tuple
from lsm.memtable.table import MemTable

class TestMemTable(unittest.TestCase):
    def setUp(self):
        """测试前初始化MemTable"""
        self.memtable = MemTable()
    
    def test_basic_operations(self):
        """测试基本的读写删除操作"""
        # 写入
        self.memtable.put("key1", "value1")
        self.memtable.put("key2", "value2")
        self.memtable.put("key3", "value3")
        
        # 读取
        self.assertEqual(self.memtable.get("key1"), "value1")
        self.assertEqual(self.memtable.get("key2"), "value2")
        self.assertEqual(self.memtable.get("key3"), "value3")
        
        # 更新
        self.memtable.put("key2", "value2_updated")
        self.assertEqual(self.memtable.get("key2"), "value2_updated")
        
        # 删除
        self.memtable.delete("key1")
        self.assertIsNone(self.memtable.get("key1"))
        
        # 删除后再写入
        self.memtable.put("key1", "value1_new")
        self.assertEqual(self.memtable.get("key1"), "value1_new")
    
    def test_large_dataset(self):
        """测试大数据集的读写"""
        # 生成大量数据
        data = {}
        for i in range(1000):
            key = f"key{i:04d}"
            value = f"value{i:04d}"
            data[key] = value
            self.memtable.put(key, value)
        
        # 验证所有数据
        for key, value in data.items():
            self.assertEqual(self.memtable.get(key), value)
    
    def test_size_tracking(self):
        """测试大小跟踪"""
        initial_size = self.memtable.size
        
        # 写入数据并检查大小增加
        self.memtable.put("key1", "value1")
        self.assertGreater(self.memtable.size, initial_size)
        
        # 更新数据并检查大小变化
        old_size = self.memtable.size
        self.memtable.put("key1", "new_value")
        self.assertNotEqual(self.memtable.size, old_size)
        
        # 删除数据并检查大小变化
        old_size = self.memtable.size
        self.memtable.delete("key1")
        self.assertLess(self.memtable.size, old_size)
    
    def test_range_scan(self):
        """测试范围查询"""
        # 准备有序数据
        data = {}
        for i in range(100):
            key = f"key{i:04d}"
            value = f"value{i:04d}"
            data[key] = value
            self.memtable.put(key, value)
        
        # 测试不同范围的查询
        test_ranges = [
            ("key0000", "key0010"),  # 开头的一段
            ("key0050", "key0060"),  # 中间的一段
            ("key0090", "key0099"),  # 结尾的一段
            ("key0000", "key0099"),  # 全部范围
            ("key0030", "key0035")   # 小范围
        ]
        
        for start_key, end_key in test_ranges:
            # 获取预期结果
            expected = {k: v for k, v in data.items() 
                       if start_key <= k <= end_key}
            
            # 执行范围查询
            result = dict(self.memtable.range_scan(start_key, end_key))
            
            # 验证结果
            self.assertEqual(result, expected)
    
    def test_deleted_entries(self):
        """测试删除标记的处理"""
        # 写入一些记录，包括删除标记
        operations = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key2", None),  # 删除key2
            ("key3", "value3"),
            ("key3", None),  # 删除key3
            ("key3", "value3_new")  # 重新写入key3
        ]
        
        for key, value in operations:
            if value is None:
                self.memtable.delete(key)
            else:
                self.memtable.put(key, value)
        
        # 验证最终状态
        expected = {
            "key1": "value1",
            "key3": "value3_new"
        }
        
        # key2应该不存在
        self.assertIsNone(self.memtable.get("key2"))
        
        # 验证其他键的值
        for key, value in expected.items():
            self.assertEqual(self.memtable.get(key), value)
    
    def test_iterator(self):
        """测试迭代器"""
        # 写入一些有序数据
        data = []
        for i in range(100):
            key = f"key{i:04d}"
            value = f"value{i:04d}"
            data.append((key, value))
            self.memtable.put(key, value)
        
        # 获取迭代器
        entries = list(self.memtable)
        
        # 验证顺序和内容
        self.assertEqual(len(entries), len(data))
        for (key1, value1), (key2, value2) in zip(data, entries):
            self.assertEqual(key1, key2)
            self.assertEqual(value1, value2)
    
    def test_empty_table(self):
        """测试空表的操作"""
        # 测试空表的获取操作
        self.assertIsNone(self.memtable.get("nonexistent"))
        
        # 测试空表的范围查询
        result = list(self.memtable.range_scan("start", "end"))
        self.assertEqual(len(result), 0)
        
        # 测试空表的迭代
        entries = list(self.memtable)
        self.assertEqual(len(entries), 0)
        
        # 测试空表的大小
        self.assertEqual(self.memtable.size, 0)

if __name__ == '__main__':
    unittest.main()
