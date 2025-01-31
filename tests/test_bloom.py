import unittest
import random
import string
from lsm.filter.bloom import BloomFilter

class TestBloomFilter(unittest.TestCase):
    def test_basic_operations(self):
        """测试基本操作"""
        # 创建过滤器
        filter = BloomFilter(size=100, hash_count=3)
        
        # 测试添加和查询
        filter.add("test1")
        filter.add("test2")
        filter.add("test3")
        
        self.assertTrue(filter.contains("test1"))
        self.assertTrue(filter.contains("test2"))
        self.assertTrue(filter.contains("test3"))
        self.assertFalse(filter.contains("test4"))
    
    def test_false_positive_rate(self):
        """测试假阳性率"""
        # 创建较大的过滤器
        size = 10000
        hash_count = 7
        filter = BloomFilter(size=size, hash_count=hash_count)
        
        # 添加一些数据
        added_items = set()
        for _ in range(1000):
            item = ''.join(random.choices(string.ascii_letters, k=10))
            filter.add(item)
            added_items.add(item)
        
        # 测试已添加的项
        for item in added_items:
            self.assertTrue(filter.contains(item))
        
        # 测试未添加的项的假阳性率
        false_positives = 0
        test_count = 10000
        
        for _ in range(test_count):
            item = ''.join(random.choices(string.ascii_letters, k=10))
            if item not in added_items and filter.contains(item):
                false_positives += 1
        
        false_positive_rate = false_positives / test_count
        # 假阳性率应该小于5%
        self.assertLess(false_positive_rate, 0.05)
    
    def test_serialization(self):
        """测试序列化和反序列化"""
        # 创建并填充过滤器
        original = BloomFilter(size=1000, hash_count=5)
        test_items = ["test1", "test2", "test3", "test4", "test5"]
        for item in test_items:
            original.add(item)
        
        # 序列化
        serialized = original.to_bytes()
        
        # 反序列化
        restored = BloomFilter.from_bytes(serialized, size=1000, hash_count=5)
        
        # 验证所有原始项都存在
        for item in test_items:
            self.assertTrue(restored.contains(item))
        
        # 验证未添加的项仍然不存在
        self.assertFalse(restored.contains("nonexistent"))
    
    def test_different_hash_counts(self):
        """测试不同数量的哈希函数"""
        test_items = ["test1", "test2", "test3"]
        test_sizes = [100, 1000, 10000]
        test_hash_counts = [1, 3, 5, 7]
        
        for size in test_sizes:
            for hash_count in test_hash_counts:
                filter = BloomFilter(size=size, hash_count=hash_count)
                
                # 添加项
                for item in test_items:
                    filter.add(item)
                
                # 验证所有项都存在
                for item in test_items:
                    self.assertTrue(filter.contains(item))
    
    def test_edge_cases(self):
        """测试边界情况"""
        # 测试空字符串
        filter = BloomFilter(size=100, hash_count=3)
        filter.add("")
        self.assertTrue(filter.contains(""))
        
        # 测试非常长的字符串
        long_string = "x" * 10000
        filter.add(long_string)
        self.assertTrue(filter.contains(long_string))
        
        # 测试特殊字符
        special_chars = "!@#$%^&*()_+-=[]{}|;:'\",.<>?/\\"
        filter.add(special_chars)
        self.assertTrue(filter.contains(special_chars))
    
    def test_performance(self):
        """测试性能（大数据集）"""
        # 创建大型过滤器
        size = 100000
        hash_count = 5
        filter = BloomFilter(size=size, hash_count=hash_count)
        
        # 添加大量数据
        item_count = 10000
        items = []
        for i in range(item_count):
            item = f"item_{i}"
            filter.add(item)
            items.append(item)
        
        # 验证所有添加的项
        for item in items:
            self.assertTrue(filter.contains(item))
        
        # 验证未添加的项
        for i in range(item_count, item_count + 1000):
            item = f"nonexistent_{i}"
            if filter.contains(item):
                # 记录假阳性，但不作为测试失败的条件
                print(f"False positive detected: {item}")

if __name__ == '__main__':
    unittest.main()
