import unittest
import tempfile
import shutil
import os
import random
import threading
import time
from typing import List, Tuple

from lsm.lsm import LSMTree
from lsm.utils.generator import (
    generate_random_kv_pairs,
    generate_sequential_kv_pairs,
    generate_random_string
)

class TestLSM(unittest.TestCase):
    def setUp(self):
        """创建临时目录"""
        self.temp_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(self.temp_dir, memtable_size=4096)  # 使用小的 MemTable 以便测试

    def tearDown(self):
        """清理临时目录"""
        self.lsm.close()
        try:
            shutil.rmtree(self.temp_dir)
        except PermissionError:
            pass  # Windows 可能会出现文件锁定问题，忽略它

    def test_basic_operations(self):
        """测试基本操作"""
        # 测试写入和读取
        self.lsm.put("key1", "value1")
        self.assertEqual(self.lsm.get("key1"), "value1")
        
        # 测试更新
        self.lsm.put("key1", "value1_new")
        self.assertEqual(self.lsm.get("key1"), "value1_new")
        
        # 测试删除
        self.lsm.delete("key1")
        self.assertIsNone(self.lsm.get("key1"))
        
        # 测试不存在的键
        self.assertIsNone(self.lsm.get("nonexistent"))

    def test_range_scan(self):
        """测试范围查询"""
        # 写入有序数据
        pairs = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4"),
            ("key5", "value5")
        ]
        
        for key, value in pairs:
            self.lsm.put(key, value)
        
        # 测试完整范围
        results = list(self.lsm.range_scan("key1", "key5"))
        self.assertEqual(len(results), 5)
        self.assertEqual(results[0][0], "key1")
        self.assertEqual(results[-1][0], "key5")
        
        # 测试部分范围
        results = list(self.lsm.range_scan("key2", "key4"))
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0][0], "key2")
        self.assertEqual(results[-1][0], "key4")
        
        # 删除一个键后再查询
        self.lsm.delete("key3")
        results = list(self.lsm.range_scan("key2", "key4"))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][0], "key2")
        self.assertEqual(results[-1][0], "key4")

    def test_compaction(self):
        """测试压缩机制"""
        # 写入足够多的数据触发 MemTable 转换为 SSTable
        pairs = generate_sequential_kv_pairs(1000)
        
        for key, value in pairs:
            self.lsm.put(key, value)
        
        # 确保数据被正确压缩
        self.assertGreater(len(self.lsm.sstables), 0)
        
        # 验证所有数据都可以被读取
        for key, expected_value in pairs:
            value = self.lsm.get(key)
            self.assertEqual(value, expected_value)

    def test_recovery(self):
        """测试崩溃恢复"""
        print("\n=== 开始测试 ===")
        
        # 写入一些数据
        pairs = generate_random_kv_pairs(100)
        print(f"\n=== 生成了 {len(pairs)} 个键值对 ===")
        print("前5个键值对示例:")
        for key, value in pairs[:5]:
            print(f"  {key} -> {value}")
        
        print("\n=== 写入数据 ===")
        for i, (key, value) in enumerate(pairs):
            self.lsm.put(key, value)
            if i < 5:  # 只检查前5个
                current = self.lsm.get(key)
                print(f"写入后立即读取 #{i}: {key} -> {current}")
                assert current == value, f"立即读取失败: 期望 {value}, 得到 {current}"
        
        print(f"\nMemTable大小: {len(self.lsm.memtable)}")
        print(f"SSTable数量: {len(self.lsm.sstables)}")
        
        # 验证写入后的数据
        print("\n=== 验证写入的数据 ===")
        errors = 0
        for i, (key, expected) in enumerate(pairs):
            value = self.lsm.get(key)
            if value != expected:
                print(f"错误 #{i}: {key} -> 得到 {value}, 期望 {expected}")
                errors += 1
                if errors >= 5:  # 只显示前5个错误
                    break
        if errors == 0:
            print("所有数据验证正确")
        
        # 关闭 LSM 树
        print("\n=== 关闭 LSM 树 ===")
        self.lsm.close()
        
        # 重新打开 LSM 树
        print("\n=== 重新打开 LSM 树 ===")
        self.lsm = LSMTree(self.temp_dir)
        
        print(f"\n恢复后:")
        print(f"SSTable数量: {len(self.lsm.sstables)}")
        if self.lsm.sstables:
            print("\nSSTable信息:")
            for i, sst in enumerate(self.lsm.sstables):
                print(f"\nSSTable #{i}:")
                print(f"  序列号: {sst.sequence}")
                if sst.metadata:
                    print(f"  数据大小: {sst.metadata.data_size}")
                    print(f"  键范围: {sst.metadata.min_key} -> {sst.metadata.max_key}")
        
        # 验证恢复后的数据
        print("\n=== 验证恢复后的数据 ===")
        errors = 0
        for i, (key, expected) in enumerate(pairs):
            value = self.lsm.get(key)
            if value != expected:
                print(f"错误 #{i}: {key} -> 得到 {value}, 期望 {expected}")
                # 尝试从每个SSTable直接读取
                print(f"  尝试从每个SSTable直接读取 {key}:")
                for j, sst in enumerate(self.lsm.sstables):
                    sst_value = sst.get(key)
                    print(f"    SSTable #{j}: {sst_value}")
                errors += 1
                if errors >= 5:  # 只显示前5个错误
                    break
        
        if errors == 0:
            print("所有数据恢复正确")
        else:
            print(f"\n总计 {errors} 个错误")
            self.fail("数据恢复验证失败")

    def test_simple_recovery(self):
        """测试简单的恢复场景"""
        # 写入一些固定的测试数据
        test_data = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ]
        
        print("\n=== 写入初始数据 ===")
        for key, value in test_data:
            self.lsm.put(key, value)
            print(f"写入: {key} -> {value}")
            # 立即读取验证
            current_value = self.lsm.get(key)
            print(f"立即读取: {key} -> {current_value}")
        
        print("\n=== MemTable 状态 ===")
        print(f"MemTable 大小: {len(self.lsm.memtable)}")
        
        # 强制进行一次 compact
        print("\n=== 强制 Compact MemTable ===")
        self.lsm._compact_memtable()
        
        print("\n=== Compact 后状态 ===")
        print(f"MemTable 大小: {len(self.lsm.memtable)}")
        print(f"SSTable 数量: {len(self.lsm.sstables)}")
        
        # 确保数据可以正确读取
        print("\n=== Compact 后验证数据 ===")
        for key, expected_value in test_data:
            value = self.lsm.get(key)
            print(f"读取: {key} -> {value} (期望: {expected_value})")
            self.assertEqual(value, expected_value)
        
        # 关闭 LSM 树
        print("\n=== 关闭 LSM 树 ===")
        self.lsm.close()
        
        # 重新打开 LSM 树
        print("\n=== 重新打开 LSM 树 ===")
        self.lsm = LSMTree(self.temp_dir)
        
        # 验证恢复后的数据
        print("\n=== 验证恢复后的数据 ===")
        for key, expected_value in test_data:
            value = self.lsm.get(key)
            print(f"读取: {key} -> {value} (期望: {expected_value})")
            self.assertEqual(value, expected_value)
        
        # 打印 SSTable 信息
        print("\n=== SSTable 信息 ===")
        print(f"SSTable 数量: {len(self.lsm.sstables)}")
        for i, sstable in enumerate(self.lsm.sstables):
            print(f"SSTable {i}:")
            print(f"  序列号: {sstable.sequence}")
            print(f"  文件路径: {sstable.file_path}")
            if sstable.metadata:
                print(f"  最小键: {sstable.metadata.min_key}")
                print(f"  最大键: {sstable.metadata.max_key}")
            
            # 尝试从这个 SSTable 读取所有键
            print("  存储的键值对:")
            for key, expected_value in test_data:
                value = sstable.get(key)
                print(f"    {key} -> {value}")

    def test_concurrent_operations(self):
        """测试并发操作"""
        def writer():
            """写入线程"""
            for i in range(100):
                key = f"key_{i}"
                value = f"value_{i}"
                self.lsm.put(key, value)
                if i % 2 == 0:
                    self.lsm.delete(key)
        
        def reader():
            """读取线程"""
            for i in range(100):
                key = f"key_{i}"
                value = self.lsm.get(key)
                if value is not None:
                    self.assertTrue(value.startswith("value_"))
        
        # 创建多个读写线程
        threads = []
        for _ in range(3):
            threads.append(threading.Thread(target=writer))
            threads.append(threading.Thread(target=reader))
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()

    def test_large_values(self):
        """测试大值"""
        # 写入一个大值
        large_value = "x" * 1000000  # 1MB
        self.lsm.put("large_key", large_value)
        
        # 读取并验证
        value = self.lsm.get("large_key")
        self.assertEqual(value, large_value)

    def test_mixed_operations(self):
        """测试混合操作"""
        operations = []
        keys = []
        
        # 生成随机操作
        for i in range(1000):
            if i < 500:  # 前500次操作主要是写入
                op = random.choice(['put'] * 8 + ['delete', 'get'])
            else:  # 后500次操作主要是读取和删除
                op = random.choice(['put', 'delete', 'get'] * 2)
            
            if op == 'put':
                key = generate_random_string(8)
                value = generate_random_string(100)
                self.lsm.put(key, value)
                keys.append((key, value))
                operations.append(('put', key, value))
            elif op == 'delete' and keys:
                key, _ = random.choice(keys)
                self.lsm.delete(key)
                operations.append(('delete', key, None))
            elif op == 'get' and keys:
                key, expected_value = random.choice(keys)
                value = self.lsm.get(key)
                # 注意：由于可能被删除，值可能为 None
                operations.append(('get', key, value))

    def test_edge_cases(self):
        """测试边界情况"""
        # 测试空键空值
        self.lsm.put("", "")
        self.assertEqual(self.lsm.get(""), "")
        
        # 测试特殊字符
        special_chars = "!@#$%^&*()_+-=[]{}|;:'\",.<>?/~`"
        self.lsm.put(special_chars, "value")
        self.assertEqual(self.lsm.get(special_chars), "value")
        
        # 测试 Unicode 字符
        unicode_str = "你好世界🌍"
        self.lsm.put(unicode_str, unicode_str)
        self.assertEqual(self.lsm.get(unicode_str), unicode_str)
        
        # 测试范围查询的边界情况
        results = list(self.lsm.range_scan("", "z"))  # 全范围查询
        self.assertGreater(len(results), 0)
        
        results = list(self.lsm.range_scan("nonexistent1", "nonexistent2"))  # 空范围
        self.assertEqual(len(results), 0)

    def test_simple_compaction(self):
        """测试简单的合并功能"""
        # 写入一些数据
        test_data = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        ]
        
        # 写入第一批数据
        for key, value in test_data:
            self.lsm.put(key, value)
        
        # 强制转换为 SSTable
        self.lsm._compact_memtable()
        
        # 更新一些数据
        self.lsm.put("key2", "new_value2")
        self.lsm._compact_memtable()
        
        # 删除一个键
        self.lsm.delete("key1")
        self.lsm._compact_memtable()
        
        # 手动触发合并
        self.lsm.compact()
        
        # 验证结果
        self.assertEqual(len(self.lsm.sstables), 1, "合并后应该只有一个 SSTable")
        self.assertIsNone(self.lsm.get("key1"), "key1 应该已被删除")
        self.assertEqual(self.lsm.get("key2"), "new_value2", "key2 应该是更新后的值")
        self.assertEqual(self.lsm.get("key3"), "value3", "key3 应该保持不变")

if __name__ == '__main__':
    unittest.main()
