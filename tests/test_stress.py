import unittest
import tempfile
import shutil
import random
import string
import time
from typing import List, Tuple

from lsm.wal.wal import WAL
from lsm.memtable.table import MemTable
from lsm.utils.generator import (
    generate_random_kv_pairs,
    generate_sequential_kv_pairs,
    generate_random_string
)

class StressTest(unittest.TestCase):
    def setUp(self):
        """创建临时目录"""
        self.temp_dir = tempfile.mkdtemp()
        self.wal = WAL(self.temp_dir)
        self.memtable = MemTable()

    def tearDown(self):
        """清理临时目录"""
        self.wal.close()
        try:
            shutil.rmtree(self.temp_dir)
        except PermissionError:
            pass  # Windows 可能会出现文件锁定问题，忽略它

    def test_large_sequential_writes(self):
        """测试大量顺序写入"""
        # 生成 1,000 个顺序的键值对
        count = 1_000
        pairs = generate_sequential_kv_pairs(count)
        
        start_time = time.time()
        
        # 写入 WAL 和 MemTable
        for key, value in pairs:
            self.wal.append(key, value)
            self.memtable.put(key, value)
        
        write_time = time.time() - start_time
        print(f"\nSequential write time for {count} entries: {write_time:.2f}s")
        
        # 验证数据完整性
        recovered_entries = list(self.wal.recover())
        self.assertEqual(len(recovered_entries), count)
        
        # 验证 MemTable 大小
        self.assertGreater(self.memtable.size, 0)

    def test_large_random_writes(self):
        """测试大量随机写入"""
        # 生成 1,000 个随机的键值对
        count = 1_000
        pairs = generate_random_kv_pairs(count, key_length=16, value_length=100)
        
        start_time = time.time()
        
        # 写入 WAL 和 MemTable
        for key, value in pairs:
            self.wal.append(key, value)
            self.memtable.put(key, value)
        
        write_time = time.time() - start_time
        print(f"\nRandom write time for {count} entries: {write_time:.2f}s")
        
        # 验证数据完整性
        recovered_entries = list(self.wal.recover())
        self.assertEqual(len(recovered_entries), count)

    def test_mixed_operations(self):
        """测试混合操作（写入、更新、删除）"""
        operations = []
        keys = []
        
        # 生成初始数据
        initial_pairs = generate_random_kv_pairs(1_000, key_length=8)
        for key, value in initial_pairs:
            keys.append(key)
            operations.append(('put', key, value))
        
        # 生成随机操作
        for _ in range(500):
            op = random.choice(['put', 'update', 'delete'])
            if op == 'put':
                # 新增
                key = generate_random_string(8)
                value = generate_random_string(100)
                keys.append(key)
                operations.append(('put', key, value))
            elif op == 'update' and keys:
                # 更新
                key = random.choice(keys)
                value = generate_random_string(100)
                operations.append(('put', key, value))
            elif op == 'delete' and keys:
                # 删除
                key = random.choice(keys)
                operations.append(('delete', key, ''))
                keys.remove(key)
        
        start_time = time.time()
        
        # 执行操作
        for op, key, value in operations:
            if op == 'put':
                self.wal.append(key, value)
                self.memtable.put(key, value)
            elif op == 'delete':
                self.wal.append(key, '')
                self.memtable.delete(key)
        
        operation_time = time.time() - start_time
        print(f"\nMixed operations time for {len(operations)} operations: {operation_time:.2f}s")
        
        # 验证数据一致性
        recovered_entries = dict(self.wal.recover())
        for key, value in self.memtable:
            self.assertEqual(recovered_entries.get(key), value)

    def test_concurrent_range_scans(self):
        """测试并发范围查询的性能"""
        # 生成有序数据
        count = 1_000
        pairs = generate_sequential_kv_pairs(count)
        
        # 写入数据
        for key, value in pairs:
            self.memtable.put(key, value)
        
        start_time = time.time()
        
        # 执行多次范围查询
        for _ in range(100):
            # 随机选择范围
            start_idx = random.randint(0, count - 100)
            end_idx = start_idx + 100
            
            start_key = f"key_{start_idx:08d}"
            end_key = f"key_{end_idx:08d}"
            
            # 执行范围查询
            results = list(self.memtable.range_scan(start_key, end_key))
            self.assertEqual(len(results), end_idx - start_idx + 1)
        
        scan_time = time.time() - start_time
        print(f"\nRange scan time for 100 queries: {scan_time:.2f}s")

    def test_recovery_performance(self):
        """测试恢复性能"""
        # 生成大量数据
        count = 1_000
        pairs = generate_random_kv_pairs(count, key_length=16, value_length=1000)
        
        # 写入数据
        write_start = time.time()
        for key, value in pairs:
            self.wal.append(key, value)
        write_time = time.time() - write_start
        print(f"\nWrite time for {count} large entries: {write_time:.2f}s")
        
        # 关闭并重新打开 WAL
        self.wal.close()
        
        # 测试恢复性能
        recovery_start = time.time()
        recovered_wal = WAL(self.temp_dir)
        recovered_entries = list(recovered_wal.recover())
        recovery_time = time.time() - recovery_start
        
        print(f"Recovery time for {count} large entries: {recovery_time:.2f}s")
        self.assertEqual(len(recovered_entries), count)

if __name__ == '__main__':
    unittest.main()
