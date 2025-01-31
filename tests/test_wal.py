import unittest
import tempfile
import shutil
import os
from lsm.wal.wal import WAL

class TestWAL(unittest.TestCase):
    def setUp(self):
        """测试前创建临时目录"""
        self.temp_dir = tempfile.mkdtemp()
        self.wal = WAL(self.temp_dir)
        
    def tearDown(self):
        """测试后清理临时目录"""
        self.wal.close()
        shutil.rmtree(self.temp_dir)
    
    def test_basic_operations(self):
        """测试基本的写入和恢复操作"""
        # 写入一些记录
        entries = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        ]
        
        for key, value in entries:
            self.wal.append(key, value)
        
        # 关闭WAL
        self.wal.close()
        
        # 重新打开WAL并恢复数据
        recovered_wal = WAL(self.temp_dir)
        recovered_entries = list(recovered_wal.recover())
        
        # 验证恢复的数据
        self.assertEqual(len(recovered_entries), len(entries))
        for (key, value), (rec_key, rec_value) in zip(entries, recovered_entries):
            self.assertEqual(key, rec_key)
            self.assertEqual(value, rec_value)
    
    def test_large_dataset(self):
        """测试大数据集的写入和恢复"""
        # 生成大量数据
        entries = []
        for i in range(1000):
            key = f"key{i:04d}"
            value = f"value{i:04d}"
            entries.append((key, value))
            self.wal.append(key, value)
        
        # 关闭并重新打开WAL
        self.wal.close()
        recovered_wal = WAL(self.temp_dir)
        recovered_entries = list(recovered_wal.recover())
        
        # 验证所有数据
        self.assertEqual(len(recovered_entries), len(entries))
        for (key, value), (rec_key, rec_value) in zip(entries, recovered_entries):
            self.assertEqual(key, rec_key)
            self.assertEqual(value, rec_value)
    
    def test_deleted_entries(self):
        """测试删除标记的处理"""
        # 写入一些记录，包括删除标记
        entries = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key2", ""),  # 删除key2
            ("key3", "value3"),
            ("key3", ""),  # 删除key3
            ("key3", "value3_new")  # 重新写入key3
        ]
        
        for key, value in entries:
            self.wal.append(key, value)
        
        # 关闭并重新打开WAL
        self.wal.close()
        recovered_wal = WAL(self.temp_dir)
        recovered_entries = list(recovered_wal.recover())
        
        # 验证最终状态
        expected = [
            ("key1", "value1"),
            ("key2", ""),
            ("key3", "value3_new")
        ]
        
        self.assertEqual(len(recovered_entries), len(expected))
        for i, (key, value) in enumerate(expected):
            rec_key, rec_value = recovered_entries[i]
            self.assertEqual(key, rec_key)
            self.assertEqual(value, rec_value)
    
    def test_corrupted_file(self):
        """测试处理损坏的WAL文件"""
        # 写入一些记录
        entries = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3")
        ]
        
        for key, value in entries:
            self.wal.append(key, value)
        
        # 关闭WAL
        self.wal.close()
        
        # 模拟文件损坏：截断最后一条记录
        wal_file = os.path.join(self.temp_dir, "wal")
        with open(wal_file, 'rb') as f:
            content = f.read()[:-10]  # 截断最后10个字节
        with open(wal_file, 'wb') as f:
            f.write(content)
        
        # 尝试恢复数据
        recovered_wal = WAL(self.temp_dir)
        recovered_entries = list(recovered_wal.recover())
        
        # 验证能恢复的数据
        self.assertGreaterEqual(len(recovered_entries), len(entries) - 1)
        for i in range(len(recovered_entries)):
            rec_key, rec_value = recovered_entries[i]
            self.assertEqual(rec_key, entries[i][0])
            self.assertEqual(rec_value, entries[i][1])
    
    def test_empty_wal(self):
        """测试空WAL文件的处理"""
        # 关闭空的WAL
        self.wal.close()
        
        # 尝试恢复数据
        recovered_wal = WAL(self.temp_dir)
        recovered_entries = list(recovered_wal.recover())
        
        # 验证结果为空
        self.assertEqual(len(recovered_entries), 0)

if __name__ == '__main__':
    unittest.main()
