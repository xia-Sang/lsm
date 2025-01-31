# Python LSM Tree Implementation

这是一个使用Python实现的LSM树(Log-Structured Merge Tree)数据库。该实现包含以下主要特性：

## 核心特性

1. **LSM树基本结构**
   - MemTable: 内存中的有序表，使用跳表(Skip List)实现
   - SSTable: 磁盘上的有序表文件
   - WAL(Write Ahead Log): 写前日志，保证数据可靠性

2. **B树索引**
   - 用于优化SSTable的查询效率
   - 实现磁盘数据的高效检索
   - 使用`bplustree`库实现B+树结构

3. **布隆过滤器**
   - 快速判断key是否存在
   - 减少不必要的磁盘IO
   - 使用`pybloom-live`库实现

4. **稀疏索引**
   - 为SSTable建立稀疏索引
   - 减少内存占用
   - 加快数据检索速度

## 技术栈

- Python 3.8+
- bplustree: B+树实现
- pybloom-live: 布隆过滤器实现
- sortedcontainers: 有序数据结构

## 项目结构

```
py_lsm/
├── src/
│   ├── memtable.py      # 内存表实现
│   ├── sstable.py       # SSTable实现
│   ├── bloom_filter.py  # 布隆过滤器包装
│   ├── index.py         # 索引实现
│   ├── wal.py          # WAL实现
│   └── db.py           # 数据库主类
├── tests/              # 测试文件
├── requirements.txt    # 依赖文件
└── README.md          # 项目文档
```

## 基本操作

- `put(key, value)`: 写入键值对
- `get(key)`: 获取键对应的值
- `delete(key)`: 删除键值对
- `scan(start_key, end_key)`: 范围查询

## 实现细节

1. **写入流程**
   - 数据首先写入WAL
   - 然后写入MemTable
   - 当MemTable达到阈值时，触发Compaction
   - Compaction过程将数据写入SSTable

2. **读取流程**
   - 首先检查布隆过滤器
   - 查询MemTable
   - 如果未找到，通过稀疏索引定位SSTable
   - 使用B树索引在SSTable中查找

3. **Compaction策略**
   - 分层压缩(Leveled Compaction)
   - 每层大小限制
   - 合并时进行去重和清理过期数据

## 性能优化

- 使用布隆过滤器减少不必要的磁盘访问
- 通过稀疏索引减少内存占用
- B树索引加速磁盘数据检索
- 批量写入优化
