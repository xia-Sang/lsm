"""
LSM树数据库配置文件
"""

class Config:
    def __init__(self):
        # MemTable配置
        self.MEMTABLE_SIZE = 1024 * 1024  # MemTable大小阈值(1MB)
        
        # SSTable配置
        self.SSTABLE_BASE_DIR = "sstable"  # SSTable存储目录名
        self.SSTABLE_SUFFIX = '.sst'  # SSTable文件后缀
        
        # SSTable文件格式配置
        self.SST_MAGIC_NUMBER = b'LSMT'  # SSTable文件魔数
        self.SST_VERSION = 1  # SSTable文件版本号
        self.SST_HEADER_SIZE = 4096  # SSTable头部大小(4KB)，包含元数据
        self.SST_BLOCK_SIZE = 4096  # 数据块大小(4KB)
        self.SST_INDEX_BLOCK_SIZE = 4096  # 索引块大小(4KB)
        
        # WAL配置
        self.WAL_DIR = "wal"  # WAL目录名
        self.WAL_FILE_NAME = "wal"  # WAL文件名
        
        # 布隆过滤器配置
        self.BLOOM_FILTER_SIZE_FACTOR = 10  # 布隆过滤器大小因子(每个key使用10位)
        self.BLOOM_FILTER_MIN_SIZE = 1024  # 布隆过滤器最小大小(1KB)
        self.BLOOM_FILTER_HASH_COUNT = 3  # 布隆过滤器哈希函数数量
        
        # 文件路径配置
        self.DEFAULT_DATA_DIR = "data"  # 默认数据目录

# 默认配置实例
default_config = Config()
