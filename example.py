import os
import random
import string
from lsm.lsm import LSMTree

def generate_random_string(length):
    """生成指定长度的随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_test_data(count):
    """生成测试数据"""
    data = []
    for i in range(count):
        # key = generate_random_string(16)  # 16字节的键
        key = f"key_{i:09d}"
        value = generate_random_string(100)  # 100字节的值
        data.append((key, value))
    return data

def main():
    # 使用当前目录下的data子目录
    data_dir = os.path.join(os.getcwd(), "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    print("\n=== 初始化 LSM 树 ===")
    print(f"数据目录: {data_dir}")
    lsm = LSMTree(data_dir)
    
    # 准备测试数据
    test_data = generate_test_data(3000)  # 生成1000个键值对
    
    print("\n=== 写入数据 ===")
    print(f"总计写入 {len(test_data)} 个键值对")
    print("\n前5个键值对示例:")
    for key, value in test_data[:5]:
        print(f"  {key} -> {value[:20]}...")

    # 写入所有数据
    for i, (key, value) in enumerate(test_data):
        lsm.put(key, value)
        if i % 100 == 0:  # 每100个打印一次进度
            print(f"已写入: {i + 1}/{len(test_data)}")
    
    print(f"\nMemTable大小: {len(lsm.memtable)}")
    print(f"SSTable数量: {len(lsm.sstables)}")
    
    # 验证写入
    print("\n=== 验证写入 ===")
    errors = 0
    for i, (key, expected) in enumerate(test_data):
        value = lsm.get(key)
        if value != expected:
            print(f"错误 #{errors + 1}: {key} -> 得到 {value[:20]}..., 期望 {expected[:20]}...")
            errors += 1
            if errors >= 5:  # 只显示前5个错误
                break
    
    if errors == 0:
        print("所有数据验证正确")
    else:
        print(f"\n发现 {errors} 个错误")
    
    # 关闭 LSM 树
    print("\n=== 关闭 LSM 树 ===")
    lsm.close()
    
    # 重新打开 LSM 树
    print("\n=== 重新打开 LSM 树 ===")
    lsm = LSMTree(data_dir)
    
    print(f"\n恢复后:")
    print(f"SSTable数量: {len(lsm.sstables)}")
    if lsm.sstables:
        print("\nSSTable信息:")
        for i, sst in enumerate(lsm.sstables):
            print(f"\nSSTable #{i}:")
            print(f"  序列号: {sst.sequence}")
            if sst.metadata:
                print(f"  数据大小: {sst.metadata.data_size}")
                print(f"  元数据范围: {sst.metadata.min_key} -> {sst.metadata.max_key}")
                print(f"  索引项数量: {len(sst.index)}")
                print("\n  实际键值:")
                # 显示所有键
 
    
    # 验证恢复
    print("\n=== 验证恢复 ===")
    errors = 0
    for i, (key, expected) in enumerate(test_data):
        value = lsm.get(key)
        if value != expected:
            print(f"错误 #{errors + 1}: {key}")
            print(f"  期望值: {expected[:50]}...")
            print(f"  实际值: {'None' if value is None else value[:50] + '...'}")
            
            # 如果验证失败，检查每个SSTable
            print(f"  直接从每个SSTable读取 {key}:")
            for j, sst in enumerate(lsm.sstables):
                sst_value = sst.get(key)
                print(f"    SSTable #{j}:")
                print(f"      值: {'None' if sst_value is None else sst_value[:50] + '...'}")
                print(f"      键范围: {sst.metadata.min_key} -> {sst.metadata.max_key}")
                print(f"      键比较结果:")
                print(f"        key < min_key: {sst._compare_keys(key, sst.metadata.min_key) < 0}")
                print(f"        key > max_key: {sst._compare_keys(key, sst.metadata.max_key) > 0}")
            errors += 1
            if errors >= 5:  # 只显示前5个错误
                print(f"\n... 还有更多错误未显示 ...")
                break
    
    if errors == 0:
        print("所有数据恢复正确")
    else:
        print(f"\n发现 {errors} 个错误")
        raise Exception("数据恢复验证失败")

if __name__ == '__main__':
    main()
