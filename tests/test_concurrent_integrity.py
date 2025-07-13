#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
并发完整性测试套件
专门测试Pinterest scraper在并发场景下的数据完整性
"""

import os
import sys
import time
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database.base import initialize_database
from src.core.database.repository import SQLiteRepository
from src.core.process_manager import ProcessManager


class ConcurrentIntegrityTester:
    """并发完整性测试器"""
    
    def __init__(self, test_name: str):
        self.test_name = test_name
        self.test_dir = f"test_concurrent_{test_name}"
        self.db_path = os.path.join(self.test_dir, f"{test_name}.db")
        self.setup_test_environment()
    
    def setup_test_environment(self):
        """设置测试环境"""
        os.makedirs(self.test_dir, exist_ok=True)
        
        # 删除已存在的数据库文件
        if os.path.exists(self.db_path):
            try:
                os.unlink(self.db_path)
            except:
                pass
        
        # 初始化数据库
        initialize_database(self.db_path)
    
    def cleanup_test_environment(self):
        """清理测试环境"""
        time.sleep(0.2)  # 等待连接关闭
        try:
            if os.path.exists(self.db_path):
                os.unlink(self.db_path)
            if os.path.exists(self.test_dir):
                os.rmdir(self.test_dir)
        except Exception as e:
            print(f"⚠️ 清理测试环境失败: {e}")
    
    def test_concurrent_same_pin_upsert(self, num_threads: int = 10) -> bool:
        """测试并发插入相同Pin的UPSERT行为"""
        print(f"\n=== 测试并发相同Pin UPSERT ({num_threads}线程) ===")
        
        def worker(worker_id):
            """工作线程：尝试插入相同的Pin"""
            repository = SQLiteRepository()
            
            pin_data = {
                'id': 'same_pin_test',
                'title': f'Worker {worker_id} Title',
                'description': f'Worker {worker_id} Description',
                'largest_image_url': 'https://example.com/same_pin.jpg',
                'stats': {'saves': worker_id * 10}
            }
            
            start_time = time.time()
            result = repository.save_pins_batch([pin_data], 'concurrent_same_pin')
            elapsed = time.time() - start_time
            
            print(f"Worker {worker_id} 完成，耗时: {elapsed:.3f}秒，结果: {result}")
            return result
        
        # 启动多个线程同时插入相同Pin
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            results = [future.result() for future in futures]
        
        # 验证结果
        repository = SQLiteRepository()
        pins = repository.load_pins_by_query('concurrent_same_pin')
        
        print(f"并发插入结果: {results}")
        print(f"最终Pin数量: {len(pins)}")
        if pins:
            print(f"最终Pin标题: {pins[0]['title']}")
        
        # 验证：所有操作都成功，但只有一个Pin
        assert all(results), "所有UPSERT操作都应该成功"
        assert len(pins) == 1, "应该只有一个Pin（去重成功）"
        
        print("✅ 并发相同Pin UPSERT测试通过")
        return True
    
    def test_concurrent_different_pins(self, num_threads: int = 10) -> bool:
        """测试并发插入不同Pin"""
        print(f"\n=== 测试并发不同Pin插入 ({num_threads}线程) ===")
        
        def worker(worker_id):
            """工作线程：插入不同的Pin"""
            repository = SQLiteRepository()
            
            pin_data = {
                'id': f'different_pin_{worker_id}',
                'title': f'Different Pin {worker_id}',
                'largest_image_url': f'https://example.com/different_{worker_id}.jpg'
            }
            
            return repository.save_pins_batch([pin_data], 'concurrent_different_pins')
        
        # 启动多个线程插入不同Pin
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            results = [future.result() for future in futures]
        
        # 验证结果
        repository = SQLiteRepository()
        pins = repository.load_pins_by_query('concurrent_different_pins')
        
        print(f"并发插入结果: {results}")
        print(f"最终Pin数量: {len(pins)}")
        
        # 验证：所有操作都成功，有对应数量的Pin
        assert all(results), "所有插入操作都应该成功"
        assert len(pins) == num_threads, f"应该有{num_threads}个不同的Pin"
        
        print("✅ 并发不同Pin插入测试通过")
        return True
    
    def test_concurrent_mixed_operations(self, num_threads: int = 8) -> bool:
        """测试并发混合操作（插入+更新）"""
        print(f"\n=== 测试并发混合操作 ({num_threads}线程) ===")
        
        # 先插入一些基础数据
        repository = SQLiteRepository()
        base_pins = []
        for i in range(5):
            pin_data = {
                'id': f'mixed_pin_{i}',
                'title': f'Base Pin {i}',
                'largest_image_url': f'https://example.com/base_{i}.jpg'
            }
            base_pins.append(pin_data)
        
        repository.save_pins_batch(base_pins, 'concurrent_mixed')
        print(f"预插入基础数据: {len(base_pins)} 个Pin")
        
        def worker(worker_id):
            """工作线程：执行混合操作"""
            repository = SQLiteRepository()
            
            if worker_id % 2 == 0:
                # 偶数线程：更新现有Pin
                pin_id = f'mixed_pin_{worker_id % 5}'
                pin_data = {
                    'id': pin_id,
                    'title': f'Updated by Worker {worker_id}',
                    'largest_image_url': f'https://example.com/updated_{worker_id}.jpg'
                }
            else:
                # 奇数线程：插入新Pin
                pin_data = {
                    'id': f'new_mixed_pin_{worker_id}',
                    'title': f'New Pin by Worker {worker_id}',
                    'largest_image_url': f'https://example.com/new_{worker_id}.jpg'
                }
            
            return repository.save_pins_batch([pin_data], 'concurrent_mixed')
        
        # 启动混合操作
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            results = [future.result() for future in futures]
        
        # 验证结果
        pins = repository.load_pins_by_query('concurrent_mixed')
        
        print(f"混合操作结果: {results}")
        print(f"最终Pin数量: {len(pins)}")
        
        # 计算预期数量：5个基础 + 4个新增（奇数线程）
        expected_count = 5 + (num_threads // 2)
        
        assert all(results), "所有混合操作都应该成功"
        assert len(pins) == expected_count, f"应该有{expected_count}个Pin"
        
        print("✅ 并发混合操作测试通过")
        return True
    
    def test_process_manager_concurrent_access(self) -> bool:
        """测试进程管理器的并发访问"""
        print(f"\n=== 测试进程管理器并发访问 ===")
        
        def worker(worker_id):
            """工作进程：尝试获取进程锁"""
            manager = ProcessManager(f"concurrent_process_test", self.test_dir)
            
            start_time = time.time()
            result = manager.acquire_lock()
            
            if result:
                print(f"Worker {worker_id} 获取锁成功")
                time.sleep(0.1)  # 模拟工作
                manager.release_lock()
                print(f"Worker {worker_id} 释放锁")
                return True
            else:
                print(f"Worker {worker_id} 获取锁失败")
                return False
        
        # 启动多个线程尝试获取锁
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker, i) for i in range(5)]
            results = [future.result() for future in futures]
        
        print(f"进程锁获取结果: {results}")
        
        # 验证：至少有一个成功获取锁
        success_count = sum(results)
        print(f"成功获取锁的线程数: {success_count}")
        
        assert success_count >= 1, "至少应该有一个线程成功获取锁"
        
        print("✅ 进程管理器并发访问测试通过")
        return True


def test_high_concurrency_stress():
    """高并发压力测试"""
    print("\n=== 高并发压力测试 ===")
    
    tester = ConcurrentIntegrityTester("stress_test")
    
    try:
        def stress_worker(worker_id):
            """压力测试工作线程"""
            repository = SQLiteRepository()
            
            # 每个线程插入多个Pin
            pins = []
            for i in range(5):
                pin_data = {
                    'id': f'stress_pin_{worker_id}_{i}',
                    'title': f'Stress Pin {worker_id}-{i}',
                    'largest_image_url': f'https://example.com/stress_{worker_id}_{i}.jpg'
                }
                pins.append(pin_data)
            
            return repository.save_pins_batch(pins, 'stress_test')
        
        # 启动大量线程
        num_threads = 20
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(stress_worker, i) for i in range(num_threads)]
            results = [future.result() for future in futures]
        
        total_time = time.time() - start_time
        
        # 验证结果
        repository = SQLiteRepository()
        pins = repository.load_pins_by_query('stress_test')
        
        print(f"压力测试结果: {sum(results)}/{len(results)} 成功")
        print(f"总耗时: {total_time:.3f}秒")
        print(f"最终Pin数量: {len(pins)}")
        print(f"预期Pin数量: {num_threads * 5}")
        
        assert all(results), "所有压力测试操作都应该成功"
        assert len(pins) == num_threads * 5, "Pin数量应该正确"
        
        print("✅ 高并发压力测试通过")
        return True
        
    finally:
        tester.cleanup_test_environment()


if __name__ == '__main__':
    print("开始并发完整性测试...")
    
    success = True
    
    # 测试1：并发相同Pin UPSERT
    tester1 = ConcurrentIntegrityTester("same_pin")
    try:
        success &= tester1.test_concurrent_same_pin_upsert(10)
    finally:
        tester1.cleanup_test_environment()
    
    # 测试2：并发不同Pin插入
    tester2 = ConcurrentIntegrityTester("different_pins")
    try:
        success &= tester2.test_concurrent_different_pins(10)
    finally:
        tester2.cleanup_test_environment()
    
    # 测试3：并发混合操作
    tester3 = ConcurrentIntegrityTester("mixed_ops")
    try:
        success &= tester3.test_concurrent_mixed_operations(8)
    finally:
        tester3.cleanup_test_environment()
    
    # 测试4：进程管理器并发访问
    tester4 = ConcurrentIntegrityTester("process_manager")
    try:
        success &= tester4.test_process_manager_concurrent_access()
    finally:
        tester4.cleanup_test_environment()
    
    # 测试5：高并发压力测试
    success &= test_high_concurrency_stress()
    
    if success:
        print("\n🎉 所有并发完整性测试通过！")
        print("✅ Pinterest scraper已成功修复IntegrityError问题")
        print("✅ 系统在高并发场景下表现稳定")
    else:
        print("\n❌ 部分并发测试失败")
        sys.exit(1)
