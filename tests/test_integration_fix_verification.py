#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest Scraper IntegrityError修复验证集成测试
验证完整的修复方案在真实场景下的效果
"""

import os
import sys
import time
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database.base import initialize_database
from src.core.database.repository import SQLiteRepository
from src.core.process_manager import ProcessManager
from src.core.database.migrate_indexes import migrate_database


class IntegrityErrorFixVerification:
    """IntegrityError修复验证器"""
    
    def __init__(self):
        self.test_dir = "integration_test_verification"
        self.setup_test_environment()
    
    def setup_test_environment(self):
        """设置测试环境"""
        os.makedirs(self.test_dir, exist_ok=True)
        print(f"测试环境设置完成: {self.test_dir}")
    
    def cleanup_test_environment(self):
        """清理测试环境"""
        try:
            import shutil
            if os.path.exists(self.test_dir):
                shutil.rmtree(self.test_dir)
            print("测试环境清理完成")
        except Exception as e:
            print(f"⚠️ 清理测试环境失败: {e}")
    
    def test_original_integrity_error_scenario(self) -> bool:
        """测试原始IntegrityError场景（应该已修复）"""
        print("\n=== 测试原始IntegrityError场景 ===")
        
        db_path = os.path.join(self.test_dir, "original_scenario.db")
        
        try:
            # 初始化数据库
            initialize_database(db_path)
            migrate_database(db_path)
            
            # 模拟用户报告的错误场景：多次中断重启
            def simulate_interrupted_process(process_id):
                """模拟被中断的进程"""
                repository = SQLiteRepository()
                
                # 模拟用户报告的Pin数据
                pin_data = {
                    'id': '76561262410443623',  # 用户报告的Pin ID
                    'title': f"Process {process_id} - an artist's rendering of a house with a swimming pool",
                    'description': "an artist's rendering of a house with a swimming pool in the foreground",
                    'largest_image_url': 'https://i.pinimg.com/originals/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg',
                    'image_urls': {
                        "1": "https://i.pinimg.com/236x/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg",
                        "2": "https://i.pinimg.com/474x/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg",
                        "4": "https://i.pinimg.com/originals/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg"
                    }
                }
                
                # 尝试保存（在修复前会导致IntegrityError）
                result = repository.save_pins_batch([pin_data], 'building')
                print(f"Process {process_id} 保存结果: {result}")
                return result
            
            # 模拟多个"重启"进程同时处理相同数据
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(simulate_interrupted_process, i) for i in range(5)]
                results = [future.result() for future in futures]
            
            # 验证结果
            repository = SQLiteRepository()
            pins = repository.load_pins_by_query('building')
            
            print(f"模拟重启进程结果: {results}")
            print(f"最终Pin数量: {len(pins)}")
            print(f"所有操作成功: {all(results)}")
            
            # 验证：所有操作都成功，没有IntegrityError
            assert all(results), "所有操作都应该成功（无IntegrityError）"
            assert len(pins) == 1, "应该只有一个Pin（去重成功）"
            
            print("✅ 原始IntegrityError场景修复验证通过")
            return True
            
        except Exception as e:
            print(f"❌ 原始场景测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_process_manager_integration(self) -> bool:
        """测试进程管理器集成"""
        print("\n=== 测试进程管理器集成 ===")
        
        try:
            def worker_with_process_manager(worker_id):
                """使用进程管理器的工作进程"""
                manager = ProcessManager("integration_test", self.test_dir)
                
                if manager.acquire_lock():
                    try:
                        # 模拟数据处理
                        repository = SQLiteRepository()
                        pin_data = {
                            'id': f'process_manager_pin_{worker_id}',
                            'title': f'Process Manager Pin {worker_id}',
                            'largest_image_url': f'https://example.com/pm_{worker_id}.jpg'
                        }
                        
                        result = repository.save_pins_batch([pin_data], 'process_manager_test')
                        time.sleep(0.1)  # 模拟处理时间
                        
                        return {'worker_id': worker_id, 'acquired_lock': True, 'save_result': result}
                    finally:
                        manager.release_lock()
                else:
                    return {'worker_id': worker_id, 'acquired_lock': False, 'save_result': False}
            
            # 启动多个进程尝试获取锁
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(worker_with_process_manager, i) for i in range(5)]
                results = [future.result() for future in futures]
            
            # 分析结果
            acquired_count = sum(1 for r in results if r['acquired_lock'])
            successful_saves = sum(1 for r in results if r['save_result'])
            
            print(f"进程管理器结果: {results}")
            print(f"获取锁的进程数: {acquired_count}")
            print(f"成功保存的进程数: {successful_saves}")
            
            # 验证数据
            repository = SQLiteRepository()
            pins = repository.load_pins_by_query('process_manager_test')
            print(f"最终Pin数量: {len(pins)}")
            
            # 验证：至少有一个进程获取锁并成功保存
            assert acquired_count >= 1, "至少应该有一个进程获取锁"
            assert successful_saves >= 1, "至少应该有一个进程成功保存"
            assert len(pins) == successful_saves, "Pin数量应该等于成功保存的进程数"
            
            print("✅ 进程管理器集成测试通过")
            return True
            
        except Exception as e:
            print(f"❌ 进程管理器集成测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_database_optimization_effects(self) -> bool:
        """测试数据库优化效果"""
        print("\n=== 测试数据库优化效果 ===")
        
        db_path = os.path.join(self.test_dir, "optimization_test.db")
        
        try:
            # 初始化数据库并应用优化
            initialize_database(db_path)
            migrate_result = migrate_database(db_path)
            print(f"数据库迁移结果: {migrate_result}")
            
            repository = SQLiteRepository()
            
            # 测试大量数据的性能
            test_pins = []
            for i in range(200):
                pin_data = {
                    'id': f'optimization_pin_{i}',
                    'title': f'Optimization Test Pin {i}',
                    'description': f'Testing database optimization with pin {i}',
                    'largest_image_url': f'https://example.com/opt_{i}.jpg',
                    'creator': {'id': f'creator_{i % 20}', 'name': f'Creator {i % 20}'},
                    'board': {'id': f'board_{i % 10}', 'name': f'Board {i % 10}'}
                }
                test_pins.append(pin_data)
            
            # 批量保存性能测试
            start_time = time.time()
            result = repository.save_pins_batch(test_pins, 'optimization_test')
            save_time = time.time() - start_time
            
            print(f"批量保存200个Pin耗时: {save_time:.3f}秒")
            print(f"平均每个Pin: {save_time/200*1000:.1f}毫秒")
            
            # 查询性能测试
            start_time = time.time()
            pins = repository.load_pins_by_query('optimization_test', limit=100)
            query_time = time.time() - start_time
            
            print(f"查询100个Pin耗时: {query_time:.3f}秒")
            print(f"查询到的Pin数量: {len(pins)}")
            
            # 验证结果
            assert result == True, "批量保存应该成功"
            assert len(pins) == 100, "应该查询到100个Pin"
            assert save_time < 5.0, "保存时间应该在合理范围内"
            assert query_time < 1.0, "查询时间应该在合理范围内"
            
            print("✅ 数据库优化效果测试通过")
            return True
            
        except Exception as e:
            print(f"❌ 数据库优化测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_concurrent_stress_scenario(self) -> bool:
        """测试并发压力场景"""
        print("\n=== 测试并发压力场景 ===")
        
        db_path = os.path.join(self.test_dir, "stress_test.db")
        
        try:
            # 初始化数据库
            initialize_database(db_path)
            migrate_database(db_path)
            
            def stress_worker(worker_id):
                """压力测试工作线程"""
                repository = SQLiteRepository()
                
                # 每个线程处理多种操作
                operations = []
                
                # 1. 插入新Pin
                for i in range(3):
                    pin_data = {
                        'id': f'stress_pin_{worker_id}_{i}',
                        'title': f'Stress Pin {worker_id}-{i}',
                        'largest_image_url': f'https://example.com/stress_{worker_id}_{i}.jpg'
                    }
                    result = repository.save_pins_batch([pin_data], 'stress_test')
                    operations.append(result)
                
                # 2. 更新现有Pin（模拟重复数据）
                if worker_id % 2 == 0:
                    update_data = {
                        'id': f'stress_pin_{worker_id}_0',
                        'title': f'Updated Stress Pin {worker_id}-0',
                        'largest_image_url': f'https://example.com/updated_stress_{worker_id}_0.jpg'
                    }
                    result = repository.save_pins_batch([update_data], 'stress_test')
                    operations.append(result)
                
                return all(operations)
            
            # 启动大量并发线程
            num_workers = 15
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(stress_worker, i) for i in range(num_workers)]
                results = [future.result() for future in futures]
            
            total_time = time.time() - start_time
            
            # 验证结果
            repository = SQLiteRepository()
            pins = repository.load_pins_by_query('stress_test')
            
            print(f"并发压力测试结果: {sum(results)}/{len(results)} 成功")
            print(f"总耗时: {total_time:.3f}秒")
            print(f"最终Pin数量: {len(pins)}")
            
            # 计算预期Pin数量：每个worker 3个新Pin + 一半worker的更新操作
            expected_pins = num_workers * 3  # 更新操作不会增加Pin数量
            
            assert all(results), "所有并发操作都应该成功"
            assert len(pins) == expected_pins, f"Pin数量应该是{expected_pins}"
            
            print("✅ 并发压力场景测试通过")
            return True
            
        except Exception as e:
            print(f"❌ 并发压力测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False


def run_integration_verification():
    """运行完整的集成验证测试"""
    print("🚀 开始Pinterest Scraper IntegrityError修复验证")
    print("=" * 60)
    
    verifier = IntegrityErrorFixVerification()
    
    try:
        success = True
        
        # 执行所有验证测试
        success &= verifier.test_original_integrity_error_scenario()
        success &= verifier.test_process_manager_integration()
        success &= verifier.test_database_optimization_effects()
        success &= verifier.test_concurrent_stress_scenario()
        
        print("\n" + "=" * 60)
        
        if success:
            print("🎉 所有集成验证测试通过！")
            print("✅ Pinterest Scraper IntegrityError问题已完全修复")
            print("✅ 系统在各种场景下表现稳定")
            print("\n📋 修复总结:")
            print("  1. ✅ 实现了UPSERT操作，消除竞态条件")
            print("  2. ✅ 添加了进程管理，防止多实例冲突")
            print("  3. ✅ 优化了数据库索引，提升性能")
            print("  4. ✅ 通过了全面的并发测试验证")
            print("  5. ✅ 系统在高压力场景下稳定运行")
        else:
            print("❌ 部分集成验证测试失败")
            print("需要进一步检查和修复")
            return False
        
    finally:
        verifier.cleanup_test_environment()
    
    return success


if __name__ == '__main__':
    success = run_integration_verification()
    sys.exit(0 if success else 1)
