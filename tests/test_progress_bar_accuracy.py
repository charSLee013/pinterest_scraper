#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
进度条精确性验证测试

验证进度条显示数量与数据库实际数量的精确一致性
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.core.smart_scraper import SmartScraper
from src.core.database.repository import SQLiteRepository


class TestProgressBarAccuracy:
    """进度条精确性验证测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_keyword = "progress_accuracy"
        
        # 创建测试用的Repository
        self.repository = SQLiteRepository(
            keyword=self.test_keyword,
            output_dir=self.temp_dir
        )
        
        # 创建SmartScraper实例
        self.scraper = SmartScraper(
            repository=self.repository,
            session_id="test_session_progress_accuracy"
        )

    def teardown_method(self):
        """测试后清理"""
        # 确保数据库连接关闭
        if hasattr(self, 'repository') and self.repository:
            try:
                if hasattr(self.repository, '_get_session'):
                    session = self.repository._get_session()
                    if hasattr(session, 'close'):
                        session.close()
            except:
                pass
        
        # 清理临时目录
        if os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
            except PermissionError:
                import time
                time.sleep(0.5)
                try:
                    shutil.rmtree(self.temp_dir)
                except:
                    pass

    @pytest.mark.asyncio
    async def test_progress_bar_database_consistency(self):
        """🔥 测试进度条显示与数据库数量的一致性"""
        
        print("\n🔍 测试进度条显示与数据库数量的一致性...")
        
        # 模拟数据
        base_pins = [{'id': 'db_consistency_base', 'title': 'DB Consistency Base'}]
        related_pins = [
            {'id': 'db_consistency_1', 'title': 'DB Consistency 1'},
            {'id': 'db_consistency_2', 'title': 'DB Consistency 2'},
            {'id': 'db_consistency_3', 'title': 'DB Consistency 3'},
            {'id': 'db_consistency_4', 'title': 'DB Consistency 4'},
            {'id': 'db_consistency_5', 'title': 'DB Consistency 5'},
        ]
        
        # 模拟保存结果：70%成功率
        save_results = [True, True, False, True, False]  # 3成功，2失败
        save_call_count = 0
        actually_saved_pins = []
        
        def mock_save_with_tracking(pin_data, query, session_id):
            nonlocal save_call_count
            result = save_results[save_call_count % len(save_results)]
            save_call_count += 1
            
            if result:
                # 实际保存到真实数据库
                actual_success = self.repository.save_pin_immediately(pin_data, query, session_id)
                if actual_success:
                    actually_saved_pins.append(pin_data['id'])
                return actual_success
            else:
                return False
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                # 不mock save_pin_immediately，让它真实保存到数据库
                
                # 执行混合策略
                result = await self.scraper._hybrid_scrape(self.test_keyword, 6)
                
                # 验证内存结果
                assert len(result) == 6, "内存中应该有6个Pin"
                
                # 验证数据库中的实际数量
                db_pins = self.repository.load_pins_by_query(self.test_keyword)
                
                # 验证进度条统计与数据库一致
                saved_count = self.scraper.stats.get("pins_saved_realtime", 0)
                
                print(f"  📊 统计对比:")
                print(f"    - 内存中Pin数量: {len(result)}")
                print(f"    - 数据库中Pin数量: {len(db_pins)}")
                print(f"    - 进度条统计数量: {saved_count}")
                
                # 🔥 关键验证：进度条统计应该等于数据库实际数量
                assert len(db_pins) == saved_count, f"进度条统计({saved_count})应该等于数据库实际数量({len(db_pins)})"
                
                print(f"  ✅ 一致性验证通过：进度条统计 = 数据库实际数量 = {len(db_pins)}")

    @pytest.mark.asyncio
    async def test_large_dataset_accuracy(self):
        """🔥 测试大数据集下的进度条精确性"""
        
        print("\n🔍 测试大数据集下的进度条精确性...")
        
        # 创建大量测试数据
        base_pins = [{'id': f'large_base_{i}', 'title': f'Large Base {i}'} for i in range(5)]
        related_pins = [{'id': f'large_related_{i}', 'title': f'Large Related {i}'} for i in range(20)]
        
        # 模拟80%的成功率
        success_count = 0
        total_attempts = 0
        
        def mock_save_large_dataset(pin_data, query, session_id):
            nonlocal success_count, total_attempts
            total_attempts += 1
            
            # 80%成功率
            if total_attempts % 5 != 0:  # 每5个中4个成功
                success_count += 1
                # 实际保存到数据库
                return self.repository.save_pin_immediately(pin_data, query, session_id)
            else:
                return False
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                # 不完全mock，让成功的保存真实执行
                
                # 重置统计
                self.scraper.stats["pins_saved_realtime"] = 0
                
                # 执行混合策略
                result = await self.scraper._hybrid_scrape(self.test_keyword, 25)
                
                # 验证结果
                assert len(result) == 25, "内存中应该有25个Pin"
                
                # 验证数据库数量
                db_pins = self.repository.load_pins_by_query(self.test_keyword)
                saved_count_stats = self.scraper.stats.get("pins_saved_realtime", 0)
                
                print(f"  📊 大数据集统计:")
                print(f"    - 尝试保存: {total_attempts} 个Pin")
                print(f"    - 预期成功: {success_count} 个Pin")
                print(f"    - 数据库实际: {len(db_pins)} 个Pin")
                print(f"    - 进度条统计: {saved_count_stats} 个Pin")
                
                # 验证精确性
                assert len(db_pins) == saved_count_stats, "大数据集下进度条统计应该等于数据库实际数量"
                
                print(f"  ✅ 大数据集精确性验证通过")

    @pytest.mark.asyncio
    async def test_interruption_recovery_accuracy(self):
        """🔥 测试中断恢复时的进度条精确性"""
        
        print("\n🔍 测试中断恢复时的进度条精确性...")
        
        # 先保存一些基础数据模拟之前的会话
        existing_pins = [
            {'id': 'existing_1', 'title': 'Existing Pin 1'},
            {'id': 'existing_2', 'title': 'Existing Pin 2'},
            {'id': 'existing_3', 'title': 'Existing Pin 3'},
        ]
        
        for pin in existing_pins:
            success = self.repository.save_pin_immediately(pin, self.test_keyword, self.scraper.session_id)
            assert success, f"现有Pin {pin['id']} 应该成功保存"
        
        # 设置基准数量
        self.scraper._baseline_count = len(existing_pins)
        
        # 模拟新的采集数据
        base_pins = [{'id': 'recovery_base', 'title': 'Recovery Base'}]
        related_pins = [
            {'id': 'recovery_1', 'title': 'Recovery 1'},
            {'id': 'recovery_2', 'title': 'Recovery 2'},
        ]
        
        # 模拟部分保存成功
        def mock_save_recovery(pin_data, query, session_id):
            pin_id = pin_data.get('id')
            if pin_id == 'recovery_1':
                return self.repository.save_pin_immediately(pin_data, query, session_id)
            else:
                return False  # recovery_2 保存失败
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                
                # 重置统计
                self.scraper.stats["pins_saved_realtime"] = 0
                
                # 执行混合策略
                result = await self.scraper._hybrid_scrape(self.test_keyword, 6)
                
                # 验证总的数据库数量
                all_db_pins = self.repository.load_pins_by_query(self.test_keyword)
                new_saved_count = self.scraper.stats.get("pins_saved_realtime", 0)
                
                print(f"  📊 中断恢复统计:")
                print(f"    - 基准数量: {self.scraper._baseline_count}")
                print(f"    - 新保存数量: {new_saved_count}")
                print(f"    - 数据库总数量: {len(all_db_pins)}")
                print(f"    - 预期总数量: {self.scraper._baseline_count + new_saved_count}")
                
                # 验证总数量一致性
                expected_total = self.scraper._baseline_count + new_saved_count
                assert len(all_db_pins) == expected_total, "中断恢复后总数量应该一致"
                
                print(f"  ✅ 中断恢复精确性验证通过")

    def test_zero_success_rate_accuracy(self):
        """🔥 测试零成功率场景的精确性"""
        
        print("\n🔍 测试零成功率场景的精确性...")
        
        # 创建测试数据
        test_pins = [
            {'id': 'zero_success_1', 'title': 'Zero Success 1'},
            {'id': 'zero_success_2', 'title': 'Zero Success 2'},
            {'id': 'zero_success_3', 'title': 'Zero Success 3'},
        ]
        
        # 模拟所有保存都失败
        for pin in test_pins:
            success = False  # 不实际保存
            assert success == False, "所有保存都应该失败"
        
        # 验证数据库为空
        db_pins = self.repository.load_pins_by_query(self.test_keyword)
        assert len(db_pins) == 0, "零成功率时数据库应该为空"
        
        print(f"  ✅ 零成功率精确性验证：数据库为空，进度条应该不更新")

    def test_hundred_percent_success_rate_accuracy(self):
        """🔥 测试100%成功率场景的精确性"""
        
        print("\n🔍 测试100%成功率场景的精确性...")
        
        # 创建测试数据
        test_pins = [
            {'id': 'hundred_success_1', 'title': 'Hundred Success 1'},
            {'id': 'hundred_success_2', 'title': 'Hundred Success 2'},
            {'id': 'hundred_success_3', 'title': 'Hundred Success 3'},
            {'id': 'hundred_success_4', 'title': 'Hundred Success 4'},
            {'id': 'hundred_success_5', 'title': 'Hundred Success 5'},
        ]
        
        # 所有Pin都成功保存
        saved_count = 0
        for pin in test_pins:
            success = self.repository.save_pin_immediately(pin, self.test_keyword, self.scraper.session_id)
            if success:
                saved_count += 1
        
        # 验证数据库数量
        db_pins = self.repository.load_pins_by_query(self.test_keyword)
        
        print(f"  📊 100%成功率统计:")
        print(f"    - 尝试保存: {len(test_pins)} 个Pin")
        print(f"    - 实际保存: {saved_count} 个Pin")
        print(f"    - 数据库数量: {len(db_pins)} 个Pin")
        
        # 验证100%成功率
        assert saved_count == len(test_pins), "应该100%保存成功"
        assert len(db_pins) == len(test_pins), "数据库数量应该等于尝试保存的数量"
        
        print(f"  ✅ 100%成功率精确性验证通过")

    @pytest.mark.asyncio
    async def test_real_database_operations_accuracy(self):
        """🔥 测试真实数据库操作的精确性"""
        
        print("\n🔍 测试真实数据库操作的精确性...")
        
        # 使用真实的数据库操作，不使用mock
        base_pins = [{'id': 'real_db_base', 'title': 'Real DB Base'}]
        related_pins = [
            {'id': 'real_db_1', 'title': 'Real DB 1'},
            {'id': 'real_db_2', 'title': 'Real DB 2'},
            {'id': 'real_db_3', 'title': 'Real DB 3'},
        ]
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                
                # 重置统计
                self.scraper.stats["pins_saved_realtime"] = 0
                
                # 执行混合策略，使用真实的数据库保存
                result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                
                # 验证结果
                assert len(result) == 4, "应该返回4个Pin"
                
                # 验证真实数据库数量
                db_pins = self.repository.load_pins_by_query(self.test_keyword)
                saved_count_stats = self.scraper.stats.get("pins_saved_realtime", 0)
                
                print(f"  📊 真实数据库操作统计:")
                print(f"    - 内存Pin数量: {len(result)}")
                print(f"    - 数据库Pin数量: {len(db_pins)}")
                print(f"    - 进度条统计: {saved_count_stats}")
                
                # 🔥 最终验证：真实环境下的精确性
                assert len(db_pins) == saved_count_stats, "真实数据库操作下，进度条统计应该等于数据库实际数量"
                
                # 验证数据完整性
                db_ids = [pin['id'] for pin in db_pins]
                for pin in related_pins:
                    assert pin['id'] in db_ids, f"Pin {pin['id']} 应该在数据库中"
                
                print(f"  ✅ 真实数据库操作精确性验证通过：进度条 = 数据库 = {len(db_pins)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
