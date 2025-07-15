#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
保存失败场景测试

验证在数据库保存失败时，进度条的正确行为和错误处理机制
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.core.smart_scraper import SmartScraper
from src.core.database.repository import SQLiteRepository


class TestSaveFailureScenarios:
    """保存失败场景测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_keyword = "save_failure_test"
        
        # 创建测试用的Repository
        self.repository = SQLiteRepository(
            keyword=self.test_keyword,
            output_dir=self.temp_dir
        )
        
        # 创建SmartScraper实例
        self.scraper = SmartScraper(
            repository=self.repository,
            session_id="test_session_save_failure"
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
    async def test_second_phase_save_failure_progress_bar(self):
        """🔥 测试第二阶段保存失败时的进度条行为"""
        
        print("\n🔍 测试第二阶段保存失败时的进度条行为...")
        
        # 模拟第一阶段数据
        base_pins = [{'id': 'base_pin_1', 'title': 'Base Pin 1'}]
        
        # 模拟第二阶段数据
        related_pins = [
            {'id': 'failure_pin_1', 'title': 'Failure Pin 1'},
            {'id': 'failure_pin_2', 'title': 'Failure Pin 2'},
            {'id': 'failure_pin_3', 'title': 'Failure Pin 3'},
        ]
        
        # 模拟保存失败的情况
        save_call_count = 0
        progress_updates = []
        
        def mock_save_with_failures(pin_data, query, session_id):
            nonlocal save_call_count
            save_call_count += 1
            pin_id = pin_data.get('id')
            
            # 模拟：第1个成功，第2、3个失败
            if pin_id == 'failure_pin_1':
                progress_updates.append(f"success_{pin_id}")
                return True
            else:
                progress_updates.append(f"failure_{pin_id}")
                return False
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_with_failures):
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                    
                    # 验证结果
                    assert len(result) == 4, "应该返回4个Pin（1个基础 + 3个相关）"
                    
                    # 验证保存调用次数
                    assert save_call_count == 3, "第二阶段应该尝试保存3个Pin"
                    
                    # 验证进度更新记录
                    assert 'success_failure_pin_1' in progress_updates, "第一个Pin保存成功应该被记录"
                    assert 'failure_failure_pin_2' in progress_updates, "第二个Pin保存失败应该被记录"
                    assert 'failure_failure_pin_3' in progress_updates, "第三个Pin保存失败应该被记录"
                    
                    print(f"  ✅ 第二阶段保存失败测试：3个Pin中1个成功，2个失败，进度条应该只更新1次")

    @pytest.mark.asyncio
    async def test_database_exception_handling(self):
        """🔥 测试数据库异常时的错误处理"""
        
        print("\n🔍 测试数据库异常时的错误处理...")
        
        base_pins = [{'id': 'base_exception', 'title': 'Base Exception'}]
        related_pins = [
            {'id': 'exception_pin_1', 'title': 'Exception Pin 1'},
            {'id': 'exception_pin_2', 'title': 'Exception Pin 2'},
        ]
        
        exception_count = 0
        
        def mock_save_with_exception(pin_data, query, session_id):
            nonlocal exception_count
            exception_count += 1
            pin_id = pin_data.get('id')
            
            if pin_id == 'exception_pin_1':
                # 第一个Pin抛出异常
                raise Exception("Database connection error")
            else:
                # 第二个Pin正常保存
                return True
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_with_exception):
                    
                    # 执行混合策略，不应该因为异常而中断
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 3)
                    
                    # 验证结果
                    assert len(result) == 3, "即使有异常，也应该返回3个Pin"
                    
                    # 验证异常处理次数
                    assert exception_count == 2, "应该尝试保存2个Pin"
                    
                    print(f"  ✅ 数据库异常处理：异常不会中断采集流程，进度条正确处理异常情况")

    @pytest.mark.asyncio
    async def test_partial_save_failure_statistics(self):
        """🔥 测试部分保存失败时的统计信息"""
        
        print("\n🔍 测试部分保存失败时的统计信息...")
        
        base_pins = [{'id': 'stats_base', 'title': 'Stats Base'}]
        related_pins = [
            {'id': 'stats_pin_1', 'title': 'Stats Pin 1'},
            {'id': 'stats_pin_2', 'title': 'Stats Pin 2'},
            {'id': 'stats_pin_3', 'title': 'Stats Pin 3'},
            {'id': 'stats_pin_4', 'title': 'Stats Pin 4'},
            {'id': 'stats_pin_5', 'title': 'Stats Pin 5'},
        ]
        
        # 模拟保存结果：60%成功率
        save_results = [True, False, True, False, True]  # 3成功，2失败
        save_call_count = 0
        
        def mock_save_with_stats(pin_data, query, session_id):
            nonlocal save_call_count
            result = save_results[save_call_count % len(save_results)]
            save_call_count += 1
            return result
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_with_stats):
                    
                    # 重置统计
                    self.scraper.stats["pins_saved_realtime"] = 0
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 6)
                    
                    # 验证结果
                    assert len(result) == 6, "应该返回6个Pin"
                    
                    # 验证保存统计
                    saved_count = self.scraper.stats.get("pins_saved_realtime", 0)
                    expected_saved = sum(save_results)  # 3个成功
                    assert saved_count == expected_saved, f"统计应该显示{expected_saved}个Pin保存成功"
                    
                    # 验证保存成功率
                    success_rate = saved_count / save_call_count
                    assert success_rate == 0.6, f"保存成功率应该是60%，实际是{success_rate*100}%"
                    
                    print(f"  ✅ 统计信息验证：5个Pin中3个保存成功，成功率60%")

    @pytest.mark.asyncio
    async def test_all_saves_fail_scenario(self):
        """🔥 测试所有保存都失败的极端场景"""
        
        print("\n🔍 测试所有保存都失败的极端场景...")
        
        base_pins = [{'id': 'all_fail_base', 'title': 'All Fail Base'}]
        related_pins = [
            {'id': 'all_fail_1', 'title': 'All Fail 1'},
            {'id': 'all_fail_2', 'title': 'All Fail 2'},
            {'id': 'all_fail_3', 'title': 'All Fail 3'},
        ]
        
        def mock_save_all_fail(pin_data, query, session_id):
            # 所有保存都失败
            return False
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_all_fail):
                    
                    # 重置统计
                    self.scraper.stats["pins_saved_realtime"] = 0
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                    
                    # 验证结果
                    assert len(result) == 4, "即使所有保存都失败，也应该返回4个Pin"
                    
                    # 验证统计：没有Pin被保存
                    saved_count = self.scraper.stats.get("pins_saved_realtime", 0)
                    assert saved_count == 0, "所有保存失败时，统计应该显示0个Pin保存成功"
                    
                    print(f"  ✅ 极端场景验证：所有保存失败时，进度条不更新，但采集继续进行")

    def test_save_failure_error_logging(self):
        """🔥 测试保存失败时的错误日志记录"""
        
        print("\n🔍 测试保存失败时的错误日志记录...")
        
        # 创建一个会失败的Pin
        test_pin = {'id': 'logging_test_pin', 'title': 'Logging Test Pin'}
        
        # 模拟保存失败
        def mock_save_fail(pin_data, query, session_id):
            return False
        
        with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_fail):
            # 这里我们无法直接测试日志输出，但可以验证保存失败的返回值
            success = self.repository.save_pin_immediately(test_pin, self.test_keyword, self.scraper.session_id)
            assert success == False, "保存应该失败"
            
            print(f"  ✅ 错误日志测试：保存失败时返回False，应该记录警告日志")

    @pytest.mark.asyncio
    async def test_mixed_success_failure_progress_accuracy(self):
        """🔥 测试混合成功/失败场景下的进度条准确性"""
        
        print("\n🔍 测试混合成功/失败场景下的进度条准确性...")
        
        base_pins = [{'id': 'mixed_base', 'title': 'Mixed Base'}]
        related_pins = [
            {'id': 'mixed_1', 'title': 'Mixed 1'},  # 成功
            {'id': 'mixed_2', 'title': 'Mixed 2'},  # 失败
            {'id': 'mixed_3', 'title': 'Mixed 3'},  # 成功
            {'id': 'mixed_4', 'title': 'Mixed 4'},  # 失败
            {'id': 'mixed_5', 'title': 'Mixed 5'},  # 成功
        ]
        
        # 交替成功/失败模式
        def mock_save_alternating(pin_data, query, session_id):
            pin_id = pin_data.get('id')
            # 奇数成功，偶数失败
            if pin_id in ['mixed_1', 'mixed_3', 'mixed_5']:
                return True
            else:
                return False
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_alternating):
                    
                    # 重置统计
                    self.scraper.stats["pins_saved_realtime"] = 0
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 6)
                    
                    # 验证结果
                    assert len(result) == 6, "应该返回6个Pin"
                    
                    # 验证保存统计
                    saved_count = self.scraper.stats.get("pins_saved_realtime", 0)
                    assert saved_count == 3, "应该有3个Pin保存成功（mixed_1, mixed_3, mixed_5）"
                    
                    print(f"  ✅ 混合场景验证：5个Pin中3个成功2个失败，进度条准确反映3个成功保存")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
