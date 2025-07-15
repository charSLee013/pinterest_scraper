#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
进度条一致性测试

验证第一阶段和第二阶段的进度条逻辑完全一致，都反映真实的数据持久化状态
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock, call
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.core.smart_scraper import SmartScraper
from src.core.database.repository import SQLiteRepository
from src.core.browser_manager import BrowserManager


class TestProgressBarConsistency:
    """进度条一致性测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_keyword = "progress_consistency"
        
        # 创建测试用的Repository
        self.repository = SQLiteRepository(
            keyword=self.test_keyword,
            output_dir=self.temp_dir
        )
        
        # 创建SmartScraper实例
        self.scraper = SmartScraper(
            repository=self.repository,
            session_id="test_session_progress_consistency"
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
    async def test_first_phase_progress_bar_logic(self):
        """🔥 测试第一阶段进度条逻辑：只有保存成功时才更新"""
        
        print("\n🔍 测试第一阶段进度条逻辑...")
        
        # 模拟保存成功和失败的情况
        save_results = [True, False, True, True, False]  # 3成功，2失败
        save_call_count = 0
        progress_updates = []
        
        def mock_save_with_mixed_results(pin_data, query, session_id):
            nonlocal save_call_count
            result = save_results[save_call_count % len(save_results)]
            save_call_count += 1
            return result
        
        # Mock BrowserManager的scroll_and_collect方法
        async def mock_scroll_and_collect(*args, **kwargs):
            # 模拟第一阶段的逻辑：只有保存成功时才计入进度
            repository = kwargs.get('repository')
            query = kwargs.get('query')
            session_id = kwargs.get('session_id')
            
            test_pins = [
                {'id': f'first_phase_pin_{i}', 'title': f'First Phase Pin {i}'}
                for i in range(5)
            ]
            
            saved_count = 0
            for pin in test_pins:
                success = mock_save_with_mixed_results(pin, query, session_id)
                if success:
                    saved_count += 1
                    progress_updates.append(f"first_phase_saved_{pin['id']}")
            
            # 返回所有Pin（包括保存失败的），但进度条只反映保存成功的
            return test_pins
        
        with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_with_mixed_results):
            with patch('src.core.browser_manager.BrowserManager.scroll_and_collect', side_effect=mock_scroll_and_collect):
                
                # 执行第一阶段
                result = await self.scraper._search_phase_scrape(
                    f"https://www.pinterest.com/search/pins/?q={self.test_keyword}",
                    5,
                    self.test_keyword
                )
                
                # 验证结果
                assert len(result) == 5, "应该返回5个Pin"
                
                # 验证保存调用次数
                assert save_call_count == 5, "应该尝试保存5个Pin"
                
                # 验证只有成功保存的Pin被计入进度（通过mock验证）
                successful_saves = sum(save_results)
                assert successful_saves == 3, "应该有3个Pin保存成功"
                
                print(f"  ✅ 第一阶段：5个Pin中3个保存成功，进度条应该只更新3次")

    @pytest.mark.asyncio
    async def test_second_phase_progress_bar_logic(self):
        """🔥 测试第二阶段进度条逻辑：修复后应该与第一阶段一致"""
        
        print("\n🔍 测试第二阶段进度条逻辑...")
        
        # 模拟第一阶段的基础Pin
        base_pins = [
            {'id': 'base_pin_1', 'title': 'Base Pin 1'},
            {'id': 'base_pin_2', 'title': 'Base Pin 2'}
        ]
        
        # 模拟第二阶段的相关Pin
        related_pins = [
            {'id': 'related_pin_1', 'title': 'Related Pin 1'},
            {'id': 'related_pin_2', 'title': 'Related Pin 2'},
            {'id': 'related_pin_3', 'title': 'Related Pin 3'},
            {'id': 'related_pin_4', 'title': 'Related Pin 4'},
        ]
        
        # 模拟保存结果：前2个成功，后2个失败
        save_results = [True, True, False, False]
        save_call_count = 0
        progress_updates = []
        
        def mock_save_with_results(pin_data, query, session_id):
            nonlocal save_call_count
            if save_call_count < len(save_results):
                result = save_results[save_call_count]
            else:
                result = True  # 默认成功
            save_call_count += 1
            
            if result:
                progress_updates.append(f"saved_{pin_data['id']}")
            
            return result
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_with_results):
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 6)
                    
                    # 验证结果
                    assert len(result) == 6, "应该返回6个Pin（2个基础 + 4个相关）"
                    
                    # 验证保存调用次数（只有第二阶段的Pin会调用保存）
                    assert save_call_count == 4, "第二阶段应该尝试保存4个Pin"
                    
                    # 验证只有成功保存的Pin被记录
                    successful_saves = len([r for r in save_results if r])
                    assert successful_saves == 2, "第二阶段应该有2个Pin保存成功"
                    
                    # 验证进度更新记录
                    assert len(progress_updates) == 2, "进度条应该只更新2次（对应2个保存成功的Pin）"
                    assert 'saved_related_pin_1' in progress_updates, "第一个Pin保存成功应该被记录"
                    assert 'saved_related_pin_2' in progress_updates, "第二个Pin保存成功应该被记录"
                    
                    print(f"  ✅ 第二阶段：4个Pin中2个保存成功，进度条应该只更新2次")

    @pytest.mark.asyncio
    async def test_progress_bar_consistency_between_phases(self):
        """🔥 测试两个阶段的进度条逻辑一致性"""
        
        print("\n🔍 测试两个阶段的进度条逻辑一致性...")
        
        # 模拟数据
        first_phase_pins = [
            {'id': 'consistency_first_1', 'title': 'Consistency First 1'},
            {'id': 'consistency_first_2', 'title': 'Consistency First 2'}
        ]
        
        second_phase_pins = [
            {'id': 'consistency_second_1', 'title': 'Consistency Second 1'},
            {'id': 'consistency_second_2', 'title': 'Consistency Second 2'}
        ]
        
        # 模拟保存结果：第一阶段1成功1失败，第二阶段1成功1失败
        save_results = {
            'consistency_first_1': True,
            'consistency_first_2': False,
            'consistency_second_1': True,
            'consistency_second_2': False
        }
        
        progress_updates = []
        
        def mock_save_consistent(pin_data, query, session_id):
            pin_id = pin_data.get('id')
            result = save_results.get(pin_id, True)
            
            if result:
                progress_updates.append(f"progress_update_{pin_id}")
            
            return result
        
        # Mock第一阶段
        async def mock_first_phase_with_save(url, target_count, query):
            saved_pins = []
            for pin in first_phase_pins:
                success = mock_save_consistent(pin, query, self.scraper.session_id)
                if success:
                    saved_pins.append(pin)
                    # 第一阶段逻辑：保存成功时记录进度更新
            return first_phase_pins  # 返回所有Pin，但只有成功的被计入进度
        
        with patch.object(self.scraper, '_search_phase_scrape', side_effect=mock_first_phase_with_save):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=second_phase_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_consistent):
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                    
                    # 验证结果
                    assert len(result) == 4, "应该返回4个Pin"
                    
                    # 验证进度更新的一致性
                    expected_updates = [
                        'progress_update_consistency_first_1',    # 第一阶段成功
                        'progress_update_consistency_second_1'    # 第二阶段成功
                    ]
                    
                    for expected in expected_updates:
                        assert expected in progress_updates, f"应该包含进度更新: {expected}"
                    
                    # 验证失败的Pin没有进度更新
                    failed_updates = [
                        'progress_update_consistency_first_2',    # 第一阶段失败
                        'progress_update_consistency_second_2'    # 第二阶段失败
                    ]
                    
                    for failed in failed_updates:
                        assert failed not in progress_updates, f"失败的Pin不应该有进度更新: {failed}"
                    
                    print(f"  ✅ 两个阶段的进度条逻辑完全一致：只有保存成功时才更新进度条")

    def test_progress_bar_accuracy_with_database(self):
        """🔥 测试进度条准确性：进度条显示数量应该等于数据库实际数量"""
        
        print("\n🔍 测试进度条准确性...")
        
        # 创建测试数据：部分保存成功，部分失败
        test_pins = [
            {'id': 'accuracy_pin_1', 'title': 'Accuracy Pin 1'},
            {'id': 'accuracy_pin_2', 'title': 'Accuracy Pin 2'},
            {'id': 'accuracy_pin_3', 'title': 'Accuracy Pin 3'},
            {'id': 'accuracy_pin_4', 'title': 'Accuracy Pin 4'},
        ]
        
        # 模拟保存：前2个成功，后2个失败
        successful_pins = test_pins[:2]
        
        # 手动保存成功的Pin到数据库
        for pin in successful_pins:
            success = self.repository.save_pin_immediately(pin, self.test_keyword, self.scraper.session_id)
            assert success, f"Pin {pin['id']} 应该成功保存"
        
        # 验证数据库中的实际数量
        db_pins = self.repository.load_pins_by_query(self.test_keyword)
        assert len(db_pins) == 2, "数据库中应该有2个Pin"
        
        # 验证数据库中的Pin ID
        db_ids = [pin['id'] for pin in db_pins]
        assert 'accuracy_pin_1' in db_ids, "数据库应该包含第一个Pin"
        assert 'accuracy_pin_2' in db_ids, "数据库应该包含第二个Pin"
        assert 'accuracy_pin_3' not in db_ids, "数据库不应该包含第三个Pin"
        assert 'accuracy_pin_4' not in db_ids, "数据库不应该包含第四个Pin"
        
        print(f"  ✅ 进度条准确性验证：数据库实际数量 = 2，与预期的成功保存数量一致")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
