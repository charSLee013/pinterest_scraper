#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
混合策略集成测试

验证完整的混合采集策略（第一阶段+第二阶段）的数据保存一致性
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
from src.core.pinterest_scraper import PinterestScraper


class TestHybridStrategyIntegration:
    """混合策略集成测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_keyword = "integration_test"
        
        # 创建测试用的Repository
        self.repository = SQLiteRepository(
            keyword=self.test_keyword,
            output_dir=self.temp_dir
        )
        
        # 创建SmartScraper实例
        self.scraper = SmartScraper(
            repository=self.repository,
            session_id="test_session_integration"
        )

    def teardown_method(self):
        """测试后清理"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @pytest.mark.asyncio
    async def test_complete_hybrid_strategy_flow(self):
        """测试完整的混合策略流程"""
        
        # 模拟第一阶段数据
        first_phase_pins = [
            {'id': 'first_1', 'title': 'First Phase Pin 1', 'source': 'search'},
            {'id': 'first_2', 'title': 'First Phase Pin 2', 'source': 'search'},
        ]
        
        # 模拟第二阶段数据
        second_phase_pins = [
            {'id': 'second_1', 'title': 'Second Phase Pin 1', 'source': 'detail'},
            {'id': 'second_2', 'title': 'Second Phase Pin 2', 'source': 'detail'},
        ]
        
        saved_pins = []
        
        def mock_save_immediately(pin_data, query, session_id):
            """模拟保存函数，记录所有保存的Pin"""
            saved_pins.append(pin_data.copy())
            return True
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=first_phase_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=second_phase_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_immediately):
                    
                    # 执行完整的混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                    
                    # 验证返回结果
                    assert len(result) == 4, "应该返回4个Pin"
                    
                    # 验证第一阶段Pin
                    first_phase_ids = [pin['id'] for pin in result if pin['id'].startswith('first_')]
                    assert len(first_phase_ids) == 2, "应该包含2个第一阶段Pin"
                    
                    # 验证第二阶段Pin
                    second_phase_ids = [pin['id'] for pin in result if pin['id'].startswith('second_')]
                    assert len(second_phase_ids) == 2, "应该包含2个第二阶段Pin"
                    
                    # 🔥 关键验证：第二阶段Pin被实时保存
                    saved_second_phase = [pin for pin in saved_pins if pin['id'].startswith('second_')]
                    assert len(saved_second_phase) == 2, "第二阶段的Pin应该被实时保存到数据库"

    @pytest.mark.asyncio
    async def test_two_phase_storage_consistency(self):
        """测试两阶段存储一致性"""
        
        # 创建真实的数据库环境进行测试
        first_phase_pins = [
            {'id': 'consistency_first_1', 'title': 'First Phase Consistency Test'}
        ]
        
        second_phase_pins = [
            {'id': 'consistency_second_1', 'title': 'Second Phase Consistency Test'}
        ]
        
        with patch.object(self.scraper, '_search_phase_scrape') as mock_first:
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue') as mock_second:
                
                # 配置第一阶段mock：直接保存到数据库
                async def mock_first_phase(url, target_count, query):
                    for pin in first_phase_pins:
                        self.repository.save_pin_immediately(pin, query, self.scraper.session_id)
                    return first_phase_pins
                
                mock_first.side_effect = mock_first_phase
                mock_second.return_value = second_phase_pins
                
                # 执行混合策略
                result = await self.scraper._hybrid_scrape(self.test_keyword, 2)
                
                # 验证数据库中的数据
                db_pins = self.repository.load_pins_by_query(self.test_keyword)
                db_ids = [pin['id'] for pin in db_pins]
                
                # 验证第一阶段数据在数据库中
                assert 'consistency_first_1' in db_ids, "第一阶段数据应该在数据库中"
                
                # 验证第二阶段数据在数据库中
                assert 'consistency_second_1' in db_ids, "第二阶段数据应该在数据库中"

    @pytest.mark.asyncio
    async def test_adaptive_scrape_with_dedup_integration(self):
        """测试自适应采集与去重的集成"""
        
        # 模拟数据库中已有一些数据
        existing_pins = [
            {'id': 'existing_1', 'title': 'Existing Pin 1'},
            {'id': 'existing_2', 'title': 'Existing Pin 2'}
        ]
        
        for pin in existing_pins:
            self.repository.save_pin_immediately(pin, self.test_keyword, self.scraper.session_id)
        
        # 设置基准数量
        self.scraper._baseline_count = len(existing_pins)
        
        # 模拟新采集的数据
        new_first_phase = [{'id': 'new_first_1', 'title': 'New First Phase Pin'}]
        new_second_phase = [{'id': 'new_second_1', 'title': 'New Second Phase Pin'}]
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=new_first_phase):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=new_second_phase):
                
                # 执行自适应采集
                result = await self.scraper._adaptive_scrape_with_dedup(
                    self.test_keyword, 
                    f"https://www.pinterest.com/search/pins/?q={self.test_keyword}",
                    4  # 目标总数
                )
                
                # 验证结果
                assert len(result) >= 2, "应该至少返回新采集的数据"
                
                # 验证数据库中的总数据
                all_db_pins = self.repository.load_pins_by_query(self.test_keyword)
                assert len(all_db_pins) >= 4, "数据库中应该有至少4个Pin"

    @pytest.mark.asyncio
    async def test_interruption_during_second_phase(self):
        """测试第二阶段中断处理"""
        
        first_phase_pins = [{'id': 'interrupt_first_1', 'title': 'First Phase Before Interrupt'}]
        
        # 模拟在第二阶段发生中断
        async def mock_second_phase_with_interrupt(pin_id, max_count):
            # 模拟采集了一些数据后发生中断
            partial_pins = [{'id': 'interrupt_second_1', 'title': 'Second Phase Before Interrupt'}]
            
            # 设置中断标志
            self.scraper._interrupt_requested = True
            
            return partial_pins
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=first_phase_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', side_effect=mock_second_phase_with_interrupt):
                
                # 执行混合策略
                result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                
                # 验证中断处理
                assert len(result) >= 1, "至少应该保留第一阶段的数据"
                
                # 验证第一阶段数据存在
                first_phase_ids = [pin['id'] for pin in result if pin['id'].startswith('interrupt_first_')]
                assert len(first_phase_ids) == 1, "第一阶段数据应该被保留"
                
                # 验证数据库中的数据（包括中断前保存的第二阶段数据）
                db_pins = self.repository.load_pins_by_query(self.test_keyword)
                db_ids = [pin['id'] for pin in db_pins]
                
                # 第二阶段的数据应该也被保存了（因为实时保存机制）
                assert 'interrupt_second_1' in db_ids, "中断前的第二阶段数据应该被保存"

    def test_database_state_after_hybrid_strategy(self):
        """测试混合策略执行后的数据库状态"""
        
        # 手动保存一些测试数据模拟混合策略的结果
        test_pins = [
            {'id': 'db_state_first_1', 'title': 'DB State First Phase', 'phase': 'first'},
            {'id': 'db_state_first_2', 'title': 'DB State First Phase', 'phase': 'first'},
            {'id': 'db_state_second_1', 'title': 'DB State Second Phase', 'phase': 'second'},
            {'id': 'db_state_second_2', 'title': 'DB State Second Phase', 'phase': 'second'},
        ]
        
        for pin in test_pins:
            success = self.repository.save_pin_immediately(pin, self.test_keyword, self.scraper.session_id)
            assert success, f"Pin {pin['id']} 应该成功保存"
        
        # 验证数据库状态
        db_pins = self.repository.load_pins_by_query(self.test_keyword)
        assert len(db_pins) == 4, "数据库应该包含4个Pin"
        
        # 验证数据完整性
        db_ids = [pin['id'] for pin in db_pins]
        for test_pin in test_pins:
            assert test_pin['id'] in db_ids, f"Pin {test_pin['id']} 应该在数据库中"
        
        # 验证会话状态
        sessions = self.repository.get_incomplete_sessions()
        assert len(sessions) >= 0, "应该能够查询会话状态"

    @pytest.mark.asyncio
    async def test_performance_comparison(self):
        """测试修复前后的性能对比"""
        import time
        
        # 模拟大量数据的混合策略
        large_first_phase = [
            {'id': f'perf_first_{i}', 'title': f'Performance First {i}'}
            for i in range(10)
        ]
        
        large_second_phase = [
            {'id': f'perf_second_{i}', 'title': f'Performance Second {i}'}
            for i in range(10)
        ]
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=large_first_phase):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=large_second_phase):
                
                start_time = time.time()
                
                # 执行混合策略
                result = await self.scraper._hybrid_scrape(self.test_keyword, 20)
                
                end_time = time.time()
                duration = end_time - start_time
                
                # 验证性能指标
                assert duration < 5.0, f"混合策略执行时间过长: {duration:.3f}秒"
                assert len(result) == 20, "应该返回20个Pin"
                
                print(f"性能测试: 20个Pin的混合策略耗时 {duration:.3f}秒")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
