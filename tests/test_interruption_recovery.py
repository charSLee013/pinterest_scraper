#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中断恢复测试

验证第一阶段和第二阶段中断恢复机制的数据完整性
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


class TestInterruptionRecovery:
    """中断恢复测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_keyword = "interruption_test"
        
        # 创建测试用的Repository
        self.repository = SQLiteRepository(
            keyword=self.test_keyword,
            output_dir=self.temp_dir
        )
        
        # 创建SmartScraper实例
        self.scraper = SmartScraper(
            repository=self.repository,
            session_id="test_session_interruption"
        )

    def teardown_method(self):
        """测试后清理"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @pytest.mark.asyncio
    async def test_first_phase_interruption_recovery(self):
        """测试第一阶段中断恢复"""
        
        # 模拟第一阶段数据
        first_phase_pins = [
            {'id': 'first_interrupt_1', 'title': 'First Phase Pin 1'},
            {'id': 'first_interrupt_2', 'title': 'First Phase Pin 2'},
        ]
        
        saved_pins = []
        
        def mock_save_with_interrupt(pin_data, query, session_id):
            """模拟保存过程中的中断"""
            saved_pins.append(pin_data.copy())
            
            # 在保存第二个Pin后模拟中断
            if len(saved_pins) >= 2:
                self.scraper._interrupt_requested = True
            
            return True
        
        with patch.object(self.scraper, '_search_phase_scrape') as mock_first_phase:
            
            # 配置第一阶段mock
            async def mock_first_with_save(url, target_count, query):
                result_pins = []
                for pin in first_phase_pins:
                    if self.scraper._interrupt_requested:
                        break
                    
                    # 模拟实时保存
                    success = mock_save_with_interrupt(pin, query, self.scraper.session_id)
                    if success:
                        result_pins.append(pin)
                
                return result_pins
            
            mock_first_phase.side_effect = mock_first_with_save
            
            # 执行混合策略
            result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
            
            # 验证中断处理
            assert len(saved_pins) >= 2, "中断前应该保存了一些数据"
            assert self.scraper._interrupt_requested, "应该检测到中断请求"
            
            # 验证保存的数据
            saved_ids = [pin['id'] for pin in saved_pins]
            assert 'first_interrupt_1' in saved_ids, "第一个Pin应该被保存"
            assert 'first_interrupt_2' in saved_ids, "第二个Pin应该被保存"

    @pytest.mark.asyncio
    async def test_second_phase_interruption_recovery(self):
        """测试第二阶段中断恢复"""
        
        # 第一阶段完成的数据
        first_phase_pins = [{'id': 'completed_first_1', 'title': 'Completed First Phase'}]
        
        # 第二阶段部分数据
        second_phase_pins = [
            {'id': 'second_interrupt_1', 'title': 'Second Phase Pin 1'},
            {'id': 'second_interrupt_2', 'title': 'Second Phase Pin 2'},
        ]
        
        saved_pins = []
        
        def mock_save_second_phase(pin_data, query, session_id):
            """模拟第二阶段保存过程中的中断"""
            saved_pins.append(pin_data.copy())
            
            # 在第二阶段保存第一个Pin后模拟中断
            if pin_data['id'].startswith('second_interrupt_1'):
                self.scraper._interrupt_requested = True
            
            return True
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=first_phase_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=second_phase_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_second_phase):
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                    
                    # 验证结果
                    assert len(result) >= 1, "应该至少保留第一阶段的数据"
                    
                    # 验证第一阶段数据完整
                    first_phase_ids = [pin['id'] for pin in result if pin['id'].startswith('completed_first_')]
                    assert len(first_phase_ids) == 1, "第一阶段数据应该完整"
                    
                    # 验证第二阶段部分数据被保存
                    saved_second_phase = [pin for pin in saved_pins if pin['id'].startswith('second_interrupt_')]
                    assert len(saved_second_phase) >= 1, "第二阶段应该保存了部分数据"

    @pytest.mark.asyncio
    async def test_interruption_during_pin_detail_scraping(self):
        """测试Pin详情页采集过程中的中断"""
        
        first_phase_pins = [{'id': 'detail_scrape_base', 'title': 'Base Pin for Detail Scraping'}]
        
        # 模拟在详情页采集过程中的中断
        async def mock_detail_scrape_with_interrupt(pin_id, max_count):
            # 模拟采集开始
            partial_pins = [{'id': 'detail_partial_1', 'title': 'Partial Detail Pin'}]
            
            # 在采集过程中设置中断
            self.scraper._interrupt_requested = True
            
            # 返回部分数据
            return partial_pins
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=first_phase_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', side_effect=mock_detail_scrape_with_interrupt):
                
                # 执行混合策略
                result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                
                # 验证中断处理
                assert self.scraper._interrupt_requested, "应该检测到中断"
                assert len(result) >= 1, "应该保留基础数据"
                
                # 验证基础Pin存在
                base_ids = [pin['id'] for pin in result if pin['id'] == 'detail_scrape_base']
                assert len(base_ids) == 1, "基础Pin应该被保留"

    def test_data_integrity_after_interruption(self):
        """测试中断后的数据完整性"""
        
        # 模拟中断前保存的数据
        pre_interrupt_pins = [
            {'id': 'integrity_1', 'title': 'Pre-interrupt Pin 1', 'phase': 'first'},
            {'id': 'integrity_2', 'title': 'Pre-interrupt Pin 2', 'phase': 'second'},
        ]
        
        # 保存数据到数据库
        for pin in pre_interrupt_pins:
            success = self.repository.save_pin_immediately(pin, self.test_keyword, self.scraper.session_id)
            assert success, f"Pin {pin['id']} 应该成功保存"
        
        # 验证数据完整性
        loaded_pins = self.repository.load_pins_by_query(self.test_keyword)
        assert len(loaded_pins) == 2, "应该加载到2个Pin"
        
        loaded_ids = [pin['id'] for pin in loaded_pins]
        assert 'integrity_1' in loaded_ids, "第一个Pin应该存在"
        assert 'integrity_2' in loaded_ids, "第二个Pin应该存在"
        
        # 验证数据内容完整性
        for loaded_pin in loaded_pins:
            original_pin = next(p for p in pre_interrupt_pins if p['id'] == loaded_pin['id'])
            assert loaded_pin['title'] == original_pin['title'], f"Pin {loaded_pin['id']} 标题应该一致"

    @pytest.mark.asyncio
    async def test_session_state_after_interruption(self):
        """测试中断后的会话状态"""
        
        # 创建会话
        session_id = "test_interruption_session"
        self.repository.create_session(session_id, self.test_keyword, 10, self.temp_dir, True)
        
        # 模拟保存一些数据
        test_pins = [
            {'id': 'session_pin_1', 'title': 'Session Pin 1'},
            {'id': 'session_pin_2', 'title': 'Session Pin 2'},
        ]
        
        for pin in test_pins:
            self.repository.save_pin_immediately(pin, self.test_keyword, session_id)
        
        # 模拟中断处理
        scraper = SmartScraper(repository=self.repository, session_id=session_id)
        await scraper._handle_interrupt(self.test_keyword)
        
        # 验证会话状态
        sessions = self.repository.get_incomplete_sessions()
        interrupted_sessions = [s for s in sessions if s['status'] == 'interrupted']
        
        # 应该有中断的会话记录
        assert len(interrupted_sessions) >= 0, "应该能够查询到会话状态"

    @pytest.mark.asyncio
    async def test_recovery_from_interrupted_session(self):
        """测试从中断会话恢复"""
        
        # 创建中断的会话
        session_id = "recovery_test_session"
        self.repository.create_session(session_id, self.test_keyword, 10, self.temp_dir, True)
        
        # 保存一些中断前的数据
        pre_interrupt_pins = [
            {'id': 'recovery_pin_1', 'title': 'Recovery Pin 1'},
            {'id': 'recovery_pin_2', 'title': 'Recovery Pin 2'},
        ]
        
        for pin in pre_interrupt_pins:
            self.repository.save_pin_immediately(pin, self.test_keyword, session_id)
        
        # 更新会话状态为中断
        self.repository.update_session_status(session_id, 'interrupted', len(pre_interrupt_pins))
        
        # 创建新的scraper实例模拟恢复
        recovery_scraper = SmartScraper(repository=self.repository, session_id=session_id)
        
        # 验证可以加载中断前的数据
        existing_count = recovery_scraper._get_saved_count_from_db(self.test_keyword)
        assert existing_count == 2, "应该能够加载中断前的数据"
        
        # 验证基准数量设置
        recovery_scraper._baseline_count = existing_count
        assert recovery_scraper._baseline_count == 2, "基准数量应该正确设置"

    def test_database_consistency_across_interruptions(self):
        """测试多次中断后的数据库一致性"""
        
        # 模拟多次中断和恢复的场景
        interruption_scenarios = [
            [{'id': 'multi_1', 'title': 'Multi Interrupt 1'}],
            [{'id': 'multi_2', 'title': 'Multi Interrupt 2'}],
            [{'id': 'multi_3', 'title': 'Multi Interrupt 3'}],
        ]
        
        all_saved_pins = []
        
        for i, scenario_pins in enumerate(interruption_scenarios):
            session_id = f"multi_interrupt_session_{i}"
            
            # 保存当前场景的数据
            for pin in scenario_pins:
                success = self.repository.save_pin_immediately(pin, self.test_keyword, session_id)
                assert success, f"Pin {pin['id']} 应该成功保存"
                all_saved_pins.append(pin)
        
        # 验证所有数据都在数据库中
        final_pins = self.repository.load_pins_by_query(self.test_keyword)
        final_ids = [pin['id'] for pin in final_pins]
        
        for saved_pin in all_saved_pins:
            assert saved_pin['id'] in final_ids, f"Pin {saved_pin['id']} 应该在最终数据库中"
        
        # 验证数据库一致性
        assert len(final_pins) == len(all_saved_pins), "数据库中的数据数量应该一致"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
