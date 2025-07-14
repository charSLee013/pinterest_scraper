#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
第二阶段实时存储测试

验证Pin详情页深度扩展阶段的实时数据库存储机制是否正常工作
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
from src.core.database.manager_factory import DatabaseManagerFactory


class TestSecondPhaseRealtimeStorage:
    """第二阶段实时存储测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_keyword = "test_second_phase"
        
        # 创建测试用的Repository
        self.repository = SQLiteRepository(
            keyword=self.test_keyword,
            output_dir=self.temp_dir
        )
        
        # 创建SmartScraper实例
        self.scraper = SmartScraper(
            repository=self.repository,
            session_id="test_session_second_phase"
        )

    def teardown_method(self):
        """测试后清理"""
        # 确保数据库连接关闭
        if hasattr(self, 'repository') and self.repository:
            try:
                # 关闭数据库连接
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
                # Windows上数据库文件可能被锁定，稍后重试
                import time
                time.sleep(0.5)
                try:
                    shutil.rmtree(self.temp_dir)
                except:
                    pass  # 忽略清理失败

    def test_second_phase_realtime_storage_enabled(self):
        """测试第二阶段实时存储机制是否启用"""
        # 验证SmartScraper配置了repository
        assert self.scraper.repository is not None
        assert self.scraper.session_id is not None
        
        # 验证repository可以正常保存数据
        test_pin = {
            'id': 'test_pin_second_phase',
            'title': 'Test Pin for Second Phase',
            'description': 'Testing second phase realtime storage'
        }
        
        success = self.repository.save_pin_immediately(
            test_pin, self.test_keyword, self.scraper.session_id
        )
        assert success, "Repository应该能够成功保存Pin数据"

    @pytest.mark.asyncio
    async def test_hybrid_scrape_second_phase_storage(self):
        """测试混合策略第二阶段的数据存储"""
        
        # Mock第一阶段返回一些基础Pin
        mock_base_pins = [
            {'id': 'base_pin_1', 'title': 'Base Pin 1'},
            {'id': 'base_pin_2', 'title': 'Base Pin 2'}
        ]
        
        # Mock第二阶段返回相关Pin
        mock_related_pins = [
            {'id': 'related_pin_1', 'title': 'Related Pin 1'},
            {'id': 'related_pin_2', 'title': 'Related Pin 2'}
        ]
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=mock_base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=mock_related_pins):
                with patch.object(self.repository, 'save_pin_immediately', return_value=True) as mock_save:
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                    
                    # 验证结果
                    assert len(result) == 4, "应该返回4个Pin"
                    
                    # 验证第二阶段的Pin被保存到数据库
                    # 第二阶段应该调用save_pin_immediately保存related_pins
                    save_calls = mock_save.call_args_list
                    
                    # 检查是否有第二阶段的Pin被保存
                    second_phase_saves = [
                        call for call in save_calls 
                        if call[0][0]['id'] in ['related_pin_1', 'related_pin_2']
                    ]
                    
                    assert len(second_phase_saves) > 0, "第二阶段的Pin应该被实时保存到数据库"

    @pytest.mark.asyncio
    async def test_second_phase_interruption_recovery(self):
        """测试第二阶段中断恢复机制"""
        
        # 模拟中断情况
        self.scraper._interrupt_requested = True
        
        # Mock数据
        mock_base_pins = [{'id': 'base_pin_1', 'title': 'Base Pin 1'}]
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=mock_base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue') as mock_detail_scrape:
                
                # 配置mock在第二阶段检测到中断
                async def mock_detail_with_interrupt(pin_id, max_count):
                    # 模拟在详情页采集过程中检测到中断
                    if self.scraper._interrupt_requested:
                        return []  # 中断时返回空列表
                    return [{'id': 'related_pin_1', 'title': 'Related Pin 1'}]
                
                mock_detail_scrape.side_effect = mock_detail_with_interrupt
                
                # 执行混合策略
                result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                
                # 验证中断处理
                assert len(result) == 1, "中断时应该只返回第一阶段的数据"
                assert result[0]['id'] == 'base_pin_1', "应该保留第一阶段的数据"

    def test_second_phase_data_consistency(self):
        """测试第二阶段数据一致性"""
        
        # 创建测试数据
        test_pins = [
            {
                'id': 'consistency_pin_1',
                'title': 'Consistency Test Pin 1',
                'description': 'Testing data consistency'
            },
            {
                'id': 'consistency_pin_2', 
                'title': 'Consistency Test Pin 2',
                'description': 'Testing data consistency'
            }
        ]
        
        # 保存测试数据
        for pin in test_pins:
            success = self.repository.save_pin_immediately(
                pin, self.test_keyword, self.scraper.session_id
            )
            assert success, f"Pin {pin['id']} 应该成功保存"
        
        # 验证数据可以正确加载
        loaded_pins = self.repository.load_pins_by_query(self.test_keyword)
        assert len(loaded_pins) == 2, "应该加载到2个Pin"
        
        loaded_ids = [pin['id'] for pin in loaded_pins]
        assert 'consistency_pin_1' in loaded_ids, "应该包含第一个测试Pin"
        assert 'consistency_pin_2' in loaded_ids, "应该包含第二个测试Pin"

    @pytest.mark.asyncio
    async def test_second_phase_performance_impact(self):
        """测试第二阶段实时存储对性能的影响"""
        import time
        
        # 创建大量测试数据
        large_pin_set = [
            {'id': f'perf_pin_{i}', 'title': f'Performance Test Pin {i}'}
            for i in range(50)
        ]
        
        # 测试批量保存性能
        start_time = time.time()
        
        for pin in large_pin_set:
            success = self.repository.save_pin_immediately(
                pin, self.test_keyword, self.scraper.session_id
            )
            assert success, f"Pin {pin['id']} 应该成功保存"
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 验证性能指标（每个Pin保存时间应该小于0.1秒）
        avg_time_per_pin = duration / len(large_pin_set)
        assert avg_time_per_pin < 0.1, f"每个Pin保存时间过长: {avg_time_per_pin:.3f}秒"
        
        print(f"性能测试结果: 50个Pin保存耗时 {duration:.3f}秒, 平均每个Pin {avg_time_per_pin:.3f}秒")

    def test_second_phase_error_handling(self):
        """测试第二阶段错误处理机制"""
        
        # 测试无效Pin数据的处理
        invalid_pins = [
            {},  # 空Pin
            {'title': 'No ID Pin'},  # 缺少ID的Pin
            {'id': '', 'title': 'Empty ID Pin'},  # 空ID的Pin
        ]
        
        for pin in invalid_pins:
            # 无效Pin应该被优雅处理，不应该抛出异常
            try:
                result = self.repository.save_pin_immediately(
                    pin, self.test_keyword, self.scraper.session_id
                )
                # 无效Pin应该返回False或被跳过
                if pin.get('id'):
                    assert result in [True, False], "应该返回布尔值"
                else:
                    assert result == True, "空Pin应该被跳过并返回True"
            except Exception as e:
                pytest.fail(f"处理无效Pin时不应该抛出异常: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
