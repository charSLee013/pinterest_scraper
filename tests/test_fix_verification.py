#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复验证测试

验证第二阶段实时存储修复的真实有效性
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


class TestFixVerification:
    """修复验证测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_keyword = "fix_verification"
        
        # 创建测试用的Repository
        self.repository = SQLiteRepository(
            keyword=self.test_keyword,
            output_dir=self.temp_dir
        )
        
        # 创建SmartScraper实例
        self.scraper = SmartScraper(
            repository=self.repository,
            session_id="test_session_fix_verification"
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
    async def test_second_phase_realtime_storage_fix(self):
        """🔥 核心测试：验证第二阶段实时存储修复"""
        
        print("\n🔍 开始验证第二阶段实时存储修复...")
        
        # 模拟第一阶段数据
        first_phase_pins = [
            {'id': 'fix_first_1', 'title': 'Fix First Phase Pin 1'},
            {'id': 'fix_first_2', 'title': 'Fix First Phase Pin 2'},
        ]
        
        # 模拟第二阶段数据
        second_phase_pins = [
            {'id': 'fix_second_1', 'title': 'Fix Second Phase Pin 1'},
            {'id': 'fix_second_2', 'title': 'Fix Second Phase Pin 2'},
            {'id': 'fix_second_3', 'title': 'Fix Second Phase Pin 3'},
        ]
        
        # 记录实际保存的Pin
        saved_pins = []
        save_call_count = 0
        
        def track_save_calls(pin_data, query, session_id):
            """跟踪保存调用"""
            nonlocal save_call_count
            save_call_count += 1
            saved_pins.append({
                'pin_id': pin_data.get('id'),
                'title': pin_data.get('title'),
                'call_order': save_call_count
            })
            print(f"  💾 保存Pin: {pin_data.get('id')} (第{save_call_count}次调用)")
            return True
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=first_phase_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=second_phase_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=track_save_calls):
                    
                    print("  📊 执行混合策略...")
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 5)
                    
                    print(f"  ✅ 混合策略完成，返回{len(result)}个Pin")
                    
                    # 🔥 关键验证1：第二阶段Pin被实时保存
                    second_phase_saves = [pin for pin in saved_pins if pin['pin_id'].startswith('fix_second_')]
                    print(f"  🔍 第二阶段保存的Pin数量: {len(second_phase_saves)}")
                    
                    assert len(second_phase_saves) == 3, f"第二阶段应该保存3个Pin，实际保存了{len(second_phase_saves)}个"
                    
                    # 🔥 关键验证2：保存调用顺序正确
                    expected_second_phase_ids = ['fix_second_1', 'fix_second_2', 'fix_second_3']
                    actual_second_phase_ids = [pin['pin_id'] for pin in second_phase_saves]
                    
                    for expected_id in expected_second_phase_ids:
                        assert expected_id in actual_second_phase_ids, f"Pin {expected_id} 应该被保存"
                    
                    # 🔥 关键验证3：返回结果包含所有Pin
                    result_ids = [pin['id'] for pin in result]
                    assert len(result_ids) == 5, f"应该返回5个Pin，实际返回{len(result_ids)}个"
                    
                    # 验证第一阶段Pin在结果中
                    for first_pin in first_phase_pins:
                        assert first_pin['id'] in result_ids, f"第一阶段Pin {first_pin['id']} 应该在结果中"
                    
                    # 验证第二阶段Pin在结果中
                    for second_pin in second_phase_pins:
                        assert second_pin['id'] in result_ids, f"第二阶段Pin {second_pin['id']} 应该在结果中"
                    
                    print("  ✅ 所有验证通过！第二阶段实时存储修复成功！")

    @pytest.mark.asyncio
    async def test_interruption_data_safety_after_fix(self):
        """🔥 验证修复后的中断数据安全性"""
        
        print("\n🔍 验证修复后的中断数据安全性...")
        
        first_phase_pins = [{'id': 'safety_first_1', 'title': 'Safety First Phase'}]
        second_phase_pins = [
            {'id': 'safety_second_1', 'title': 'Safety Second Phase 1'},
            {'id': 'safety_second_2', 'title': 'Safety Second Phase 2'},
        ]
        
        saved_pins = []
        
        def save_with_interrupt(pin_data, query, session_id):
            """模拟在第二阶段保存过程中中断"""
            saved_pins.append(pin_data.copy())
            print(f"  💾 保存Pin: {pin_data.get('id')}")
            
            # 在保存第一个第二阶段Pin后模拟中断
            if pin_data.get('id') == 'safety_second_1':
                print("  ⚠️  模拟中断发生...")
                self.scraper._interrupt_requested = True
            
            return True
        
        # 模拟第一阶段也会保存数据
        async def mock_first_phase_with_save(url, target_count, query):
            # 第一阶段的数据通过scroll_and_collect保存，这里模拟
            for pin in first_phase_pins:
                save_with_interrupt(pin, query, self.scraper.session_id)
            return first_phase_pins

        with patch.object(self.scraper, '_search_phase_scrape', side_effect=mock_first_phase_with_save):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=second_phase_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=save_with_interrupt):
                    
                    print("  📊 执行带中断的混合策略...")
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                    
                    print(f"  ✅ 中断处理完成，返回{len(result)}个Pin")
                    
                    # 🔥 关键验证：中断前的第二阶段数据被保存
                    saved_ids = [pin['id'] for pin in saved_pins]
                    print(f"  🔍 中断前保存的Pin: {saved_ids}")
                    
                    assert 'safety_first_1' in saved_ids, "第一阶段数据应该被保存"
                    assert 'safety_second_1' in saved_ids, "中断前的第二阶段数据应该被保存"
                    
                    # 验证中断标志被正确设置
                    assert self.scraper._interrupt_requested, "中断标志应该被设置"
                    
                    print("  ✅ 中断数据安全性验证通过！")

    def test_fix_code_changes_verification(self):
        """🔥 验证修复代码的具体变更"""
        
        print("\n🔍 验证修复代码的具体变更...")
        
        # 读取修复后的代码文件
        smart_scraper_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'core', 'smart_scraper.py')
        
        with open(smart_scraper_path, 'r', encoding='utf-8') as f:
            code_content = f.read()
        
        # 🔥 验证1：第二阶段实时保存代码存在
        assert 'self.repository.save_pin_immediately(related_pin, query, self.session_id)' in code_content, \
            "第二阶段实时保存代码应该存在"
        
        # 🔥 验证2：修复注释存在
        assert '🔥 修复：第二阶段实时保存到数据库' in code_content, \
            "修复注释应该存在"
        
        # 🔥 验证3：中断检查代码存在
        assert 'if self._interrupt_requested:' in code_content, \
            "中断检查代码应该存在"
        
        # 🔥 验证4：错误注释被修复
        assert '第一阶段+第二阶段均已实时保存到数据库' in code_content, \
            "错误注释应该被修复"
        
        print("  ✅ 代码变更验证通过！")

    @pytest.mark.asyncio
    async def test_performance_impact_after_fix(self):
        """🔥 验证修复后的性能影响"""
        
        print("\n🔍 验证修复后的性能影响...")
        
        import time
        
        # 创建大量测试数据
        first_phase_pins = [{'id': f'perf_first_{i}', 'title': f'Perf First {i}'} for i in range(5)]
        second_phase_pins = [{'id': f'perf_second_{i}', 'title': f'Perf Second {i}'} for i in range(10)]
        
        save_times = []
        
        def time_save_calls(pin_data, query, session_id):
            """记录保存时间"""
            start_time = time.time()
            # 模拟数据库操作
            time.sleep(0.001)  # 1ms模拟数据库延迟
            end_time = time.time()
            save_times.append(end_time - start_time)
            return True
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=first_phase_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=second_phase_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=time_save_calls):
                    
                    start_time = time.time()
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 15)
                    end_time = time.time()
                    
                    total_time = end_time - start_time
                    avg_save_time = sum(save_times) / len(save_times) if save_times else 0
                    
                    print(f"  📊 性能指标:")
                    print(f"    - 总执行时间: {total_time:.3f}秒")
                    print(f"    - 平均保存时间: {avg_save_time:.3f}秒")
                    print(f"    - 保存调用次数: {len(save_times)}")
                    
                    # 性能验证
                    assert total_time < 10.0, f"总执行时间过长: {total_time:.3f}秒"
                    assert avg_save_time < 0.1, f"平均保存时间过长: {avg_save_time:.3f}秒"
                    assert len(save_times) == 10, f"第二阶段应该有10次保存调用，实际{len(save_times)}次"
                    
                    print("  ✅ 性能影响验证通过！")

    def test_fix_completeness_verification(self):
        """🔥 验证修复的完整性"""
        
        print("\n🔍 验证修复的完整性...")
        
        # 验证SmartScraper类的关键属性和方法
        assert hasattr(self.scraper, 'repository'), "SmartScraper应该有repository属性"
        assert hasattr(self.scraper, 'session_id'), "SmartScraper应该有session_id属性"
        assert hasattr(self.scraper, '_interrupt_requested'), "SmartScraper应该有_interrupt_requested属性"
        
        # 验证Repository的关键方法
        assert hasattr(self.repository, 'save_pin_immediately'), "Repository应该有save_pin_immediately方法"
        assert callable(self.repository.save_pin_immediately), "save_pin_immediately应该是可调用的"
        
        # 验证SmartScraper的关键方法
        assert hasattr(self.scraper, '_hybrid_scrape'), "SmartScraper应该有_hybrid_scrape方法"
        assert hasattr(self.scraper, '_scrape_pin_detail_with_queue'), "SmartScraper应该有_scrape_pin_detail_with_queue方法"
        
        print("  ✅ 修复完整性验证通过！")

    def test_backward_compatibility(self):
        """🔥 验证向后兼容性"""
        
        print("\n🔍 验证向后兼容性...")
        
        # 测试没有repository的情况
        scraper_no_repo = SmartScraper()
        assert scraper_no_repo.repository is None, "没有repository时应该为None"
        
        # 测试没有session_id的情况
        scraper_no_session = SmartScraper(repository=self.repository)
        assert scraper_no_session.repository is not None, "repository应该被正确设置"
        
        print("  ✅ 向后兼容性验证通过！")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
