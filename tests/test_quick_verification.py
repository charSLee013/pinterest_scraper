#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
快速验证测试

快速验证进度条逻辑修复的核心功能
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.core.smart_scraper import SmartScraper
from src.core.database.repository import SQLiteRepository


class TestQuickVerification:
    """快速验证测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_keyword = "quick_verification"
        
        # 创建测试用的Repository
        self.repository = SQLiteRepository(
            keyword=self.test_keyword,
            output_dir=self.temp_dir
        )
        
        # 创建SmartScraper实例
        self.scraper = SmartScraper(
            repository=self.repository,
            session_id="test_session_quick"
        )

    def teardown_method(self):
        """测试后清理"""
        if os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
            except:
                pass

    @pytest.mark.asyncio
    async def test_progress_bar_fix_verification(self):
        """🔥 快速验证进度条修复效果"""
        
        print("\n🔍 快速验证进度条修复效果...")
        
        # 模拟数据
        base_pins = [{'id': 'quick_base', 'title': 'Quick Base'}]
        related_pins = [
            {'id': 'quick_1', 'title': 'Quick 1'},  # 成功
            {'id': 'quick_2', 'title': 'Quick 2'},  # 失败
            {'id': 'quick_3', 'title': 'Quick 3'},  # 成功
        ]
        
        # 模拟保存结果
        save_results = {'quick_1': True, 'quick_2': False, 'quick_3': True}
        progress_updates = 0
        
        def mock_save_quick(pin_data, query, session_id):
            nonlocal progress_updates
            pin_id = pin_data.get('id')
            result = save_results.get(pin_id, True)
            
            if result:
                progress_updates += 1
                print(f"  ✅ 保存成功: {pin_id} (进度条应该更新)")
            else:
                print(f"  ❌ 保存失败: {pin_id} (进度条不应该更新)")
            
            return result
        
        with patch.object(self.scraper, '_search_phase_scrape', return_value=base_pins):
            with patch.object(self.scraper, '_scrape_pin_detail_with_queue', return_value=related_pins):
                with patch.object(self.repository, 'save_pin_immediately', side_effect=mock_save_quick):
                    
                    # 重置统计
                    self.scraper.stats["pins_saved_realtime"] = 0
                    
                    # 执行混合策略
                    result = await self.scraper._hybrid_scrape(self.test_keyword, 4)
                    
                    # 验证结果
                    assert len(result) == 4, "应该返回4个Pin"
                    
                    # 验证进度条统计
                    saved_count = self.scraper.stats.get("pins_saved_realtime", 0)
                    expected_saves = 2  # quick_1 和 quick_3 成功
                    
                    print(f"  📊 验证结果:")
                    print(f"    - 尝试保存: 3个Pin")
                    print(f"    - 成功保存: {saved_count}个Pin")
                    print(f"    - 预期成功: {expected_saves}个Pin")
                    
                    assert saved_count == expected_saves, f"进度条统计应该是{expected_saves}，实际是{saved_count}"
                    
                    print(f"  🎉 修复验证成功：进度条只在保存成功时更新！")

    def test_code_changes_verification(self):
        """🔥 验证代码修改是否正确应用"""
        
        print("\n🔍 验证代码修改是否正确应用...")
        
        # 读取修复后的代码文件
        smart_scraper_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'core', 'smart_scraper.py')
        
        with open(smart_scraper_path, 'r', encoding='utf-8') as f:
            code_content = f.read()
        
        # 验证关键修改
        checks = [
            ('pbar.update(1)', '进度条立即更新代码'),
            ('saved_pins_count', '保存成功计数变量'),
            ('💾 第二阶段实时保存Pin', '第二阶段保存日志'),
            ('⚠️  第二阶段保存失败', '第二阶段失败日志'),
            ('本轮保存', '进度条后缀信息'),
            ('实际保存统计', '最终统计信息'),
        ]
        
        for check_text, description in checks:
            assert check_text in code_content, f"应该包含{description}: {check_text}"
            print(f"  ✅ {description}: 已正确应用")
        
        print(f"  🎉 代码修改验证成功：所有关键修改都已正确应用！")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
