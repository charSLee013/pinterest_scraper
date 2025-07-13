#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫核心功能测试 - 激进重构版
"""

import pytest
import os
import sys
import tempfile
import shutil
from unittest.mock import Mock, patch

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.pinterest_scraper import PinterestScraper


class TestPinterestScraper:
    """PinterestScraper测试类 - 统一API测试"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        self.scraper = PinterestScraper(
            output_dir=self.temp_dir,
            download_images=False,
            debug=False
        )
    
    def teardown_method(self):
        """测试后清理"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_scraper_initialization(self):
        """测试爬虫初始化"""
        assert self.scraper.output_dir == self.temp_dir
        assert self.scraper.download_images == False
        assert self.scraper.debug == False
        assert os.path.exists(self.temp_dir)
        # 验证SmartScraper实例的初始状态
        smart_scraper = self.scraper.scraper
        assert len(smart_scraper.collected_pins) == 0
        assert len(smart_scraper.seen_pin_ids) == 0

    def test_scraper_with_custom_config(self):
        """测试自定义配置"""
        custom_scraper = PinterestScraper(
            output_dir=self.temp_dir,
            proxy="http://test:test@proxy:8080",
            download_images=True,
            debug=True
        )

        assert custom_scraper.proxy == "http://test:test@proxy:8080"
        assert custom_scraper.download_images == True
        assert custom_scraper.debug == True

    def test_unified_hybrid_strategy(self):
        """测试统一的hybrid策略"""
        # 验证所有数据量级都使用hybrid策略
        scraper = self.scraper.scraper  # 获取SmartScraper实例

        # 测试hybrid策略方法存在
        assert hasattr(scraper, '_hybrid_scrape')

        # 测试过时的策略选择方法已被移除
        assert not hasattr(scraper, '_select_strategy')
        assert not hasattr(scraper, '_execute_strategy')
        assert not hasattr(scraper, '_simple_scrape')
        assert not hasattr(scraper, '_enhanced_scrape')

    def test_scrape_parameter_validation(self):
        """测试scrape方法参数验证"""
        # 测试缺少参数 - 应该返回空列表
        result = self.scraper.scrape()
        assert result == []

        # 测试正常参数
        # 注意：这里不会实际执行网络请求，只是测试参数处理
        # 实际的网络测试应该在集成测试中进行

    def test_deduplicate_pins(self):
        """测试Pin去重功能"""
        smart_scraper = self.scraper.scraper

        # 测试去重功能
        pins = [
            {"id": "pin1", "title": "Test Pin 1"},
            {"id": "pin2", "title": "Test Pin 2"},
            {"id": "pin1", "title": "Duplicate Pin"},  # 重复
            {"id": "pin3", "title": "Test Pin 3"},
            {"title": "No ID Pin"}  # 无ID
        ]

        unique_pins = smart_scraper._deduplicate_pins(pins)

        # 验证去重结果
        assert len(unique_pins) == 3  # pin1, pin2, pin3
        pin_ids = [pin.get('id') for pin in unique_pins]
        assert "pin1" in pin_ids
        assert "pin2" in pin_ids
        assert "pin3" in pin_ids

    def test_build_url(self):
        """测试URL构建"""
        smart_scraper = self.scraper.scraper

        # 测试查询URL
        query_url = smart_scraper._build_url("nature photography", None)
        assert "nature" in query_url
        assert "photography" in query_url
        assert "pinterest.com/search" in query_url

        # 测试直接URL
        direct_url = "https://www.pinterest.com/test/"
        result_url = smart_scraper._build_url(None, direct_url)
        assert result_url == direct_url

        # 测试无效参数
        result_none = smart_scraper._build_url(None, None)
        assert result_none is None

class TestIntegration:
    """集成测试类 - 简化版"""

    def test_scraper_basic_functionality(self):
        """测试爬虫基本功能"""
        temp_dir = tempfile.mkdtemp()

        try:
            scraper = PinterestScraper(output_dir=temp_dir, download_images=False)
            assert scraper is not None
            assert scraper.output_dir == temp_dir

        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
