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
        assert len(self.scraper.collected_pins) == 0
        assert len(self.scraper.pin_ids_seen) == 0

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

    def test_strategy_selection(self):
        """测试智能策略选择"""
        assert self.scraper._select_strategy(50) == "basic"
        assert self.scraper._select_strategy(500) == "enhanced"
        assert self.scraper._select_strategy(2000) == "hybrid"

    def test_scrape_parameter_validation(self):
        """测试scrape方法参数验证"""
        # 测试缺少参数
        with pytest.raises(ValueError, match="必须提供query或url参数"):
            self.scraper.scrape()

        # 测试同时提供两个参数
        with pytest.raises(ValueError, match="query和url参数不能同时提供"):
            self.scraper.scrape(query="test", url="http://test.com")

    def test_add_pin_if_new(self):
        """测试Pin去重功能"""
        # 添加第一个Pin
        pin1 = {"id": "pin1", "title": "Test Pin 1"}
        result1 = self.scraper._add_pin_if_new(pin1)

        assert result1 == True
        assert len(self.scraper.collected_pins) == 1
        assert "pin1" in self.scraper.pin_ids_seen

        # 尝试添加重复Pin
        pin1_duplicate = {"id": "pin1", "title": "Duplicate Pin"}
        result2 = self.scraper._add_pin_if_new(pin1_duplicate)

        assert result2 == False
        assert len(self.scraper.collected_pins) == 1  # 数量不变

        # 添加新Pin
        pin2 = {"id": "pin2", "title": "Test Pin 2"}
        result3 = self.scraper._add_pin_if_new(pin2)

        assert result3 == True
        assert len(self.scraper.collected_pins) == 2
        assert "pin2" in self.scraper.pin_ids_seen

    def test_build_url(self):
        """测试URL构建"""
        # 测试查询URL
        query_url = self.scraper._build_url("nature photography", None)
        assert "nature+photography" in query_url
        assert "pinterest.com/search" in query_url

        # 测试直接URL
        direct_url = "https://www.pinterest.com/test/"
        result_url = self.scraper._build_url(None, direct_url)
        assert result_url == direct_url

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
