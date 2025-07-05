#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest混合动力爬虫第一阶段测试脚本

测试get_auth_credentials函数是否能成功捕获Pinterest的认证凭证。
这是一个集成测试，因为它会实际启动浏览器并访问Pinterest。
"""

import os
import sys
import unittest
import time
import shutil

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from network_analysis.hybrid_scraper import get_auth_credentials
from loguru import logger

# 配置loguru，只显示INFO级别及以上，避免测试输出过于冗长
logger.remove()
logger.add(sys.stdout, level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")

class TestGetAuthCredentials(unittest.TestCase):
    """测试get_auth_credentials函数"""

    def test_get_auth_credentials_success(self):
        """
        端到端测试：验证get_auth_credentials能否成功获取凭证。
        此测试将实际启动浏览器并访问Pinterest。
        """
        logger.info("开始测试：get_auth_credentials能否成功获取凭证")
        
        # 使用一个真实的Pinterest搜索URL
        test_url = "https://www.pinterest.com/search/pins/?q=nature"
        
        credentials = get_auth_credentials(test_url)
        
        logger.info("完成测试：get_auth_credentials获取凭证")

        # 验证凭证是否被成功获取
        self.assertIsNotNone(credentials, "凭证字典不应为空")
        self.assertIn("headers", credentials, "凭证中应包含headers")
        self.assertIn("cookies", credentials, "凭证中应包含cookies")
        
        # 验证是否获取到X-CSRFToken (通常是POST请求中携带) 或至少有有效的Cookie
        has_csrf_token = "X-CSRFToken" in credentials["headers"] and credentials["headers"]["X-CSRFToken"]
        has_cookies = len(credentials["cookies"]) > 0

        self.assertTrue(has_csrf_token or has_cookies, "应至少获取到X-CSRFToken或有效的Cookie")

        if has_csrf_token:
            logger.info(f"成功获取X-CSRFToken: {credentials['headers']['X-CSRFToken']}")
        if has_cookies:
            logger.info(f"成功获取Cookie数量: {len(credentials['cookies'])}")

        # 验证是否存在User-Agent和Accept-Language
        self.assertIn("User-Agent", credentials["headers"], "headers中应包含User-Agent")
        self.assertIn("Accept-Language", credentials["headers"], "headers中应包含Accept-Language")
        self.assertIn("X-Requested-With", credentials["headers"], "headers中应包含X-Requested-With")
        
        logger.info("get_auth_credentials测试通过！")

if __name__ == "__main__":
    unittest.main() 