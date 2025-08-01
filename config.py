#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest 爬虫配置
"""

# Pinterest相关配置
PINTEREST_BASE_URL = "https://www.pinterest.com"
PINTEREST_SEARCH_URL = "https://www.pinterest.com/search/pins/?q={query}"
PINTEREST_PIN_SELECTORS = [
    # 旧版Pinterest选择器
    "[data-test-id='pin']",
    "[data-test-id='pinWrapper']",
    # 新版Pinterest选择器
    "div[data-test-id='pin-card']",
    "div[role='listitem']",
    # 通用备用选择器
    ".Grid__Item",
    ".Collection-Item",
]

# 图片尺寸配置
IMAGE_SIZES = [136, 170, 236, 474, 564, 736, 1200]
ORIGINAL_SIZE_MARKER = "originals"

# 等待和重试设置
SCROLL_PAUSE_TIME = 1.0  # 滚动暂停时间(秒)
DEFAULT_TIMEOUT = 30  # 默认超时时间(秒)
MAX_RETRIES = 3  # 最大重试次数
RETRY_DELAY = 2.0  # 重试延迟(秒)
MAX_SCROLL_ATTEMPTS = 5000  # 最大滚动尝试次数
# 并发配置
DEFAULT_THREAD_COUNT = 16  # 默认下载线程数
MAX_THREAD_COUNT = 32  # 最大下载线程数

# Chrome驱动配置
CHROME_OPTIONS = [
    "--headless=False",  # 注释的话可以看到页面来实时debug卡点在哪
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-infobars",
]

# 日志配置
DEBUG_ENABLED = True
DEBUG_HTTP_ENABLED = False  # 是否记录HTTP请求细节
LOG_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
BLOCKED_RESOURCE_TYPES = ["image", "font"]

# Cookie配置
COOKIE_FILE_PATH = "cookies.json"
