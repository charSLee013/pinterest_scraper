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
INITIAL_WAIT_TIME = 3.0  # 初始页面加载等待时间(秒)
DEFAULT_TIMEOUT = 60  # 默认超时时间(秒) - 优化为60秒
MAX_RETRIES = 3  # 最大重试次数
RETRY_DELAY = 2.0  # 重试延迟(秒)
MAX_SCROLL_ATTEMPTS = 5000  # 最大滚动尝试次数
# 并发配置
DEFAULT_THREAD_COUNT = 16  # 默认下载线程数
MAX_THREAD_COUNT = 32  # 最大下载线程数

# 浏览器启动配置（用于Patchright）
BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-infobars",
    "--disable-web-security",
    "--disable-features=VizDisplayCompositor",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-field-trial-config",
    "--disable-ipc-flooding-protection",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-default-apps",
    "--disable-component-update",
    "--disable-sync",
    "--disable-translate",
    "--disable-background-networking",
    "--disable-background-downloads",
    "--disable-add-to-shelf",
    "--disable-client-side-phishing-detection",
    "--disable-datasaver-prompt",
    "--disable-domain-reliability",
    "--disable-features=TranslateUI",
    "--disable-hang-monitor",
    "--disable-prompt-on-repost",
    "--disable-web-resources",
    "--metrics-recording-only",
    "--no-crash-upload",
    "--enable-automation",
    "--password-store=basic",
    "--use-mock-keychain"
]

# 日志配置
DEBUG_ENABLED = True
DEBUG_HTTP_ENABLED = False  # 是否记录HTTP请求细节
LOG_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
BLOCKED_RESOURCE_TYPES = ["image", "font"]

# Cookie配置
COOKIE_FILE_PATH = "cookies.json"

# 网络分析配置
NETWORK_ANALYSIS = {
    'enabled': True,
    'output_directory': 'network_analysis_results',
    'request_timeout': 30,
    'max_concurrent_requests': 10,
    'api_endpoints_to_monitor': [
        'api.pinterest.com',
        'www.pinterest.com/resource/',
        'pinterest.com/resource/'
    ],
    'response_size_limit': 10 * 1024 * 1024,  # 10MB
    'save_response_bodies': True,
    'log_level': 'INFO'
}
