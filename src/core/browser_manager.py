#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
统一浏览器管理器 - 激进重构版

消除重复的浏览器初始化代码，提供统一的浏览器管理接口
"""

import json
import os
import time
from typing import Dict, List, Optional, Callable, Any

from fake_useragent import UserAgent
from loguru import logger
from patchright.sync_api import sync_playwright, Page, BrowserContext, Error
from tqdm import tqdm

from . import config


class BrowserManager:
    """统一的浏览器管理器
    
    整合所有浏览器相关操作，消除重复代码
    """

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = config.DEFAULT_TIMEOUT,
        cookie_path: Optional[str] = None,
        headless: bool = True,
        enable_network_interception: bool = False
    ):
        """初始化浏览器管理器
        
        Args:
            proxy: 代理服务器地址
            timeout: 默认超时时间(秒)
            cookie_path: Cookie文件路径
            headless: 是否无头模式
            enable_network_interception: 是否启用网络拦截
        """
        self.proxy = proxy
        self.timeout = timeout
        self.cookie_path = cookie_path or config.COOKIE_FILE_PATH
        self.headless = headless
        self.enable_network_interception = enable_network_interception
        
        # 浏览器实例
        self.playwright_instance = None
        self.browser = None
        self.browser_context = None
        self.page = None
        
        # 网络拦截回调
        self.request_handlers = []
        self.response_handlers = []
        
        # 用户代理
        self.user_agent = UserAgent().random

    def start(self) -> bool:
        """启动浏览器
        
        Returns:
            启动是否成功
        """
        try:
            logger.info("启动浏览器管理器...")
            
            # 启动Playwright
            self.playwright_instance = sync_playwright().start()
            
            # 配置启动选项
            launch_options = {
                "headless": self.headless,
                "args": [
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-infobars",
                ]
            }
            
            # 代理配置
            if self.proxy:
                proxy_config = {"server": self.proxy}
                launch_options["proxy"] = proxy_config
                logger.info(f"使用代理: {self.proxy}")
            
            # 启动浏览器
            self.browser = self.playwright_instance.chromium.launch(**launch_options)
            
            # 创建浏览器上下文
            context_options = {
                "user_agent": self.user_agent,
                "viewport": {"width": 1920, "height": 1080},
                "java_script_enabled": True,
            }
            
            # 加载Cookie
            if os.path.exists(self.cookie_path):
                try:
                    with open(self.cookie_path, 'r') as f:
                        storage_state = json.load(f)
                    context_options["storage_state"] = storage_state
                    logger.info(f"成功加载Cookie: {self.cookie_path}")
                except Exception as e:
                    logger.warning(f"加载Cookie失败: {e}")
            
            self.browser_context = self.browser.new_context(**context_options)
            self.page = self.browser_context.new_page()
            
            # 阻止不必要的资源
            if hasattr(config, "BLOCKED_RESOURCE_TYPES"):
                self.page.route("**/*", lambda route: (
                    route.abort()
                    if route.request.resource_type in config.BLOCKED_RESOURCE_TYPES
                    else route.continue_()
                ))
            
            # 设置超时
            self.page.set_default_timeout(self.timeout * 1000)
            
            # 设置网络拦截
            if self.enable_network_interception:
                self.page.on("request", self._handle_request)
                self.page.on("response", self._handle_response)
            
            logger.info("浏览器管理器启动成功")
            return True
            
        except Exception as e:
            logger.error(f"浏览器启动失败: {e}")
            return False

    def stop(self):
        """关闭浏览器"""
        try:
            if self.page:
                self.browser_context.close()
                self.browser.close()
                self.playwright_instance.stop()
                logger.info("浏览器已关闭")
        except Exception as e:
            logger.error(f"关闭浏览器出错: {e}")
        finally:
            self.page = None
            self.browser_context = None
            self.browser = None
            self.playwright_instance = None

    def navigate(self, url: str) -> bool:
        """导航到指定URL
        
        Args:
            url: 目标URL
            
        Returns:
            导航是否成功
        """
        try:
            logger.info(f"导航到: {url}")
            self.page.goto(url, timeout=self.timeout * 1000, wait_until="domcontentloaded")
            return True
        except Error as e:
            logger.error(f"导航失败: {e}")
            return False

    def scroll_and_collect(
        self,
        target_count: int,
        extract_func: Callable[[str], List[Dict]],
        max_scrolls: int = 100,
        scroll_pause: float = 2.0,
        no_new_data_limit: int = 10
    ) -> List[Dict]:
        """智能滚动并收集数据

        Args:
            target_count: 目标数据数量
            extract_func: 数据提取函数
            max_scrolls: 最大滚动次数
            scroll_pause: 滚动间隔时间
            no_new_data_limit: 连续无新数据的限制次数

        Returns:
            收集到的数据列表
        """
        collected_data = []
        seen_ids = set()
        consecutive_no_new = 0
        scroll_count = 0

        logger.info(f"开始数据采集，目标: {target_count}")

        # 首次提取页面数据（滚动前）
        html = self.page.content()
        initial_items = extract_func(html)
        for item in initial_items:
            item_id = item.get('id')
            if item_id and item_id not in seen_ids:
                seen_ids.add(item_id)
                collected_data.append(item)
                if len(collected_data) >= target_count:
                    break

        # 创建进度条
        pbar = tqdm(total=target_count, desc="采集进度", unit="pins",
                   initial=len(collected_data), leave=False)

        # 退出条件：达到目标数量 OR 达到最大滚动次数 OR 连续无新数据
        while (len(collected_data) < target_count and
               scroll_count < max_scrolls and
               consecutive_no_new < no_new_data_limit):

            # 滚动页面
            scroll_count += 1
            self.page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(scroll_pause)

            # 等待页面加载
            try:
                self.page.wait_for_load_state('networkidle', timeout=5000)
            except:
                # 如果等待超时，继续执行
                pass

            # 提取当前页面数据
            html = self.page.content()
            new_items = extract_func(html)

            # 去重并添加新数据
            items_before = len(collected_data)
            for item in new_items:
                item_id = item.get('id')
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)
                    collected_data.append(item)

            items_after = len(collected_data)
            new_items_count = items_after - items_before

            if new_items_count > 0:
                consecutive_no_new = 0
                # 更新进度条
                pbar.update(new_items_count)
                pbar.set_postfix({"滚动": scroll_count, "连续无新": 0, "总数": len(collected_data)})
            else:
                consecutive_no_new += 1
                pbar.set_postfix({"滚动": scroll_count, "连续无新": consecutive_no_new, "总数": len(collected_data)})

            # 硬性目标检查：达到目标数量立即退出
            if len(collected_data) >= target_count:
                logger.info(f"已达到目标数量 {target_count}，立即退出")
                break

        # 关闭进度条
        pbar.close()

        # 记录停止原因
        if len(collected_data) >= target_count:
            stop_reason = "达到目标数量"
        elif scroll_count >= max_scrolls:
            stop_reason = f"达到最大滚动次数"
        elif consecutive_no_new >= no_new_data_limit:
            stop_reason = f"连续无新数据"
        else:
            stop_reason = "未知原因"

        logger.info(f"采集完成: {len(collected_data)} 个数据 ({stop_reason})")
        # 严格按照目标数量返回数据
        return collected_data[:target_count] if len(collected_data) > target_count else collected_data

    def add_request_handler(self, handler: Callable):
        """添加请求处理器"""
        self.request_handlers.append(handler)

    def add_response_handler(self, handler: Callable):
        """添加响应处理器"""
        self.response_handlers.append(handler)

    def _handle_request(self, request):
        """处理请求事件"""
        for handler in self.request_handlers:
            try:
                handler(request)
            except Exception as e:
                logger.error(f"请求处理器出错: {e}")

    def _handle_response(self, response):
        """处理响应事件"""
        for handler in self.response_handlers:
            try:
                handler(response)
            except Exception as e:
                logger.error(f"响应处理器出错: {e}")

    def wait_for_load_state(self, state: str = "networkidle", timeout: int = 30000):
        """等待页面加载状态"""
        try:
            self.page.wait_for_load_state(state, timeout=timeout)
        except Exception as e:
            logger.debug(f"等待加载状态超时: {e}")

    def get_html(self) -> str:
        """获取当前页面HTML"""
        return self.page.content()

    def is_ready(self) -> bool:
        """检查浏览器是否就绪"""
        return self.page is not None
