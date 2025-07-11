#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫的浏览器操作模块
"""

import json
import os
import random
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

from fake_useragent import UserAgent
from loguru import logger
from patchright.sync_api import sync_playwright, Page, BrowserContext, Error

from . import config


class Browser:
    """浏览器管理类，处理浏览器初始化、滚动和元素查找"""

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = config.DEFAULT_TIMEOUT,
        viewport_width: int = 1920,
        viewport_height: int = 1080,
        cookie_path: Optional[str] = None,
        headless: bool = False,
    ):
        """初始化浏览器

        Args:
            proxy: 代理服务器地址，例如 http://user:pass@host:port
            timeout: 默认超时时间(秒)
            viewport_width: 浏览器视口宽度
            viewport_height: 浏览器视口高度
            cookie_path: Cookie文件路径
            headless: 是否以无头模式启动浏览器
        """
        self.playwright_instance = None
        self.browser = None
        self.browser_context = None
        self.page = None
        self.proxy = proxy
        self.timeout = timeout
        self.user_agent = UserAgent().random # Directly initialize user_agent
        self.viewport_width = viewport_width
        self.viewport_height = viewport_height
        self.cookie_path = cookie_path
        self.monitoring_active = False
        self.monitoring_thread = None
        self.screenshot_dir = None
        self.browser_type = 'chromium'
        self.headless = headless

    def start_monitoring(self, session_id):
        """启动浏览器监控线程

        Args:
            session_id: 会话ID，用于创建监控文件夹
        """
        if (
            not hasattr(config, "ENABLE_LIVE_MONITORING")
            or not config.ENABLE_LIVE_MONITORING
        ):
            return

        # 创建监控文件夹
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.screenshot_dir = os.path.join(
            "output", str(session_id), "monitoring", timestamp
        )
        os.makedirs(self.screenshot_dir, exist_ok=True)

        # 启动监控线程
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop)
        self.monitoring_thread.daemon = True
        self.monitoring_thread.start()
        logger.info(f"浏览器监控已启动，截图保存在: {self.screenshot_dir}")

    def stop_monitoring(self):
        """停止浏览器监控"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=2)
            self.monitoring_thread = None
            logger.info("浏览器监控已停止")

    def _monitoring_loop(self):
        """监控线程主循环"""
        screenshot_interval = getattr(config, "SCREENSHOT_INTERVAL", 5)
        counter = 0

        while self.monitoring_active and self.page:
            try:
                # 截图
                screenshot_path = os.path.join(
                    self.screenshot_dir, f"screenshot_{counter:04d}.png"
                )
                self.page.screenshot(path=screenshot_path)

                # 保存当前页面HTML
                html_path = os.path.join(
                    self.screenshot_dir, f"page_{counter:04d}.html"
                )
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(self.page.content())

                # 记录浏览器状态
                scroll_position = self.page.evaluate(
                    "window.pageYOffset;"
                )
                page_height = self.get_page_height()
                status_info = {
                    "timestamp": datetime.now().isoformat(),
                    "scroll_position": scroll_position,
                    "page_height": page_height,
                    "viewport_height": self.viewport_height,
                    "is_page_end": scroll_position + self.viewport_height
                    >= page_height,
                    "url": self.page.url,
                }

                status_path = os.path.join(self.screenshot_dir, "browser_status.json")
                with open(status_path, "w", encoding="utf-8") as f:
                    json.dump(status_info, f, indent=2)

                logger.debug(
                    f"监控: 截图 #{counter}, 滚动位置: {scroll_position}/{page_height}"
                )
                counter += 1

                # 等待下一次截图
                time.sleep(screenshot_interval)
            except Exception as e:
                logger.error(f"监控线程异常: {e}")
                time.sleep(screenshot_interval)

    def start(self) -> bool:
        """启动浏览器

        Returns:
            启动是否成功
        """
        if self.page:
            # 浏览器已经启动
            return True

        try:
            logger.info("初始化浏览器...")
            # self.start_monitoring(session_id=1) # 暂时禁用监控以减少干扰

            self.playwright_instance = sync_playwright().start()
            launch_options = {
                "headless": self.headless,
                "args": config.CHROME_OPTIONS,
            }

            # 设置代理(如果有)
            if self.proxy:
                parsed_proxy = {}
                if self.proxy.startswith("http://") or self.proxy.startswith("https://"):
                    parsed_proxy["server"] = self.proxy
                else:
                    parsed_proxy["server"] = f"http://{self.proxy}"
                launch_options["proxy"] = parsed_proxy

            self.browser = self.playwright_instance.chromium.launch(**launch_options)

            # 设置浏览器上下文
            context_options = {
                "user_agent": self.user_agent,
                "viewport": {
                    "width": self.viewport_width,
                    "height": self.viewport_height
                },
                "base_url": "about:blank", # Prevent automatic navigation
                "java_script_enabled": True,
            }

            # 加载Cookie
            if self.cookie_path and os.path.exists(self.cookie_path):
                try:
                    with open(self.cookie_path, 'r') as f:
                        storage_state = json.load(f)
                    context_options["storage_state"] = storage_state
                    logger.info(f"成功从 {self.cookie_path} 加载Cookie")
                except Exception as e:
                    logger.error(f"从 {self.cookie_path} 加载Cookie失败: {e}")

            self.browser_context = self.browser.new_context(**context_options)
            self.page = self.browser_context.new_page()

            # 阻止不必要的资源加载
            if hasattr(config, "BLOCKED_RESOURCE_TYPES") and config.BLOCKED_RESOURCE_TYPES:
                logger.info(f"将阻止以下资源类型: {config.BLOCKED_RESOURCE_TYPES}")
                self.page.route("**/*", lambda route: (
                    route.abort()
                    if route.request.resource_type in config.BLOCKED_RESOURCE_TYPES
                    else route.continue_()
                ))
            
            # 设置默认超时
            self.page.set_default_timeout(self.timeout * 1000)

            logger.info("浏览器初始化成功")
            return True

        except Exception as e:
            logger.error(f"浏览器启动异常: {e}")
            return False


    def stop(self):
        """关闭浏览器"""
        # 停止监控
        self.stop_monitoring()

        if self.page:
            try:
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

    def get_url(self, url: str) -> bool:
        """访问URL

        Args:
            url: 要访问的网址

        Returns:
            访问是否成功
        """
        try:
            logger.info(f"访问URL: {url}")
            self.page.goto(url, timeout=self.timeout * 1000, wait_until="domcontentloaded")
            return True
        except Error as e:
            logger.error(f"访问 {url} 失败: {e}")
            return False

    def wait_for_load_state(self, state: str = 'domcontentloaded', timeout: int = 0):
        """等待页面加载状态

        Args:
            state: 等待的状态，可以是 'load', 'domcontentloaded', 'networkidle'
            timeout: 等待超时时间（毫秒）
        """
        if not self.page:
            return
        try:
            actual_timeout = timeout if timeout > 0 else self.timeout * 1000
            logger.info(f"等待页面状态: {state}, 超时: {actual_timeout}ms")
            self.page.wait_for_load_state(state, timeout=actual_timeout)
        except Error as e:
            logger.warning(f"等待页面状态 '{state}' 时发生超时或错误: {e}")

    def wait_for_element(self, selector: str, timeout: int = 10) -> bool:
        """等待元素出现

        Args:
            selector: CSS选择器
            timeout: 超时时间(秒)

        Returns:
            元素是否出现
        """
        if not self.page:
            return False

        try:
            self.page.wait_for_selector(selector, timeout=timeout * 1000)
            return True
        except Exception as e:
            logger.warning(f"等待元素 '{selector}' 超时: {e}")
            return False

    def find_elements(self, selector: str) -> List:
        """查找页面元素

        Args:
            selector: CSS选择器

        Returns:
            找到的元素列表
        """
        if not self.page:
            return []

        try:
            elements = self.page.locator(selector).all()
            return elements
        except Exception as e:
            logger.error(f"查找元素失败: {e}")
            return []

    def scroll_to_bottom(self):
        """滚动到页面底部"""
        if not self.page:
            return

        try:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        except Exception as e:
            logger.error(f"滚动到底部出错: {e}")

    def scroll_by(self, pixels: int):
        """向下滚动指定像素

        Args:
            pixels: 滚动的像素数
        """
        if not self.page:
            return

        try:
            self.page.evaluate(f"window.scrollBy(0, {pixels});")
        except Exception as e:
            logger.error(f"滚动出错: {e}")

    def get_page_height(self) -> int:
        """获取页面高度

        Returns:
            页面高度(像素)
        """
        if not self.page:
            return 0

        try:
            # 尝试获取document.documentElement.scrollHeight，更具兼容性
            page_height = self.page.evaluate("document.documentElement.scrollHeight")
            if page_height is None:
                # 如果document.documentElement.scrollHeight为None，尝试document.body.scrollHeight
                page_height = self.page.evaluate("document.body.scrollHeight")
            return page_height if page_height is not None else 0
        except Exception as e:
            logger.error(f"获取页面高度出错: {e}")
            return 0

    def take_screenshot(self, filepath: str) -> bool:
        """截图

        Args:
            filepath: 保存路径

        Returns:
            是否成功
        """
        if not self.page:
            return False

        try:
            self.page.screenshot(path=filepath)
            logger.debug(f"截图已保存: {filepath}")
            return True
        except Exception as e:
            logger.error(f"截图失败: {e}")
            return False

    def scroll_page_down(self):
        """模拟按下PageDown键进行滚动"""
        if not self.page:
            return
        try:
            self.page.evaluate('window.scrollBy(0, window.innerHeight);')
            logger.info("执行页面向下滚动 (PageDown)")
        except Exception as e:
            logger.error(f"执行PageDown滚动失败: {e}")

    def get_page_source(self) -> str:
        """获取页面源码

        Returns:
            页面HTML源码
        """
        if not self.page:
            return ""

        return self.page.content()

    def get_network_requests(self) -> List[Dict]:
        """获取页面网络请求数据

        Returns:
            网络请求数据列表
        """
        if not self.page or not config.DEBUG_HTTP_ENABLED:
            # Playwright network interception requires different approach, returning empty for now.
            # Refer to Playwright documentation for proper network logging implementation.
            logger.warning("get_network_requests 方法暂不支持，返回空列表。")
            return []

    def simple_scroll_and_extract(
        self, target_count: int, extract_func, new_item_selector: str, max_scroll_attempts: int = 5000
    ) -> List:
        """简化的滚动并提取数据函数

        这个函数使用更简单直接的滚动策略，边滚动边提取数据

        Args:
            target_count: 目标数量
            extract_func: 提取函数，接收页面源码并返回数据项列表
            new_item_selector: 新项目选择器
            max_scroll_attempts: 最大滚动尝试次数

        Returns:
            提取的数据列表
        """
        if not self.page:
            logger.error("浏览器未初始化")
            return []

        logger.info(f"开始滚动提取，目标数量: {target_count}, 视口高度: {self.viewport_height}")

        # 初始化数据收集
        results = []
        scroll_count = 0
        no_change_count = 0
        scroll_position = 0
        consecutive_no_new_data = 0
        last_height = 0
        stuck_count = 0  # 新增：记录页面高度停滞的次数
        max_stuck_count = 10  # 新增：最大停滞次数

        # 使用集合存储已处理的项目ID，避免重复
        processed_ids = set()
        recent_new_counts = [] # 新增: 记录最近几次滚动的新增数量

        # 优化滚动速度，使用更大的滚动步长
        base_scroll_step = int(self.viewport_height * 0.8)  # 增加到80%的视口高度
        min_scroll_step = int(self.viewport_height * 0.3)  # 最小滚动步长
        max_scroll_step = int(self.viewport_height * 1.2)  # 最大滚动步长

        while len(results) < target_count and scroll_count < max_scroll_attempts:
            scroll_count += 1
            current_height = self.get_page_height()

            # 输出调试信息
            logger.debug(
                f"滚动 #{scroll_count}, 当前高度: {current_height}px, 已收集: {len(results)}, "
                f"滚动位置: {scroll_position}/{current_height}, 停滞计数: {stuck_count}, 累计收集: {len(results)}/{target_count}"
            )

            # 提取当前页面上的数据
            page_source = self.get_page_source()
            new_items = extract_func(page_source)
            logger.debug(f"从当前页面提取到 {len(new_items)} 个原始项目")

            # 过滤并添加新项目
            new_added = 0
            new_items_count = len(new_items)
            
            for item in new_items:
                item_id = item.get("id", "")
                if item_id and item_id not in processed_ids:
                    processed_ids.add(item_id)
                    results.append(item)
                    new_added += 1

                    if len(results) >= target_count:
                        logger.info(f"已收集到足够数量: {len(results)}/{target_count}, 提前退出。")
                        break
            
            duplicate_count = new_items_count - new_added
            logger.info(f"滚动 #{scroll_count}: 新增 {new_added}，重复 {duplicate_count} | 累计 {len(results)} / 目标 {target_count}")

            # 更新最近新增数量列表
            recent_new_counts.append(new_added)
            if len(recent_new_counts) > 3:
                recent_new_counts.pop(0)

            # 检查是否有新的项目被添加
            if new_added == 0:
                consecutive_no_new_data += 1
                logger.debug(f"连续 {consecutive_no_new_data} 次滚动未获取新数据")
            else:
                consecutive_no_new_data = 0
                logger.debug("重置连续无新数据计数")

            # 检查页面高度是否停滞
            if current_height == last_height:
                stuck_count += 1
                if stuck_count >= max_stuck_count:
                    logger.warning(f"页面高度停滞 {stuck_count} 次，尝试特殊滚动策略")
                    # 尝试强制滚动到底部
                    self.page.evaluate(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    time.sleep(2)
                    # 回到当前位置
                    self.page.evaluate(
                        f"window.scrollTo(0, {scroll_position});"
                    )
                    time.sleep(1)
                    stuck_count = 0
            else:
                stuck_count = 0
                last_height = current_height

            # 新的终止逻辑
            if consecutive_no_new_data >= 3:
                logger.warning(
                    f"连续 {consecutive_no_new_data} 次未获取到新数据，根据规则停止滚动。"
                )
                break

            if scroll_count > 3 and len(recent_new_counts) == 3 and sum(recent_new_counts) < 5:
                logger.warning(
                    f"滚动次数超过3次，且最近3次新增数量 ({sum(recent_new_counts)}) 小于5，根据规则停止滚动。"
                )
                break

            # 执行常规滚动
            self.scroll_page_down()

            # 记录滚动位置
            scroll_position = self.page.evaluate("window.pageYOffset;")

            # 随机等待时间，模拟真实用户行为
            time.sleep(
                random.uniform(
                    config.SCROLL_PAUSE_TIME * 0.8, config.SCROLL_PAUSE_TIME * 1.5
                )
            )

        # 返回收集的结果
        logger.info(f"滚动完成，共收集 {len(results)} 项")
        return results
