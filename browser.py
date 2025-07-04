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

import config


class Browser:
    """浏览器管理类，处理浏览器初始化、滚动和元素查找"""

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = config.DEFAULT_TIMEOUT,
        viewport_width: int = 1920,
        viewport_height: int = 1080,
        cookie_path: Optional[str] = None,
    ):
        """初始化浏览器

        Args:
            proxy: 代理服务器地址，例如 http://user:pass@host:port
            timeout: 默认超时时间(秒)
            viewport_width: 浏览器视口宽度
            viewport_height: 浏览器视口高度
            cookie_path: Cookie文件路径
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
            self.start_monitoring(session_id=1)

            self.playwright_instance = sync_playwright().start()
            launch_options = {
                "headless": True if "--headless=new" in config.CHROME_OPTIONS else False,
                "args": [opt for opt in config.CHROME_OPTIONS if "--headless" not in opt],
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
        except Exception as e:
            logger.error(f"访问URL失败: {e}")
            return False

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
            logger.debug("执行PageDown滚动")
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
            for item in new_items:
                item_id = item.get("id", "")
                if item_id and item_id not in processed_ids:
                    processed_ids.add(item_id)
                    results.append(item)
                    new_added += 1
                    logger.debug(f"新增项目: ID={item_id}, URL={item.get('url', 'N/A')}")

                    if len(results) >= target_count:
                        logger.info(f"已收集到足够数量: {len(results)}/{target_count}, 提前退出。")
                        break
                elif item_id and item_id in processed_ids: # Existing item
                    old_item = next((res_item for res_item in results if res_item.get("id") == item_id), None)
                    if old_item:
                        logger.debug(f"已处理项目 (重复): ID={item_id}, 已有URL:{old_item.get('url', '')}, 新的URL:{item.get('url', '')}")
                else: # Item without ID
                    logger.warning(f"发现缺少ID的项目: {item}")

            logger.debug(f"这次滚动添加了 {new_added} 个新项目, 当前总收集: {len(results)}, 已处理ID: {len(processed_ids)}")

            # 检查是否有新的项目被添加
            if new_added == 0 and len(new_items) > 0:
                logger.debug(f"所有 {len(new_items)} 个提取的项目都是重复的。")
                consecutive_no_new_data += 1
                logger.debug(f"连续 {consecutive_no_new_data} 次滚动未获取新数据")
            elif new_added == 0 and len(new_items) == 0:
                logger.debug("当前页面未提取到任何项目。")
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

            # 如果连续多次没有新数据，尝试不同的加载策略
            if consecutive_no_new_data >= 3 and len(results) < target_count:
                logger.warning(
                    f"连续 {consecutive_no_new_data} 次未获取到新数据，尝试特殊滚动策略"
                )

                if consecutive_no_new_data == 3:
                    # 策略1: 随机滚动步长
                    random_step = random.randint(min_scroll_step, max_scroll_step)
                    self.page.evaluate(f"window.scrollBy(0, {random_step});")
                    time.sleep(2)

                elif consecutive_no_new_data == 4:
                    # 策略2: 向上滚动一段距离再向下滚动
                    up_scroll = min(int(self.viewport_height * 1.5), scroll_position)
                    self.page.evaluate(f"window.scrollBy(0, -{up_scroll});")
                    time.sleep(2)
                    self.page.evaluate(
                        f"window.scrollBy(0, {up_scroll + 200});"
                    )
                    time.sleep(2)

                elif consecutive_no_new_data == 5:
                    # 策略3: 模拟真实用户滚动行为
                    current_pos = scroll_position
                    target_pos = min(
                        current_pos + int(self.viewport_height * 2), current_height
                    )
                    steps = random.randint(5, 10)
                    for i in range(steps):
                        step = (target_pos - current_pos) // steps
                        self.page.evaluate(f"window.scrollBy(0, {step});")
                        time.sleep(random.uniform(0.1, 0.3))

                elif consecutive_no_new_data == 6:
                    # 策略4: 刷新页面并快速滚动到当前位置
                    current_url = self.page.url
                    self.page.reload()
                    time.sleep(5)

                    # 使用更自然的滚动行为回到当前位置
                    scroll_position_int = int(scroll_position)
                    steps = random.randint(8, 12)
                    for i in range(steps):
                        step = scroll_position_int // steps
                        self.page.evaluate(
                            f"window.scrollTo(0, {step * (i + 1)});"
                        )
                        time.sleep(random.uniform(0.2, 0.4))

                elif consecutive_no_new_data == 7:
                    # 策略5: 尝试点击"显示更多"按钮
                    try:
                        load_more_selectors = [
                            "button[aria-label='更多想法']",
                            "button:has-text('更多')",
                            "button:has-text('加载更多')",
                            "button:has-text('Show more')",
                            "button:has-text('Load more')",
                            "[data-test-id='scrollContainer'] button",
                        ]

                        for selector in load_more_selectors:
                            try:
                                elements = self.page.locator(selector).all()
                                if elements:
                                    logger.info(f"找到可能的加载更多按钮: {selector}")
                                    elements[0].click()
                                    time.sleep(3)
                                    break
                            except Exception as click_e:
                                logger.debug(f"点击加载更多按钮失败 ({selector}): {click_e}")
                                continue

                        # 尝试执行JavaScript点击
                        self.page.evaluate("""
                            (function() {
                                var buttons = document.querySelectorAll('button');
                                for(var i=0; i<buttons.length; i++) {
                                    if(buttons[i].innerText.includes('更多') || 
                                       buttons[i].innerText.includes('more') ||
                                       buttons[i].innerText.toLowerCase().includes('load')) {
                                        buttons[i].click();
                                        return true;
                                    }
                                }
                                return false;
                            })()
                        """)
                        time.sleep(3)
                    except Exception as e:
                        logger.debug(f"尝试点击加载更多按钮失败: {e}")

                elif consecutive_no_new_data > 10:
                    logger.warning(
                        f"多次尝试后仍无法获取新数据，当前已收集 {len(results)} 项"
                    )
                    if consecutive_no_new_data > 40: # 提高阈值
                        logger.warning("达到最大尝试次数，停止滚动")
                        break # 达到最大尝试次数，停止滚动

                continue

            # 执行常规滚动
            self.scroll_page_down()

            # 显式等待新的Pin元素出现
            try:
                # 使用new_item_selector等待新元素，超时时间设置为DEFAULT_TIMEOUT的两倍
                self.page.wait_for_selector(new_item_selector, state='attached', timeout=config.DEFAULT_TIMEOUT * 2 * 1000)
                logger.debug(f"成功等待到新的Pin元素: {new_item_selector}")
                # 如果成功等待到新元素，重置无新数据计数器
                consecutive_no_new_data = 0
            except Error as e: # Playwright's general Error class covers TimeoutError
                logger.warning(f"等待新的Pin元素超时或失败 ({new_item_selector}): {e}")
                # 如果等待失败，增加无新数据计数
                consecutive_no_new_data += 1

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
