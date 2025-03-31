#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫的浏览器操作模块
"""

import json
import os
import platform
import random
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

from fake_useragent import UserAgent
from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import config


class Browser:
    """浏览器管理类，处理浏览器初始化、滚动和元素查找"""

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = config.DEFAULT_TIMEOUT,
        viewport_width: int = 1920,
        viewport_height: int = 1080,
    ):
        """初始化浏览器

        Args:
            proxy: 代理服务器地址，例如 http://user:pass@host:port
            timeout: 默认超时时间(秒)
            viewport_width: 浏览器视口宽度
            viewport_height: 浏览器视口高度
            zoom_level: 缩放级别(百分比，例如30表示30%)
        """
        self.driver = None
        self.proxy = proxy
        self.timeout = timeout
        self.ua = UserAgent()
        self.user_agent = self.ua.random
        self.viewport_width = viewport_width
        self.viewport_height = viewport_height
        self.zoom_level = config.ZOOM_LEVEL
        self.monitoring_active = False
        self.monitoring_thread = None
        self.screenshot_dir = None

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

        while self.monitoring_active and self.driver:
            try:
                # 截图
                screenshot_path = os.path.join(
                    self.screenshot_dir, f"screenshot_{counter:04d}.png"
                )
                self.driver.save_screenshot(screenshot_path)

                # 保存当前页面HTML
                html_path = os.path.join(
                    self.screenshot_dir, f"page_{counter:04d}.html"
                )
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)

                # 记录浏览器状态
                scroll_position = self.driver.execute_script(
                    "return window.pageYOffset;"
                )
                page_height = self.get_page_height()
                status_info = {
                    "timestamp": datetime.now().isoformat(),
                    "scroll_position": scroll_position,
                    "page_height": page_height,
                    "viewport_height": self.viewport_height,
                    "is_page_end": scroll_position + self.viewport_height
                    >= page_height,
                    "url": self.driver.current_url,
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
        if self.driver:
            # 浏览器已经启动
            return True

        try:
            logger.info("初始化浏览器...")
            self.start_monitoring(session_id=1)

            # 设置Chrome选项
            chrome_options = Options()
            for option in config.CHROME_OPTIONS:
                chrome_options.add_argument(option)

            # 设置随机用户代理
            chrome_options.add_argument(f"user-agent={self.user_agent}")

            # 设置代理(如果有)
            if self.proxy:
                chrome_options.add_argument(f"--proxy-server={self.proxy}")

            # 设置性能日志(如果启用HTTP调试)
            if config.DEBUG_HTTP_ENABLED:
                chrome_options.set_capability(
                    "goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"}
                )

            # 设置浏览器窗口尺寸
            chrome_options.add_argument(
                f"--window-size={self.viewport_width},{self.viewport_height}"
            )

            # 第一次尝试：使用标准方法
            logger.debug(f"操作系统: {platform.system()} {platform.release()}")
            try:
                if platform.system() == "Windows":
                    # Windows特殊处理
                    driver_path = ChromeDriverManager().install()
                    service = Service(driver_path)
                    self.driver = webdriver.Chrome(
                        service=service, options=chrome_options
                    )
                else:
                    # 其它操作系统
                    service = Service(ChromeDriverManager().install())
                    self.driver = webdriver.Chrome(
                        service=service, options=chrome_options
                    )

                logger.info("浏览器初始化成功")

            except Exception as e:
                # 第二次尝试：备用方法
                logger.warning(f"标准初始化失败: {e}，尝试备用方法")
                try:
                    self.driver = webdriver.Chrome(options=chrome_options)
                    logger.info("使用备用方法初始化成功")
                except Exception as e2:
                    logger.error(f"浏览器初始化失败: {e2}")
                    return False

            # 设置超时
            self.driver.set_page_load_timeout(self.timeout)
            self.driver.set_script_timeout(self.timeout)

            # 设置窗口大小
            self.driver.set_window_size(self.viewport_width, self.viewport_height)

            # 应用自定义CSS缩放
            self.apply_zoom()

            return True

        except Exception as e:
            logger.error(f"浏览器启动异常: {e}")
            return False

    def apply_zoom(self):
        """应用缩放到页面"""
        if not self.driver:
            return

        try:
            # 使用CSS缩放方式
            zoom_script = f"""
            document.body.style.zoom = '{self.zoom_level}%';
            document.body.style.cssText += '; -moz-transform: scale({self.zoom_level / 100}); -moz-transform-origin: 0 0;';
            """
            self.driver.execute_script(zoom_script)

            # 备用方法：使用Chrome DevTools Protocol设置缩放比例
            self.driver.execute_cdp_cmd(
                "Emulation.setPageScaleFactor",
                {"pageScaleFactor": self.zoom_level / 100},
            )

            logger.debug(f"页面缩放设置为 {self.zoom_level}%")
        except Exception as e:
            logger.warning(f"设置页面缩放失败: {e}")

    def stop(self):
        """关闭浏览器"""
        # 停止监控
        self.stop_monitoring()

        if self.driver:
            try:
                self.driver.quit()
                logger.info("浏览器已关闭")
            except Exception as e:
                logger.error(f"关闭浏览器出错: {e}")
            finally:
                self.driver = None

    def get_url(self, url: str) -> bool:
        """访问URL

        Args:
            url: 要访问的网址

        Returns:
            访问是否成功
        """
        if not self.driver:
            logger.error("浏览器未初始化")
            return False

        try:
            logger.info(f"访问URL: {url}")
            self.driver.get(url)

            # 应用缩放 (因为新页面加载后可能会重置缩放)
            self.apply_zoom()

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
        if not self.driver:
            return False

        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
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
        if not self.driver:
            return []

        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
            return elements
        except Exception as e:
            logger.error(f"查找元素失败: {e}")
            return []

    def scroll_to_bottom(self):
        """滚动到页面底部"""
        if not self.driver:
            return

        try:
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )
        except Exception as e:
            logger.error(f"滚动到底部出错: {e}")

    def scroll_by(self, pixels: int):
        """向下滚动指定像素

        Args:
            pixels: 滚动的像素数
        """
        if not self.driver:
            return

        try:
            self.driver.execute_script(f"window.scrollBy(0, {pixels});")
        except Exception as e:
            logger.error(f"滚动出错: {e}")

    def get_page_height(self) -> int:
        """获取页面高度

        Returns:
            页面高度(像素)
        """
        if not self.driver:
            return 0

        try:
            return self.driver.execute_script("return document.body.scrollHeight")
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
        if not self.driver:
            return False

        try:
            self.driver.save_screenshot(filepath)
            logger.debug(f"截图已保存: {filepath}")
            return True
        except Exception as e:
            logger.error(f"截图失败: {e}")
            return False

    def get_page_source(self) -> str:
        """获取页面源码

        Returns:
            页面HTML源码
        """
        if not self.driver:
            return ""

        return self.driver.page_source

    def get_network_requests(self) -> List[Dict]:
        """获取页面网络请求数据

        Returns:
            网络请求数据列表
        """
        if not self.driver or not config.DEBUG_HTTP_ENABLED:
            return []

        try:
            logs = self.driver.get_log("performance")
            request_data = []

            for entry in logs:
                try:
                    log = json.loads(entry["message"])["message"]
                    if (
                        "Network.response" in log["method"]
                        or "Network.request" in log["method"]
                    ):
                        request_data.append(log)
                except:
                    pass

            return request_data
        except Exception as e:
            logger.error(f"获取网络请求数据失败: {e}")
            return []

    def simple_scroll_and_extract(
        self, target_count: int, extract_func, max_scroll_attempts: int = 5000
    ) -> List:
        """简化的滚动并提取数据函数

        这个函数使用更简单直接的滚动策略，边滚动边提取数据

        Args:
            target_count: 目标数量
            extract_func: 提取函数，接收页面源码并返回数据项列表
            max_scroll_attempts: 最大滚动尝试次数

        Returns:
            提取的数据列表
        """
        if not self.driver:
            logger.error("浏览器未初始化")
            return []

        logger.info(f"开始滚动提取，目标数量: {target_count}")

        # 初始化数据收集
        results = []
        scroll_count = 0
        no_change_count = 0
        scroll_position = 0
        consecutive_no_new_data = 0
        last_height = 0
        stuck_count = 0  # 新增：记录页面高度停滞的次数
        max_stuck_count = 5  # 新增：最大停滞次数

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
                f"滚动位置: {scroll_position}/{current_height}, 停滞计数: {stuck_count}"
            )

            # 提取当前页面上的数据
            page_source = self.get_page_source()
            new_items = extract_func(page_source)

            # 过滤并添加新项目
            new_added = 0
            for item in new_items:
                item_id = item.get("id", "")
                if item_id and item_id not in processed_ids:
                    processed_ids.add(item_id)
                    results.append(item)
                    new_added += 1

                    if len(results) >= target_count:
                        break
                else:
                    # find id in results
                    old_item = next(
                        (item for item in results if item.get("id") == item_id), None
                    )
                    if old_item:
                        logger.debug(
                            f"已处理项目: {item_id},已有信息url:{old_item.get('url', '')} 新的itemurl:{item.get('url', '')}"
                        )

            logger.debug(f"这次滚动添加了 {new_added} 个新项目")

            # 检查是否有新的项目被添加
            if new_added == 0:
                consecutive_no_new_data += 1
                logger.debug(f"连续 {consecutive_no_new_data} 次滚动未获取新数据")
            else:
                consecutive_no_new_data = 0
                logger.debug("重置连续无新数据计数")

            # 如果已找到足够数量，结束
            if len(results) >= target_count:
                logger.info(f"已收集到足够数量: {len(results)}/{target_count}")
                break

            # 检查页面高度是否停滞
            if current_height == last_height:
                stuck_count += 1
                if stuck_count >= max_stuck_count:
                    logger.warning(f"页面高度停滞 {stuck_count} 次，尝试特殊滚动策略")
                    # 尝试强制滚动到底部
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    time.sleep(2)
                    # 回到当前位置
                    self.driver.execute_script(
                        f"window.scrollTo(0, {scroll_position});"
                    )
                    time.sleep(1)
                    stuck_count = 0
            else:
                stuck_count = 0
                last_height = current_height

            # 如果连续多次没有新数据，尝试不同的加载策略
            if consecutive_no_new_data >= 3:
                logger.warning(
                    f"连续 {consecutive_no_new_data} 次未获取到新数据，尝试特殊滚动策略"
                )

                if consecutive_no_new_data == 3:
                    # 策略1: 随机滚动步长
                    random_step = random.randint(min_scroll_step, max_scroll_step)
                    self.driver.execute_script(f"window.scrollBy(0, {random_step});")
                    time.sleep(2)

                elif consecutive_no_new_data == 4:
                    # 策略2: 向上滚动一段距离再向下滚动
                    up_scroll = min(int(self.viewport_height * 1.5), scroll_position)
                    self.driver.execute_script(f"window.scrollBy(0, -{up_scroll});")
                    time.sleep(2)
                    self.driver.execute_script(
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
                        self.driver.execute_script(f"window.scrollBy(0, {step});")
                        time.sleep(random.uniform(0.1, 0.3))

                elif consecutive_no_new_data == 6:
                    # 策略4: 刷新页面并快速滚动到当前位置
                    current_url = self.driver.current_url
                    self.driver.refresh()
                    time.sleep(5)

                    # 使用更自然的滚动行为回到当前位置
                    scroll_position_int = int(scroll_position)
                    steps = random.randint(8, 12)
                    for i in range(steps):
                        step = scroll_position_int // steps
                        self.driver.execute_script(
                            f"window.scrollTo(0, {step * (i + 1)});"
                        )
                        time.sleep(random.uniform(0.2, 0.4))

                elif consecutive_no_new_data == 7:
                    # 策略5: 尝试点击"显示更多"按钮
                    try:
                        load_more_selectors = [
                            "button[aria-label='更多想法']",
                            "button:contains('更多')",
                            "button:contains('加载更多')",
                            "button:contains('Show more')",
                            "button:contains('Load more')",
                            "[data-test-id='scrollContainer'] button",
                        ]

                        for selector in load_more_selectors:
                            try:
                                elements = self.driver.find_elements(
                                    By.CSS_SELECTOR, selector
                                )
                                if elements:
                                    logger.info(f"找到可能的加载更多按钮: {selector}")
                                    elements[0].click()
                                    time.sleep(3)
                                    break
                            except:
                                continue

                        # 尝试执行JavaScript点击
                        self.driver.execute_script("""
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
                        """)
                        time.sleep(3)
                    except Exception as e:
                        logger.debug(f"尝试点击加载更多按钮失败: {e}")

                elif consecutive_no_new_data > 10:
                    logger.warning(
                        f"多次尝试后仍无法获取新数据，当前已收集 {len(results)} 项"
                    )
                    if consecutive_no_new_data > 15:
                        logger.warning("达到最大尝试次数，停止滚动")
                        break

                continue

            # 执行常规滚动，使用随机步长
            random_step = random.randint(min_scroll_step, max_scroll_step)
            self.scroll_by(random_step)

            # 记录滚动位置
            scroll_position = self.driver.execute_script("return window.pageYOffset;")

            # 随机等待时间，模拟真实用户行为
            time.sleep(
                random.uniform(
                    config.SCROLL_PAUSE_TIME * 0.8, config.SCROLL_PAUSE_TIME * 1.5
                )
            )

        # 返回收集的结果
        logger.info(f"滚动完成，共收集 {len(results)} 项")
        return results[:target_count]  # 确保不超过目标数量
