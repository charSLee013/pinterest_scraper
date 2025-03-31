#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫的浏览器操作模块
"""

import json
import platform
import random
import time
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
        zoom_level: int = 76,  # 30% 的缩放级别，值越小缩放越大
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
        self.zoom_level = zoom_level

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
        self, target_count: int, extract_func, max_scroll_attempts: int = 50
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
        consecutive_empty_results = 0
        scroll_position = 0
        consecutive_no_new_data = 0  # 连续几次没有新数据的计数
        last_result_count = 0  # 上次的结果数量

        # 使用集合存储已处理的项目ID，避免重复
        processed_ids = set()

        # 优化滚动速度，使用较大的滚动步长
        scroll_step = int(self.viewport_height * 0.8)  # 80%的视口高度

        while len(results) < target_count and scroll_count < max_scroll_attempts:
            # 增加滚动计数
            scroll_count += 1

            # 获取当前高度
            current_height = self.get_page_height()

            # 输出调试信息
            logger.debug(
                f"滚动 #{scroll_count}, 当前高度: {current_height}px, 已收集: {len(results)}, 滚动位置: {scroll_position}/{current_height}"
            )

            # 提取当前页面上的数据
            page_source = self.get_page_source()
            new_items = extract_func(page_source)

            # 记录当前结果数
            current_result_count = len(results)

            # 过滤并添加新项目
            new_added = 0
            for item in new_items:
                item_id = item.get("id", "")
                if item_id and item_id not in processed_ids:
                    processed_ids.add(item_id)
                    results.append(item)
                    new_added += 1

                    # 如果收集足够数量，提前退出
                    if len(results) >= target_count:
                        break

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

            # 如果连续多次没有新数据，尝试不同的加载策略
            if consecutive_no_new_data >= 4:
                logger.warning(
                    f"连续 {consecutive_no_new_data} 次未获取到新数据，尝试特殊滚动策略"
                )

                if consecutive_no_new_data == 4:
                    # 策略1: 快速向上滚动一段距离再向下滚动，触发新的加载
                    logger.debug("策略1: 向上滚动后再向下")
                    up_scroll = min(int(self.viewport_height * 1.5), scroll_position)
                    self.driver.execute_script(f"window.scrollBy(0, -{up_scroll});")
                    time.sleep(1)
                    self.driver.execute_script(
                        f"window.scrollBy(0, {up_scroll + 300});"
                    )
                    time.sleep(1.5)

                elif consecutive_no_new_data == 5:
                    # 策略2: 跳到页面底部然后回到当前位置
                    logger.debug("策略2: 跳到页面底部再回来")
                    current_pos = scroll_position
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    time.sleep(2)
                    self.driver.execute_script(f"window.scrollTo(0, {current_pos});")
                    time.sleep(1)
                elif consecutive_no_new_data == 6:
                    # 策略3: 刷新页面，重新开始
                    logger.debug("策略3: 刷新页面")
                    current_url = self.driver.current_url
                    self.driver.refresh()
                    time.sleep(3)
                    # 快速滚动到当前位置
                    # Convert scroll_position to int to avoid TypeError
                    scroll_position_int = int(scroll_position)
                    for pos in range(0, scroll_position_int, scroll_step):
                        self.driver.execute_script(f"window.scrollTo(0, {pos});")
                        time.sleep(0.1)
                    time.sleep(1)

                elif consecutive_no_new_data > 8:
                    logger.warning(
                        f"多次尝试后仍无法获取新数据，当前已收集 {len(results)} 项，停止滚动"
                    )
                    break

                # 不立即继续常规滚动
                continue

            # 执行滚动
            self.scroll_by(scroll_step)
            scroll_position = self.driver.execute_script("return window.pageYOffset;")

            # 等待加载
            time.sleep(config.SCROLL_PAUSE_TIME)

            # 检查高度变化 - 仅作为参考，不作为主要决策依据
            new_height = self.get_page_height()
            if new_height == current_height:
                no_change_count += 1
                if no_change_count >= 3:
                    logger.debug("页面高度停止变化，但继续检查是否有新内容")

                    # 仍然尝试滚动触发加载，但不强制终止
                    if no_change_count % 3 == 0:
                        # 尝试滚动到顶部再回来以触发加载
                        random_offset = random.randint(100, 300)
                        logger.debug(
                            f"尝试滚动触发：当前位置 {scroll_position} ± {random_offset}"
                        )

                        # 随机向上或向下稍微滚动一点以触发新加载
                        if random.random() > 0.5:
                            self.driver.execute_script(
                                f"window.scrollBy(0, {random_offset});"
                            )
                        else:
                            self.driver.execute_script(
                                f"window.scrollBy(0, -{random_offset});"
                            )
                        time.sleep(0.5)
                        # 回到原位置
                        self.driver.execute_script(
                            f"window.scrollTo(0, {scroll_position});"
                        )
                        time.sleep(0.5)
            else:
                # 高度有变化，重置计数
                no_change_count = 0
                logger.debug(f"页面高度从 {current_height} 变为 {new_height}")

            # 保存上次结果数量
            last_result_count = len(results)

        # 返回收集的结果
        logger.info(f"滚动完成，共收集 {len(results)} 项")
        return results[:target_count]  # 确保不超过目标数量
