#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫主模块
"""

import os
import parser
import time
from datetime import datetime
from typing import Dict, List, Optional

from loguru import logger

import browser
import config
import downloader
import utils


class PinterestScraper:
    """Pinterest爬虫主类"""

    def __init__(
        self,
        output_dir: str = "output",
        proxy: Optional[str] = None,
        debug: bool = False,
        timeout: int = config.DEFAULT_TIMEOUT,
        max_workers: int = 0,
        download_images: bool = True,
        viewport_width: int = 1920,
        viewport_height: int = 1080,
        cookie_path: Optional[str] = config.COOKIE_FILE_PATH,
    ):
        """初始化Pinterest爬虫

        Args:
            output_dir: 输出目录
            proxy: 代理服务器地址，例如 http://user:pass@host:port
            debug: 是否启用调试模式
            timeout: 请求超时时间(秒)
            max_workers: 并发下载的最大线程数，0表示自动设置
            download_images: 是否下载图片
            viewport_width: 浏览器视口宽度
            viewport_height: 浏览器视口高度
            cookie_path: Cookie文件路径
        """
        self.output_dir = output_dir
        self.proxy = proxy
        self.debug = debug
        self.timeout = timeout
        self.max_workers = max_workers
        self.download_images = download_images
        self.viewport_width = viewport_width
        self.viewport_height = viewport_height
        self.cookie_path = cookie_path

        # 创建主输出目录
        os.makedirs(output_dir, exist_ok=True)

        # 初始化浏览器
        self.browser = browser.Browser(
            proxy=proxy,
            timeout=timeout,
            viewport_width=viewport_width,
            viewport_height=viewport_height,
            cookie_path=cookie_path,
        )


        logger.info(
            f"Pinterest爬虫初始化完成。输出目录: {output_dir}, 调试模式: {debug}, 下载图片: {download_images}, "
            f"视口: {viewport_width}x{viewport_height}"
        )

    def search(self, query: str, count: int = 50) -> List[Dict]:
        """搜索Pinterest

        Args:
            query: 搜索关键词
            count: 需要获取的pin数量

        Returns:
            包含pin数据的字典列表
        """
        logger.info(f"搜索Pinterest，关键词: '{query}'，目标数量: {count}")

        try:
            # 为当前搜索词设置专用目录
            self.dirs = utils.setup_directories(self.output_dir, query, self.debug)
            safe_term = utils.sanitize_filename(query)

            # 检查缓存
            cache_file = os.path.join(self.dirs["cache"], f"{safe_term}_cache.json")
            cached_pins = (
                utils.get_cached_pins(cache_file) if os.path.exists(cache_file) else []
            )

            # 如果缓存中有足够的数据，直接返回
            if len(cached_pins) >= count:
                logger.info(f"缓存中有 {len(cached_pins)} 个pins，直接使用缓存数据")
                result_pins = cached_pins[:count]

                # 如果需要下载图片，确保所有图片已下载
                if self.download_images:
                    # 检查是否有未下载的图片
                    need_download = any(
                        not pin.get("downloaded", False) for pin in result_pins
                    )
                    if need_download:
                        logger.info("缓存中有未下载的图片，正在下载")
                        result_pins = downloader.download_images_with_cache(
                            result_pins,
                            self.dirs["images"],
                            safe_term,
                            self.dirs["cache"],
                            self.max_workers,
                        )

                return result_pins[:count]

            # 启动浏览器
            if not self.browser.start():
                logger.error("浏览器启动失败")
                return []

            # 构建搜索URL并访问 - 添加重试机制
            search_url = config.PINTEREST_SEARCH_URL.format(
                query=query.replace(" ", "+")
            )

            # 重试机制
            max_retries = 3
            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    retry_count += 1
                    logger.info(
                        f"尝试访问搜索URL (尝试 {retry_count}/{max_retries}): {search_url}"
                    )

                    # 尝试访问URL，增加超时容忍度
                    if not self.browser.get_url(search_url):
                        if retry_count < max_retries:
                            logger.warning(
                                f"访问失败，将在 {retry_count * 2} 秒后重试..."
                            )
                            time.sleep(retry_count * 2)  # 渐进式增加等待时间

                            # 如果是超时问题，可能需要重启浏览器
                            if retry_count > 1:
                                logger.info("重启浏览器...")
                                self.browser.stop()
                                time.sleep(1)
                                if not self.browser.start():
                                    logger.error("浏览器重启失败")
                                    return []
                        else:
                            logger.error(f"访问搜索URL失败: {search_url}")
                            return []
                    else:
                        success = True
                        logger.info("成功访问搜索URL")
                except Exception as e:
                    logger.error(f"访问URL时发生异常: {e}")
                    if retry_count < max_retries:
                        logger.warning(f"将在 {retry_count * 2} 秒后重试...")
                        time.sleep(retry_count * 2)
                    else:
                        raise

            # 等待页面加载 - 增加更灵活的等待策略
            logger.debug("等待搜索结果加载")
            load_success = False

            # 使用更长的超时和多个选择器提高成功率
            for wait_time in [5, 8, 10]:  # 逐步增加等待时间
                for selector in config.PINTEREST_PIN_SELECTORS:
                    if self.browser.wait_for_element(selector, timeout=wait_time):
                        logger.debug(f"找到匹配的pin元素: {selector}")
                        load_success = True
                        break

                if load_success:
                    break

                logger.warning(f"等待 {wait_time} 秒后仍未找到元素，继续尝试...")

            if not load_success:
                logger.warning("未找到pin元素，但仍将继续尝试提取")

                # 保存调试截图
            if self.debug:
                screenshot_path = os.path.join(
                    self.dirs["debug_screenshots"],
                    f"search_{safe_term}_{int(time.time())}.png",
                )
                self.browser.take_screenshot(screenshot_path)

                # 保存HTML源码
                html_path = os.path.join(
                    self.dirs["debug_html"],
                    f"search_{safe_term}_{int(time.time())}.html",
                )
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(self.browser.get_page_source())

                # 保存网络请求数据
                if config.DEBUG_HTTP_ENABLED:
                    requests_path = os.path.join(
                        self.dirs["debug_network"],
                        f"search_{safe_term}_requests_{int(time.time())}.json",
                    )
                    network_data = self.browser.get_network_requests()
                    utils.save_json(network_data, requests_path)

            # 执行滚动并提取数据
            def extract_pins_from_page(html):
                return parser.extract_pins_from_html(html)

            # 使用更高的最大滚动尝试次数以获取更多图片
            pins = self.browser.simple_scroll_and_extract(
                target_count=count,
                extract_func=extract_pins_from_page,
                new_item_selector="div[data-test-id='pin-card']",
                max_scroll_attempts=config.MAX_SCROLL_ATTEMPTS
            )
            logger.info(f"simple_scroll_and_extract 返回 {len(pins)} 个pins")

            # 保存结果
            # 获取当前日期
            current_date = datetime.now().strftime("%Y-%m-%d")
            output_path = os.path.join(
                self.dirs["json"], f"pinterest_search_{safe_term}_{current_date}.json"
            )
            utils.save_json(pins, output_path)

            # 下载图片
            if self.download_images and pins:
                pins = downloader.download_images_with_cache(
                    pins,
                    self.dirs["images"],
                    safe_term,
                    self.dirs["cache"],
                    self.max_workers,
                )

                # 保存带有下载状态的结果
                utils.save_json(pins, output_path)

                # 更新缓存
                utils.update_cache_with_pins(pins, cache_file)

            logger.info(f"搜索完成，获取了 {len(pins)} 个pins")
            return pins

        except Exception as e:
            logger.error(f"搜索过程中出错: {e}")
            return []

        finally:
            # 关闭浏览器
            self.browser.stop()

    def scrape_url(self, url: str, count: int = 50) -> List[Dict]:
        """爬取单个Pinterest URL

        Args:
            url: Pinterest URL
            count: 需要获取的pin数量

        Returns:
            包含pin数据的字典列表
        """
        logger.info(f"爬取URL: {url}，目标数量: {count}")

        try:
            # 获取URL的安全文件名作为目录名
            url_term = utils.sanitize_filename(url)

            # 为当前URL设置专用目录
            self.dirs = utils.setup_directories(self.output_dir, url_term, self.debug)

            # 检查缓存
            cache_file = os.path.join(self.dirs["cache"], f"{url_term}_cache.json")
            cached_pins = (
                utils.get_cached_pins(cache_file) if os.path.exists(cache_file) else []
            )

            # 如果缓存中有足够的数据，直接返回
            if len(cached_pins) >= count:
                logger.info(f"缓存中有 {len(cached_pins)} 个pins，直接使用缓存数据")
                result_pins = cached_pins[:count]

                # 如果需要下载图片，确保所有图片已下载
                if self.download_images:
                    # 检查是否有未下载的图片
                    need_download = any(
                        not pin.get("downloaded", False) for pin in result_pins
                    )
                    if need_download:
                        logger.info("缓存中有未下载的图片，正在下载")
                        result_pins = downloader.download_images_with_cache(
                            result_pins,
                            self.dirs["images"],
                            url_term,
                            self.dirs["cache"],
                            self.max_workers,
                        )

                return result_pins[:count]

            # 启动浏览器
            if not self.browser.start():
                logger.error("浏览器启动失败")
                return []

            # 启动浏览器监控
            if hasattr(self.browser, "start_monitoring"):
                self.browser.start_monitoring(url_term)
                logger.info("浏览器监控已启动")

            # 访问URL
            if not self.browser.get_url(url):
                logger.error(f"访问URL失败: {url}")
                return []

            # 等待页面加载
            logger.debug("等待页面元素加载")
            for selector in config.PINTEREST_PIN_SELECTORS:
                if self.browser.wait_for_element(selector, timeout=5):
                    logger.debug(f"找到匹配的pin元素: {selector}")
                    break
            else:
                logger.warning("未找到pin元素，但仍将继续尝试提取")

            # 保存调试截图
            if self.debug:
                screenshot_path = os.path.join(
                    self.dirs["debug_screenshots"],
                    f"url_{url_term}_{int(time.time())}.png",
                )
                self.browser.take_screenshot(screenshot_path)

                # 保存HTML源码
                html_path = os.path.join(
                    self.dirs["debug_html"],
                    f"url_{url_term}_{int(time.time())}.html",
                )
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(self.browser.get_page_source())

                # 保存网络请求数据
                if config.DEBUG_HTTP_ENABLED:
                    requests_path = os.path.join(
                        self.dirs["debug_network"],
                        f"url_{url_term}_requests_{int(time.time())}.json",
                    )
                    network_data = self.browser.get_network_requests()
                    utils.save_json(network_data, requests_path)

            # 执行滚动并提取数据
            def extract_pins_from_page(html):
                return parser.extract_pins_from_html(html)

            pins = self.browser.simple_scroll_and_extract(
                target_count=count,
                extract_func=extract_pins_from_page,
                new_item_selector="div[data-test-id='pin-card']",
                max_scroll_attempts=config.MAX_SCROLL_ATTEMPTS
            )
            logger.info(f"simple_scroll_and_extract 返回 {len(pins)} 个pins")

            # 保存结果
            # 获取当前日期
            current_date = datetime.now().strftime("%Y-%m-%d")
            output_path = os.path.join(
                self.dirs["json"], f"pinterest_url_{url_term}_{current_date}.json"
            )
            utils.save_json(pins, output_path)

            # 下载图片
            if self.download_images and pins:
                pins = downloader.download_images_with_cache(
                    pins,
                    self.dirs["images"],
                    url_term,
                    self.dirs["cache"],
                    self.max_workers,
                )

                # 保存带有下载状态的结果
                utils.save_json(pins, output_path)

                # 更新缓存
                utils.update_cache_with_pins(pins, cache_file)

            logger.info(f"URL爬取完成，获取了 {len(pins)} 个pins")
            return pins

        except Exception as e:
            logger.error(f"爬取URL过程中出错: {e}")
            return []

        finally:
            # 停止监控并关闭浏览器
            if hasattr(self.browser, "stop_monitoring"):
                self.browser.stop_monitoring()
            self.browser.stop()

    def scrape_urls(
        self, urls: List[str], count_per_url: int = 50
    ) -> Dict[str, List[Dict]]:
        """爬取多个Pinterest URL

        Args:
            urls: URL列表
            count_per_url: 每个URL获取的pin数量

        Returns:
            URL到pin列表的映射字典
        """
        logger.info(f"爬取 {len(urls)} 个URL，每个URL获取 {count_per_url} 个pins")

        results = {}
        for i, url in enumerate(urls):
            logger.info(f"爬取第 {i + 1}/{len(urls)} 个URL: {url}")
            pins = self.scrape_url(url, count_per_url)
            results[url] = pins

        return results
