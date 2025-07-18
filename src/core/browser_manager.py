#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
异步浏览器管理器

提供统一的异步浏览器管理接口，支持数据采集和网络拦截功能
"""

import json
import os
import time
import asyncio
from typing import Dict, List, Optional, Callable, Any

from loguru import logger
from patchright.async_api import async_playwright, Page, BrowserContext, Error
from tqdm import tqdm

from . import config


class BrowserManager:
    """异步浏览器管理器

    提供统一的异步浏览器管理接口，支持数据采集、滚动收集和网络拦截功能
    """

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = config.DEFAULT_TIMEOUT,
        cookie_path: Optional[str] = None,
        headless: bool = True,
        enable_network_interception: bool = False,
        browser_type: str = 'chromium'
    ):
        """初始化浏览器管理器

        Args:
            proxy: 代理服务器地址
            timeout: 默认超时时间(秒)
            cookie_path: Cookie文件路径
            headless: 是否无头模式
            enable_network_interception: 是否启用网络拦截
            browser_type: 浏览器类型 ('chromium', 'firefox', 'webkit')
        """
        self.proxy = proxy
        self.timeout = timeout or config.DEFAULT_TIMEOUT  # 使用优化的默认超时
        self.cookie_path = cookie_path or config.COOKIE_FILE_PATH
        self.headless = headless
        self.enable_network_interception = enable_network_interception
        self.browser_type = browser_type
        
        # 浏览器实例
        self.playwright_instance = None
        self.browser = None
        self.browser_context = None
        self.page = None
        
        # 网络拦截回调
        self.request_handlers = []
        self.response_handlers = []

        # 事件处理器跟踪
        self._registered_handlers = []
        
        # 用户代理 - 使用更可靠的UA
        self.user_agent = self._get_reliable_user_agent()

    async def start(self) -> bool:
        """启动浏览器

        Returns:
            启动是否成功
        """
        try:
            logger.debug("启动浏览器管理器...")

            # 启动Playwright
            self.playwright_instance = await async_playwright().start()
            
            # 配置启动选项
            launch_options = {
                "headless": self.headless,
                "args": config.BROWSER_ARGS,
                "slow_mo": 100,  # 添加100ms延迟，减少检测风险
                "timeout": 60000,  # 60秒启动超时
            }

            # 代理配置
            if self.proxy:
                proxy_config = {"server": self.proxy}
                launch_options["proxy"] = proxy_config
                logger.info(f"使用代理: {self.proxy}")
            
            # 启动浏览器 - 根据browser_type选择
            if self.browser_type == 'chromium':
                self.browser = await self.playwright_instance.chromium.launch(**launch_options)
            elif self.browser_type == 'firefox':
                self.browser = await self.playwright_instance.firefox.launch(**launch_options)
            elif self.browser_type == 'webkit':
                self.browser = await self.playwright_instance.webkit.launch(**launch_options)
            else:
                # 默认使用chromium
                self.browser = await self.playwright_instance.chromium.launch(**launch_options)
                logger.warning(f"未知的浏览器类型 {self.browser_type}，使用默认的chromium")

            # 创建浏览器上下文 - 增强反爬虫配置
            context_options = {
                "user_agent": self.user_agent,
                "viewport": {"width": 1920, "height": 1080},
                "java_script_enabled": True,
                "locale": "en-US",
                "timezone_id": "America/New_York",
                "permissions": ["geolocation"],
                "extra_http_headers": self._get_anti_detection_headers(),
            }
            
            # 加载Cookie
            if os.path.exists(self.cookie_path):
                try:
                    with open(self.cookie_path, 'r') as f:
                        storage_state = json.load(f)
                    context_options["storage_state"] = storage_state
                    logger.debug(f"成功加载Cookie: {self.cookie_path}")
                except Exception as e:
                    logger.warning(f"加载Cookie失败: {e}")
            
            self.browser_context = await self.browser.new_context(**context_options)
            self.page = await self.browser_context.new_page()
            
            # 阻止不必要的资源
            if hasattr(config, "BLOCKED_RESOURCE_TYPES"):
                await self.page.route("**/*", lambda route: (
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
                self._registered_handlers.extend([
                    ("request", self._handle_request),
                    ("response", self._handle_response)
                ])
            
            logger.debug("浏览器管理器启动成功")
            return True
            
        except Exception as e:
            logger.error(f"浏览器启动失败: {e}")
            return False

    async def stop(self):
        """关闭浏览器"""
        try:
            logger.debug("开始关闭浏览器...")

            # 注销事件处理器
            if self.page and self._registered_handlers:
                for event_type, handler in self._registered_handlers:
                    try:
                        self.page.off(event_type, handler)
                    except Exception as e:
                        logger.debug(f"注销{event_type}处理器失败: {e}")
                self._registered_handlers.clear()

            # 按顺序关闭资源
            if self.page:
                try:
                    await self.page.close()
                    logger.debug("页面已关闭")
                except Exception as e:
                    logger.debug(f"关闭页面失败: {e}")

            if self.browser_context:
                try:
                    await self.browser_context.close()
                    logger.debug("浏览器上下文已关闭")
                except Exception as e:
                    logger.debug(f"关闭浏览器上下文失败: {e}")

            if self.browser:
                try:
                    await self.browser.close()
                    logger.debug("浏览器已关闭")
                except Exception as e:
                    logger.debug(f"关闭浏览器失败: {e}")

            if self.playwright_instance:
                try:
                    await self.playwright_instance.stop()
                    logger.debug("Playwright实例已停止")
                except Exception as e:
                    logger.debug(f"停止Playwright实例失败: {e}")

            logger.debug("浏览器资源清理完成")

        except Exception as e:
            logger.error(f"关闭浏览器出错: {e}")
        finally:
            # 确保所有引用都被清理
            self.page = None
            self.browser_context = None
            self.browser = None
            self.playwright_instance = None
            self.request_handlers.clear()
            self.response_handlers.clear()

    async def navigate(self, url: str) -> bool:
        """导航到指定URL - 默认优化版本，集成重试和延迟

        Args:
            url: 目标URL

        Returns:
            导航是否成功
        """
        # 固定3次重试，间隔递增：5s, 10s, 15s
        max_retries = 3
        retry_delays = [5, 10, 15]

        for attempt in range(max_retries):
            try:
                logger.info(f"导航到: {url} (尝试 {attempt + 1}/{max_retries})")

                # 如果是Pinterest搜索URL，先建立会话
                if "pinterest.com/search" in url:
                    await self._establish_pinterest_session()

                # 使用120秒超时，等待文档加载完成即可（不需要网络空闲）
                await self.page.goto(url, timeout=120000, wait_until="domcontentloaded")

                # 导航成功后的人类行为延迟
                await self._human_like_delay(2.0, 4.0)

                # 检查页面状态
                current_url = self.page.url
                title = await self.page.title()

                # 检测错误页面
                error_indicators = ["error", "blocked", "captcha", "robot", "denied"]
                if any(indicator in current_url.lower() or indicator in title.lower()
                       for indicator in error_indicators):
                    raise Error(f"检测到错误页面: {current_url} - {title}")

                logger.info(f"✅ 导航成功: {current_url}")
                return True

            except Exception as e:
                error_msg = str(e)
                logger.warning(f"导航失败 (尝试 {attempt + 1}/{max_retries}): {error_msg}")

                # 特殊处理连接关闭错误
                if "ERR_CONNECTION_CLOSED" in error_msg or "net::ERR_CONNECTION_CLOSED" in error_msg:
                    logger.warning("检测到连接关闭错误，可能是网络不稳定或Pinterest反爬虫机制")

                    # 对于连接关闭错误，增加更长的等待时间
                    if attempt < max_retries - 1:
                        wait_time = retry_delays[attempt] * 2  # 双倍等待时间
                        logger.info(f"连接关闭错误，等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)

                        # 尝试重新建立浏览器连接
                        try:
                            await self.page.reload(timeout=30000)
                            await asyncio.sleep(3)
                        except:
                            logger.debug("页面重载失败，继续重试")
                    else:
                        logger.error(f"导航最终失败 (连接关闭): {url}")
                        return False
                elif attempt < max_retries - 1:
                    # 使用固定的重试延迟
                    wait_time = retry_delays[attempt]
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"导航最终失败，已重试 {max_retries} 次: {error_msg}")
                    return False

        return False

    async def scroll_and_collect(
        self,
        target_count: int,
        extract_func: Callable[[str], List[Dict]],
        max_scrolls: int = 100,
        scroll_pause: float = 2.0,
        no_new_data_limit: int = 10,
        initial_count: int = 0,
        repository=None,
        query: str = None,
        session_id: str = None
    ) -> List[Dict]:
        """智能滚动并收集数据 - 实时保存版本

        Args:
            target_count: 目标数据数量
            extract_func: 数据提取函数
            max_scrolls: 最大滚动次数
            scroll_pause: 滚动间隔时间
            no_new_data_limit: 连续无新数据的限制次数
            initial_count: 初始已有数据数量（用于进度条显示）
            repository: 数据库Repository实例，用于实时保存
            query: 搜索关键词
            session_id: 会话ID

        Returns:
            从数据库加载的数据列表（确保数据一致性）
        """
        # 实时保存模式检查
        realtime_save_enabled = repository is not None and query is not None
        if realtime_save_enabled:
            logger.info(f"🔄 启用实时保存模式，目标: {target_count}")
        else:
            logger.warning("⚠️  未启用实时保存模式，数据将存储在内存中（不推荐）")

        seen_ids = set()
        consecutive_no_new = 0
        scroll_count = 0
        saved_count = 0  # 实际保存到数据库的数量

        logger.info(f"开始数据采集，目标: {target_count}")

        # 首次提取页面数据（滚动前）
        html = await self.page.content()
        initial_items = extract_func(html)

        # 处理初始数据
        for item in initial_items:
            item_id = item.get('id')
            if item_id and item_id not in seen_ids:
                seen_ids.add(item_id)

                # 实时保存到数据库
                if realtime_save_enabled:
                    try:
                        success = repository.save_pin_immediately(item, query, session_id)
                        if success:
                            saved_count += 1
                            logger.debug(f"💾 实时保存Pin: {item_id} (总计: {saved_count})")
                        else:
                            logger.error(f"❌ 保存失败: {item_id}")
                    except Exception as e:
                        logger.error(f"❌ 保存异常: {item_id}, 错误: {e}")

                if saved_count >= target_count:
                    break

        # 创建进度条，基于实际保存的数据量
        current_progress = initial_count + saved_count
        pbar = tqdm(total=target_count + initial_count, desc="实时保存", unit="pins",
                   initial=current_progress, leave=False)

        # 退出条件：达到目标数量 OR 达到最大滚动次数 OR 连续无新数据
        while (saved_count < target_count and
               scroll_count < max_scrolls and
               consecutive_no_new < no_new_data_limit):

            # 使用真实的PageDown键盘事件滚动（比JavaScript更自然）
            scroll_count += 1
            await self.page.keyboard.press("PageDown")
            time.sleep(scroll_pause)

            # 等待页面加载（使用domcontentloaded而不是networkidle）
            try:
                await self.page.wait_for_load_state('domcontentloaded', timeout=5000)
            except:
                # 如果等待超时，继续执行
                pass

            # 提取当前页面数据
            html = await self.page.content()
            new_items = extract_func(html)

            # 实时保存新数据
            items_before = saved_count
            for item in new_items:
                item_id = item.get('id')
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)

                    # 实时保存到数据库
                    if realtime_save_enabled:
                        try:
                            success = repository.save_pin_immediately(item, query, session_id)
                            if success:
                                saved_count += 1
                                logger.debug(f"💾 实时保存Pin: {item_id} (总计: {saved_count})")
                                # 立即更新进度条
                                pbar.update(1)
                            else:
                                logger.error(f"❌ 保存失败: {item_id}")
                        except Exception as e:
                            logger.error(f"❌ 保存异常: {item_id}, 错误: {e}")
                    else:
                        # 降级到内存存储（不推荐）
                        logger.warning(f"⚠️  降级到内存存储: {item_id}")

                    # 检查是否达到目标
                    if saved_count >= target_count:
                        break

            # 检查是否有新数据
            items_after = saved_count
            if items_after > items_before:
                consecutive_no_new = 0
            else:
                consecutive_no_new += 1

            # 更新进度条描述
            pbar.set_postfix({
                '滚动': scroll_count,
                '连续无新': consecutive_no_new,
                '已保存': saved_count
            })

            # 硬性目标检查：达到目标数量立即退出
            if saved_count >= target_count:
                logger.info(f"✅ 已达到目标数量 {target_count}，立即退出")
                break

        # 关闭进度条
        pbar.close()

        # 记录停止原因
        if saved_count >= target_count:
            stop_reason = "达到目标数量"
        elif scroll_count >= max_scrolls:
            stop_reason = f"达到最大滚动次数"
        elif consecutive_no_new >= no_new_data_limit:
            stop_reason = f"连续无新数据"
        else:
            stop_reason = "未知原因"

        # 从数据库加载最终结果，确保数据一致性
        if realtime_save_enabled:
            try:
                final_pins = repository.load_pins_by_query(query, limit=target_count)
                logger.info(f"✅ 实时保存完成: {len(final_pins)}/{target_count} ({stop_reason})")
                return final_pins
            except Exception as e:
                logger.error(f"❌ 从数据库加载数据失败: {e}")
                return []
        else:
            logger.warning(f"⚠️  未启用实时保存，返回空列表 ({stop_reason})")
            return []

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
                # 检查是否是异步处理器
                if asyncio.iscoroutinefunction(handler):
                    # 创建任务来处理异步处理器
                    asyncio.create_task(handler(response))
                else:
                    handler(response)
            except Exception as e:
                logger.error(f"响应处理器出错: {e}")

    async def wait_for_load_state(self, state: str = "domcontentloaded", timeout: int = 30000):
        """等待页面加载状态"""
        try:
            await self.page.wait_for_load_state(state, timeout=timeout)
        except Exception as e:
            logger.debug(f"等待加载状态超时: {e}")

    async def get_html(self) -> str:
        """获取当前页面HTML"""
        return await self.page.content()

    def is_ready(self) -> bool:
        """检查浏览器是否就绪"""
        return self.page is not None

    def _get_reliable_user_agent(self) -> str:
        """获取可靠的User-Agent - 默认优化行为"""
        # 精选的真实浏览器User-Agent池，经过验证的高成功率UA
        RELIABLE_USER_AGENTS = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
        ]
        import random
        return random.choice(RELIABLE_USER_AGENTS)

    def _get_anti_detection_headers(self) -> Dict:
        """获取反检测Headers - 默认优化配置"""
        # 完整的反检测Headers，模拟真实浏览器环境
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Cache-Control": "max-age=0",
            "Sec-Ch-Ua": '"Chromium";v="138", "Not=A?Brand";v="8", "Google Chrome";v="138"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Ch-Ua-Platform-Version": '"15.0.0"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Connection": "keep-alive",
            "DNT": "1",
            "Sec-GPC": "1"
        }

    async def _human_like_delay(self, min_delay: float = 2.0, max_delay: float = 5.0):
        """人类行为延迟 - 默认优化行为"""
        import random
        # 使用正态分布模拟更真实的人类行为延迟
        delay = random.uniform(min_delay, max_delay)
        logger.debug(f"人类行为延迟: {delay:.2f}秒")
        await asyncio.sleep(delay)

    async def _establish_pinterest_session(self):
        """建立Pinterest会话 - 集成延迟优化"""
        try:
            logger.debug("建立Pinterest会话...")

            # 先访问主页（使用120秒超时）
            await self.page.goto("https://www.pinterest.com/", timeout=120000, wait_until="domcontentloaded")
            await self._human_like_delay(2.0, 4.0)  # 默认延迟

            # 模拟人类行为
            await self.page.evaluate("window.scrollBy(0, 500)")
            await self._human_like_delay(1.0, 2.0)  # 滚动后延迟

            logger.debug("Pinterest会话建立完成")

        except Exception as e:
            logger.warning(f"建立Pinterest会话失败: {e}")
            # 不抛出异常，继续执行
