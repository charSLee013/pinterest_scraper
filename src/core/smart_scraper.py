#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
智能采集引擎 - 激进重构版

统一的Pinterest数据采集引擎，自动选择最优策略
"""

import time
import asyncio
from typing import Dict, List, Optional, Set
from collections import deque

from loguru import logger
from tqdm import tqdm

from .browser_manager import BrowserManager
from .parser import extract_pins_from_html
from ..utils.network_interceptor import NetworkInterceptor, PinDataExtractor
from . import config


class SmartScraper:
    """智能Pinterest采集引擎

    统一使用hybrid混合采集策略，适用于所有数据量级

    策略特点：
    - 统一策略：所有数据量级都使用hybrid策略
    - 智能调整：根据目标数量动态调整采集参数
    - 多阶段采集：关键词搜索 + Pin详情页深度扩展
    """

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = config.DEFAULT_TIMEOUT,
        cookie_path: Optional[str] = None,
        debug: bool = False,
        repository=None,
        session_id: Optional[str] = None
    ):
        """初始化智能采集引擎

        Args:
            proxy: 代理服务器地址
            timeout: 超时时间
            cookie_path: Cookie文件路径
            debug: 调试模式
            repository: Repository实例，用于实时保存
            session_id: 会话ID，用于会话追踪
        """
        self.proxy = proxy
        self.timeout = timeout
        self.cookie_path = cookie_path
        self.debug = debug

        # 实时保存相关
        self.repository = repository
        self.session_id = session_id
        self._interrupt_requested = False
        self._saved_count = 0  # 已保存的Pin数量

        # 数据收集状态
        self.collected_pins = []
        self.seen_pin_ids = set()

        # 性能统计
        self.stats = {
            "total_scrolls": 0,
            "api_calls_intercepted": 0,
            "html_extractions": 0,
            "duplicates_filtered": 0,
            "pins_saved_realtime": 0  # 实时保存的Pin数量
        }

    async def scrape(
        self,
        query: Optional[str] = None,
        url: Optional[str] = None,
        target_count: int = 50,
        repository=None,
        session_id: Optional[str] = None
    ) -> List[Dict]:
        """智能采集Pinterest数据 - 实时去重版

        采用"实时去重并持续采集直到达到目标数量"的策略，确保最终返回
        用户指定数量的去重后唯一Pin。

        Args:
            query: 搜索关键词
            url: 直接URL
            target_count: 目标去重后唯一Pin数量
            repository: Repository实例，用于实时保存（优先级高于构造函数参数）
            session_id: 会话ID，用于会话追踪（优先级高于构造函数参数）

        Returns:
            采集到的去重后Pin数据列表
        """
        # 更新实时保存参数（方法参数优先级高于构造函数参数）
        if repository is not None:
            self.repository = repository
        if session_id is not None:
            self.session_id = session_id

        logger.info(f"开始智能采集，目标: {target_count} 个去重后唯一Pin")
        if self.repository:
            logger.info("启用实时保存模式")

        # 记录采集开始时的基准数量（在重置状态之前）
        self._baseline_count = self._get_saved_count_from_db(query) if self.repository else 0
        logger.debug(f"📊 采集基准: 数据库中已有 {self._baseline_count} 个Pin")

        # 重置状态（但保留基准数量信息）
        self._reset_state()

        # 恢复基准数量（因为_reset_state会重置它）
        self._baseline_count = self._get_saved_count_from_db(query) if self.repository else 0

        # 构建目标URL
        target_url = self._build_url(query, url)
        if not target_url:
            logger.error("无效的查询参数")
            return []

        # 统一使用hybrid混合策略
        logger.info("使用统一的hybrid混合策略")

        try:
            # 实时去重采集主循环
            return await self._adaptive_scrape_with_dedup(query, target_url, target_count)
        except KeyboardInterrupt:
            logger.warning("检测到用户中断，保存已采集数据...")
            await self._handle_interrupt(query)
            raise

    async def _adaptive_scrape_with_dedup(
        self,
        query: Optional[str],
        target_url: str,
        target_count: int
    ) -> List[Dict]:
        """自适应采集，基于数据库的实时去重直到达到目标数量

        Args:
            query: 搜索关键词
            target_url: 目标URL
            target_count: 目标去重后数量

        Returns:
            去重后的Pin数据列表（从数据库加载）
        """
        attempted_strategies = []
        max_rounds = 5  # 最大采集轮次

        logger.info(f"开始基于数据库的自适应采集，最大轮次: {max_rounds}")

        for round_num in range(max_rounds):
            # 检查中断请求
            if self._interrupt_requested:
                logger.info("检测到中断请求，停止采集")
                break

            # 从数据库获取当前已保存的Pin数量
            current_total_count = self._get_saved_count_from_db(query)
            current_new_count = current_total_count - self._baseline_count  # 本次采集新增的数量
            remaining_needed = target_count - current_new_count

            logger.debug(f"🔢 采集状态: 总数={current_total_count}, 基准={self._baseline_count}, 新增={current_new_count}, 目标={target_count}, 还需={remaining_needed}")

            if remaining_needed <= 0:
                logger.info(f"已达到目标数量 {target_count}，采集完成")
                break

            logger.info(f"第 {round_num + 1} 轮采集，总数: {current_total_count}，新增: {current_new_count}，还需: {remaining_needed}")

            # 简化逻辑：直接使用剩余需要的数量
            current_target = remaining_needed

            logger.info(f"本轮目标: {current_target} 个去重后唯一Pin")

            # 执行hybrid混合策略
            logger.info(f"执行hybrid策略，目标: {current_target}")
            new_pins = await self._hybrid_scrape(query, current_target)
            attempted_strategies.append("hybrid")

            if not new_pins:
                logger.warning("hybrid策略未获取到数据")
                continue

            # 实时保存到数据库（去重在数据库层面处理）
            new_unique_count = await self._save_pins_to_db(new_pins, query)

            logger.info(f"本轮新增唯一Pin: {new_unique_count}")

            # 检查去重率，如果过高则停止
            if round_num > 0 and new_unique_count == 0:
                logger.warning("本轮未获得新的唯一Pin，可能数据源已枯竭")
                break

            # 重新检查数据库中的新增数量
            current_total_count = self._get_saved_count_from_db(query)
            current_new_count = current_total_count - self._baseline_count
            if current_new_count >= target_count:
                logger.info(f"已达到目标数量，提前结束采集")
                break

        # 从数据库加载最终结果（只返回本次采集的目标数量）
        all_pins = self._load_pins_from_db(query, None)  # 加载所有Pin
        final_pins = all_pins[-target_count:] if len(all_pins) >= target_count else all_pins  # 取最新的target_count个
        final_count = len(final_pins)

        logger.info(f"采集完成: {final_count}/{target_count} 个唯一Pin (使用策略: {', '.join(set(attempted_strategies))})")

        return final_pins

    def _estimate_dedup_rate(self, collected_pins: List[Dict], round_num: int) -> float:
        """估算去重率，用于调整采集目标

        Args:
            collected_pins: 已收集的Pin数据
            round_num: 当前轮次

        Returns:
            预估去重率（0-100）
        """
        if round_num == 0 or len(collected_pins) < 10:
            # 首轮或数据太少时，使用保守估计
            return 20.0  # 假设20%的去重率

        # 基于已有数据估算去重率
        # 这里简化处理，实际可以根据数据源特征进行更精确的估算
        if len(collected_pins) < 100:
            return 25.0
        elif len(collected_pins) < 500:
            return 30.0
        else:
            return 35.0  # 数据量大时去重率通常更高

    def _calculate_adjusted_target(self, remaining_needed: int, estimated_dedup_rate: float) -> int:
        """根据预估去重率计算调整后的采集目标

        Args:
            remaining_needed: 还需要的唯一Pin数量
            estimated_dedup_rate: 预估去重率

        Returns:
            调整后的采集目标
        """
        # 根据去重率调整目标，确保有足够的原始数据
        multiplier = 1.0 / (1.0 - estimated_dedup_rate / 100.0)
        adjusted_target = int(remaining_needed * multiplier * 1.2)  # 额外20%缓冲

        # 设置合理的上下限
        min_target = remaining_needed
        max_target = remaining_needed * 5  # 最多5倍

        return max(min_target, min(adjusted_target, max_target))





    async def _save_pins_to_db(self, pins: List[Dict], query: Optional[str]) -> int:
        """将Pin数据保存到数据库，返回新增的唯一Pin数量

        Args:
            pins: 要保存的Pin数据列表
            query: 搜索关键词

        Returns:
            新增的唯一Pin数量
        """
        if not self.repository or not query:
            logger.warning("无Repository或query，无法保存Pin数据")
            return 0

        new_unique_count = 0

        for pin in pins:
            pin_id = pin.get('id')
            if not pin_id:
                continue

            try:
                # 检查Pin是否已存在
                if not self._is_pin_exists_in_db(pin_id, query):
                    # 直接保存到数据库（不使用缓冲区）
                    success = self.repository.save_pins_batch([pin], query, self.session_id)
                    if success:
                        new_unique_count += 1
                        self._saved_count += 1
                        self.stats["pins_saved_realtime"] += 1
                        logger.debug(f"保存新Pin到数据库: {pin_id} (总计: {self._saved_count})")
                    else:
                        logger.error(f"保存Pin到数据库失败: {pin_id}")
                else:
                    logger.debug(f"Pin已存在，跳过: {pin_id}")

            except Exception as e:
                logger.error(f"保存Pin时出错: {pin_id}, 错误: {e}")

            # 检查中断请求
            if self._interrupt_requested:
                logger.info(f"检测到中断请求，停止保存Pin")
                break

        return new_unique_count

    def _get_saved_count_from_db(self, query: Optional[str]) -> int:
        """从数据库获取已保存的Pin数量"""
        if not self.repository or not query:
            return 0

        try:
            pins = self.repository.load_pins_by_query(query, limit=None)
            return len(pins)
        except Exception as e:
            logger.error(f"从数据库获取Pin数量失败: {e}")
            return 0

    def _load_pins_from_db(self, query: Optional[str], limit: int) -> List[Dict]:
        """从数据库加载Pin数据"""
        if not self.repository or not query:
            return []

        try:
            return self.repository.load_pins_by_query(query, limit=limit)
        except Exception as e:
            logger.error(f"从数据库加载Pin数据失败: {e}")
            return []

    def _is_pin_exists_in_db(self, pin_id: str, query: str) -> bool:
        """检查Pin是否已存在于数据库中"""
        if not self.repository:
            return False

        try:
            # 使用简单的查询检查Pin是否存在
            pins = self.repository.load_pins_by_query(query, limit=None)
            return any(pin.get('id') == pin_id for pin in pins)
        except Exception as e:
            logger.error(f"检查Pin是否存在时出错: {e}")
            return False







    async def _hybrid_scrape(self, query: str, target_count: int) -> List[Dict]:
        """混合采集策略 - 搜索 + Pin详情页深度扩展

        策略说明：
        1. 第一阶段：关键词搜索，滚动直到连续3次无新数据
        2. 第二阶段：Pin详情页深度采集，每个Pin详情页滚动直到连续3次无新数据
        3. 循环第二阶段，直到连续30个Pin都无法获取新数据才退出
        """
        logger.info(f"执行混合采集策略，目标: {target_count}")

        # 第一阶段：关键词搜索采集
        search_url = f"https://www.pinterest.com/search/pins/?q={query}"
        logger.info(f"第一阶段：关键词搜索 - {search_url}")
        logger.info(f"第一阶段目标: {target_count} 个去重后唯一Pin")

        # 第一阶段直接使用用户指定的目标数量
        # 移除硬编码的100个Pin下限，严格按照用户指定数量采集
        base_pins = await self._search_phase_scrape(search_url, target_count)

        if len(base_pins) >= target_count:
            logger.info(f"第一阶段已达到目标，获得 {len(base_pins)} 个Pin")
            return base_pins[:target_count]

        logger.info(f"第一阶段完成，获得 {len(base_pins)} 个Pin，开始Pin详情页深度扩展")
        
        # 第二阶段：Pin详情页深度扩展
        all_pins = list(base_pins)
        pin_queue = deque([pin.get('id') for pin in base_pins if pin.get('id')])
        visited_pins = set()
        no_new_data_streak = 0
        max_no_new_data_streak = 30  # 连续30个Pin无新数据才退出

        logger.info(f"第二阶段：Pin详情页深度扩展，初始队列: {len(pin_queue)} 个Pin")

        # 创建进度条
        pbar = tqdm(total=target_count, desc="深度采集", unit="pins",
                   initial=len(all_pins), leave=False)

        try:
            while pin_queue and len(all_pins) < target_count and no_new_data_streak < max_no_new_data_streak:
                pin_id = pin_queue.popleft()

                # 跳过已访问的Pin
                if pin_id in visited_pins:
                    continue

                visited_pins.add(pin_id)

                # 采集Pin详情页的相关推荐
                related_pins = await self._scrape_pin_detail_with_queue(pin_id, target_count - len(all_pins))

                if related_pins:
                    # 有新数据，重置计数器
                    no_new_data_streak = 0
                    pins_before = len(all_pins)

                    # 去重添加新Pin
                    for related_pin in related_pins:
                        if len(all_pins) >= target_count:
                            break

                        related_id = related_pin.get('id')
                        if related_id and related_id not in self.seen_pin_ids:
                            self.seen_pin_ids.add(related_id)
                            all_pins.append(related_pin)

                            # 将新Pin加入队列用于进一步扩展
                            if related_id not in visited_pins:
                                pin_queue.append(related_id)

                    new_pins_count = len(all_pins) - pins_before
                    pbar.update(new_pins_count)
                    pbar.set_postfix({
                        "队列": len(pin_queue),
                        "无新数据": no_new_data_streak,
                        "当前Pin": pin_id[:8]
                    })

                    logger.debug(f"Pin {pin_id} 获得 {new_pins_count} 个新Pin，队列剩余: {len(pin_queue)}")
                else:
                    # 无新数据，增加计数器
                    no_new_data_streak += 1
                    pbar.set_postfix({
                        "队列": len(pin_queue),
                        "无新数据": no_new_data_streak,
                        "当前Pin": pin_id[:8]
                    })

                    logger.debug(f"Pin {pin_id} 无新数据，连续无新数据: {no_new_data_streak}/{max_no_new_data_streak}")

        finally:
            pbar.close()

        # 记录停止原因
        if len(all_pins) >= target_count:
            stop_reason = "达到目标数量"
        elif no_new_data_streak >= max_no_new_data_streak:
            stop_reason = f"连续 {no_new_data_streak} 个Pin无新数据"
        elif not pin_queue:
            stop_reason = "Pin队列已空"
        else:
            stop_reason = "未知原因"

        actual_collected = len(all_pins)
        logger.info(f"混合采集完成: {actual_collected}/{target_count} ({stop_reason})")
        logger.info(f"混合策略详情: 第一阶段 {len(base_pins)} + 第二阶段 {actual_collected - len(base_pins)} = 总计 {actual_collected}")

        # 返回实际采集到的所有Pin，不截断
        return all_pins

    async def _scrape_pin_detail_with_queue(self, pin_id: str, max_count: int = 50) -> List[Dict]:
        """采集单个Pin详情页的相关推荐 - 带队列管理版本

        实现策略：
        1. 访问Pin详情页
        2. 滚动收集相关推荐Pin
        3. 连续3次无新数据时停止
        """
        pin_url = f"https://www.pinterest.com/pin/{pin_id}/"

        # 使用异步上下文管理器确保资源正确清理
        async with NetworkInterceptor(max_cache_size=max_count * 2, verbose=False, target_count=0) as interceptor:
            browser = BrowserManager(
                proxy=self.proxy,
                timeout=30,
                cookie_path=self.cookie_path,
                headless=True,
                enable_network_interception=True
            )

            try:
                if not await browser.start():
                    return []

                browser.add_request_handler(interceptor._handle_request)
                browser.add_response_handler(interceptor._handle_response)

                if not await browser.navigate(pin_url):
                    return []

                # 页面加载后的人类行为延迟
                import random
                await asyncio.sleep(random.uniform(2.0, 4.0))

                # 滚动获取相关推荐，直到连续3次无新数据
                consecutive_no_new = 0
                max_consecutive = 3
                scroll_count = 0
                max_scrolls = 20  # 最大滚动次数

                while (len(interceptor.extracted_pins) < max_count and
                       consecutive_no_new < max_consecutive and
                       scroll_count < max_scrolls):

                    pins_before = len(interceptor.extracted_pins)

                    # 滚动页面
                    await browser.page.evaluate("window.scrollBy(0, window.innerHeight)")
                    # 滚动后的人类行为延迟
                    await asyncio.sleep(random.uniform(1.5, 3.0))
                    scroll_count += 1

                    # 等待页面加载
                    try:
                        await browser.page.wait_for_load_state('networkidle', timeout=3000)
                    except:
                        pass

                    pins_after = len(interceptor.extracted_pins)

                    if pins_after > pins_before:
                        consecutive_no_new = 0
                    else:
                        consecutive_no_new += 1

                result_pins = list(interceptor.extracted_pins)[:max_count]
                logger.debug(f"Pin {pin_id} 详情页采集: {len(result_pins)} 个相关Pin (滚动 {scroll_count} 次)")

                return result_pins

            except Exception as e:
                logger.debug(f"Pin详情页采集出错: {e}")
                return []
            finally:
                await browser.stop()
                # NetworkInterceptor会在async with退出时自动清理

    async def _scrape_pin_detail(self, pin_id: str, max_count: int = 50) -> List[Dict]:
        """采集单个Pin详情页的相关推荐 - 兼容性方法"""
        return await self._scrape_pin_detail_with_queue(pin_id, max_count)

    def _build_url(self, query: Optional[str], url: Optional[str]) -> Optional[str]:
        """构建目标URL"""
        if url:
            return url
        elif query:
            return f"https://www.pinterest.com/search/pins/?q={query}"
        else:
            return None

    def _reset_state(self):
        """重置采集状态"""
        self.collected_pins.clear()
        self.seen_pin_ids.clear()
        self._interrupt_requested = False
        self._saved_count = 0
        self._baseline_count = 0  # 采集基准数量
        self.stats = {
            "total_scrolls": 0,
            "api_calls_intercepted": 0,
            "html_extractions": 0,
            "duplicates_filtered": 0,
            "pins_saved_realtime": 0
        }

    def get_stats(self) -> Dict:
        """获取采集统计信息"""
        return self.stats.copy()

    def _deduplicate_pins(self, pins: List[Dict]) -> List[Dict]:
        """去重处理Pin数据

        Args:
            pins: Pin数据列表

        Returns:
            去重后的Pin数据列表
        """
        seen_ids = set()
        unique_pins = []
        duplicates_count = 0
        no_id_count = 0

        for pin in pins:
            pin_id = pin.get('id')
            if pin_id and pin_id not in seen_ids:
                seen_ids.add(pin_id)
                unique_pins.append(pin)
            elif pin_id:
                duplicates_count += 1
            else:
                no_id_count += 1

        # 详细的去重统计
        total_input = len(pins)
        if total_input > 0:
            dedup_rate = duplicates_count / total_input * 100
            if duplicates_count > 0:
                logger.debug(f"去重统计: 输入 {total_input}, 输出 {len(unique_pins)}, 重复 {duplicates_count} ({dedup_rate:.1f}%), 无ID {no_id_count}")
            self.stats["duplicates_filtered"] += duplicates_count

        return unique_pins

    async def _search_phase_scrape(self, url: str, target_count: int) -> List[Dict]:
        """搜索阶段采集 - 基础滚动采集

        Args:
            url: 搜索URL
            target_count: 目标数量

        Returns:
            采集到的Pin数据列表
        """
        browser = BrowserManager(
            proxy=self.proxy,
            timeout=self.timeout,
            cookie_path=self.cookie_path,
            headless=True
        )

        try:
            if not await browser.start():
                return []

            if not await browser.navigate(url):
                return []

            time.sleep(config.INITIAL_WAIT_TIME)

            # 滚动策略：基于目标数量动态调整
            min_scrolls = 10
            max_scrolls = max(target_count * 3, min_scrolls)
            no_new_data_limit = 10

            logger.debug(f"搜索阶段滚动策略: 连续{no_new_data_limit}次无新数据停止，最大滚动{max_scrolls}次")

            # 滚动收集
            pins = await browser.scroll_and_collect(
                target_count=target_count,
                extract_func=extract_pins_from_html,
                max_scrolls=max_scrolls,
                scroll_pause=1.5,
                no_new_data_limit=no_new_data_limit,
                initial_count=self._baseline_count
            )

            self.stats["total_scrolls"] = max_scrolls
            self.stats["html_extractions"] = len(pins)

            return pins

        except Exception as e:
            logger.error(f"搜索阶段采集出错: {e}")
            return []

    async def _handle_interrupt(self, query: Optional[str]):
        """处理用户中断，数据已实时保存"""
        if self.repository and query:
            try:
                logger.info(f"中断处理完成，已实时保存 {self._saved_count} 个Pin")

                # 更新会话状态为中断
                if self.session_id:
                    self.repository.update_session_status(
                        self.session_id, 'interrupted', self._saved_count
                    )
                    logger.info(f"会话状态已更新为中断: {self.session_id}")

            except Exception as e:
                logger.error(f"中断处理失败: {e}")
        else:
            logger.warning("无Repository或query，无法更新中断状态")

    def request_interrupt(self):
        """请求中断采集（用于外部调用）"""
        self._interrupt_requested = True
        logger.info("已请求中断采集")

    def get_saved_count(self) -> int:
        """获取已保存的Pin数量"""
        return self._saved_count

    async def close(self):
        """关闭智能采集器，清理浏览器资源"""
        try:
            logger.debug("正在清理智能采集器资源...")
            # SmartScraper本身不持有长期的浏览器实例
            # 浏览器实例在每个方法中创建和销毁
            # 这里主要是为了接口一致性
            logger.debug("智能采集器资源清理完成")
        except Exception as e:
            logger.error(f"清理智能采集器资源时出错: {e}")
