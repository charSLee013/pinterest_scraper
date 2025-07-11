#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
智能采集引擎 - 激进重构版

统一的Pinterest数据采集引擎，自动选择最优策略
"""

import time
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
        debug: bool = False
    ):
        """初始化智能采集引擎
        
        Args:
            proxy: 代理服务器地址
            timeout: 超时时间
            cookie_path: Cookie文件路径
            debug: 调试模式
        """
        self.proxy = proxy
        self.timeout = timeout
        self.cookie_path = cookie_path
        self.debug = debug
        
        # 数据收集状态
        self.collected_pins = []
        self.seen_pin_ids = set()
        
        # 性能统计
        self.stats = {
            "total_scrolls": 0,
            "api_calls_intercepted": 0,
            "html_extractions": 0,
            "duplicates_filtered": 0
        }

    def scrape(
        self,
        query: Optional[str] = None,
        url: Optional[str] = None,
        target_count: int = 50
    ) -> List[Dict]:
        """智能采集Pinterest数据 - 实时去重版

        采用"实时去重并持续采集直到达到目标数量"的策略，确保最终返回
        用户指定数量的去重后唯一Pin。

        Args:
            query: 搜索关键词
            url: 直接URL
            target_count: 目标去重后唯一Pin数量

        Returns:
            采集到的去重后Pin数据列表
        """
        logger.info(f"开始智能采集，目标: {target_count} 个去重后唯一Pin")

        # 重置状态
        self._reset_state()

        # 构建目标URL
        target_url = self._build_url(query, url)
        if not target_url:
            logger.error("无效的查询参数")
            return []

        # 统一使用hybrid混合策略
        logger.info("使用统一的hybrid混合策略")

        # 实时去重采集主循环
        return self._adaptive_scrape_with_dedup(query, target_url, target_count)

    def _adaptive_scrape_with_dedup(
        self,
        query: Optional[str],
        target_url: str,
        target_count: int
    ) -> List[Dict]:
        """自适应采集，实时去重直到达到目标数量

        Args:
            query: 搜索关键词
            target_url: 目标URL
            target_count: 目标去重后数量

        Returns:
            去重后的Pin数据列表
        """
        collected_pins = []
        seen_ids = set()
        attempted_strategies = []
        max_rounds = 5  # 最大采集轮次
        max_total_raw_pins = target_count * 4  # 防止采集过多无用数据

        logger.info(f"开始自适应采集，最大轮次: {max_rounds}")

        for round_num in range(max_rounds):
            current_unique_count = len(collected_pins)
            remaining_needed = target_count - current_unique_count

            if remaining_needed <= 0:
                logger.info(f"已达到目标数量 {target_count}，采集完成")
                break

            logger.info(f"第 {round_num + 1} 轮采集，当前: {current_unique_count}，还需: {remaining_needed}")

            # 简化逻辑：直接使用剩余需要的数量，不进行复杂的"智能调整"
            # 用户要多少就采集多少，采集到目标数量就停止
            current_target = remaining_needed

            logger.info(f"本轮目标: {current_target} 个去重后唯一Pin")

            # 执行hybrid混合策略
            logger.info(f"执行hybrid策略，目标: {current_target}")
            new_pins = self._hybrid_scrape(query, current_target)
            attempted_strategies.append("hybrid")

            if not new_pins:
                logger.warning("hybrid策略未获取到数据")
                continue

            # 实时去重合并
            before_merge = len(collected_pins)
            collected_pins, new_unique_count = self._merge_and_deduplicate_incremental(
                collected_pins, new_pins, seen_ids
            )

            logger.info(f"本轮新增唯一Pin: {new_unique_count}，累计: {len(collected_pins)}")

            # 安全检查：防止采集过多数据
            total_raw_pins = len(collected_pins) + len(new_pins)
            if total_raw_pins > max_total_raw_pins:
                logger.warning(f"采集数据量过大 ({total_raw_pins})，停止采集")
                break

            # 检查去重率，如果过高则停止
            if round_num > 0 and new_unique_count == 0:
                logger.warning("本轮未获得新的唯一Pin，可能数据源已枯竭")
                break

            # 如果已达到目标，提前退出
            if len(collected_pins) >= target_count:
                logger.info(f"已达到目标数量，提前结束采集")
                break

        final_count = len(collected_pins)
        logger.info(f"采集完成: {final_count}/{target_count} 个唯一Pin (使用策略: {', '.join(set(attempted_strategies))})")

        # 返回目标数量的Pin（如果超过了目标数量）
        return collected_pins[:target_count]

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

    def _select_round_strategy(
        self,
        initial_strategy: str,
        attempted_strategies: List[str],
        remaining_needed: int,
        round_num: int
    ) -> str:
        """为当前轮次选择最佳策略 - 统一使用hybrid策略

        Args:
            initial_strategy: 初始策略
            attempted_strategies: 已尝试的策略
            remaining_needed: 还需要的数量
            round_num: 当前轮次

        Returns:
            选择的策略名称
        """
        # 统一使用hybrid策略，适用于所有轮次和数据量级
        return "hybrid"

    def _execute_strategy(
        self,
        strategy: str,
        query: Optional[str],
        target_url: str,
        target_count: int
    ) -> List[Dict]:
        """执行指定的采集策略

        Args:
            strategy: 策略名称
            query: 搜索关键词
            target_url: 目标URL
            target_count: 目标数量

        Returns:
            采集到的Pin数据列表
        """
        try:
            if strategy == "simple":
                return self._simple_scrape(target_url, target_count)
            elif strategy == "enhanced":
                return self._enhanced_scrape(target_url, target_count)
            elif strategy == "hybrid":
                return self._hybrid_scrape(query, target_count)
            else:
                logger.error(f"未知策略: {strategy}")
                return []
        except Exception as e:
            logger.error(f"执行策略 {strategy} 时出错: {e}")
            return []

    def _merge_and_deduplicate_incremental(
        self,
        existing_pins: List[Dict],
        new_pins: List[Dict],
        seen_ids: set
    ) -> tuple[List[Dict], int]:
        """增量合并和去重Pin数据

        Args:
            existing_pins: 已有的Pin数据
            new_pins: 新采集的Pin数据
            seen_ids: 已见过的Pin ID集合

        Returns:
            (合并后的Pin列表, 新增唯一Pin数量)
        """
        merged_pins = existing_pins.copy()
        new_unique_count = 0

        for pin in new_pins:
            pin_id = pin.get('id')
            if pin_id and pin_id not in seen_ids:
                seen_ids.add(pin_id)
                merged_pins.append(pin)
                new_unique_count += 1

        return merged_pins, new_unique_count

    def _select_strategy(self, target_count: int) -> str:
        """智能选择采集策略 - 统一使用hybrid策略

        Args:
            target_count: 目标数量

        Returns:
            策略名称
        """
        # 统一使用hybrid策略，适用于所有数据量级
        return "hybrid"

    def _simple_scrape(self, url: str, target_count: int) -> List[Dict]:
        """简单采集策略 - 基础滚动

        策略说明：
        - 滚动页面直到连续无新数据
        - target_count用作安全上限，不是硬性目标
        - 真实停止条件：连续无新数据 OR 达到安全上限
        """
        browser = BrowserManager(
            proxy=self.proxy,
            timeout=self.timeout,
            cookie_path=self.cookie_path,
            headless=True  # 始终使用无头模式，避免显示浏览器窗口
        )

        try:
            if not browser.start():
                return []

            if not browser.navigate(url):
                return []

            time.sleep(config.INITIAL_WAIT_TIME)

            # 滚动策略：真实的"连续无新数据"退出逻辑
            # 基于目标数量动态设置滚动次数，移除硬编码限制
            min_scrolls = 10  # 最少滚动次数
            max_scrolls = max(target_count * 3, min_scrolls)  # 基于目标数量动态调整，移除200的硬编码限制

            # 连续无新数据的限制：真实反映Pinterest页面的加载特性
            no_new_data_limit = 10  # 连续10次无新数据才停止，更符合实际情况

            logger.info(f"滚动策略: 连续{no_new_data_limit}次无新数据停止，最大滚动{max_scrolls}次")

            # 简单滚动收集
            pins = browser.scroll_and_collect(
                target_count=target_count,
                extract_func=extract_pins_from_html,
                max_scrolls=max_scrolls,
                scroll_pause=1.5,
                no_new_data_limit=no_new_data_limit
            )

            self.stats["total_scrolls"] = max_scrolls
            self.stats["html_extractions"] = len(pins)

            return pins

        except Exception as e:
            logger.error(f"简单采集出错: {e}")
            return []
        finally:
            browser.stop()

    def _enhanced_scrape(self, url: str, target_count: int) -> List[Dict]:
        """增强采集策略 - 网络拦截 + 深度滚动"""
        logger.info(f"执行增强采集策略，目标: {target_count}")

        # 创建网络拦截器，传入目标数量用于进度条
        interceptor = NetworkInterceptor(max_cache_size=target_count * 2, verbose=False, target_count=target_count)

        browser = BrowserManager(
            proxy=self.proxy,
            timeout=self.timeout,
            cookie_path=self.cookie_path,
            headless=True,  # 始终使用无头模式，避免显示浏览器窗口
            enable_network_interception=True
        )
        
        try:
            if not browser.start():
                return []
            
            # 添加网络拦截处理器
            browser.add_request_handler(interceptor._handle_request)
            browser.add_response_handler(interceptor._handle_response)
            
            if not browser.navigate(url):
                return []
            
            time.sleep(config.INITIAL_WAIT_TIME)

            # 深度滚动直到达到目标
            consecutive_no_new = 0
            max_consecutive = 20  # 增加耐心，避免过早停止
            scroll_count = 0
            max_scrolls = max(target_count // 2, 50)  # 基于目标数量动态调整，移除300的硬编码限制

            logger.info(f"开始深度滚动，最大滚动: {max_scrolls}, 无新数据限制: {max_consecutive}")

            while (len(interceptor.extracted_pins) < target_count and
                   consecutive_no_new < max_consecutive and
                   scroll_count < max_scrolls):

                pins_before = len(interceptor.extracted_pins)

                # 滚动页面
                browser.page.evaluate("window.scrollBy(0, window.innerHeight)")
                time.sleep(2)
                browser.wait_for_load_state('networkidle', timeout=10000)

                pins_after = len(interceptor.extracted_pins)
                scroll_count += 1

                if pins_after > pins_before:
                    consecutive_no_new = 0
                else:
                    consecutive_no_new += 1

            # 关闭进度条
            interceptor.close_progress_bar()

            # 记录停止原因
            if len(interceptor.extracted_pins) >= target_count:
                stop_reason = "达到目标数量"
            elif scroll_count >= max_scrolls:
                stop_reason = f"达到最大滚动次数 ({max_scrolls})"
            elif consecutive_no_new >= max_consecutive:
                stop_reason = f"连续 {consecutive_no_new} 次无新数据"
            else:
                stop_reason = "未知原因"

            collected_count = len(interceptor.extracted_pins)
            logger.info(f"增强采集完成: {collected_count}/{target_count} ({stop_reason})")

            self.stats["total_scrolls"] = scroll_count
            self.stats["api_calls_intercepted"] = len(interceptor.api_responses)

            return list(interceptor.extracted_pins)[:target_count]
            
        except Exception as e:
            logger.error(f"增强采集出错: {e}")
            return []
        finally:
            browser.stop()

    def _hybrid_scrape(self, query: str, target_count: int) -> List[Dict]:
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

        # 第一阶段直接使用用户指定的目标数量，设置合理的安全下限
        max_first_phase = max(target_count, 100)  # 使用target_count，最小100作为安全下限
        base_pins = self._simple_scrape(search_url, max_first_phase)

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
                related_pins = self._scrape_pin_detail_with_queue(pin_id, target_count - len(all_pins))

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

    def _scrape_pin_detail_with_queue(self, pin_id: str, max_count: int = 50) -> List[Dict]:
        """采集单个Pin详情页的相关推荐 - 带队列管理版本

        实现策略：
        1. 访问Pin详情页
        2. 滚动收集相关推荐Pin
        3. 连续3次无新数据时停止
        """
        pin_url = f"https://www.pinterest.com/pin/{pin_id}/"

        interceptor = NetworkInterceptor(max_cache_size=max_count * 2, verbose=False, target_count=0)
        browser = BrowserManager(
            proxy=self.proxy,
            timeout=30,
            cookie_path=self.cookie_path,
            headless=True,
            enable_network_interception=True
        )

        try:
            if not browser.start():
                return []

            browser.add_request_handler(interceptor._handle_request)
            browser.add_response_handler(interceptor._handle_response)

            if not browser.navigate(pin_url):
                return []

            time.sleep(2)

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
                browser.page.evaluate("window.scrollBy(0, window.innerHeight)")
                time.sleep(1.5)
                scroll_count += 1

                # 等待页面加载
                try:
                    browser.page.wait_for_load_state('networkidle', timeout=3000)
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
            browser.stop()

    def _scrape_pin_detail(self, pin_id: str, max_count: int = 50) -> List[Dict]:
        """采集单个Pin详情页的相关推荐 - 兼容性方法"""
        return self._scrape_pin_detail_with_queue(pin_id, max_count)

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
        self.stats = {
            "total_scrolls": 0,
            "api_calls_intercepted": 0,
            "html_extractions": 0,
            "duplicates_filtered": 0
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
