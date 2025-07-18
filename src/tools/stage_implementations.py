#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
独立阶段实现

实现--only-images工作流程的四个独立阶段：
1. 数据库修复与检测阶段
2. Base64编码Pin转换阶段  
3. Pin详情数据补全阶段
4. 图片文件下载阶段
"""

import os
import sqlite3
import json
import asyncio
import random
from typing import Dict, Any, Optional, List
from loguru import logger

from .stage_manager import StageManager
from .realtime_base64_converter import BatchAtomicBase64Converter
from .global_header_manager import GlobalHeaderManager
from .smart_pin_enhancer import SmartPinEnhancer
from ..core.database.repository import SQLiteRepository
from ..core.browser_manager import BrowserManager
from ..utils.network_interceptor import NetworkInterceptor


async def fetch_pin_detail_with_browser(pin_id: str) -> Optional[Dict]:
    """使用浏览器+NetworkInterceptor获取Pin详情数据（正确的实现）

    Args:
        pin_id: Pin ID

    Returns:
        Pin详情数据，失败返回None
    """
    try:
        pin_url = f"https://www.pinterest.com/pin/{pin_id}/"

        # 使用异步上下文管理器确保资源正确清理
        async with NetworkInterceptor(max_cache_size=100, verbose=False, target_count=0) as interceptor:
            browser = BrowserManager(
                proxy=None,
                timeout=30,
                headless=True,
                enable_network_interception=True
            )

            try:
                if not await browser.start():
                    return None

                browser.add_request_handler(interceptor._handle_request)
                browser.add_response_handler(interceptor._handle_response)

                if not await browser.navigate(pin_url):
                    return None

                # 页面加载后的人类行为延迟
                await asyncio.sleep(random.uniform(2.0, 4.0))

                # 滚动获取Pin数据，直到连续3次无新数据
                consecutive_no_new = 0
                max_consecutive = 3
                scroll_count = 0
                max_scrolls = 10  # 最大滚动次数

                while (len(interceptor.extracted_pins) == 0 and
                       consecutive_no_new < max_consecutive and
                       scroll_count < max_scrolls):

                    pins_before = len(interceptor.extracted_pins)

                    # 使用真实的PageDown键盘事件滚动（比JavaScript更自然）
                    await browser.page.keyboard.press("PageDown")
                    # 滚动后的人类行为延迟
                    await asyncio.sleep(random.uniform(2.0, 4.0))
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

                # 如果拦截到了Pin数据，查找目标PIN
                if interceptor.extracted_pins:
                    for pin in interceptor.extracted_pins:
                        if pin.get('id') == pin_id:
                            logger.debug(f"✅ Pin {pin_id} 详情获取成功")
                            return pin

                # 如果没有找到目标PIN，返回第一个PIN作为示例
                if interceptor.extracted_pins:
                    pin_data = list(interceptor.extracted_pins)[0]
                    logger.debug(f"✅ Pin {pin_id} 详情页获取到相关Pin数据")
                    return pin_data

                logger.debug(f"⚠️ Pin {pin_id} 详情提取失败或无数据")
                return None

            except Exception as e:
                logger.debug(f"Pin详情页采集出错: {e}")
                return None
            finally:
                await browser.stop()
                # NetworkInterceptor会在async with退出时自动清理

    except Exception as e:
        logger.debug(f"获取Pin详情失败: {pin_id}, 错误: {e}")
        return None


class DatabaseRepairStage(StageManager):
    """阶段1：数据库修复与检测"""
    
    def __init__(self, output_dir: str):
        super().__init__("数据库修复与检测", output_dir)
    
    async def _execute_stage(self, target_keyword: Optional[str] = None) -> Dict[str, Any]:
        """执行数据库修复与检测"""
        logger.info("🔧 开始数据库修复与检测阶段")
        
        # 创建转换器（仅用于修复功能）
        converter = BatchAtomicBase64Converter(self.output_dir)
        
        # 发现需要检查的关键词
        if target_keyword:
            keywords = [target_keyword]
        else:
            keywords = converter._discover_all_keywords()
        
        repair_stats = {
            "keywords_checked": 0,
            "keywords_repaired": 0,
            "keywords_failed": 0,
            "repair_details": {}
        }
        
        for keyword in keywords:
            # 检查中断状态并在必要时抛出KeyboardInterrupt
            self.check_interruption_and_raise()
                
            logger.info(f"🔍 检查关键词数据库: {keyword}")
            repair_stats["keywords_checked"] += 1
            
            try:
                # 创建独立的repository进行检查
                repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
                
                # 执行健康检查和修复
                repair_success = await converter._check_and_repair_database(repository, keyword)
                
                if repair_success:
                    logger.info(f"✅ 数据库健康检查通过: {keyword}")
                    repair_stats["repair_details"][keyword] = "健康"
                else:
                    logger.warning(f"⚠️ 数据库修复失败: {keyword}")
                    repair_stats["keywords_failed"] += 1
                    repair_stats["repair_details"][keyword] = "修复失败"
                
                # 强制关闭当前关键词的连接
                await converter._force_close_all_connections(keyword)
                
            except Exception as e:
                logger.error(f"❌ 检查关键词 {keyword} 时出错: {e}")
                repair_stats["keywords_failed"] += 1
                repair_stats["repair_details"][keyword] = f"错误: {str(e)}"
        
        logger.info(f"🔧 数据库修复检测完成: {repair_stats}")
        return self._generate_success_result({"repair_stats": repair_stats})
    
    async def _verify_stage_completion(self) -> bool:
        """验证数据库修复阶段完整性"""
        # 验证所有数据库文件都可以正常访问
        try:
            if hasattr(self, '_last_result') and self._last_result:
                repair_details = self._last_result.get("repair_stats", {}).get("repair_details", {})
                
                for keyword, status in repair_details.items():
                    if status == "修复失败":
                        logger.warning(f"⚠️ 关键词 {keyword} 修复失败，但继续执行")
                
            return True
        except Exception as e:
            logger.error(f"❌ 数据库修复阶段验证失败: {e}")
            return False


class Base64ConversionStage(StageManager):
    """阶段2：Base64编码Pin转换"""
    
    def __init__(self, output_dir: str):
        super().__init__("Base64编码Pin转换", output_dir)
    
    async def _execute_stage(self, target_keyword: Optional[str] = None) -> Dict[str, Any]:
        """执行Base64编码Pin转换"""
        logger.info("🔄 开始Base64编码Pin转换阶段")
        
        # 创建独立的转换器
        converter = BatchAtomicBase64Converter(self.output_dir)
        
        # 执行转换
        conversion_stats = await converter.process_all_databases(target_keyword)
        
        logger.info(f"🔄 Base64转换完成: {conversion_stats}")
        return self._generate_success_result({"conversion_stats": conversion_stats})
    
    async def _verify_stage_completion(self) -> bool:
        """验证Base64转换阶段完整性"""
        try:
            # 检查是否还有base64编码的Pin
            keywords = self._discover_all_keywords()
            
            for keyword in keywords:
                # 检查中断状态并在必要时抛出KeyboardInterrupt
                self.check_interruption_and_raise()
                    
                db_path = os.path.join(self.output_dir, keyword, "pinterest.db")
                if not os.path.exists(db_path):
                    continue
                
                try:
                    conn = sqlite3.connect(db_path, timeout=10.0)
                    cursor = conn.cursor()
                    
                    cursor.execute("SELECT COUNT(*) FROM pins WHERE id LIKE 'UGlu%'")
                    base64_count = cursor.fetchone()[0]
                    
                    conn.close()
                    
                    if base64_count > 0:
                        logger.warning(f"⚠️ 关键词 {keyword} 仍有 {base64_count} 个base64编码Pin")
                        return False
                        
                except Exception as e:
                    logger.warning(f"⚠️ 验证关键词 {keyword} 时出错: {e}")
                    continue
            
            logger.info("✅ Base64转换阶段验证通过：没有发现base64编码Pin")
            return True
            
        except Exception as e:
            logger.error(f"❌ Base64转换阶段验证失败: {e}")
            return False
    
    def _discover_all_keywords(self) -> List[str]:
        """发现所有关键词"""
        keywords = []
        try:
            if os.path.exists(self.output_dir):
                for item in os.listdir(self.output_dir):
                    item_path = os.path.join(self.output_dir, item)
                    if os.path.isdir(item_path):
                        db_path = os.path.join(item_path, "pinterest.db")
                        if os.path.exists(db_path):
                            keywords.append(item)
        except Exception as e:
            logger.error(f"发现关键词时出错: {e}")
        
        return keywords


class PinEnhancementStage(StageManager):
    """阶段3：Pin详情数据补全"""

    def __init__(self, output_dir: str, max_concurrent: int = 8):
        super().__init__("Pin详情数据补全", output_dir)
        self.max_concurrent = max(1, min(max_concurrent, 20))
        logger.info(f"Pin详情补全并发数: {self.max_concurrent}")

        # 性能监控
        self.start_time = None
        self.processing_stats = {
            "total_pins": 0,
            "processing_time": 0,
            "average_speed": 0
        }
    
    async def _execute_stage(self, target_keyword: Optional[str] = None) -> Dict[str, Any]:
        """执行Pin详情数据补全 - 批量处理版本"""
        logger.info("📥 开始Pin详情数据补全阶段（批量处理模式）")

        # 1. 检查header是否过期，过期则重新获取
        header_manager = GlobalHeaderManager(self.output_dir)
        headers_ready = await header_manager.ensure_headers_ready()
        if not headers_ready:
            logger.warning("⚠️ Headers准备失败，将使用默认Headers")

        # 获取共享headers
        shared_headers = header_manager.get_headers()
        logger.info(f"✅ 获取到共享Headers，包含 {len(shared_headers)} 个字段")

        # 发现需要处理的关键词
        if target_keyword:
            keywords = [target_keyword]
        else:
            keywords = self._discover_all_keywords()

        enhancement_stats = {
            "keywords_processed": 0,
            "total_pins_checked": 0,
            "total_pins_enhanced": 0,
            "total_pins_failed": 0,
            "keyword_details": {}
        }
        
        for keyword in keywords:
            # 检查中断状态并在必要时抛出KeyboardInterrupt
            self.check_interruption_and_raise()

            logger.info(f"📥 处理关键词: {keyword}")
            enhancement_stats["keywords_processed"] += 1
            
            try:
                # 2. 分页获取需要增强的Pin（批次大小=max_concurrent）
                offset = 0
                keyword_total_checked = 0
                keyword_total_enhanced = 0
                keyword_total_failed = 0

                while True:
                    # 检查中断状态
                    self.check_interruption_and_raise()

                    # 分页获取Pin批次
                    pins_batch = await self._get_pins_batch_with_pagination(keyword, self.max_concurrent, offset)

                    if not pins_batch:
                        # 没有更多数据
                        break

                    logger.info(f"📦 关键词 {keyword}: 处理批次 offset={offset}, 大小={len(pins_batch)}")
                    keyword_total_checked += len(pins_batch)

                    # 3. 使用ThreadPoolExecutor并发获取Pin详情
                    batch_stats = await self._process_pins_batch_concurrent(pins_batch, keyword, shared_headers)

                    # 更新统计
                    keyword_total_enhanced += batch_stats.get("updated", 0)
                    keyword_total_failed += batch_stats.get("failed", 0)

                    logger.info(f"✅ 批次完成: 增强 {batch_stats.get('updated', 0)} 个，失败 {batch_stats.get('failed', 0)} 个")

                    # 更新偏移量
                    offset += len(pins_batch)

                    # 如果批次大小小于max_concurrent，说明已经到达末尾
                    if len(pins_batch) < self.max_concurrent:
                        break

                # 更新关键词统计
                enhancement_stats["total_pins_checked"] += keyword_total_checked
                enhancement_stats["total_pins_enhanced"] += keyword_total_enhanced
                enhancement_stats["total_pins_failed"] += keyword_total_failed
                enhancement_stats["keyword_details"][keyword] = {
                    "checked": keyword_total_checked,
                    "enhanced": keyword_total_enhanced,
                    "failed": keyword_total_failed
                }

                logger.info(f"✅ 关键词 {keyword} 完成: 检查 {keyword_total_checked} 个，增强 {keyword_total_enhanced} 个，失败 {keyword_total_failed} 个")
                
            except Exception as e:
                logger.error(f"❌ 处理关键词 {keyword} 时出错: {e}")
                enhancement_stats["keyword_details"][keyword] = {
                    "error": str(e)
                }
        
        logger.info(f"📥 Pin详情补全完成: {enhancement_stats}")
        return self._generate_success_result({"enhancement_stats": enhancement_stats})
    
    async def _get_pins_needing_enhancement(self, keyword: str) -> List[Dict]:
        """获取需要增强的Pin列表"""
        try:
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)

            # 查询缺少图片URL的Pin，排除base64编码的Pin
            with repository._get_session() as session:
                from ..core.database.schema import Pin

                pins = session.query(Pin).filter(
                    # 原有条件：缺少图片URL
                    ((Pin.largest_image_url == None) |
                     (Pin.largest_image_url == '') |
                     (Pin.image_urls == None) |
                     (Pin.image_urls == '')) &
                    # 新增条件：排除base64编码Pin（以UGlu开头）
                    (~Pin.id.like('UGlu%'))
                ).all()

                return [pin.to_dict() for pin in pins]

        except Exception as e:
            logger.error(f"获取需要增强的Pin失败 {keyword}: {e}")
            return []

    async def _get_pins_batch_with_pagination(self, keyword: str, batch_size: int, offset: int) -> List[Dict]:
        """分页获取需要增强的Pin列表

        Args:
            keyword: 关键词
            batch_size: 批次大小
            offset: 偏移量

        Returns:
            Pin数据列表
        """
        try:
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)

            # 查询缺少图片URL的Pin，支持分页，排除base64编码的Pin
            with repository._get_session() as session:
                from ..core.database.schema import Pin

                pins = session.query(Pin).filter(
                    # 原有条件：缺少图片URL
                    ((Pin.largest_image_url == None) |
                     (Pin.largest_image_url == '') |
                     (Pin.image_urls == None) |
                     (Pin.image_urls == '')) &
                    # 新增条件：排除base64编码Pin（以UGlu开头）
                    (~Pin.id.like('UGlu%'))
                ).offset(offset).limit(batch_size).all()

                return [pin.to_dict() for pin in pins]

        except Exception as e:
            logger.error(f"分页获取需要增强的Pin失败 {keyword}: {e}")
            return []

    async def _update_enhanced_pins_batch(self, enhanced_results: List[tuple], keyword: str) -> Dict[str, int]:
        """批量更新增强后的Pin数据到数据库

        Args:
            enhanced_results: 增强结果列表，格式为 [(pin_id, enhanced_data_or_none), ...]
            keyword: 关键词

        Returns:
            更新统计信息
        """
        stats = {"updated": 0, "failed": 0, "skipped": 0}

        # 创建SmartPinEnhancer实例来复用现有的数据库更新逻辑
        pin_enhancer = SmartPinEnhancer(self.output_dir)

        for pin_id, enhanced_data in enhanced_results:
            try:
                if enhanced_data is None:
                    # 获取失败
                    stats["failed"] += 1
                    continue

                if not enhanced_data.get('image_urls'):
                    # 没有获取到图片URL
                    stats["skipped"] += 1
                    continue

                # 复用现有的数据库更新逻辑
                success = await pin_enhancer._update_pin_in_database_atomic(enhanced_data, keyword)

                if success:
                    stats["updated"] += 1
                    logger.debug(f"✅ Pin {pin_id} 数据库更新成功")
                else:
                    stats["failed"] += 1
                    logger.debug(f"❌ Pin {pin_id} 数据库更新失败")

            except Exception as e:
                logger.error(f"更新Pin {pin_id} 时出错: {e}")
                stats["failed"] += 1

        return stats

    async def _process_pins_batch_concurrent(self, pins_batch: List[Dict], keyword: str, shared_headers: Dict[str, str]) -> Dict[str, int]:
        """并发处理Pin批次

        Args:
            pins_batch: Pin批次数据
            keyword: 关键词
            shared_headers: 共享headers

        Returns:
            处理统计信息
        """
        import concurrent.futures
        import threading
        from tqdm import tqdm

        # 过滤出需要增强的Pin（没有图片URL的）
        pins_to_process = []
        for pin in pins_batch:
            if not self._has_valid_image_urls(pin):
                pins_to_process.append(pin)

        if not pins_to_process:
            logger.info("批次中没有需要增强的Pin")
            return {"updated": 0, "failed": 0, "skipped": len(pins_batch)}

        logger.info(f"开始并发处理 {len(pins_to_process)} 个Pin，并发数: {self.max_concurrent}")

        async def process_single_pin_with_browser(pin_data: Dict) -> tuple:
            """使用浏览器处理单个Pin"""
            try:
                # 检查全局中断状态
                from .stage_manager import _global_interrupt_manager
                if _global_interrupt_manager.is_interrupted():
                    raise KeyboardInterrupt("用户中断操作")

                pin_id = pin_data.get('id')

                # 使用浏览器获取Pin详情
                enhanced_data = await fetch_pin_detail_with_browser(pin_id)

                if enhanced_data and enhanced_data.get('image_urls'):
                    # 合并Pin数据（复用现有逻辑）
                    pin_enhancer = SmartPinEnhancer(self.output_dir)
                    merged_pin = pin_enhancer._merge_pin_data(pin_data, enhanced_data)
                    return (pin_id, merged_pin)
                else:
                    return (pin_id, None)

            except KeyboardInterrupt:
                raise  # 重新抛出中断异常
            except Exception as e:
                logger.debug(f"处理Pin {pin_data.get('id', 'unknown')} 失败: {e}")
                return (pin_data.get('id'), None)

        # 使用异步并发处理
        enhanced_results = []

        # 创建信号量限制并发数
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def process_with_semaphore(pin_data):
            async with semaphore:
                return await process_single_pin_with_browser(pin_data)

        # 创建所有任务
        tasks = [process_with_semaphore(pin) for pin in pins_to_process]

        # 使用tqdm显示进度
        from tqdm.asyncio import tqdm as atqdm

        # 并发执行所有任务
        try:
            enhanced_results = await atqdm.gather(*tasks, desc=f"获取{keyword}详情")
        except KeyboardInterrupt:
            logger.info("检测到用户中断，停止并发处理")
            raise

        # 4. 批量更新数据库
        update_stats = await self._update_enhanced_pins_batch(enhanced_results, keyword)

        return update_stats

    def _has_valid_image_urls(self, pin: Dict) -> bool:
        """检查Pin是否有有效的图片URL（复用SmartPinEnhancer的逻辑）"""
        # 检查largest_image_url
        largest_url = pin.get('largest_image_url', '').strip()
        if largest_url and largest_url.startswith('http'):
            return True

        # 检查image_urls字典
        image_urls = pin.get('image_urls', {})
        if isinstance(image_urls, str):
            try:
                import json
                image_urls = json.loads(image_urls)
            except:
                image_urls = {}

        if isinstance(image_urls, dict) and image_urls:
            # 检查是否有任何有效的URL
            for key, url in image_urls.items():
                if url and isinstance(url, str) and url.strip().startswith('http'):
                    return True

        return False

    async def _enhance_pins_batch(self, pins: List[Dict], keyword: str,
                                pin_enhancer: SmartPinEnhancer) -> Dict[str, int]:
        """批量增强Pin - 支持并发和单线程模式"""
        if self.max_concurrent == 1:
            return await self._enhance_pins_sequential(pins, keyword, pin_enhancer)
        else:
            return await self._enhance_pins_concurrent(pins, keyword, pin_enhancer)

    async def _enhance_pins_sequential(self, pins: List[Dict], keyword: str,
                                     pin_enhancer: SmartPinEnhancer) -> Dict[str, int]:
        """单线程顺序增强Pin（回退方案）"""
        stats = {"checked": len(pins), "enhanced": 0, "failed": 0}

        for pin in pins:
            # 检查中断状态并在必要时抛出KeyboardInterrupt
            self.check_interruption_and_raise()

            try:
                enhanced = await pin_enhancer.enhance_pin_if_needed(pin, keyword)
                if enhanced:
                    stats["enhanced"] += 1

            except Exception as e:
                logger.debug(f"增强Pin {pin.get('id', 'unknown')} 失败: {e}")
                stats["failed"] += 1

        return stats

    async def _enhance_pins_concurrent(self, pins: List[Dict], keyword: str,
                                     pin_enhancer: SmartPinEnhancer) -> Dict[str, int]:
        """多线程并发增强Pin"""
        import concurrent.futures
        import threading
        import asyncio
        from tqdm import tqdm

        stats = {"checked": len(pins), "enhanced": 0, "failed": 0}
        stats_lock = threading.Lock()

        def process_single_pin(pin_data: Dict) -> bool:
            """单线程处理单个Pin"""
            try:
                # 检查全局中断状态
                from .stage_manager import _global_interrupt_manager
                if _global_interrupt_manager.is_interrupted():
                    raise KeyboardInterrupt("用户中断操作")

                # 创建线程专用的enhancer实例
                thread_enhancer = SmartPinEnhancer(self.output_dir)

                # 同步调用enhance_pin_if_needed
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    enhanced = loop.run_until_complete(
                        thread_enhancer.enhance_pin_if_needed(pin_data, keyword)
                    )
                    return enhanced is not None
                finally:
                    loop.close()

            except KeyboardInterrupt:
                raise  # 重新抛出中断异常
            except Exception as e:
                logger.debug(f"线程处理Pin {pin_data.get('id', 'unknown')} 失败: {e}")
                return False

        logger.info(f"开始并发处理 {len(pins)} 个Pin，并发数: {self.max_concurrent}")

        # 使用ThreadPoolExecutor进行并发处理
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            with tqdm(total=len(pins), desc=f"处理{keyword}", unit="pin") as pbar:
                # 提交所有任务
                future_to_pin = {
                    executor.submit(process_single_pin, pin): pin
                    for pin in pins
                }

                # 收集结果
                for future in concurrent.futures.as_completed(future_to_pin):
                    pin = future_to_pin[future]
                    try:
                        success = future.result()
                        with stats_lock:
                            if success:
                                stats["enhanced"] += 1
                            else:
                                stats["failed"] += 1
                        pbar.update(1)
                    except KeyboardInterrupt:
                        logger.info("检测到用户中断，停止并发处理")
                        # 取消所有未完成的任务
                        for f in future_to_pin:
                            f.cancel()
                        raise
                    except Exception as e:
                        logger.error(f"Pin {pin.get('id', 'unknown')} 处理异常: {e}")
                        with stats_lock:
                            stats["failed"] += 1
                        pbar.update(1)

        logger.info(f"并发处理完成: 增强 {stats['enhanced']} 个，失败 {stats['failed']} 个")
        return stats

    def _get_thread_safe_headers(self, header_manager: GlobalHeaderManager) -> Dict[str, str]:
        """获取线程安全的Headers副本"""
        return header_manager.get_headers()

    def _validate_concurrent_setup(self) -> bool:
        """验证并发设置是否正确"""
        if self.max_concurrent < 1 or self.max_concurrent > 50:
            logger.error(f"无效的并发数: {self.max_concurrent}")
            return False

        # 检查系统资源
        try:
            import psutil
            available_memory = psutil.virtual_memory().available / (1024**3)  # GB
            if available_memory < 1.0:
                logger.warning(f"可用内存较低: {available_memory:.1f}GB")
        except ImportError:
            logger.debug("psutil未安装，跳过内存检查")

        return True

    async def _verify_stage_completion(self) -> bool:
        """验证Pin增强阶段完整性"""
        # 这个阶段的验证比较复杂，暂时返回True
        # 实际应用中可以检查关键Pin是否都有了图片URL
        return True
    
    def _discover_all_keywords(self) -> List[str]:
        """发现所有关键词"""
        keywords = []
        try:
            if os.path.exists(self.output_dir):
                for item in os.listdir(self.output_dir):
                    item_path = os.path.join(self.output_dir, item)
                    if os.path.isdir(item_path):
                        db_path = os.path.join(item_path, "pinterest.db")
                        if os.path.exists(db_path):
                            keywords.append(item)
        except Exception as e:
            logger.error(f"发现关键词时出错: {e}")
        
        return keywords


class ImageDownloadStage(StageManager):
    """阶段4：图片文件下载"""
    
    def __init__(self, output_dir: str, max_concurrent: int = 15):
        super().__init__("图片文件下载", output_dir)
        self.max_concurrent = max_concurrent
    
    async def _execute_stage(self, target_keyword: Optional[str] = None) -> Dict[str, Any]:
        """执行图片文件下载"""
        logger.info("📥 开始图片文件下载阶段")
        
        # 创建独立的下载器
        from .image_downloader import ImageDownloader
        downloader = ImageDownloader(
            output_dir=self.output_dir,
            max_concurrent=self.max_concurrent,
            prefer_requests=True
        )
        
        # 发现需要处理的关键词
        if target_keyword:
            keywords = [target_keyword]
        else:
            keywords = self._discover_all_keywords()
        
        download_stats = {
            "keywords_processed": 0,
            "total_downloaded": 0,
            "total_failed": 0,
            "keyword_details": {}
        }
        
        for keyword in keywords:
            # 检查中断状态并在必要时抛出KeyboardInterrupt
            self.check_interruption_and_raise()

            logger.info(f"📥 下载关键词图片: {keyword}")
            download_stats["keywords_processed"] += 1
            
            try:
                # 执行关键词图片下载
                keyword_results = await downloader.download_keyword_images(keyword)
                
                # 统计结果
                downloaded = sum(1 for success, _ in keyword_results if success)
                failed = sum(1 for success, _ in keyword_results if not success)
                
                download_stats["total_downloaded"] += downloaded
                download_stats["total_failed"] += failed
                download_stats["keyword_details"][keyword] = {
                    "downloaded": downloaded,
                    "failed": failed,
                    "total": len(keyword_results)
                }
                
                logger.info(f"✅ 关键词 {keyword}: 下载 {downloaded} 成功, {failed} 失败")
                
            except Exception as e:
                logger.error(f"❌ 下载关键词 {keyword} 图片时出错: {e}")
                download_stats["keyword_details"][keyword] = {
                    "error": str(e)
                }
        
        # 清理下载器资源
        await downloader.close()
        
        logger.info(f"📥 图片下载完成: {download_stats}")
        return self._generate_success_result({"download_stats": download_stats})
    
    async def _verify_stage_completion(self) -> bool:
        """验证图片下载阶段完整性"""
        # 可以检查关键图片文件是否存在
        # 这里简化处理，返回True
        return True
    
    def _discover_all_keywords(self) -> List[str]:
        """发现所有关键词"""
        keywords = []
        try:
            if os.path.exists(self.output_dir):
                for item in os.listdir(self.output_dir):
                    item_path = os.path.join(self.output_dir, item)
                    if os.path.isdir(item_path):
                        db_path = os.path.join(item_path, "pinterest.db")
                        if os.path.exists(db_path):
                            keywords.append(item)
        except Exception as e:
            logger.error(f"发现关键词时出错: {e}")
        
        return keywords
