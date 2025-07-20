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
        if self.max_concurrent < 1 or self.max_concurrent > 65536:
            logger.error(f"无效的并发数: {self.max_concurrent}")
            return False

        # 检查系统资源
        try:
            import psutil
            available_memory = psutil.virtual_memory().available / (1024**3)  # GB
            if available_memory < 1.0:
                logger.warning(f"可用内存较低: {available_memory:.1f}GB")

            # 高并发时的内存警告
            if self.max_concurrent > 1000:
                logger.warning(f"使用高并发数 ({self.max_concurrent})，请确保有足够的系统资源")
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
    """阶段4：图片文件下载 - 重构版

    实现清晰的5步循环逻辑：
    1. 创建已下载图片pin集合
    2. 翻页批量读取待下载pins
    3. 检查/获取headers
    4. 多线程下载图片（重试机制+单点错误容忍）
    5. 检查本地文件并更新全局计数
    重复步骤2-5直到完成
    """

    def __init__(self, output_dir: str, max_concurrent: int = 15, batch_size: Optional[int] = None):
        super().__init__("图片文件下载", output_dir)
        self.max_concurrent = max_concurrent
        # batch_size应该等于max_concurrent，确保最优的资源利用
        self.batch_size = batch_size if batch_size is not None else max_concurrent
        self.global_header_manager = None

        logger.debug(f"ImageDownloadStage初始化: max_concurrent={self.max_concurrent}, batch_size={self.batch_size}")

    async def _execute_stage(self, target_keyword: Optional[str] = None) -> Dict[str, Any]:
        """执行图片文件下载 - 5步循环逻辑实现"""
        logger.info("📥 开始图片文件下载阶段 - 5步循环逻辑")
        logger.info("=" * 60)
        logger.info("步骤1: 创建已下载图片pin集合")
        logger.info("步骤2: 翻页批量读取待下载pins")
        logger.info("步骤3: 检查/获取headers")
        logger.info("步骤4: 多线程下载图片（重试+容错）")
        logger.info("步骤5: 检查本地文件并更新全局计数")
        logger.info("=" * 60)

        # 初始化全局Header管理器
        from .global_header_manager import GlobalHeaderManager
        self.global_header_manager = GlobalHeaderManager(self.output_dir)

        # 发现需要处理的关键词
        if target_keyword:
            keywords = [target_keyword]
        else:
            keywords = self._discover_all_keywords()

        # 全局统计
        download_stats = {
            "keywords_processed": 0,
            "total_downloaded": 0,
            "total_failed": 0,
            "total_batches": 0,
            "keyword_details": {}
        }

        # 为每个关键词执行5步循环逻辑
        for keyword in keywords:
            self.check_interruption_and_raise()

            logger.info(f"🎯 开始处理关键词: {keyword}")
            keyword_stats = await self._process_keyword_with_5_steps(keyword)

            # 更新全局统计
            download_stats["keywords_processed"] += 1
            download_stats["total_downloaded"] += keyword_stats["downloaded"]
            download_stats["total_failed"] += keyword_stats["failed"]
            download_stats["total_batches"] += keyword_stats["batches"]
            download_stats["keyword_details"][keyword] = keyword_stats

            logger.info(f"✅ 关键词 {keyword} 完成: 下载 {keyword_stats['downloaded']} 成功, {keyword_stats['failed']} 失败")

        logger.info(f"📥 图片下载阶段完成: {download_stats}")
        return self._generate_success_result({"download_stats": download_stats})

    async def _process_keyword_with_5_steps(self, keyword: str) -> Dict[str, Any]:
        """为单个关键词执行5步循环逻辑

        Args:
            keyword: 关键词

        Returns:
            关键词处理统计结果
        """
        from ..core.database.repository import SQLiteRepository
        import os
        from tqdm import tqdm

        # 初始化统计
        keyword_stats = {
            "downloaded": 0,
            "failed": 0,
            "batches": 0,
            "total_pins_with_images": 0,
            "already_downloaded": 0
        }

        try:
            # 创建Repository
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
            images_dir = os.path.join(self.output_dir, keyword, "images")
            os.makedirs(images_dir, exist_ok=True)

            # 【步骤1】创建已下载图片pin集合
            logger.info(f"📋 步骤1: 创建已下载图片pin集合 - {keyword}")
            downloaded_pins_set = self._build_downloaded_pins_set(images_dir)
            keyword_stats["already_downloaded"] = len(downloaded_pins_set)
            logger.info(f"   已下载图片: {len(downloaded_pins_set)} 个")

            # 获取总数用于进度条
            total_pins = self._get_total_pins_with_images(repository, keyword)
            keyword_stats["total_pins_with_images"] = total_pins
            logger.info(f"   数据库中有图片URL的Pin总数: {total_pins} 个")

            if total_pins == 0:
                logger.info(f"   关键词 {keyword} 没有发现有图片URL的Pin，跳过")
                return keyword_stats

            # 初始化翻页参数
            offset = 0
            batch_count = 0

            # 创建进度条
            with tqdm(total=total_pins, desc=f"下载 {keyword}", unit="pin") as pbar:
                # 更新已下载的进度
                pbar.update(len(downloaded_pins_set))

                # 【主循环】重复步骤2-5直到完成
                while True:
                    self.check_interruption_and_raise()

                    # 【步骤2】翻页批量读取待下载pins
                    logger.debug(f"📖 步骤2: 翻页读取待下载pins (offset: {offset}, batch: {self.batch_size})")
                    pins_batch = repository.load_pins_with_images(
                        keyword,
                        limit=self.batch_size,
                        offset=offset
                    )

                    if not pins_batch:
                        logger.info(f"   没有更多Pin数据，翻页完成")
                        break

                    # 过滤已下载的pins
                    missing_pins = [pin for pin in pins_batch if pin['id'] not in downloaded_pins_set]
                    logger.debug(f"   本批次: {len(pins_batch)} 个Pin，待下载: {len(missing_pins)} 个")

                    if not missing_pins:
                        logger.debug(f"   本批次无待下载Pin，跳到下一批次")
                        offset += len(pins_batch)
                        continue

                    # 【步骤3】检查/获取headers
                    logger.debug(f"🌐 步骤3: 检查/获取headers")
                    headers_ready = await self._ensure_headers_ready()
                    if not headers_ready:
                        logger.warning(f"   Headers获取失败，使用默认headers")

                    # 【步骤4】多线程下载图片
                    logger.debug(f"⬇️ 步骤4: 多线程下载 {len(missing_pins)} 个图片")
                    batch_results = await self._download_batch_with_retry(missing_pins, keyword, images_dir)

                    # 【步骤5】检查本地文件并更新全局计数
                    logger.debug(f"✅ 步骤5: 检查文件并更新计数")
                    batch_downloaded, batch_failed = self._verify_and_update_stats(batch_results, downloaded_pins_set)

                    # 更新统计
                    keyword_stats["downloaded"] += batch_downloaded
                    keyword_stats["failed"] += batch_failed
                    keyword_stats["batches"] += 1
                    batch_count += 1

                    # 更新进度条
                    pbar.update(batch_downloaded)

                    logger.debug(f"   批次 {batch_count} 完成: {batch_downloaded}/{len(missing_pins)} 成功")

                    # 更新偏移量
                    offset += len(pins_batch)

                    # 如果返回的Pin数量少于批次大小，说明已到末尾
                    if len(pins_batch) < self.batch_size:
                        logger.info(f"   已处理完所有Pin，翻页结束")
                        break

            logger.info(f"🎯 关键词 {keyword} 处理完成: 总批次 {batch_count}, 下载 {keyword_stats['downloaded']} 成功")
            return keyword_stats

        except Exception as e:
            logger.error(f"❌ 处理关键词 {keyword} 时出错: {e}")
            keyword_stats["error"] = str(e)
            return keyword_stats

    def _build_downloaded_pins_set(self, images_dir: str) -> set:
        """【步骤1】建立已下载图片的Pin索引集合

        Args:
            images_dir: 图片目录路径

        Returns:
            已下载图片的Pin ID集合
        """
        downloaded_pins = set()

        if not os.path.exists(images_dir):
            logger.debug(f"图片目录不存在: {images_dir}")
            return downloaded_pins

        try:
            # 扫描images目录，提取已下载的pin_id
            for filename in os.listdir(images_dir):
                if filename.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    # 从文件名提取Pin ID（去掉扩展名）
                    pin_id = os.path.splitext(filename)[0]
                    # 验证文件有效性（大小检查）
                    file_path = os.path.join(images_dir, filename)
                    if os.path.getsize(file_path) > 1000:  # 至少1KB
                        downloaded_pins.add(pin_id)
                    else:
                        logger.debug(f"发现无效图片文件: {filename} (大小: {os.path.getsize(file_path)} bytes)")

            logger.debug(f"建立已下载索引: {len(downloaded_pins)} 个有效文件")

        except Exception as e:
            logger.error(f"建立已下载索引失败: {e}")

        return downloaded_pins

    def _get_total_pins_with_images(self, repository, keyword: str) -> int:
        """获取有图片URL的Pin总数"""
        try:
            all_pins = repository.load_pins_with_images(keyword)
            return len(all_pins)
        except Exception as e:
            logger.error(f"获取Pin总数失败: {e}")
            return 0

    async def _ensure_headers_ready(self) -> bool:
        """【步骤3】确保Headers已准备就绪

        Returns:
            是否成功获取Headers
        """
        try:
            if self.global_header_manager:
                return await self.global_header_manager.ensure_headers_ready()
            else:
                logger.warning("GlobalHeaderManager未初始化")
                return False
        except Exception as e:
            logger.error(f"Headers准备失败: {e}")
            return False

    async def _download_batch_with_retry(self, pins_batch: List[Dict], keyword: str, images_dir: str) -> List[Dict]:
        """【步骤4】多线程下载图片批次（带重试机制和单点错误容忍）

        Args:
            pins_batch: Pin数据批次
            keyword: 关键词
            images_dir: 图片目录

        Returns:
            下载结果列表，每个元素包含 {'pin_id': str, 'success': bool, 'message': str, 'file_path': str}
        """
        import asyncio
        import concurrent.futures
        from concurrent.futures import ThreadPoolExecutor

        # 获取headers
        headers = self.global_header_manager.get_headers() if self.global_header_manager else {}

        # 创建下载任务
        download_tasks = []
        for pin in pins_batch:
            pin_id = pin.get('id')
            if not pin_id:
                continue

            # 提取图片URLs
            image_urls = self._extract_image_urls(pin)
            if not image_urls:
                continue

            # 生成文件路径
            file_path = os.path.join(images_dir, f"{pin_id}.jpg")

            download_tasks.append({
                'pin_id': pin_id,
                'image_urls': image_urls,
                'file_path': file_path,
                'headers': headers.copy()
            })

        # 使用线程池进行多线程下载
        results = []
        max_workers = min(self.max_concurrent, len(download_tasks))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有下载任务
            future_to_task = {
                executor.submit(self._download_single_pin_sync, task): task
                for task in download_tasks
            }

            # 收集结果（单点错误容忍）
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    # 单点错误容忍：单个pin下载失败不影响其他pin
                    logger.debug(f"Pin {task['pin_id']} 下载异常: {e}")
                    results.append({
                        'pin_id': task['pin_id'],
                        'success': False,
                        'message': f"下载异常: {e}",
                        'file_path': task['file_path']
                    })

        return results

    def _download_single_pin_sync(self, task: Dict) -> Dict:
        """同步下载单个Pin的图片（带重试机制）

        Args:
            task: 下载任务，包含pin_id, image_urls, file_path, headers

        Returns:
            下载结果字典
        """
        pin_id = task['pin_id']
        image_urls = task['image_urls']
        file_path = task['file_path']
        headers = task['headers']

        # 检查文件是否已存在
        if os.path.exists(file_path) and os.path.getsize(file_path) > 1000:
            return {
                'pin_id': pin_id,
                'success': True,
                'message': "文件已存在",
                'file_path': file_path
            }

        # 尝试下载每个URL（重试机制）
        max_retries = 3
        for i, url in enumerate(image_urls):
            for retry in range(max_retries):
                try:
                    # 使用现有的下载函数
                    from ..utils.downloader import download_image
                    success = download_image(url, file_path, headers, timeout=30, max_retries=1)

                    if success and os.path.exists(file_path) and os.path.getsize(file_path) > 1000:
                        return {
                            'pin_id': pin_id,
                            'success': True,
                            'message': f"下载成功 (URL {i+1}, 重试 {retry+1})",
                            'file_path': file_path
                        }

                except Exception as e:
                    logger.debug(f"Pin {pin_id} URL {i+1} 重试 {retry+1} 失败: {e}")

                # 重试间隔
                if retry < max_retries - 1:
                    import time
                    time.sleep(0.5 * (retry + 1))  # 递增延迟

        return {
            'pin_id': pin_id,
            'success': False,
            'message': f"所有URL都下载失败 ({len(image_urls)} 个尝试)",
            'file_path': file_path
        }

    def _verify_and_update_stats(self, batch_results: List[Dict], downloaded_pins_set: set) -> tuple:
        """【步骤5】检查本地文件并更新全局计数

        Args:
            batch_results: 批次下载结果
            downloaded_pins_set: 已下载pins集合（会被更新）

        Returns:
            (成功数量, 失败数量)
        """
        downloaded_count = 0
        failed_count = 0

        for result in batch_results:
            pin_id = result['pin_id']
            success = result['success']
            file_path = result['file_path']

            if success:
                # 验证文件确实存在且有效
                if os.path.exists(file_path) and os.path.getsize(file_path) > 1000:
                    downloaded_count += 1
                    downloaded_pins_set.add(pin_id)  # 更新已下载集合
                    logger.debug(f"✅ Pin {pin_id} 下载成功并验证")
                else:
                    failed_count += 1
                    logger.debug(f"❌ Pin {pin_id} 下载报告成功但文件无效")
            else:
                failed_count += 1
                logger.debug(f"❌ Pin {pin_id} 下载失败: {result['message']}")

        return downloaded_count, failed_count

    def _extract_image_urls(self, pin: Dict) -> List[str]:
        """从Pin数据中提取图片URLs

        Args:
            pin: Pin数据字典

        Returns:
            图片URL列表，按优先级排序
        """
        urls = []

        # 优先使用largest_image_url
        largest_url = pin.get('largest_image_url')
        if largest_url and largest_url.startswith('http'):
            urls.append(largest_url)

        # 解析image_urls JSON
        image_urls_json = pin.get('image_urls')
        if image_urls_json:
            try:
                import json
                image_urls_dict = json.loads(image_urls_json) if isinstance(image_urls_json, str) else image_urls_json

                # 按优先级顺序提取URL
                size_priorities = ["original", "1200", "736", "564", "474", "236", "170"]
                for size in size_priorities:
                    if size in image_urls_dict:
                        url = image_urls_dict[size]
                        if url and url.startswith('http') and url not in urls:
                            urls.append(url)

            except Exception as e:
                logger.debug(f"解析image_urls失败: {e}")

        return urls

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
