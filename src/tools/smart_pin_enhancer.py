#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
智能Pin增强器

按照用户指定的逻辑实现：
1. 在下载阶段，检查每个Pin是否缺少图片下载链接
2. 只有缺少图片URL的Pin才获取详情数据
3. 获取到数据后第一时间将数据补全到数据库中
4. 然后再走正常的图片下载流程
"""

import json
from typing import Dict, Optional
from loguru import logger

from ..core.database.repository import SQLiteRepository
from ..utils.improved_pin_detail_extractor import ImprovedPinDetailExtractor


class SmartPinEnhancer:
    """智能Pin增强器
    
    在下载阶段按需增强Pin数据，确保每个Pin都有可用的图片URL
    """
    
    def __init__(self, output_dir: str):
        """初始化增强器
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        self.enhancement_stats = {
            "pins_checked": 0,
            "pins_enhanced": 0,
            "pins_failed": 0,
            "pins_skipped": 0
        }
    
    async def enhance_pin_if_needed(self, pin: Dict, keyword: str) -> Dict:
        """如果Pin缺少图片URL，则获取详情数据并更新数据库
        
        Args:
            pin: Pin数据
            keyword: 关键词
            
        Returns:
            增强后的Pin数据
        """
        self.enhancement_stats["pins_checked"] += 1
        
        # 1. 检查是否需要增强
        if self._has_valid_image_urls(pin):
            logger.debug(f"Pin {pin['id']} 已有图片URL，无需增强")
            self.enhancement_stats["pins_skipped"] += 1
            return pin
        
        logger.debug(f"Pin {pin['id']} 缺少图片URL，开始获取详情...")
        
        try:
            # 2. 获取Pin详情
            pin_detail = await self._fetch_pin_detail(pin['id'])
            if not pin_detail or not pin_detail.get('image_urls'):
                logger.warning(f"Pin {pin['id']} 详情获取失败或无图片URL")
                self.enhancement_stats["pins_failed"] += 1
                return pin
            
            # 3. 合并Pin数据
            enhanced_pin = self._merge_pin_data(pin, pin_detail)
            
            # 4. 立即更新数据库
            success = await self._update_pin_in_database_atomic(enhanced_pin, keyword)
            
            if success:
                logger.info(f"✅ Pin {pin['id']} 增强成功，获得 {len(pin_detail.get('image_urls', {}))} 个图片URL")
                self.enhancement_stats["pins_enhanced"] += 1
                return enhanced_pin
            else:
                logger.warning(f"❌ Pin {pin['id']} 数据库更新失败")
                self.enhancement_stats["pins_failed"] += 1
                return pin
                
        except Exception as e:
            logger.error(f"增强Pin {pin['id']} 失败: {e}")
            self.enhancement_stats["pins_failed"] += 1
            return pin
    
    def _has_valid_image_urls(self, pin: Dict) -> bool:
        """检查Pin是否有有效的图片URL
        
        Args:
            pin: Pin数据
            
        Returns:
            是否有有效的图片URL
        """
        # 检查largest_image_url
        largest_url = pin.get('largest_image_url', '').strip()
        if largest_url and largest_url.startswith('http'):
            return True
        
        # 检查image_urls字典
        image_urls = pin.get('image_urls', {})
        if isinstance(image_urls, str):
            try:
                image_urls = json.loads(image_urls)
            except:
                image_urls = {}
        
        if isinstance(image_urls, dict) and image_urls:
            # 检查是否有任何有效的URL
            for key, url in image_urls.items():
                if url and isinstance(url, str) and url.strip().startswith('http'):
                    return True
        
        return False
    
    async def _fetch_pin_detail(self, pin_id: str) -> Optional[Dict]:
        """获取Pin详情数据
        
        Args:
            pin_id: Pin ID
            
        Returns:
            Pin详情数据，失败返回None
        """
        try:
            async with ImprovedPinDetailExtractor() as extractor:
                pin_details = await extractor.extract_pin_details([pin_id])
                return pin_details.get(pin_id)
        except Exception as e:
            logger.error(f"获取Pin详情失败: {pin_id}, 错误: {e}")
            return None
    
    def _merge_pin_data(self, original_pin: Dict, pin_detail: Dict) -> Dict:
        """合并Pin数据
        
        Args:
            original_pin: 原始Pin数据
            pin_detail: Pin详情数据
            
        Returns:
            合并后的Pin数据
        """
        enhanced_pin = original_pin.copy()
        
        # 更新图片URL相关字段
        if pin_detail.get('image_urls'):
            enhanced_pin['image_urls'] = pin_detail['image_urls']
        
        if pin_detail.get('largest_image_url'):
            enhanced_pin['largest_image_url'] = pin_detail['largest_image_url']
        
        # 更新其他字段（如果原始数据为空）
        if pin_detail.get('title') and not enhanced_pin.get('title'):
            enhanced_pin['title'] = pin_detail['title']
        
        if pin_detail.get('description') and not enhanced_pin.get('description'):
            enhanced_pin['description'] = pin_detail['description']
        
        # 更新创建者信息
        if pin_detail.get('creator'):
            creator = pin_detail['creator']
            if creator.get('name') and not enhanced_pin.get('creator_name'):
                enhanced_pin['creator_name'] = creator['name']
            if creator.get('id') and not enhanced_pin.get('creator_id'):
                enhanced_pin['creator_id'] = creator['id']
        
        # 更新板块信息
        if pin_detail.get('board'):
            board = pin_detail['board']
            if board.get('name') and not enhanced_pin.get('board_name'):
                enhanced_pin['board_name'] = board['name']
            if board.get('id') and not enhanced_pin.get('board_id'):
                enhanced_pin['board_id'] = board['id']
        
        # 更新统计信息
        if pin_detail.get('stats'):
            enhanced_pin['stats'] = pin_detail['stats']
        
        # 更新原始数据
        if pin_detail.get('raw_data'):
            enhanced_pin['raw_data'] = pin_detail['raw_data']
        
        return enhanced_pin
    
    async def _update_pin_in_database_atomic(self, pin: Dict, keyword: str) -> bool:
        """原子性更新Pin数据到数据库
        
        Args:
            pin: Pin数据
            keyword: 关键词
            
        Returns:
            是否更新成功
        """
        try:
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
            
            with repository._get_session() as session:
                from src.core.database.schema import Pin
                
                # 开始事务
                session.begin()
                
                try:
                    # 查找现有Pin
                    existing_pin = session.query(Pin).filter_by(id=pin['id']).first()
                    
                    if existing_pin:
                        # 更新现有Pin
                        if pin.get('image_urls'):
                            if isinstance(pin['image_urls'], dict):
                                existing_pin.image_urls = json.dumps(pin['image_urls'])
                            else:
                                existing_pin.image_urls = pin['image_urls']
                        
                        if pin.get('largest_image_url'):
                            existing_pin.largest_image_url = pin['largest_image_url']
                        
                        if pin.get('title'):
                            existing_pin.title = pin['title']
                        
                        if pin.get('description'):
                            existing_pin.description = pin['description']
                        
                        if pin.get('creator_name'):
                            existing_pin.creator_name = pin['creator_name']
                        
                        if pin.get('creator_id'):
                            existing_pin.creator_id = pin['creator_id']
                        
                        if pin.get('board_name'):
                            existing_pin.board_name = pin['board_name']
                        
                        if pin.get('board_id'):
                            existing_pin.board_id = pin['board_id']
                        
                        if pin.get('stats'):
                            if isinstance(pin['stats'], dict):
                                existing_pin.stats = json.dumps(pin['stats'])
                            else:
                                existing_pin.stats = pin['stats']
                        
                        if pin.get('raw_data'):
                            if isinstance(pin['raw_data'], dict):
                                existing_pin.raw_data = json.dumps(pin['raw_data'])
                            else:
                                existing_pin.raw_data = pin['raw_data']
                        
                        # 提交事务
                        session.commit()
                        
                        logger.debug(f"Pin {pin['id']} 数据库更新成功")
                        return True
                    else:
                        logger.warning(f"Pin {pin['id']} 不存在，无法更新")
                        session.rollback()
                        return False
                        
                except Exception as e:
                    session.rollback()
                    logger.error(f"更新Pin数据事务失败: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"更新Pin {pin['id']} 到数据库失败: {e}")
            return False
    
    def get_enhancement_stats(self) -> Dict[str, int]:
        """获取增强统计信息
        
        Returns:
            增强统计信息
        """
        return self.enhancement_stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        self.enhancement_stats = {
            "pins_checked": 0,
            "pins_enhanced": 0,
            "pins_failed": 0,
            "pins_skipped": 0
        }
