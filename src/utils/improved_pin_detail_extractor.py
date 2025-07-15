#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
改进的Pin详情提取器

基于用户验证的方案：通过访问Pin详情页面HTML来获取完整的图片信息
解决第二阶段采集中base64编码Pin无法获取图片URL的问题
"""

import asyncio
import json
import re
import base64
from typing import Dict, List, Optional, Tuple
from loguru import logger

from ..core.browser_manager import BrowserManager


class ImprovedPinDetailExtractor:
    """改进的Pin详情提取器
    
    专门解决第二阶段采集中的问题：
    1. 解码base64编码的Pin ID
    2. 访问Pin详情页面获取完整数据
    3. 从HTML中提取图片URL和其他信息
    """
    
    def __init__(self, proxy: Optional[str] = None, headless: bool = True, timeout: int = 30):
        """初始化提取器
        
        Args:
            proxy: 代理服务器
            headless: 是否无头模式
            timeout: 超时时间
        """
        self.proxy = proxy
        self.headless = headless
        self.timeout = timeout
        self.browser = None
        
    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self._initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self._cleanup()
        
    async def _initialize(self) -> bool:
        """初始化浏览器"""
        try:
            self.browser = BrowserManager(
                proxy=self.proxy,
                timeout=self.timeout,
                headless=self.headless,
                enable_network_interception=False  # 不需要网络拦截，直接解析HTML
            )
            
            if not await self.browser.start():
                logger.error("浏览器启动失败")
                return False
                
            logger.debug("Pin详情提取器初始化成功")
            return True
            
        except Exception as e:
            logger.error(f"初始化失败: {e}")
            return False
            
    async def _cleanup(self):
        """清理资源"""
        if self.browser:
            try:
                await self.browser.stop()
                logger.debug("Pin详情提取器资源清理完成")
            except Exception as e:
                logger.warning(f"清理资源时出错: {e}")
                
    def decode_pin_id(self, encoded_pin_id: str) -> Optional[str]:
        """解码base64编码的Pin ID
        
        Args:
            encoded_pin_id: base64编码的Pin ID
            
        Returns:
            解码后的数字Pin ID，失败返回None
        """
        try:
            if encoded_pin_id.startswith('UGlu'):  # Base64编码的"Pin:"前缀
                decoded = base64.b64decode(encoded_pin_id).decode('utf-8')
                if decoded.startswith('Pin:'):
                    return decoded[4:]  # 移除"Pin:"前缀
            return encoded_pin_id  # 如果不是base64编码，直接返回
        except Exception as e:
            logger.error(f"解码Pin ID失败: {e}")
            return None
            
    async def extract_pin_details(self, pin_ids: List[str]) -> Dict[str, Dict]:
        """批量提取Pin详情
        
        Args:
            pin_ids: Pin ID列表（可能包含base64编码的ID）
            
        Returns:
            Dict[pin_id, pin_data]: Pin详情数据字典
        """
        results = {}
        
        logger.info(f"开始提取{len(pin_ids)}个Pin的详情数据")
        
        if not self.browser:
            if not await self._initialize():
                logger.error("浏览器初始化失败")
                return results
        
        for i, pin_id in enumerate(pin_ids, 1):
            logger.info(f"提取Pin详情 ({i}/{len(pin_ids)}): {pin_id}")
            
            # 解码Pin ID
            decoded_id = self.decode_pin_id(pin_id)
            if not decoded_id:
                logger.warning(f"无法解码Pin ID: {pin_id}")
                continue
                
            # 提取Pin详情
            pin_data = await self._extract_single_pin(decoded_id)
            if pin_data:
                # 保留原始ID和解码后的ID
                pin_data['original_id'] = pin_id
                pin_data['decoded_id'] = decoded_id
                results[decoded_id] = pin_data
                logger.info(f"✅ Pin {decoded_id} 详情提取成功")
            else:
                logger.warning(f"⚠️  Pin {decoded_id} 详情提取失败")
            
            # 添加延迟避免被检测
            if i < len(pin_ids):
                await asyncio.sleep(2)
        
        logger.info(f"Pin详情提取完成，成功: {len(results)}/{len(pin_ids)}")
        return results
        
    async def _extract_single_pin(self, pin_id: str) -> Optional[Dict]:
        """提取单个Pin的详情
        
        Args:
            pin_id: 数字格式的Pin ID
            
        Returns:
            Pin详情数据，失败返回None
        """
        try:
            # 访问Pin详情页
            pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
            
            if not await self.browser.navigate(pin_url):
                logger.error(f"Pin页面导航失败: {pin_url}")
                return None
            
            # 等待页面加载
            await asyncio.sleep(3)
            
            # 获取页面HTML内容
            html_content = await self.browser.page.content()
            
            # 从HTML中提取Pin数据
            pin_data = self._extract_pin_data_from_html(html_content, pin_id)
            
            return pin_data
            
        except Exception as e:
            logger.error(f"提取Pin {pin_id} 详情时发生异常: {e}")
            return None
            
    def _extract_pin_data_from_html(self, html_content: str, target_pin_id: str) -> Optional[Dict]:
        """从HTML内容中提取Pin数据
        
        Args:
            html_content: Pin详情页面的HTML内容
            target_pin_id: 目标Pin ID
            
        Returns:
            提取到的Pin数据，如果失败返回None
        """
        try:
            # 方法1: 查找 window.__PWS_DATA__ 数据
            pws_pattern = r'window\.__PWS_DATA__\s*=\s*({.*?});'
            pws_matches = re.findall(pws_pattern, html_content, re.DOTALL)
            
            for match in pws_matches:
                try:
                    data = json.loads(match)
                    pin_data = self._search_pin_in_data(data, target_pin_id)
                    if pin_data:
                        logger.debug("从 window.__PWS_DATA__ 中找到Pin数据")
                        return self._normalize_pin_data(pin_data)
                except json.JSONDecodeError:
                    continue
            
            # 方法2: 查找其他可能的JSON数据结构
            script_patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
                r'window\.__REDUX_STATE__\s*=\s*({.*?});',
            ]
            
            for pattern in script_patterns:
                matches = re.findall(pattern, html_content, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        pin_data = self._search_pin_in_data(data, target_pin_id)
                        if pin_data:
                            logger.debug("从脚本数据中找到Pin数据")
                            return self._normalize_pin_data(pin_data)
                    except json.JSONDecodeError:
                        continue
            
            # 方法3: 直接提取图片URL作为后备方案
            logger.debug("尝试直接提取图片URL作为后备方案")
            image_urls = self._extract_image_urls_directly(html_content)

            if image_urls:
                logger.info(f"通过直接URL提取找到 {len(image_urls)} 个图片URL")
                # 创建基本的Pin数据结构
                pin_data = {
                    'id': target_pin_id,
                    'image_urls': {},
                    'largest_image_url': '',
                    'title': '',
                    'description': '',
                    'creator': {'name': '', 'id': ''},
                    'board': {'name': '', 'id': ''},
                    'stats': {},
                    'raw_data': {'extracted_method': 'direct_url_extraction'}
                }

                # 构建图片URL字典
                for i, url in enumerate(image_urls):
                    if 'originals' in url:
                        pin_data['image_urls']['original'] = url
                        pin_data['largest_image_url'] = url
                    elif '736x' in url:
                        pin_data['image_urls']['736x'] = url
                    elif '564x' in url:
                        pin_data['image_urls']['564x'] = url
                    else:
                        pin_data['image_urls'][f'url_{i}'] = url

                # 如果没有设置largest_image_url，使用第一个URL
                if not pin_data['largest_image_url'] and image_urls:
                    pin_data['largest_image_url'] = image_urls[0]

                logger.info(f"✅ 通过直接URL提取成功创建Pin数据")
                return pin_data

            logger.warning(f"未能从HTML中提取到Pin {target_pin_id} 的数据")
            return None
            
        except Exception as e:
            logger.error(f"提取Pin数据时发生异常: {e}")
            return None
            
    def _search_pin_in_data(self, data: Dict, target_pin_id: str) -> Optional[Dict]:
        """在数据结构中递归搜索指定的Pin
        
        Args:
            data: 要搜索的数据结构
            target_pin_id: 目标Pin ID
            
        Returns:
            找到的Pin数据，如果没找到返回None
        """
        if not isinstance(data, dict):
            return None
        
        # 检查当前层级是否包含Pin数据
        if 'id' in data and str(data['id']) == str(target_pin_id):
            return data
        
        # 递归搜索所有值
        for key, value in data.items():
            if isinstance(value, dict):
                result = self._search_pin_in_data(value, target_pin_id)
                if result:
                    return result
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        result = self._search_pin_in_data(item, target_pin_id)
                        if result:
                            return result
        
        return None

    def _extract_image_urls_directly(self, html_content: str) -> List[str]:
        """直接从HTML中提取图片URL

        Args:
            html_content: HTML内容

        Returns:
            提取到的图片URL列表
        """
        image_urls = []

        try:
            # 图片URL模式
            image_patterns = [
                r'https://i\.pinimg\.com/originals/[^"\'>\s]+\.(?:jpg|jpeg|png|gif|webp)',
                r'https://i\.pinimg\.com/736x/[^"\'>\s]+\.(?:jpg|jpeg|png|gif|webp)',
                r'https://i\.pinimg\.com/564x/[^"\'>\s]+\.(?:jpg|jpeg|png|gif|webp)',
                r'https://i\.pinimg\.com/474x/[^"\'>\s]+\.(?:jpg|jpeg|png|gif|webp)',
            ]

            for pattern in image_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                image_urls.extend(matches)

            # 去重并保持顺序
            seen = set()
            unique_urls = []
            for url in image_urls:
                if url not in seen:
                    seen.add(url)
                    unique_urls.append(url)

            logger.debug(f"直接提取到 {len(unique_urls)} 个唯一图片URL")
            return unique_urls

        except Exception as e:
            logger.error(f"直接提取图片URL时发生异常: {e}")
            return []

    def _normalize_pin_data(self, raw_pin_data: Dict) -> Dict:
        """标准化Pin数据格式
        
        Args:
            raw_pin_data: 原始Pin数据
            
        Returns:
            标准化后的Pin数据
        """
        try:
            # 提取图片信息
            image_urls = {}
            largest_image_url = ""
            
            # 从images字段提取图片URL
            if 'images' in raw_pin_data and isinstance(raw_pin_data['images'], dict):
                images = raw_pin_data['images']
                
                # 收集所有尺寸的图片URL
                for size, img_data in images.items():
                    if isinstance(img_data, dict) and 'url' in img_data:
                        image_urls[size] = img_data['url']
                
                # 优先使用原图作为largest_image_url
                if 'orig' in images and isinstance(images['orig'], dict):
                    largest_image_url = images['orig'].get('url', '')
                elif '1200x' in images and isinstance(images['1200x'], dict):
                    largest_image_url = images['1200x'].get('url', '')
                elif image_urls:
                    # 使用最大尺寸
                    largest_image_url = max(image_urls.values(), key=len)
            
            # 构建标准化的Pin数据
            pin_data = {
                'id': str(raw_pin_data.get('id', '')),
                'title': raw_pin_data.get('title', ''),
                'description': raw_pin_data.get('description', ''),
                'url': raw_pin_data.get('url', ''),
                'link': raw_pin_data.get('link', ''),
                'image_urls': image_urls,
                'largest_image_url': largest_image_url,
                'creator': {
                    'id': raw_pin_data.get('creator', {}).get('id', '') or raw_pin_data.get('pinner', {}).get('id', ''),
                    'name': (raw_pin_data.get('creator', {}).get('username', '') or 
                            raw_pin_data.get('creator', {}).get('name', '') or
                            raw_pin_data.get('pinner', {}).get('username', '')),
                },
                'board': {
                    'id': raw_pin_data.get('board', {}).get('id', ''),
                    'name': raw_pin_data.get('board', {}).get('name', ''),
                },
                'stats': raw_pin_data.get('pin_metrics', {}),
                'created_at': raw_pin_data.get('created_at', ''),
                'dominant_color': raw_pin_data.get('dominant_color', ''),
                'raw_data': raw_pin_data  # 保留原始数据
            }
            
            return pin_data
            
        except Exception as e:
            logger.error(f"标准化Pin数据时发生异常: {e}")
            return {}
