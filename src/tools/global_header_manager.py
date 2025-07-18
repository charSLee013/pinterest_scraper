#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
全局Header管理器

按照用户指定的逻辑实现：
1. 在进入下载逻辑之前，启动浏览器访问Pinterest官网
2. 获取并缓存浏览器headers到全局中，方便后续使用
3. 每次执行只运行一次，不是每次下载前都运行
4. 提供全局headers供整个会话使用
"""

import json
import os
from typing import Dict, Optional
from pathlib import Path
from loguru import logger

from ..core.download.browser_session import BrowserSessionManager


class GlobalHeaderManager:
    """全局Header管理器
    
    单例模式，确保整个会话期间只初始化一次浏览器会话
    """
    
    _instance = None
    _headers = None
    _session_valid = False
    _cache_file = "browser_headers_cache.json"
    
    def __new__(cls, output_dir: str = "output"):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.output_dir = output_dir
            cls._instance._cache_path = Path(output_dir) / cls._cache_file
        return cls._instance
    
    async def ensure_headers_ready(self) -> bool:
        """确保Headers已准备就绪
        
        Returns:
            是否成功获取Headers
        """
        # 如果已有有效的headers，直接返回
        if self._session_valid and self._headers:
            logger.debug("使用已缓存的全局Headers")
            return True
        
        # 尝试从缓存文件加载
        if self._load_headers_from_cache():
            logger.info("✅ 从缓存文件加载全局Headers成功")
            return True
        
        # 启动浏览器获取新的headers
        return await self._fetch_fresh_headers()
    
    async def _fetch_fresh_headers(self) -> bool:
        """启动浏览器获取新的headers
        
        Returns:
            是否成功获取
        """
        logger.info("🌐 启动浏览器获取全局Headers...")
        
        try:
            # 创建浏览器会话管理器
            session_manager = BrowserSessionManager(headless=True)
            
            # 初始化会话
            if await session_manager.initialize_session():
                # 获取headers
                self._headers = session_manager.get_session_headers()
                
                if self._headers and len(self._headers) > 0:
                    self._session_valid = True
                    
                    # 缓存到文件
                    self._save_headers_to_cache()
                    
                    logger.info(f"✅ 全局Headers获取成功，包含 {len(self._headers)} 个字段")
                    logger.debug(f"Headers内容: {list(self._headers.keys())}")
                    
                    # 清理浏览器资源
                    await session_manager.close()

                    return True
                else:
                    logger.error("❌ 获取到的Headers为空")
                    await session_manager.close()
                    return False
            else:
                logger.error("❌ 浏览器会话初始化失败")
                await session_manager.close()
                return False
                
        except Exception as e:
            logger.error(f"❌ 获取全局Headers失败: {e}")
            return False
    
    def get_headers(self) -> Dict[str, str]:
        """获取缓存的Headers
        
        Returns:
            Headers字典，如果没有则返回默认Headers
        """
        if self._headers:
            return self._headers.copy()
        else:
            logger.warning("全局Headers不可用，使用默认Headers")
            return self._get_default_headers()
    
    def _get_default_headers(self) -> Dict[str, str]:
        """获取默认Headers
        
        Returns:
            默认Headers字典
        """
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.pinterest.com/',
        }
    
    def _save_headers_to_cache(self) -> bool:
        """保存Headers到缓存文件
        
        Returns:
            是否保存成功
        """
        try:
            # 确保输出目录存在
            os.makedirs(self.output_dir, exist_ok=True)
            
            cache_data = {
                'headers': self._headers,
                'timestamp': __import__('time').time(),
                'version': '1.0'
            }
            
            with open(self._cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Headers已缓存到: {self._cache_path}")
            return True
            
        except Exception as e:
            logger.warning(f"保存Headers缓存失败: {e}")
            return False
    
    def _load_headers_from_cache(self) -> bool:
        """从缓存文件加载Headers
        
        Returns:
            是否加载成功
        """
        try:
            if not self._cache_path.exists():
                logger.debug("Headers缓存文件不存在")
                return False
            
            with open(self._cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 检查缓存是否过期（10分钟）
            current_time = __import__('time').time()
            cache_time = cache_data.get('timestamp', 0)

            if current_time - cache_time > 10 * 60:  # 10分钟过期
                logger.debug("Headers缓存已过期")
                return False
            
            # 验证缓存数据
            headers = cache_data.get('headers', {})
            if not headers or not isinstance(headers, dict):
                logger.debug("Headers缓存数据无效")
                return False
            
            # 检查必要的headers字段
            if 'User-Agent' not in headers:
                logger.debug("Headers缓存缺少必要字段")
                return False
            
            self._headers = headers
            self._session_valid = True
            
            logger.debug("从缓存加载Headers成功")
            return True
            
        except Exception as e:
            logger.debug(f"加载Headers缓存失败: {e}")
            return False
    
    def is_headers_valid(self) -> bool:
        """检查Headers是否有效
        
        Returns:
            Headers是否有效
        """
        return self._session_valid and self._headers is not None
    
    def get_headers_info(self) -> Dict[str, any]:
        """获取Headers信息
        
        Returns:
            Headers信息字典
        """
        return {
            'valid': self.is_headers_valid(),
            'count': len(self._headers) if self._headers else 0,
            'has_user_agent': bool(self._headers and 'User-Agent' in self._headers),
            'has_cookies': bool(self._headers and 'Cookie' in self._headers),
            'cache_exists': self._cache_path.exists() if hasattr(self, '_cache_path') else False
        }
    
    def clear_cache(self) -> bool:
        """清除缓存
        
        Returns:
            是否清除成功
        """
        try:
            if self._cache_path.exists():
                self._cache_path.unlink()
                logger.info("Headers缓存已清除")
            
            self._headers = None
            self._session_valid = False
            
            return True
            
        except Exception as e:
            logger.error(f"清除Headers缓存失败: {e}")
            return False
