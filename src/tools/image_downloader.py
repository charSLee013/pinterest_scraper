#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
图片下载工具模块

专门用于从现有数据库中下载缺失的图片文件。
支持单关键词和全关键词的图片下载。
"""

import os
import asyncio
import json
import re
import requests
import base64
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from collections import defaultdict
import requests

from loguru import logger
from tqdm import tqdm

from ..core.database.repository import SQLiteRepository
from ..core.database.manager_factory import DatabaseManagerFactory
from ..core.download.task_manager import DownloadTaskManager
from ..utils.utils import sanitize_filename
from ..utils.downloader import download_image_with_fallback


class ImageDownloader:
    """图片下载工具类
    
    从现有数据库中发现并下载缺失的图片文件。
    支持单关键词和全关键词模式。
    """
    
    def __init__(self, output_dir: str = "output", max_concurrent: int = 15,
                 proxy: Optional[str] = None, prefer_requests: bool = True):
        """初始化图片下载器

        Args:
            output_dir: 输出目录
            max_concurrent: 最大并发下载数
            proxy: 代理服务器
            prefer_requests: 是否使用高性能模式
        """
        self.output_dir = output_dir
        self.max_concurrent = max_concurrent
        self.proxy = proxy
        self.prefer_requests = prefer_requests
        self.browser_session = None  # 浏览器会话管理器

        logger.debug(f"图片下载器初始化: {output_dir}, 并发数: {max_concurrent}")

    async def _ensure_browser_session(self):
        """确保浏览器会话可用"""
        if self.browser_session is None:
            try:
                # 动态导入以避免循环依赖
                from ..core.download.browser_session import BrowserSessionManager

                logger.info("正在初始化浏览器会话以获取真实Headers...")
                self.browser_session = BrowserSessionManager(
                    proxy=self.proxy,
                    headless=True
                )

                # 先尝试加载已保存的会话
                if self.browser_session.load_session_from_file():
                    logger.info("已从文件加载浏览器会话")
                    return True

                # 创建新的浏览器会话
                logger.info("创建新的浏览器会话...")
                session_initialized = await self.browser_session.initialize_session()

                if session_initialized:
                    # 保存会话到文件
                    try:
                        self.browser_session.save_session_to_file()
                        logger.debug("浏览器会话信息已保存到文件")
                    except Exception as save_error:
                        logger.warning(f"保存会话文件失败: {save_error}")

                    logger.info("浏览器会话初始化成功")
                    return True
                else:
                    logger.warning("浏览器会话初始化失败，将使用默认headers")
                    self.browser_session = None
                    return False

            except ImportError as e:
                logger.error(f"无法导入BrowserSessionManager: {e}")
                self.browser_session = None
                return False
            except Exception as e:
                logger.error(f"初始化浏览器会话出错: {e}")
                self.browser_session = None
                return False

        return True

    def _get_session_headers(self) -> Dict[str, str]:
        """获取会话headers，优先使用真实浏览器会话"""
        # 尝试使用真实浏览器会话headers
        if self.browser_session:
            try:
                headers = self.browser_session.get_session_headers()
                if headers and isinstance(headers, dict) and len(headers) > 0:
                    logger.debug("使用真实浏览器会话headers")
                    if 'User-Agent' in headers:
                        return headers
                    else:
                        logger.warning("浏览器会话headers缺少User-Agent，回退到默认headers")
                else:
                    logger.warning("浏览器会话headers为空，回退到默认headers")
            except Exception as e:
                logger.warning(f"获取浏览器会话headers失败: {e}，回退到默认headers")

        # 回退到默认headers
        logger.debug("使用默认headers")
        return self._get_default_headers()

    def _get_default_headers(self) -> Dict[str, str]:
        """获取默认的请求headers"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Microsoft Edge";v="138"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'image',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'cross-site',
            'Referer': 'https://www.pinterest.com/',
        }

    def _extract_all_image_urls(self, pin: Dict) -> Dict[str, str]:
        """从Pin数据中提取所有可用的图片URL - 增强版

        增强功能:
        1. 从专用字段提取（第一阶段数据）
        2. 从raw_data提取（第二阶段数据）
        3. 从URL生成图片URL（第二阶段数据）
        4. 多层级提取策略，提高鲁棒性
        """
        urls = {}

        # 策略1: 从专用字段提取（第一阶段数据）
        # 1. 从largest_image_url提取
        if pin.get('largest_image_url'):
            urls['largest'] = pin['largest_image_url']

        # 2. 从image_urls字段提取（JSON格式）
        if pin.get('image_urls'):
            try:
                if isinstance(pin['image_urls'], str):
                    image_urls_data = json.loads(pin['image_urls'])
                else:
                    image_urls_data = pin['image_urls']

                if isinstance(image_urls_data, dict):
                    urls.update(image_urls_data)
                    logger.debug(f"从image_urls提取到 {len(image_urls_data)} 个尺寸")

            except (json.JSONDecodeError, TypeError) as e:
                logger.debug(f"解析image_urls失败: {e}")

        # 3. 从images字段提取（如果存在）
        if pin.get('images'):
            try:
                if isinstance(pin['images'], str):
                    images_data = json.loads(pin['images'])
                else:
                    images_data = pin['images']

                # 处理不同的images数据格式
                if isinstance(images_data, dict):
                    urls.update(images_data)
                elif isinstance(images_data, list) and images_data:
                    # 如果是列表，取第一个元素
                    if isinstance(images_data[0], dict):
                        urls.update(images_data[0])

            except (json.JSONDecodeError, TypeError) as e:
                logger.debug(f"解析images失败: {e}")

        # 策略2: 从raw_data提取（第二阶段数据）
        # 如果前面的策略没有提取到URL，尝试从raw_data中提取
        if not urls and pin.get('raw_data'):
            try:
                raw_data = pin['raw_data']
                if isinstance(raw_data, str):
                    raw_data = json.loads(raw_data)

                # 从images字段提取
                if 'images' in raw_data and isinstance(raw_data['images'], dict):
                    images = raw_data['images']

                    # 优先使用orig（原图）
                    if 'orig' in images and isinstance(images['orig'], dict) and 'url' in images['orig']:
                        urls['orig'] = images['orig']['url']

                    # 添加其他尺寸
                    for size, img_data in images.items():
                        if isinstance(img_data, dict) and 'url' in img_data:
                            urls[size] = img_data['url']

                # 从image字段提取
                if not urls and 'image' in raw_data:
                    image_data = raw_data['image']
                    if isinstance(image_data, dict) and 'url' in image_data:
                        urls['image'] = image_data['url']

                # 从image_urls字段提取
                if not urls and 'image_urls' in raw_data and raw_data['image_urls']:
                    if isinstance(raw_data['image_urls'], dict):
                        urls.update(raw_data['image_urls'])
                    elif isinstance(raw_data['image_urls'], str):
                        try:
                            image_urls_data = json.loads(raw_data['image_urls'])
                            if isinstance(image_urls_data, dict):
                                urls.update(image_urls_data)
                        except:
                            pass

                logger.debug(f"从raw_data提取到 {len(urls)} 个URL")

            except Exception as e:
                logger.debug(f"从raw_data提取URL失败: {e}")

        # 策略3: 从Pin ID生成图片URL（第二阶段数据）
        # 如果前面的策略都没有提取到URL，尝试从Pin ID生成
        if not urls and pin.get('id'):
            try:
                pin_id = pin.get('id')

                # 尝试从URL中提取图片ID
                if pin.get('url') and '/pin/' in pin.get('url', ''):
                    # 从URL中提取Pin ID
                    import re
                    url_match = re.search(r'/pin/([^/]+)/', pin.get('url', ''))
                    if url_match:
                        pin_id = url_match.group(1)

                # 如果是数字ID，可以尝试生成图片URL
                if pin_id.isdigit() or (len(pin_id) > 8 and not '=' in pin_id):
                    # 尝试生成常见的Pinterest图片URL格式
                    urls['generated_orig'] = f"https://i.pinimg.com/originals/xx/xx/{pin_id}.jpg"
                    urls['generated_736'] = f"https://i.pinimg.com/736x/xx/xx/{pin_id}.jpg"
                    urls['generated_564'] = f"https://i.pinimg.com/564x/xx/xx/{pin_id}.jpg"

                    logger.debug(f"从Pin ID生成了 {len(urls)} 个URL")
            except Exception as e:
                logger.debug(f"从Pin ID生成URL失败: {e}")

        logger.debug(f"Pin {pin.get('id', 'unknown')} 提取到 {len(urls)} 个URL: {list(urls.keys())}")
        return urls

    def _parse_pinterest_url_size(self, url: str) -> Tuple[int, int, str]:
        """解析Pinterest URL中的尺寸信息

        Args:
            url: Pinterest图片URL

        Returns:
            (width, height, type) - 宽度、高度、类型
        """
        if not url:
            return (0, 0, 'unknown')

        # 原图优先级最高
        if '/originals/' in url:
            return (999999, 999999, 'original')

        # 解析 /WIDTHx/ 或 /WIDTHxHEIGHT/ 格式
        pattern = r'/(\d+)x(\d*)?/'
        match = re.search(pattern, url)

        if match:
            width = int(match.group(1))
            height_str = match.group(2)
            height = int(height_str) if height_str else 0
            return (width, height, 'sized')

        # 无法解析
        return (0, 0, 'unknown')

    def _calculate_url_priority(self, url: str) -> int:
        """计算URL的优先级分数"""
        width, height, url_type = self._parse_pinterest_url_size(url)

        # 原图最高优先级
        if url_type == 'original':
            return 1000000

        # 对于有具体尺寸的，计算像素总数
        if url_type == 'sized' and height > 0:
            return width * height

        # 对于只有宽度的，假设高度为宽度的1.5倍
        if url_type == 'sized':
            estimated_height = int(width * 1.5)
            return width * estimated_height

        # 无法解析的最低优先级
        return 0

    def _get_prioritized_urls(self, pin: Dict) -> List[str]:
        """获取按优先级排序的URL列表

        Args:
            pin: Pin数据

        Returns:
            按优先级排序的URL列表（从大到小）
        """
        # 提取所有可用URL
        all_urls = self._extract_all_image_urls(pin)

        if not all_urls:
            logger.warning(f"Pin {pin.get('id')} 没有找到任何图片URL")
            return []

        # 按URL优先级排序
        url_with_priority = []
        for key, url in all_urls.items():
            priority = self._calculate_url_priority(url)
            url_with_priority.append((key, url, priority))

            width, height, url_type = self._parse_pinterest_url_size(url)
            logger.debug(f"URL分析: {key} -> {width}x{height} ({url_type}) 优先级: {priority}")

        # 按优先级降序排序
        url_with_priority.sort(key=lambda x: x[2], reverse=True)

        # 提取URL并去重
        prioritized_urls = []
        seen_urls = set()

        for key, url, priority in url_with_priority:
            if url not in seen_urls:
                prioritized_urls.append(url)
                seen_urls.add(url)
                logger.debug(f"添加URL: {key} (优先级: {priority}) -> {url[:100]}...")

        logger.info(f"Pin {pin.get('id')} 生成 {len(prioritized_urls)} 个候选URL")
        return prioritized_urls
    
    def discover_keyword_databases(self, target_keyword: Optional[str] = None) -> List[Dict]:
        """发现关键词数据库
        
        Args:
            target_keyword: 目标关键词，为None时发现所有关键词
            
        Returns:
            数据库信息列表
        """
        databases = []
        output_path = Path(self.output_dir)
        
        if not output_path.exists():
            logger.warning(f"输出目录不存在: {self.output_dir}")
            return databases
        
        for item in output_path.iterdir():
            if not item.is_dir():
                continue
                
            keyword = item.name
            db_path = item / 'pinterest.db'
            images_dir = item / 'images'
            
            # 检查数据库文件是否存在
            if not db_path.exists():
                logger.debug(f"跳过目录 {keyword}: 数据库文件不存在")
                continue
            
            # 如果指定了关键词，只处理匹配的
            if target_keyword:
                safe_target = sanitize_filename(target_keyword)
                if keyword != safe_target:
                    continue
            
            databases.append({
                'keyword': keyword,
                'db_path': str(db_path),
                'images_dir': str(images_dir),
                'keyword_dir': str(item)
            })
            
            logger.debug(f"发现数据库: {keyword} -> {db_path}")
        
        logger.info(f"发现 {len(databases)} 个关键词数据库")
        return databases
    
    def find_missing_images(self, db_info: Dict) -> List[Dict]:
        """检测缺失的图片
        
        Args:
            db_info: 数据库信息字典
            
        Returns:
            缺失图片任务列表
        """
        keyword = db_info['keyword']
        images_dir = db_info['images_dir']
        
        # 确保图片目录存在
        os.makedirs(images_dir, exist_ok=True)
        
        # 创建repository
        repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
        
        # 查询所有有图片URL的Pin
        try:
            pins = repository.load_pins_by_query(keyword)
            logger.info(f"从数据库加载 {len(pins)} 个Pin: {keyword}")
        except Exception as e:
            logger.error(f"加载Pin数据失败: {keyword}, 错误: {e}")
            return []
        
        missing_tasks = []

        for pin in pins:
            pin_id = pin.get('id')

            if not pin_id:
                logger.debug(f"跳过Pin: 缺少ID")
                continue

            # 使用新的智能URL提取
            prioritized_urls = self._get_prioritized_urls(pin)

            if not prioritized_urls:
                logger.debug(f"跳过Pin {pin_id}: 没有可用的图片URL")
                continue

            # 生成预期的文件路径
            expected_path = self._generate_image_path(pin, images_dir)

            # 检查文件是否存在且有效
            file_exists = self._is_valid_image_file(expected_path)
            logger.debug(f"Pin {pin_id}: 文件 {expected_path} 存在={file_exists}")

            if not file_exists:
                missing_tasks.append({
                    'pin_id': pin_id,
                    'candidate_urls': prioritized_urls,  # 多个候选URL
                    'expected_path': expected_path,
                    'keyword': keyword,
                    'pin_data': pin  # 保留完整数据用于调试
                })
                logger.debug(f"添加缺失图片任务: {pin_id} ({len(prioritized_urls)} 个候选URL)")
            else:
                logger.debug(f"跳过已存在图片: {pin_id}")
        
        logger.info(f"关键词 {keyword}: 发现 {len(missing_tasks)} 个缺失图片")
        return missing_tasks
    
    def _generate_image_path(self, pin: Dict, images_dir: str) -> str:
        """生成图片文件路径
        
        Args:
            pin: Pin数据
            images_dir: 图片目录
            
        Returns:
            图片文件路径
        """
        pin_id = pin.get('id', 'unknown')
        image_url = pin.get('largest_image_url', '')
        
        # 从URL获取文件扩展名
        from urllib.parse import urlparse
        parsed_url = urlparse(image_url)
        path = parsed_url.path
        ext = os.path.splitext(path)[1] if path else '.jpg'
        
        # 确保扩展名有效
        if not ext or ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
            ext = '.jpg'
        
        # 生成文件名
        filename = f"{pin_id}{ext}"
        return os.path.join(images_dir, filename)
    
    def _is_valid_image_file(self, file_path: str) -> bool:
        """检查图片文件是否存在且有效

        Args:
            file_path: 图片文件路径

        Returns:
            文件是否有效
        """
        if not os.path.exists(file_path):
            return False

        try:
            file_size = os.path.getsize(file_path)
            # 文件大小至少10KB（避免下载到错误页面）
            if file_size < 10240:
                logger.debug(f"文件太小: {file_size} bytes")
                return False

            # 检查文件头验证是否为有效图片
            with open(file_path, 'rb') as f:
                header = f.read(16)

            # 检查常见图片格式的文件头
            if header.startswith(b'\xFF\xD8\xFF'):  # JPEG
                return True
            elif header.startswith(b'\x89PNG\r\n\x1a\n'):  # PNG
                return True
            elif header.startswith(b'GIF8'):  # GIF
                return True
            elif header.startswith(b'RIFF') and b'WEBP' in header:  # WEBP
                return True
            else:
                logger.debug(f"未知文件格式: {header[:8].hex()}")
                return False

        except OSError as e:
            logger.debug(f"文件检查异常: {e}")
            return False

    def _download_image_with_fallback(self, task: Dict) -> Tuple[bool, str]:
        """使用回退机制下载图片 - 集成真实浏览器会话"""
        pin_id = task['pin_id']
        candidate_urls = task['candidate_urls']
        output_path = task['expected_path']

        # 确保目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 获取真实浏览器会话headers
        headers = self._get_session_headers()

        # 逐个尝试URL
        for i, url in enumerate(candidate_urls):
            try:
                logger.debug(f"尝试下载 {pin_id} URL {i+1}/{len(candidate_urls)}: {url[:100]}...")

                # 使用真实浏览器会话进行下载（带重试机制）
                success = self._download_with_session(url, output_path, headers, max_retries=3)

                if success and self._is_valid_image_file(output_path):
                    logger.debug(f"下载成功: {pin_id}")
                    return True, "下载成功"
                else:
                    logger.debug(f"URL {i+1} 下载失败或文件无效")
                    # 删除无效文件
                    if os.path.exists(output_path):
                        os.remove(output_path)

            except Exception as e:
                logger.debug(f"URL {i+1} 下载异常: {e}")
                # 删除可能的损坏文件
                if os.path.exists(output_path):
                    try:
                        os.remove(output_path)
                    except:
                        pass
                continue

        return False, f"所有URL都下载失败 ({len(candidate_urls)} 个尝试)"

    def _download_with_session(self, url: str, output_path: str, headers: Dict[str, str], max_retries: int = 3) -> bool:
        """使用真实浏览器会话下载图片 - 带重试机制"""
        import time
        import random

        for attempt in range(max_retries):
            try:
                # 重试延迟（第一次不延迟）
                if attempt > 0:
                    delay = random.uniform(0.5, 2.0) * attempt
                    time.sleep(delay)
                    logger.debug(f"重试下载 {url[:100]}... (尝试 {attempt + 1}/{max_retries})")

                # 获取下载会话
                session = self._get_download_session(headers)

                # 执行下载
                response = session.get(url, timeout=30, stream=True)

                if response.status_code == 200:
                    # 检查内容类型，避免下载错误页面
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'text/html' in content_type and 'image' not in content_type:
                        logger.warning(f"Pinterest返回了错误页面而不是图片 (尝试 {attempt + 1}/{max_retries})")
                        if attempt == max_retries - 1:
                            return False
                        continue

                    # 写入文件
                    with open(output_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    # 验证下载的文件
                    if self._validate_downloaded_file(output_path):
                        logger.debug(f"下载成功: {output_path}")
                        return True
                    else:
                        logger.debug(f"下载的文件验证失败 (尝试 {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            continue

                elif response.status_code == 403:
                    logger.warning(f"403 Forbidden - 可能触发反爬虫 (尝试 {attempt + 1}/{max_retries})")
                    # 403错误需要更长延迟和重新获取headers
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(2.0, 5.0))
                        # 重新获取headers
                        headers = self._get_session_headers()
                        continue
                elif response.status_code == 404:
                    logger.debug(f"图片URL不存在 (404) - 跳过重试")
                    return False  # 404不需要重试
                else:
                    logger.debug(f"HTTP错误: {response.status_code} (尝试 {attempt + 1}/{max_retries})")

            except requests.exceptions.Timeout:
                logger.warning(f"下载超时 (尝试 {attempt + 1}/{max_retries})")
            except requests.exceptions.ConnectionError:
                logger.warning(f"连接错误 (尝试 {attempt + 1}/{max_retries})")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP错误: {e} (尝试 {attempt + 1}/{max_retries})")
            except Exception as e:
                logger.warning(f"下载异常: {e} (尝试 {attempt + 1}/{max_retries})")

        # 所有重试都失败
        logger.debug(f"下载失败，已重试 {max_retries} 次")
        return False

    def _get_download_session(self, headers: Dict[str, str]):
        """获取下载会话"""
        # 优先使用真实浏览器会话配置
        if self.browser_session:
            try:
                session_config = self.browser_session.get_requests_session_config()
                session = session_config['session']
                logger.debug("使用真实浏览器会话进行下载")
                return session
            except Exception as session_error:
                logger.warning(f"获取浏览器会话配置失败: {session_error}")

        # 回退到默认配置
        import requests
        session = requests.Session()
        session.headers.update(headers)
        logger.debug("使用默认配置进行下载")
        return session

    def _validate_downloaded_file(self, file_path: str) -> bool:
        """验证下载的文件是否有效 - 使用现有的验证逻辑"""
        is_valid = self._is_valid_image_file(file_path)
        if not is_valid:
            # 删除无效文件
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except OSError:
                pass
        return is_valid

    def _analyze_download_results(self, results: List[Tuple[bool, str]], keyword: str) -> Dict:
        """分析下载结果并提供详细统计"""
        total = len(results)
        success = sum(1 for success, _ in results if success)
        failed = total - success

        # 分析失败原因
        failure_reasons = defaultdict(int)
        for success, message in results:
            if not success:
                message_lower = message.lower()
                if "超时" in message_lower or "timeout" in message_lower:
                    failure_reasons["网络超时"] += 1
                elif "403" in message_lower or "forbidden" in message_lower:
                    failure_reasons["反爬虫拦截"] += 1
                elif "404" in message_lower:
                    failure_reasons["URL不存在"] += 1
                elif "文件太小" in message_lower or "invalid" in message_lower:
                    failure_reasons["无效文件"] += 1
                elif "连接" in message_lower or "connection" in message_lower:
                    failure_reasons["连接错误"] += 1
                else:
                    failure_reasons["其他错误"] += 1

        success_rate = success / total * 100 if total > 0 else 0

        # 记录详细统计
        logger.info(f"关键词 {keyword} 下载统计:")
        logger.info(f"  总数: {total}, 成功: {success}, 失败: {failed}")
        logger.info(f"  成功率: {success_rate:.1f}%")

        if failure_reasons:
            logger.info(f"  失败原因分布:")
            for reason, count in failure_reasons.items():
                percentage = count / failed * 100
                logger.info(f"    {reason}: {count} 个 ({percentage:.1f}%)")

        return {
            'total': total,
            'success': success,
            'failed': failed,
            'success_rate': success_rate,
            'failure_reasons': dict(failure_reasons)
        }

    async def _download_images_concurrently(self, missing_tasks: List[Dict], keyword: str) -> List[Tuple[bool, str]]:
        """异步并发下载图片"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        tasks = []

        async def download_with_semaphore(task):
            async with semaphore:
                # 在线程池中执行同步下载
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None,
                    self._download_image_with_fallback,
                    task
                )

        try:
            # 创建并发任务
            tasks = [download_with_semaphore(task) for task in missing_tasks]

            # 使用tqdm显示进度
            results = []
            with tqdm(total=len(tasks), desc=f"下载 {keyword}", unit="img") as pbar:
                for coro in asyncio.as_completed(tasks):
                    result = await coro
                    results.append(result)

                    # 更新进度条
                    success, message = result
                    if success:
                        logger.debug(f"下载成功: {message}")
                    else:
                        logger.debug(f"下载失败: {message}")

                    pbar.update(1)

            return results

        except (KeyboardInterrupt, asyncio.CancelledError):
            # 中断时直接退出，不管pending的任务
            logger.warning(f"下载被中断，直接退出程序")
            import os
            os._exit(0)  # 强制退出，不执行清理

    async def close(self):
        """清理资源 - 改进版本"""
        if self.browser_session:
            try:
                # 确保浏览器正确关闭
                await self.browser_session.close()
                logger.debug("浏览器会话已关闭")
            except Exception as e:
                logger.warning(f"关闭浏览器会话失败: {e}")
            finally:
                self.browser_session = None

            # 额外等待确保进程完全关闭
            await asyncio.sleep(0.5)
    
    async def download_missing_images_for_keyword(self, keyword: str) -> Dict:
        """下载单个关键词的缺失图片

        Args:
            keyword: 关键词

        Returns:
            下载统计结果: {'downloaded': int, 'failed': int, 'skipped': int}
        """
        # 发现数据库
        databases = self.discover_keyword_databases(keyword)
        if not databases:
            logger.warning(f"未找到关键词数据库: {keyword}")
            return {'downloaded': 0, 'failed': 0, 'skipped': 0}

        db_info = databases[0]

        # 查找缺失图片
        missing_tasks = self.find_missing_images(db_info)
        if not missing_tasks:
            logger.info(f"关键词 {keyword}: 没有缺失的图片")
            return {'downloaded': 0, 'failed': 0, 'skipped': 0}

        if not missing_tasks:
            logger.info(f"关键词 {keyword}: 没有缺失的图片")
            return {'downloaded': 0, 'failed': 0, 'skipped': 0}

        logger.info(f"关键词 {keyword}: 发现 {len(missing_tasks)} 个缺失图片，开始真实下载...")

        # 确保浏览器会话可用
        await self._ensure_browser_session()

        # 异步并发下载
        downloaded_count = 0
        failed_count = 0

        # 使用异步并发下载
        results = await self._download_images_concurrently(missing_tasks, keyword)

        # 分析下载结果
        stats = self._analyze_download_results(results, keyword)
        downloaded_count = stats['success']
        failed_count = stats['failed']

        return {
            'downloaded': downloaded_count,
            'failed': failed_count,
            'skipped': 0
        }

    async def download_keyword_images(self, keyword: str) -> List[Tuple[bool, str]]:
        """兼容性接口：下载指定关键词的图片

        这是 download_missing_images_for_keyword 的兼容性包装器，
        返回格式适配现有调用方的期望。

        Args:
            keyword: 关键词

        Returns:
            List[Tuple[bool, str]]: [(成功标志, 错误信息), ...]

        Note:
            这个方法是为了保持与现有调用方的兼容性而添加的。
            推荐使用 download_missing_images_for_keyword 方法获取详细统计信息。
        """
        logger.debug(f"使用兼容性接口下载关键词图片: {keyword}")

        # 调用核心逻辑
        stats = await self.download_missing_images_for_keyword(keyword)

        # 转换返回格式以兼容现有调用方
        results = []
        downloaded = stats.get('downloaded', 0)
        failed = stats.get('failed', 0)

        # 生成兼容的结果列表
        for _ in range(downloaded):
            results.append((True, "下载成功"))
        for _ in range(failed):
            results.append((False, "下载失败"))

        logger.debug(f"兼容性接口返回: {len(results)} 个结果 (成功: {downloaded}, 失败: {failed})")
        return results
    
    async def download_all_missing_images(self) -> Dict:
        """下载所有关键词的缺失图片
        
        Returns:
            总体下载统计结果
        """
        # 发现所有数据库
        databases = self.discover_keyword_databases()
        if not databases:
            logger.warning("未找到任何关键词数据库")
            return {'downloaded': 0, 'failed': 0, 'skipped': 0, 'keywords': 0}
        
        total_stats = {'downloaded': 0, 'failed': 0, 'skipped': 0, 'keywords': 0}
        
        logger.info(f"开始处理 {len(databases)} 个关键词数据库")
        
        for db_info in databases:
            keyword = db_info['keyword']
            logger.info(f"处理关键词: {keyword}")
            
            try:
                stats = await self.download_missing_images_for_keyword(keyword)
                
                total_stats['downloaded'] += stats['downloaded']
                total_stats['failed'] += stats['failed']
                total_stats['skipped'] += stats['skipped']
                total_stats['keywords'] += 1
                
                logger.info(f"关键词 {keyword} 完成: 下载 {stats['downloaded']}, 失败 {stats['failed']}")
                
            except Exception as e:
                logger.error(f"处理关键词 {keyword} 时出错: {e}")
                total_stats['failed'] += 1
        
        return total_stats


class ImageDownloadProducer:
    """图片下载分页生产者 - 基于PinDataProducer模式"""

    def __init__(self, repository: SQLiteRepository, keyword: str, images_dir: str, page_size: int = 500):
        """初始化图片下载生产者

        Args:
            repository: 数据库Repository
            keyword: 关键词
            images_dir: 图片目录路径
            page_size: 每页大小
        """
        self.repository = repository
        self.keyword = keyword
        self.images_dir = images_dir
        self.page_size = page_size
        self.total_count = None
        self.current_offset = 0
        self.finished = False
        self.downloaded_cache = self._build_downloaded_index()

    def _build_downloaded_index(self) -> set:
        """建立已下载图片的Pin索引缓存

        Returns:
            已下载图片的Pin ID集合
        """
        downloaded_pins = set()

        if not os.path.exists(self.images_dir):
            logger.debug(f"图片目录不存在: {self.images_dir}")
            return downloaded_pins

        try:
            # 扫描images目录，提取已下载的pin_id
            for filename in os.listdir(self.images_dir):
                if filename.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                    # 从文件名提取Pin ID（去掉扩展名）
                    pin_id = os.path.splitext(filename)[0]
                    downloaded_pins.add(pin_id)

            logger.debug(f"建立已下载索引: {len(downloaded_pins)} 个文件，关键词: {self.keyword}")

        except Exception as e:
            logger.error(f"建立已下载索引失败: {e}")

        return downloaded_pins

    def get_total_pins_with_images(self) -> int:
        """获取有图片URL的Pin总数"""
        if self.total_count is None:
            try:
                # 使用Repository查询有图片的Pin总数
                all_pins = self.repository.load_pins_with_images(self.keyword)
                self.total_count = len(all_pins)
            except Exception as e:
                logger.error(f"获取Pin总数失败: {e}")
                self.total_count = 0

        return self.total_count

    async def produce_missing_pins(self, queue: asyncio.Queue) -> int:
        """生产缺失图片的Pin数据到队列

        Args:
            queue: 任务队列

        Returns:
            生产的缺失Pin数量
        """
        total_produced = 0

        logger.info(f"开始生产缺失图片Pin: {self.keyword}, 已下载: {len(self.downloaded_cache)}")

        while not self.finished:
            # 分页加载有图片URL的Pin数据
            pins = self.repository.load_pins_with_images(
                self.keyword,
                limit=self.page_size,
                offset=self.current_offset
            )

            if not pins:
                # 没有更多数据
                self.finished = True
                break

            # 过滤已下载的pins
            missing_pins = [pin for pin in pins if pin['id'] not in self.downloaded_cache]

            # 将缺失的Pin添加到队列
            for pin in missing_pins:
                await queue.put(pin)
                total_produced += 1

            logger.debug(f"生产了 {len(missing_pins)}/{len(pins)} 个缺失Pin (offset: {self.current_offset})")

            # 更新偏移量
            self.current_offset += len(pins)

            # 如果返回的Pin数量少于页面大小，说明已经到达末尾
            if len(pins) < self.page_size:
                self.finished = True
                break

        logger.info(f"缺失Pin生产完成: {total_produced} 个Pin")
        return total_produced


class BatchImageDownloader:
    """批量图片下载器 - 实现批量并发下载逻辑"""

    def __init__(self, image_downloader: 'ImageDownloader', batch_size: int = 500):
        """初始化批量图片下载器

        Args:
            image_downloader: ImageDownloader实例
            batch_size: 批次大小
        """
        self.image_downloader = image_downloader
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(image_downloader.max_concurrent)

    async def _process_batches_atomic(self, keyword: str) -> Dict:
        """批量原子处理关键词的图片下载

        Args:
            keyword: 关键词

        Returns:
            下载统计结果
        """
        stats = {
            'downloaded': 0,
            'failed': 0,
            'skipped': 0,
            'total_batches': 0,
            'keyword': keyword
        }

        try:
            # 创建Repository和图片目录路径
            repository = SQLiteRepository(keyword=keyword, output_dir=self.image_downloader.output_dir)
            images_dir = os.path.join(self.image_downloader.output_dir, keyword, "images")
            os.makedirs(images_dir, exist_ok=True)

            # 创建ImageDownloadProducer
            producer = ImageDownloadProducer(repository, keyword, images_dir, self.batch_size)
            total_pins_with_images = producer.get_total_pins_with_images()

            if total_pins_with_images == 0:
                logger.info(f"关键词 {keyword}: 没有发现有图片URL的Pin")
                return stats

            logger.info(f"关键词 {keyword}: 发现 {total_pins_with_images} 个有图片URL的Pin")
            logger.info(f"关键词 {keyword}: 已下载 {len(producer.downloaded_cache)} 个图片")

            # 创建任务队列
            queue = asyncio.Queue(maxsize=self.batch_size * 2)  # 缓冲区大小

            # 启动生产者任务
            producer_task = asyncio.create_task(producer.produce_missing_pins(queue))

            # 批量处理下载
            batch_count = 0
            current_batch = []

            # 使用tqdm显示整体进度
            with tqdm(total=total_pins_with_images, desc=f"下载 {keyword}", unit="pin") as pbar:
                # 更新已下载的进度
                pbar.update(len(producer.downloaded_cache))

                while True:
                    try:
                        # 从队列获取Pin，设置超时避免死锁
                        pin = await asyncio.wait_for(queue.get(), timeout=1.0)
                        current_batch.append(pin)

                        # 当批次满了或者生产者完成时，处理当前批次
                        if len(current_batch) >= self.batch_size or producer.finished:
                            if current_batch:
                                batch_results = await self._download_batch_concurrent(
                                    current_batch, keyword, pbar
                                )

                                # 统计结果
                                batch_downloaded = sum(1 for success, _ in batch_results if success)
                                batch_failed = len(batch_results) - batch_downloaded

                                stats['downloaded'] += batch_downloaded
                                stats['failed'] += batch_failed
                                stats['total_batches'] += 1
                                batch_count += 1

                                logger.debug(f"批次 {batch_count} 完成: {batch_downloaded}/{len(current_batch)} 成功")

                                # 清空当前批次
                                current_batch = []

                        # 标记任务完成
                        queue.task_done()

                    except asyncio.TimeoutError:
                        # 超时检查生产者是否完成
                        if producer.finished and queue.empty():
                            break
                        continue
                    except Exception as e:
                        logger.error(f"处理Pin时出错: {e}")
                        if current_batch:
                            queue.task_done()
                        continue

            # 等待生产者完成
            await producer_task

            logger.info(f"关键词 {keyword} 批量下载完成: {stats['downloaded']} 成功, {stats['failed']} 失败, {stats['total_batches']} 批次")

        except Exception as e:
            logger.error(f"批量处理关键词 {keyword} 失败: {e}")
            stats['failed'] += 1

        return stats

    async def _download_batch_concurrent(self, pins_batch: List[Dict], keyword: str, pbar: tqdm) -> List[Tuple[bool, str]]:
        """并发下载单个批次的图片

        Args:
            pins_batch: Pin数据批次
            keyword: 关键词
            pbar: 进度条

        Returns:
            下载结果列表
        """
        async def download_with_semaphore(pin):
            """带信号量控制的下载函数"""
            async with self.semaphore:
                return await self._download_single_pin_async(pin, keyword)

        # 创建并发任务
        tasks = [download_with_semaphore(pin) for pin in pins_batch]

        # 执行并发下载
        results = []
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                results.append(result)

                # 更新进度条
                pbar.update(1)

            except Exception as e:
                logger.error(f"批次下载任务异常: {e}")
                results.append((False, f"下载异常: {e}"))
                pbar.update(1)

        return results

    async def _download_single_pin_async(self, pin: Dict, keyword: str) -> Tuple[bool, str]:
        """异步下载单个Pin的图片

        Args:
            pin: Pin数据
            keyword: 关键词

        Returns:
            (是否成功, 消息)
        """
        pin_id = pin.get('id')
        if not pin_id:
            return False, "Pin ID为空"

        try:
            # 构建输出路径
            images_dir = os.path.join(self.image_downloader.output_dir, keyword, "images")
            output_path = os.path.join(images_dir, f"{pin_id}.jpg")

            # 检查文件是否已存在且有效
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                return True, "文件已存在"

            # 获取候选URL
            candidate_urls = self.image_downloader._get_prioritized_urls(pin)
            if not candidate_urls:
                return False, "没有可用的图片URL"

            # 在线程池中执行同步下载（重用现有的下载逻辑）
            loop = asyncio.get_event_loop()
            success, message = await loop.run_in_executor(
                None,
                self.image_downloader._download_image_with_fallback,
                {
                    'pin_id': pin_id,
                    'candidate_urls': candidate_urls,
                    'expected_path': output_path
                }
            )

            return success, message

        except Exception as e:
            logger.error(f"异步下载Pin {pin_id} 失败: {e}")
            return False, f"下载异常: {e}"
