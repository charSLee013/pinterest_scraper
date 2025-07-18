#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest网络请求拦截器

用于监听和分析Pinterest页面的网络请求，识别API端点和参数结构
"""

import json
import os
import time
import threading
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any, Union
from urllib.parse import urlparse, parse_qs
from collections import deque

from loguru import logger
from patchright.sync_api import sync_playwright, Page, BrowserContext, Response, Request
from tqdm import tqdm

from ..core import config
# Browser import removed - using BrowserManager instead


class PinDataExtractor:
    """Pinterest Pin数据提取器

    从API响应中提取结构化的Pin数据，支持多种Pinterest API格式
    """

    @staticmethod
    def extract_pins_from_response(json_data: Dict) -> List[Dict]:
        """从API响应中提取Pin数据列表

        Args:
            json_data: Pinterest API响应的JSON数据

        Returns:
            提取的Pin数据列表
        """
        if not isinstance(json_data, dict):
            return []

        pins = []

        # 尝试多种可能的Pin数据位置
        possible_paths = [
            # 搜索API响应格式
            ["resource_response", "data"],
            ["resource_response", "data", "results"],
            # GraphQL响应格式 - Pin详情页相关推荐
            ["data", "v3RelatedPinsForPinSeoQuery", "data", "connection", "edges"],
            # 其他GraphQL格式
            ["data", "node", "pins"],
            ["data", "viewer", "pins"],
            # 直接结果格式
            ["results"],
            ["data", "results"],
            ["pins"],
            ["data", "pins"],
            # Pin详情页相关推荐格式
            ["resource_response", "data", "related_pins"],
            ["data", "related_pins"],
        ]

        for path in possible_paths:
            extracted_pins = PinDataExtractor._extract_from_path(json_data, path)
            if extracted_pins:
                pins.extend(extracted_pins)
                logger.debug(f"从路径 {' -> '.join(path)} 提取到 {len(extracted_pins)} 个Pin")

        # 去重处理（基于Pin ID）
        unique_pins = {}
        for pin in pins:
            pin_id = pin.get('id')
            if pin_id and pin_id not in unique_pins:
                unique_pins[pin_id] = pin

        result = list(unique_pins.values())
        return result

    @staticmethod
    def _extract_from_path(data: Dict, path: List[str]) -> List[Dict]:
        """从指定路径提取Pin数据

        Args:
            data: JSON数据
            path: 数据路径

        Returns:
            Pin数据列表
        """
        current_data = data
        for key in path:
            if isinstance(current_data, dict) and key in current_data:
                current_data = current_data[key]
            else:
                return []

        if isinstance(current_data, list):
            # 验证是否为Pin数据
            valid_pins = []
            for item in current_data:
                # 处理GraphQL edges格式
                if isinstance(item, dict) and "node" in item:
                    # GraphQL edge格式: {"node": {"__typename": "Pin", ...}}
                    pin_data = item["node"]
                    if isinstance(pin_data, dict) and PinDataExtractor._is_valid_pin(pin_data):
                        normalized_pin = PinDataExtractor._normalize_pin_data(pin_data)
                        valid_pins.append(normalized_pin)
                # 处理直接Pin格式
                elif isinstance(item, dict) and PinDataExtractor._is_valid_pin(item):
                    normalized_pin = PinDataExtractor._normalize_pin_data(item)
                    valid_pins.append(normalized_pin)
            return valid_pins

        return []

    @staticmethod
    def _normalize_pin_id(pin_id: str) -> str:
        """标准化Pin ID格式，处理Base64编码的GraphQL ID

        Args:
            pin_id: 原始Pin ID

        Returns:
            标准化后的Pin ID (纯数字格式)
        """
        if not pin_id:
            return ""

        # 检查是否为Base64编码的GraphQL ID (以UGlu开头)
        if pin_id.startswith('UGlu'):
            try:
                import base64
                decoded = base64.b64decode(pin_id).decode('utf-8')
                if decoded.startswith('Pin:'):
                    # 去掉'Pin:'前缀，返回纯数字ID
                    return decoded[4:]
            except Exception as e:
                logger.error(f"解码Pin ID失败: {e}")

        return pin_id

    @staticmethod
    def _is_valid_pin(data: Dict) -> bool:
        """验证数据是否为有效的Pin数据

        Args:
            data: 待验证的数据

        Returns:
            是否为有效Pin数据
        """
        # 必须有有效的Pin ID
        pin_id = data.get('id') or data.get('entityId', '')
        if not pin_id:
            return False

        # 标准化Pin ID并验证是否为数字
        normalized_id = PinDataExtractor._normalize_pin_id(str(pin_id))
        if not normalized_id or not normalized_id.isdigit():
            return False

        # 必须包含图片信息（支持GraphQL格式的imageSpec_*字段）
        has_images = (
            data.get('images') or
            data.get('image') or
            data.get('image_signature') or
            any(key.startswith('imageSpec_') for key in data.keys()) or  # GraphQL格式
            (data.get('__typename') == 'Pin' and 'images' in data)
        )

        if not has_images:
            return False

        # 检查GraphQL Pin格式
        if data.get('__typename') == 'Pin':
            return True

        # 检查传统格式的关键字段
        required_fields = ['title', 'description', 'url']
        return any(field in data for field in required_fields)

    @staticmethod
    def _normalize_pin_data(raw_pin: Dict) -> Dict:
        """标准化Pin数据格式

        Args:
            raw_pin: 原始Pin数据

        Returns:
            标准化后的Pin数据
        """
        # 决策理由：统一数据格式，便于后续处理和存储
        normalized = {
            "id": "",
            "title": "",
            "description": "",
            "image_urls": {},
            "largest_image_url": "",
            "url": "",
            "creator": {},
            "stats": {},
            "board": {},
            "created_at": "",
            "source_link": "",
            "categories": [],
            "extracted_at": datetime.now().isoformat()
        }

        # 提取Pin ID (优先使用entityId，这是真实的数字ID)
        pin_id = raw_pin.get("entityId") or raw_pin.get("id", "")
        normalized["id"] = str(pin_id) if pin_id else ""

        # 提取标题和描述
        normalized["title"] = raw_pin.get("title", "")
        normalized["description"] = raw_pin.get("description", "")

        # 提取图片URLs
        image_urls = PinDataExtractor._extract_image_urls(raw_pin)
        normalized["image_urls"] = image_urls
        normalized["largest_image_url"] = PinDataExtractor._get_largest_image_url(image_urls)

        # 提取Pin URL
        pin_id = normalized["id"]
        if pin_id:
            normalized["url"] = f"https://www.pinterest.com/pin/{pin_id}/"
        elif "url" in raw_pin:
            normalized["url"] = raw_pin["url"]

        # 提取创建者信息
        if "pinner" in raw_pin:
            creator_data = raw_pin["pinner"]
            normalized["creator"] = {
                "id": creator_data.get("id", ""),
                "username": creator_data.get("username", ""),
                "full_name": creator_data.get("full_name", ""),
                "image_url": creator_data.get("image_medium_url", "")
            }

        # 提取统计信息
        stats = {}
        if "like_count" in raw_pin:
            stats["likes"] = raw_pin["like_count"]
        if "repin_count" in raw_pin:
            stats["saves"] = raw_pin["repin_count"]
        if "comment_count" in raw_pin:
            stats["comments"] = raw_pin["comment_count"]
        normalized["stats"] = stats

        # 提取Board信息
        if "board" in raw_pin:
            board_data = raw_pin["board"]
            normalized["board"] = {
                "id": board_data.get("id", ""),
                "name": board_data.get("name", ""),
                "url": board_data.get("url", "")
            }

        # 提取其他元数据
        if "created_at" in raw_pin:
            normalized["created_at"] = raw_pin["created_at"]
        if "link" in raw_pin:
            normalized["source_link"] = raw_pin["link"]

        return normalized

    @staticmethod
    def _extract_image_urls(pin_data: Dict) -> Dict[str, str]:
        """提取Pin的图片URLs

        Args:
            pin_data: Pin数据

        Returns:
            图片URL字典，键为尺寸，值为URL
        """
        image_urls = {}

        # 处理GraphQL格式的imageSpec_*字段（新格式）
        for key, value in pin_data.items():
            if key.startswith("imageSpec_") and isinstance(value, dict) and "url" in value:
                size_key = key.replace("imageSpec_", "")
                if size_key == "orig":
                    image_urls["original"] = value["url"]
                else:
                    image_urls[size_key] = value["url"]

        # 处理images字段（标准格式）
        if "images" in pin_data and isinstance(pin_data["images"], dict):
            for size_key, img_data in pin_data["images"].items():
                if isinstance(img_data, dict) and "url" in img_data:
                    if size_key == "orig":
                        image_urls["original"] = img_data["url"]
                    else:
                        # 提取尺寸信息
                        size = size_key.replace("x", "")
                        image_urls[size] = img_data["url"]

        # 处理单个image字段
        elif "image" in pin_data:
            image_data = pin_data["image"]
            if isinstance(image_data, dict):
                # 处理多尺寸格式
                for size_key, url in image_data.items():
                    if isinstance(url, str) and url.startswith("http"):
                        image_urls[size_key] = url
            elif isinstance(image_data, str):
                image_urls["default"] = image_data

        return image_urls

    @staticmethod
    def _get_largest_image_url(image_urls: Dict[str, str]) -> str:
        """获取最大尺寸的图片URL

        Args:
            image_urls: 图片URL字典

        Returns:
            最大尺寸图片的URL
        """
        if not image_urls:
            return ""

        # 优先级顺序：original > 数字尺寸（从大到小）> default
        if "original" in image_urls:
            return image_urls["original"]

        # 按数字尺寸排序
        numeric_sizes = []
        for size_key, url in image_urls.items():
            if size_key.isdigit():
                numeric_sizes.append((int(size_key), url))

        if numeric_sizes:
            numeric_sizes.sort(reverse=True)
            return numeric_sizes[0][1]

        # 返回任意可用的URL
        return next(iter(image_urls.values()))


class CredentialExtractor:
    """Pinterest认证凭证提取器

    从网络请求中提取认证信息，为后续HTTP API调用做准备
    """

    def __init__(self):
        self.credentials = {
            "cookies": {},
            "csrf_token": "",
            "headers": {},
            "user_agent": "",
            "app_version": "",
            "extracted_at": ""
        }
        self._lock = threading.Lock()

    def extract_from_request(self, request_data: Dict) -> None:
        """从请求数据中提取认证信息

        Args:
            request_data: 请求数据字典
        """
        with self._lock:
            headers = request_data.get("headers", {})

            # 提取Cookie
            cookie_header = headers.get("cookie", "")
            if cookie_header:
                self._parse_cookies(cookie_header)

            # 提取CSRF Token
            csrf_token = headers.get("x-csrftoken", "")
            if csrf_token:
                self.credentials["csrf_token"] = csrf_token

            # 提取重要的请求头
            important_headers = [
                "x-pinterest-appstate",
                "x-requested-with",
                "x-pinterest-source-url",
                "x-pinterest-pws-handler",
                "accept",
                "accept-language",
                "referer"
            ]

            for header_name in important_headers:
                header_value = headers.get(header_name, "")
                if header_value:
                    self.credentials["headers"][header_name] = header_value

            # 提取User-Agent
            user_agent = headers.get("user-agent", "")
            if user_agent:
                self.credentials["user_agent"] = user_agent

            # 更新提取时间
            self.credentials["extracted_at"] = datetime.now().isoformat()

            logger.debug(f"更新认证凭证: CSRF={bool(csrf_token)}, Cookies={len(self.credentials['cookies'])}")

    def _parse_cookies(self, cookie_header: str) -> None:
        """解析Cookie字符串

        Args:
            cookie_header: Cookie头字符串
        """
        for cookie_pair in cookie_header.split(";"):
            if "=" in cookie_pair:
                name, value = cookie_pair.strip().split("=", 1)
                self.credentials["cookies"][name] = value

    def get_credentials(self) -> Dict:
        """获取当前的认证凭证

        Returns:
            认证凭证字典
        """
        with self._lock:
            return self.credentials.copy()

    def is_valid(self) -> bool:
        """检查认证凭证是否有效

        Returns:
            凭证是否有效
        """
        return bool(self.credentials["csrf_token"] and self.credentials["cookies"])


class NetworkInterceptor:
    """Pinterest网络请求拦截器 - 增强版

    支持数据提取、凭证管理和Pin详情页滚动策略
    """

    def __init__(self, output_dir: str = "network_analysis/results", max_cache_size: int = 1000, verbose: bool = True, target_count: int = 0, mode: str = "full"):
        """初始化网络拦截器

        Args:
            output_dir: 输出目录路径
            max_cache_size: 最大缓存大小，防止内存泄漏
            verbose: 是否输出详细日志
            target_count: 目标采集数量，用于进度条显示
            mode: 拦截模式 ("full": 全部API, "related_only": 仅RelatedModulesResource)
        """
        self.verbose = verbose
        self.output_dir = output_dir
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.max_cache_size = max_cache_size
        self.target_count = target_count
        self.mode = mode

        # 使用deque实现滑动窗口，防止内存泄漏
        self.network_logs = deque(maxlen=max_cache_size)
        self.api_responses = deque(maxlen=max_cache_size)

        # 新增：提取的Pin数据和认证凭证
        self.extracted_pins = deque(maxlen=max_cache_size)
        self.pin_extractor = PinDataExtractor()
        self.credential_extractor = CredentialExtractor()

        # 线程安全锁
        self._lock = threading.Lock()

        # 资源清理跟踪
        self._cleanup_handlers = []
        self._is_cleaned_up = False

        # 进度条（如果设置了目标数量）
        self.pbar = None
        if target_count > 0:
            self.pbar = tqdm(total=target_count, desc="API采集", unit="pins", leave=False)

        # 统计信息
        self.stats = {
            "total_requests": 0,
            "total_responses": 0,
            "total_pins_extracted": 0,
            "api_endpoints_hit": set(),
            "session_start_time": time.time()
        }
        
        # 创建会话专用目录
        self.session_dir = os.path.join(output_dir, f"session_{self.session_id}")
        os.makedirs(self.session_dir, exist_ok=True)
        os.makedirs(os.path.join(self.session_dir, "network_logs"), exist_ok=True)
        os.makedirs(os.path.join(self.session_dir, "api_responses"), exist_ok=True)
        
        # 根据模式设置API拦截规则
        if self.mode == "related_only":
            # 第二阶段：只关注GraphQL端点的v3RelatedPinsForPinSeoQuery
            self.pinterest_api_patterns = [
                "_/graphql/"
            ]
            if self.verbose:
                logger.info("🎯 NetworkInterceptor模式: 仅拦截GraphQL端点")
        else:
            # 第一阶段：全部API模式
            self.pinterest_api_patterns = [
                "api.pinterest.com",
                "v3/search/pins",
                "BoardFeedResource",
                "SearchResource",
                "BaseSearchResource",  # 搜索API
                "UserPinsResource",
                "RelatedPinsResource",  # Pin详情页相关推荐
                "RelatedModulesResource",  # 🔥 修复：添加关键的RelatedModulesResource
                "PinResource",
                "VisualSearchResource",  # 视觉搜索
                "HomefeedResource",  # 首页推荐
                "resource/",
                "/v3/",
                "graphql",
                "_/graphql/",  # GraphQL端点
                "CloseupDetailsResource",  # Pin详情
                "MoreLikeThisResource",  # 更多相似内容
                "RelatedPinFeedResource"  # 相关Pin推荐
            ]
            if self.verbose:
                logger.info("🔍 NetworkInterceptor模式: 拦截所有Pinterest API")
        
        if self.verbose:
            logger.info(f"网络拦截器初始化完成，会话ID: {self.session_id}")
            logger.info(f"输出目录: {self.session_dir}")
    
    def _is_pinterest_api_request(self, url: str) -> bool:
        """判断是否为Pinterest API请求

        Args:
            url: 请求URL

        Returns:
            是否为Pinterest API请求
        """
        for pattern in self.pinterest_api_patterns:
            if pattern in url:
                if self.verbose:
                    logger.debug(f"✅ 匹配Pinterest API: {url} (模式: {pattern})")
                return True
        # 移除未匹配的DEBUG日志，减少冗余输出
        return False
    
    def _extract_request_info(self, request: Request) -> Dict:
        """提取请求信息
        
        Args:
            request: Playwright请求对象
            
        Returns:
            请求信息字典
        """
        parsed_url = urlparse(request.url)
        query_params = parse_qs(parsed_url.query)
        
        request_info = {
            "timestamp": datetime.now().isoformat(),
            "method": request.method,
            "url": request.url,
            "domain": parsed_url.netloc,
            "path": parsed_url.path,
            "query_params": query_params,
            "headers": dict(request.headers),
            "resource_type": request.resource_type,
            "post_data": request.post_data if request.method == "POST" else None
        }
        
        return request_info
    
    async def _extract_response_info(self, response: Response) -> Dict:
        """提取响应信息

        Args:
            response: Playwright响应对象

        Returns:
            响应信息字典
        """
        response_info = {
            "timestamp": datetime.now().isoformat(),
            "status": response.status,
            "status_text": response.status_text,
            "url": response.url,
            "headers": dict(response.headers),
            "content_type": response.headers.get("content-type", ""),
            "size": 0  # 默认为0，稍后尝试获取
        }
        
        # 安全地获取响应体大小
        try:
            body = await response.body()
            response_info["size"] = len(body) if body else 0
        except Exception as e:
            logger.debug(f"无法获取响应体大小: {e}")
            response_info["size"] = 0
        
        # 尝试提取JSON数据
        try:
            if "application/json" in response_info["content_type"]:
                json_data = await response.json()
                response_info["json_data"] = json_data
                
                # 分析JSON结构中的关键信息
                if isinstance(json_data, dict):
                    # 查找常见的Pinterest数据结构
                    if "resource_response" in json_data:
                        response_info["has_resource_response"] = True
                        resource_data = json_data["resource_response"]
                        if "data" in resource_data:
                            response_info["data_keys"] = list(resource_data["data"].keys()) if isinstance(resource_data["data"], dict) else []
                    
                    # 查找分页信息
                    if "bookmarks" in json_data:
                        response_info["has_bookmarks"] = True
                        response_info["bookmarks"] = json_data["bookmarks"]
                    
                    # 查找pins数据
                    if "results" in json_data:
                        response_info["has_results"] = True
                        if isinstance(json_data["results"], list):
                            response_info["results_count"] = len(json_data["results"])
                
        except Exception as e:
            logger.debug(f"无法解析JSON响应: {e}")
            response_info["json_parse_error"] = str(e)
        
        return response_info
    
    def _handle_request(self, request: Request):
        """处理请求事件 - 增强版，支持认证信息提取

        Args:
            request: Playwright请求对象
        """
        try:
            if self._is_pinterest_api_request(request.url):
                request_info = self._extract_request_info(request)

                # 提取认证凭证
                self.credential_extractor.extract_from_request(request_info)

                # 线程安全地添加到网络日志
                with self._lock:
                    log_entry = {
                        "type": "request",
                        "data": request_info,
                        "timestamp": time.time()
                    }
                    self.network_logs.append(log_entry)
                    self.stats["total_requests"] += 1

                    # 记录API端点
                    parsed_url = urlparse(request.url)
                    endpoint = parsed_url.path
                    self.stats["api_endpoints_hit"].add(endpoint)

                if self.verbose:
                    logger.debug(f"🔍 拦截API请求: {request.method} {request.url}")
                    logger.debug(f"📊 当前日志数量: {len(self.network_logs)}, 认证凭证有效: {self.credential_extractor.is_valid()}")
        except Exception as e:
            logger.error(f"处理请求事件时发生错误: {e}")
            logger.error(f"请求URL: {request.url}")
            import traceback
            logger.error(f"错误详情: {traceback.format_exc()}")
    
    async def _handle_response(self, response: Response):
        """处理响应事件 - 增强版，支持Pin数据提取

        Args:
            response: Playwright响应对象
        """
        try:
            is_pinterest_api = self._is_pinterest_api_request(response.url)
            if self.verbose and is_pinterest_api:
                logger.debug(f"📥 处理Pinterest API响应: {response.url}, 状态: {response.status}")
            if is_pinterest_api:
                response_info = await self._extract_response_info(response)

                # 线程安全地处理响应
                with self._lock:
                    # 查找关联请求
                    related_request = next((log for log in reversed(self.network_logs)
                                            if log['type'] == 'request' and log['data']['url'] == response.request.url), None)

                    if related_request:
                        # 将请求和响应的关键信息整合
                        log_entry = {
                            "request_url": related_request['data']['url'],
                            "request_method": related_request['data']['method'],
                            "request_headers": related_request['data']['headers'],
                            "post_data": related_request['data'].get('post_data'),
                            "response_status": response_info['status'],
                            "response_headers": response_info['headers'],
                            "response_json": response_info.get('json_data'),
                            "timestamp": time.time()
                        }

                        # 添加到日志
                        self.network_logs.append({
                            "type": "response_paired",
                            "data": log_entry,
                            "timestamp": time.time()
                        })

                        # 如果是成功的JSON响应，进行数据提取
                        if response.status == 200 and "json_data" in response_info:
                            self.api_responses.append(log_entry)
                            self.stats["total_responses"] += 1

                            # 提取Pin数据
                            json_data = response_info["json_data"]
                            extracted_pins = self.pin_extractor.extract_pins_from_response(json_data)

                            if extracted_pins:
                                # 添加提取的Pin数据
                                for pin in extracted_pins:
                                    pin["source_api"] = response.url
                                    pin["extraction_timestamp"] = time.time()
                                    self.extracted_pins.append(pin)

                                self.stats["total_pins_extracted"] += len(extracted_pins)

                                # 使用进度条替代频繁日志
                                if self.pbar:
                                    self.pbar.update(len(extracted_pins))
                                    self.pbar.set_postfix({"总计": len(self.extracted_pins)})
                                elif self.verbose:
                                    # 只在没有进度条时才输出日志
                                    logger.debug(f"提取到 {len(extracted_pins)} 个Pin，总计: {len(self.extracted_pins)}")
                            else:
                                # 调试：记录未提取到Pin的情况
                                if self.verbose and "_/graphql/" in response.url:
                                    logger.debug(f"GraphQL响应未提取到Pin数据: {response.url}")
                                    if "data" in json_data:
                                        data_keys = list(json_data["data"].keys()) if isinstance(json_data["data"], dict) else []
                                        logger.debug(f"GraphQL响应data字段包含: {data_keys}")

                                        # 详细检查v3RelatedPinsForPinSeoQuery结构
                                        if "v3RelatedPinsForPinSeoQuery" in json_data["data"]:
                                            query_data = json_data["data"]["v3RelatedPinsForPinSeoQuery"]
                                            logger.debug(f"v3RelatedPinsForPinSeoQuery结构: {list(query_data.keys()) if isinstance(query_data, dict) else type(query_data)}")

                                            if isinstance(query_data, dict) and "data" in query_data:
                                                inner_data = query_data["data"]
                                                logger.debug(f"v3RelatedPinsForPinSeoQuery.data结构: {list(inner_data.keys()) if isinstance(inner_data, dict) else type(inner_data)}")

                                                if isinstance(inner_data, dict) and "connection" in inner_data:
                                                    connection = inner_data["connection"]
                                                    logger.debug(f"connection结构: {list(connection.keys()) if isinstance(connection, dict) else type(connection)}")

                                                    if isinstance(connection, dict) and "edges" in connection:
                                                        edges = connection["edges"]
                                                        logger.debug(f"edges数量: {len(edges) if isinstance(edges, list) else type(edges)}")

                                                        if isinstance(edges, list) and len(edges) > 0:
                                                            first_edge = edges[0]
                                                            logger.debug(f"第一个edge结构: {list(first_edge.keys()) if isinstance(first_edge, dict) else type(first_edge)}")

                                                            if isinstance(first_edge, dict) and "node" in first_edge:
                                                                node = first_edge["node"]
                                                                logger.debug(f"第一个node结构: {list(node.keys())[:10] if isinstance(node, dict) else type(node)}")
                                                                logger.debug(f"node有entityId: {'entityId' in node if isinstance(node, dict) else False}")
                                                                logger.debug(f"node有imageSpec_orig: {'imageSpec_orig' in node if isinstance(node, dict) else False}")

                            if self.verbose:
                                logger.debug(f"📊 API响应处理完成: {response.url} (状态: {response.status})")
                        else:
                            if self.verbose:
                                logger.debug(f"📄 API响应: {response.url} (状态: {response.status}), Content-Type: {response_info.get('content_type', 'N/A')}, Has JSON: {'json_data' in response_info}")
                    else:
                        # 如果找不到关联请求，按原方式记录
                        self.network_logs.append({
                            "type": "response_unpaired",
                            "data": response_info,
                            "timestamp": time.time()
                        })
                        logger.warning(f"未找到关联请求的响应: {response.url}, 状态: {response_info['status']}")
        except Exception as e:
            logger.error(f"处理响应事件时发生错误: {e}")
            logger.error(f"响应URL: {response.url}")
            logger.error(f"响应状态: {response.status}")
            logger.error(f"响应Content-Type: {response.headers.get('content-type', 'N/A')}")
            import traceback
            logger.error(f"错误详情: {traceback.format_exc()}")
    
    def get_request_by_filter(self, filter_func: Callable[[Dict], bool]) -> Optional[Dict]:
        """
        根据指定的过滤函数查找并返回第一个匹配的请求日志。

        Args:
            filter_func: 一个接收请求字典并返回布尔值的过滤函数。

        Returns:
            第一个匹配的请求字典，如果未找到则返回 None。
        """
        for log_entry in self.api_responses:
            # 我们在api_responses中存储了整合后的请求/响应对
            if filter_func(log_entry):
                # 返回一个更适合凭证提取的结构
                return {
                    "url": log_entry.get("request_url"),
                    "method": log_entry.get("request_method"),
                    "headers": log_entry.get("request_headers"),
                    "post_data": log_entry.get("post_data")
                }
        
        logger.warning("未通过过滤器找到匹配的请求。")
        return None

    def get_extracted_pins(self) -> List[Dict]:
        """获取提取的Pin数据列表

        Returns:
            提取的Pin数据列表
        """
        with self._lock:
            return list(self.extracted_pins)

    def get_credentials(self) -> Dict:
        """获取提取的认证凭证

        Returns:
            认证凭证字典
        """
        return self.credential_extractor.get_credentials()

    def get_session_stats(self) -> Dict:
        """获取会话统计信息

        Returns:
            统计信息字典
        """
        with self._lock:
            current_time = time.time()
            session_duration = current_time - self.stats["session_start_time"]

            return {
                "session_id": self.session_id,
                "session_duration_seconds": session_duration,
                "total_requests": self.stats["total_requests"],
                "total_responses": self.stats["total_responses"],
                "total_pins_extracted": self.stats["total_pins_extracted"],
                "unique_api_endpoints": len(self.stats["api_endpoints_hit"]),
                "api_endpoints": list(self.stats["api_endpoints_hit"]),
                "credentials_valid": self.credential_extractor.is_valid(),
                "current_cache_sizes": {
                    "network_logs": len(self.network_logs),
                    "api_responses": len(self.api_responses),
                    "extracted_pins": len(self.extracted_pins)
                }
            }

    # 移除了重复的浏览器管理方法，现在专注于数据提取
    # 浏览器管理由BrowserManager统一处理
    
    def _save_results(self):
        """保存分析结果到文件 - 增强版，支持Pin数据和认证凭证"""
        try:
            with self._lock:
                # 保存网络日志
                network_log_file = os.path.join(self.session_dir, "network_logs", "requests.json")
                with open(network_log_file, 'w', encoding='utf-8') as f:
                    json.dump(list(self.network_logs), f, indent=2, ensure_ascii=False)

                # 保存API响应
                api_response_file = os.path.join(self.session_dir, "api_responses", "responses.json")
                with open(api_response_file, 'w', encoding='utf-8') as f:
                    json.dump(list(self.api_responses), f, indent=2, ensure_ascii=False)

                # 保存提取的Pin数据
                pins_data_file = os.path.join(self.session_dir, "extracted_pins.json")
                with open(pins_data_file, 'w', encoding='utf-8') as f:
                    json.dump(list(self.extracted_pins), f, indent=2, ensure_ascii=False)

                # 保存认证凭证
                credentials_file = os.path.join(self.session_dir, "credentials.json")
                with open(credentials_file, 'w', encoding='utf-8') as f:
                    json.dump(self.credential_extractor.get_credentials(), f, indent=2, ensure_ascii=False)

                # 保存会话统计
                stats_file = os.path.join(self.session_dir, "session_stats.json")
                with open(stats_file, 'w', encoding='utf-8') as f:
                    stats = self.get_session_stats()
                    # 转换set为list以便JSON序列化
                    if "api_endpoints" in stats:
                        stats["api_endpoints"] = list(stats["api_endpoints"])
                    json.dump(stats, f, indent=2, ensure_ascii=False)

                logger.info(f"分析结果已保存:")
                logger.info(f"  网络日志: {network_log_file}")
                logger.info(f"  API响应: {api_response_file}")
                logger.info(f"  提取Pin数据: {pins_data_file} ({len(self.extracted_pins)} 个Pin)")
                logger.info(f"  认证凭证: {credentials_file}")
                logger.info(f"  会话统计: {stats_file}")

        except Exception as e:
            logger.error(f"保存分析结果时发生错误: {e}")
            import traceback
            logger.error(f"错误详情: {traceback.format_exc()}")
    
    def _generate_summary(self) -> Dict:
        """生成分析摘要 - 增强版，包含Pin数据和认证信息

        Returns:
            分析摘要字典
        """
        with self._lock:
            # 统计请求类型
            request_methods = {}
            api_endpoints = set()
            domains = set()

            for log in self.network_logs:
                if log["type"] == "request":
                    method = log["data"]["method"]
                    request_methods[method] = request_methods.get(method, 0) + 1

                    parsed_url = urlparse(log["data"]["url"])
                    domains.add(parsed_url.netloc)
                    api_endpoints.add(parsed_url.path)

            # 统计响应状态
            response_statuses = {}
            successful_responses = 0

            for response in self.api_responses:
                status = response.get("response_status", 0)
                response_statuses[status] = response_statuses.get(status, 0) + 1
                if status == 200:
                    successful_responses += 1

            # 分析提取的Pin数据
            pin_stats = {
                "total_pins": len(self.extracted_pins),
                "pins_with_images": 0,
                "pins_with_stats": 0,
                "unique_creators": set(),
                "api_sources": set()
            }

            for pin in self.extracted_pins:
                if pin.get("image_urls"):
                    pin_stats["pins_with_images"] += 1
                if pin.get("stats"):
                    pin_stats["pins_with_stats"] += 1
                if pin.get("creator", {}).get("id"):
                    pin_stats["unique_creators"].add(pin["creator"]["id"])
                if pin.get("source_api"):
                    pin_stats["api_sources"].add(pin["source_api"])

            # 转换set为list以便JSON序列化
            pin_stats["unique_creators"] = len(pin_stats["unique_creators"])
            pin_stats["api_sources"] = list(pin_stats["api_sources"])

            summary = {
                "session_id": self.session_id,
                "session_stats": self.get_session_stats(),
                "network_analysis": {
                    "total_requests": len([log for log in self.network_logs if log["type"] == "request"]),
                    "total_responses": len([log for log in self.network_logs if log["type"] in ["response_paired", "response_unpaired"]]),
                    "successful_api_responses": successful_responses,
                    "request_methods": request_methods,
                    "response_statuses": response_statuses,
                    "unique_domains": list(domains),
                    "unique_endpoints": list(api_endpoints)
                },
                "pin_extraction": pin_stats,
                "credentials": {
                    "is_valid": self.credential_extractor.is_valid(),
                    "has_csrf_token": bool(self.credential_extractor.credentials.get("csrf_token")),
                    "cookie_count": len(self.credential_extractor.credentials.get("cookies", {})),
                    "header_count": len(self.credential_extractor.credentials.get("headers", {}))
                },
                "output_directory": self.session_dir,
                "generated_at": datetime.now().isoformat()
            }

            return summary

    def export_for_http_api(self, output_file: Optional[str] = None) -> Dict:
        """导出数据用于后续HTTP API调用

        Args:
            output_file: 可选的输出文件路径

        Returns:
            包含所有必要数据的字典
        """
        with self._lock:
            # 准备HTTP API调用所需的数据
            export_data = {
                "session_info": {
                    "session_id": self.session_id,
                    "exported_at": datetime.now().isoformat(),
                    "total_pins_extracted": len(self.extracted_pins)
                },
                "credentials": self.credential_extractor.get_credentials(),
                "pins_data": list(self.extracted_pins),
                "api_patterns": {
                    "search_endpoints": [
                        "BaseSearchResource",
                        "SearchResource",
                        "VisualSearchResource"
                    ],
                    "pin_detail_endpoints": [
                        "RelatedPinsResource",
                        "CloseupDetailsResource",
                        "MoreLikeThisResource"
                    ],
                    "common_headers": self.credential_extractor.credentials.get("headers", {}),
                    "user_agent": self.credential_extractor.credentials.get("user_agent", "")
                },
                "sample_requests": self._get_sample_requests_for_http(),
                "usage_guide": {
                    "description": "此数据可用于构建HTTP API请求，绕过浏览器自动化",
                    "steps": [
                        "1. 使用credentials中的cookies和headers构建HTTP请求",
                        "2. 参考sample_requests中的URL模式和参数",
                        "3. 使用pins_data中的Pin ID构建详情页请求",
                        "4. 实现分页机制以获取更多数据"
                    ],
                    "important_headers": [
                        "x-csrftoken",
                        "x-pinterest-appstate",
                        "x-requested-with",
                        "cookie",
                        "user-agent"
                    ]
                }
            }

            # 如果指定了输出文件，保存数据
            if output_file:
                try:
                    output_path = os.path.join(self.session_dir, output_file)
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(export_data, f, indent=2, ensure_ascii=False)
                    logger.info(f"HTTP API数据已导出到: {output_path}")
                except Exception as e:
                    logger.error(f"导出HTTP API数据失败: {e}")

            return export_data

    def _get_sample_requests_for_http(self) -> List[Dict]:
        """获取用于HTTP API的示例请求

        Returns:
            示例请求列表
        """
        sample_requests = []

        # 从已捕获的API响应中提取示例
        for response in list(self.api_responses)[-5:]:  # 取最近5个响应作为示例
            if response.get("request_url") and response.get("request_method"):
                sample_request = {
                    "method": response["request_method"],
                    "url": response["request_url"],
                    "headers": response.get("request_headers", {}),
                    "post_data": response.get("post_data"),
                    "response_status": response.get("response_status"),
                    "api_type": self._classify_api_endpoint(response["request_url"])
                }
                sample_requests.append(sample_request)

        return sample_requests

    def _classify_api_endpoint(self, url: str) -> str:
        """分类API端点类型

        Args:
            url: API URL

        Returns:
            API类型
        """
        if "BaseSearchResource" in url or "SearchResource" in url:
            return "search"
        elif "RelatedPinsResource" in url or "MoreLikeThisResource" in url:
            return "related_pins"
        elif "CloseupDetailsResource" in url or "PinResource" in url:
            return "pin_detail"
        elif "graphql" in url:
            return "graphql"
        else:
            return "other"

    def clear_cache(self):
        """清理缓存数据，释放内存

        决策理由：长时间运行时防止内存泄漏
        """
        with self._lock:
            self.network_logs.clear()
            self.api_responses.clear()
            self.extracted_pins.clear()

            # 重置统计信息
            self.stats = {
                "total_requests": 0,
                "total_responses": 0,
                "total_pins_extracted": 0,
                "api_endpoints_hit": set(),
                "session_start_time": time.time()
            }

            logger.info("缓存已清理，内存已释放")

    def close_progress_bar(self):
        """关闭进度条"""
        if self.pbar:
            self.pbar.close()
            self.pbar = None

    async def cleanup(self):
        """清理资源，注销事件处理器"""
        if self._is_cleaned_up:
            return

        try:
            # 关闭进度条
            self.close_progress_bar()

            # 清理缓存
            self.clear_cache()

            # 执行所有清理函数
            for cleanup_func in self._cleanup_handlers:
                try:
                    if asyncio.iscoroutinefunction(cleanup_func):
                        await cleanup_func()
                    else:
                        cleanup_func()
                except Exception as e:
                    logger.debug(f"清理处理器时出错: {e}")

            self._cleanup_handlers.clear()
            self._is_cleaned_up = True
            logger.debug("NetworkInterceptor资源已清理")
        except Exception as e:
            logger.error(f"NetworkInterceptor清理失败: {e}")

    async def __aenter__(self):
        """异步上下文管理器入口"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.cleanup()

    def add_cleanup_handler(self, handler):
        """添加清理处理器"""
        if not self._is_cleaned_up:
            self._cleanup_handlers.append(handler)