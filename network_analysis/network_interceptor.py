#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest网络请求拦截器

用于监听和分析Pinterest页面的网络请求，识别API端点和参数结构
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable
from urllib.parse import urlparse, parse_qs

from loguru import logger
from patchright.sync_api import sync_playwright, Page, BrowserContext, Response, Request

import config
from browser import Browser


class NetworkInterceptor:
    """Pinterest网络请求拦截器"""
    
    def __init__(self, output_dir: str = "network_analysis/results"):
        """初始化网络拦截器
        
        Args:
            output_dir: 输出目录路径
        """
        self.output_dir = output_dir
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.network_logs = []
        self.api_responses = []
        
        # 创建会话专用目录
        self.session_dir = os.path.join(output_dir, f"session_{self.session_id}")
        os.makedirs(self.session_dir, exist_ok=True)
        os.makedirs(os.path.join(self.session_dir, "network_logs"), exist_ok=True)
        os.makedirs(os.path.join(self.session_dir, "api_responses"), exist_ok=True)
        
        # Pinterest API相关的URL模式
        self.pinterest_api_patterns = [
            "api.pinterest.com",
            "v3/search/pins",
            "BoardFeedResource",
            "SearchResource",
            "UserPinsResource",
            "RelatedPinsResource",
            "PinResource",
            "resource/",
            "/v3/",
            "graphql"
        ]
        
        logger.info(f"网络拦截器初始化完成，会话ID: {self.session_id}")
        logger.info(f"输出目录: {self.session_dir}")
    
    def _is_pinterest_api_request(self, url: str) -> bool:
        """判断是否为Pinterest API请求
        
        Args:
            url: 请求URL
            
        Returns:
            是否为Pinterest API请求
        """
        return any(pattern in url for pattern in self.pinterest_api_patterns)
    
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
    
    def _extract_response_info(self, response: Response) -> Dict:
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
            body = response.body()
            response_info["size"] = len(body) if body else 0
        except Exception as e:
            logger.debug(f"无法获取响应体大小: {e}")
            response_info["size"] = 0
        
        # 尝试提取JSON数据
        try:
            if "application/json" in response_info["content_type"]:
                json_data = response.json()
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
        """处理请求事件
        
        Args:
            request: Playwright请求对象
        """
        if self._is_pinterest_api_request(request.url):
            request_info = self._extract_request_info(request)
            self.network_logs.append({
                "type": "request",
                "data": request_info
            })
            
            logger.debug(f"拦截到API请求: {request.method} {request.url}")
    
    def _handle_response(self, response: Response):
        """处理响应事件
        
        Args:
            response: Playwright响应对象
        """
        if self._is_pinterest_api_request(response.url):
            response_info = self._extract_response_info(response)
            self.network_logs.append({
                "type": "response",
                "data": response_info
            })
            
            # 如果是成功的JSON响应，保存到API响应列表
            if response.status == 200 and "json_data" in response_info:
                self.api_responses.append(response_info)
                logger.info(f"捕获到API响应: {response.url} (状态: {response.status})")
            else:
                logger.debug(f"拦截到响应: {response.url} (状态: {response.status})")
    
    def start_analysis(self, url: str, scroll_count: int = 10, wait_time: int = 2) -> Dict:
        """开始网络分析
        
        Args:
            url: 要分析的Pinterest URL
            scroll_count: 滚动次数
            wait_time: 每次滚动后的等待时间(秒)
            
        Returns:
            分析结果摘要
        """
        logger.info(f"开始网络分析: {url}")
        logger.info(f"滚动次数: {scroll_count}, 等待时间: {wait_time}秒")
        
        # 使用现有的Browser类配置
        browser = Browser(
            cookie_path=getattr(config, 'COOKIE_PATH', 'cookies.json'),
            timeout=getattr(config, 'DEFAULT_TIMEOUT', 30)
        )
        
        try:
            # 启动浏览器 (headless=True)
            if not browser.start():
                raise Exception("浏览器启动失败")
            
            # 设置网络监听器
            browser.page.on("request", self._handle_request)
            browser.page.on("response", self._handle_response)
            
            # 访问页面
            if not browser.get_url(url):
                raise Exception(f"无法访问URL: {url}")
            
            logger.info("页面加载完成，开始滚动分析...")
            
            # 执行滚动分析
            for i in range(scroll_count):
                logger.info(f"执行滚动 {i+1}/{scroll_count}")
                
                # 滚动页面
                browser.scroll_by(browser.viewport_height)
                
                # 等待网络请求完成
                time.sleep(wait_time)
                
                # 记录当前状态
                current_api_count = len(self.api_responses)
                logger.info(f"当前已捕获 {current_api_count} 个API响应")
            
            # 保存分析结果
            self._save_results()
            
            # 生成分析摘要
            summary = self._generate_summary()
            logger.info("网络分析完成")
            
            return summary
            
        except Exception as e:
            logger.error(f"网络分析失败: {e}")
            raise
        finally:
            browser.stop()
    
    def _save_results(self):
        """保存分析结果到文件"""
        # 保存网络日志
        network_log_file = os.path.join(self.session_dir, "network_logs", "requests.json")
        with open(network_log_file, 'w', encoding='utf-8') as f:
            json.dump(self.network_logs, f, indent=2, ensure_ascii=False)
        
        # 保存API响应
        api_response_file = os.path.join(self.session_dir, "api_responses", "responses.json")
        with open(api_response_file, 'w', encoding='utf-8') as f:
            json.dump(self.api_responses, f, indent=2, ensure_ascii=False)
        
        logger.info(f"分析结果已保存:")
        logger.info(f"  网络日志: {network_log_file}")
        logger.info(f"  API响应: {api_response_file}")
    
    def _generate_summary(self) -> Dict:
        """生成分析摘要
        
        Returns:
            分析摘要字典
        """
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
            status = response["status"]
            response_statuses[status] = response_statuses.get(status, 0) + 1
            if status == 200:
                successful_responses += 1
        
        summary = {
            "session_id": self.session_id,
            "total_requests": len([log for log in self.network_logs if log["type"] == "request"]),
            "total_responses": len([log for log in self.network_logs if log["type"] == "response"]),
            "successful_api_responses": successful_responses,
            "request_methods": request_methods,
            "response_statuses": response_statuses,
            "unique_domains": list(domains),
            "unique_endpoints": list(api_endpoints),
            "output_directory": self.session_dir
        }
        
        return summary 