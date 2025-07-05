#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest API分析器

用于分析捕获的网络请求数据，提取API端点、参数结构和分页信息
"""

import json
import os
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, parse_qs
from collections import defaultdict

from loguru import logger


class APIAnalyzer:
    """Pinterest API分析器"""
    
    def __init__(self):
        """初始化API分析器"""
        self.api_endpoints = {}
        self.pagination_info = {}
        self.data_structures = {}
        self.request_patterns = defaultdict(list)
        
    def analyze_session(self, session_dir: str) -> Dict:
        """分析会话数据
        
        Args:
            session_dir: 会话目录路径
            
        Returns:
            分析结果
        """
        logger.info(f"开始分析会话数据: {session_dir}")
        
        # 加载网络日志
        network_log_file = os.path.join(session_dir, "network_logs", "requests.json")
        api_response_file = os.path.join(session_dir, "api_responses", "responses.json")
        
        if not os.path.exists(network_log_file):
            raise FileNotFoundError(f"网络日志文件不存在: {network_log_file}")
        
        if not os.path.exists(api_response_file):
            raise FileNotFoundError(f"API响应文件不存在: {api_response_file}")
        
        # 读取数据
        with open(network_log_file, 'r', encoding='utf-8') as f:
            network_logs = json.load(f)
        
        with open(api_response_file, 'r', encoding='utf-8') as f:
            api_responses = json.load(f)
        
        # 分析请求模式
        self._analyze_request_patterns(network_logs)
        
        # 分析API端点
        self._analyze_api_endpoints(api_responses)
        
        # 分析分页信息
        self._analyze_pagination_info(api_responses)
        
        # 分析数据结构
        self._analyze_data_structures(api_responses)
        
        # 生成分析报告
        analysis_result = self._generate_analysis_report()
        
        # 保存分析结果
        self._save_analysis_result(session_dir, analysis_result)
        
        logger.info("会话数据分析完成")
        return analysis_result
    
    def _analyze_request_patterns(self, network_logs: List[Dict]):
        """分析请求模式
        
        Args:
            network_logs: 网络日志数据
        """
        logger.info("分析请求模式...")
        
        for log in network_logs:
            if log["type"] == "request":
                data = log["data"]
                url = data["url"]
                method = data["method"]
                
                parsed_url = urlparse(url)
                endpoint = parsed_url.path
                
                # 记录请求模式
                pattern_key = f"{method}:{endpoint}"
                self.request_patterns[pattern_key].append({
                    "url": url,
                    "query_params": data.get("query_params", {}),
                    "headers": data.get("headers", {}),
                    "timestamp": data.get("timestamp")
                })
    
    def _analyze_api_endpoints(self, api_responses: List[Dict]):
        """分析API端点
        
        Args:
            api_responses: API响应数据
        """
        logger.info("分析API端点...")
        
        for response in api_responses:
            url = response["url"]
            parsed_url = urlparse(url)
            endpoint = parsed_url.path
            
            if endpoint not in self.api_endpoints:
                self.api_endpoints[endpoint] = {
                    "url_pattern": endpoint,
                    "domain": parsed_url.netloc,
                    "method": "GET",  # 大多数Pinterest API是GET请求
                    "response_count": 0,
                    "success_count": 0,
                    "sample_urls": [],
                    "query_parameters": set(),
                    "response_structure": {}
                }
            
            endpoint_info = self.api_endpoints[endpoint]
            endpoint_info["response_count"] += 1
            
            if response["status"] == 200:
                endpoint_info["success_count"] += 1
            
            # 记录样本URL
            if len(endpoint_info["sample_urls"]) < 5:
                endpoint_info["sample_urls"].append(url)
            
            # 分析查询参数
            query_params = parse_qs(parsed_url.query)
            for param in query_params.keys():
                endpoint_info["query_parameters"].add(param)
            
            # 分析响应结构
            if "json_data" in response:
                self._analyze_response_structure(endpoint_info, response["json_data"])
    
    def _analyze_response_structure(self, endpoint_info: Dict, json_data: Dict):
        """分析响应结构
        
        Args:
            endpoint_info: 端点信息
            json_data: JSON响应数据
        """
        if not isinstance(json_data, dict):
            return
        
        # 分析顶级键
        top_level_keys = set(json_data.keys())
        if "top_level_keys" not in endpoint_info["response_structure"]:
            endpoint_info["response_structure"]["top_level_keys"] = set()
        endpoint_info["response_structure"]["top_level_keys"].update(top_level_keys)
        
        # 分析resource_response结构
        if "resource_response" in json_data:
            resource_data = json_data["resource_response"]
            if isinstance(resource_data, dict):
                if "resource_response_structure" not in endpoint_info["response_structure"]:
                    endpoint_info["response_structure"]["resource_response_structure"] = set()
                endpoint_info["response_structure"]["resource_response_structure"].update(resource_data.keys())
                
                # 分析data字段
                if "data" in resource_data:
                    data_field = resource_data["data"]
                    if isinstance(data_field, dict):
                        if "data_structure" not in endpoint_info["response_structure"]:
                            endpoint_info["response_structure"]["data_structure"] = set()
                        endpoint_info["response_structure"]["data_structure"].update(data_field.keys())
                    elif isinstance(data_field, list) and data_field:
                        # 分析列表中的第一个元素结构
                        if isinstance(data_field[0], dict):
                            if "data_item_structure" not in endpoint_info["response_structure"]:
                                endpoint_info["response_structure"]["data_item_structure"] = set()
                            endpoint_info["response_structure"]["data_item_structure"].update(data_field[0].keys())
    
    def _analyze_pagination_info(self, api_responses: List[Dict]):
        """分析分页信息
        
        Args:
            api_responses: API响应数据
        """
        logger.info("分析分页信息...")
        
        for response in api_responses:
            if "json_data" in response:
                json_data = response["json_data"]
                url = response["url"]
                parsed_url = urlparse(url)
                endpoint = parsed_url.path
                
                # 查找分页相关信息
                pagination_data = {}
                
                # 检查bookmarks
                if "bookmarks" in json_data:
                    pagination_data["bookmarks"] = json_data["bookmarks"]
                
                # 检查resource_response中的分页信息
                if "resource_response" in json_data:
                    resource_data = json_data["resource_response"]
                    if isinstance(resource_data, dict):
                        if "bookmark" in resource_data:
                            pagination_data["bookmark"] = resource_data["bookmark"]
                        if "has_more" in resource_data:
                            pagination_data["has_more"] = resource_data["has_more"]
                        if "next_page_token" in resource_data:
                            pagination_data["next_page_token"] = resource_data["next_page_token"]
                
                # 分析URL中的分页参数
                query_params = parse_qs(parsed_url.query)
                pagination_params = {}
                for param, values in query_params.items():
                    if any(keyword in param.lower() for keyword in ["bookmark", "page", "cursor", "offset", "limit"]):
                        pagination_params[param] = values
                
                if pagination_data or pagination_params:
                    if endpoint not in self.pagination_info:
                        self.pagination_info[endpoint] = {
                            "response_pagination": [],
                            "url_pagination_params": set()
                        }
                    
                    if pagination_data:
                        self.pagination_info[endpoint]["response_pagination"].append(pagination_data)
                    
                    for param in pagination_params.keys():
                        self.pagination_info[endpoint]["url_pagination_params"].add(param)
    
    def _analyze_data_structures(self, api_responses: List[Dict]):
        """分析数据结构
        
        Args:
            api_responses: API响应数据
        """
        logger.info("分析数据结构...")
        
        for response in api_responses:
            if "json_data" in response:
                json_data = response["json_data"]
                url = response["url"]
                parsed_url = urlparse(url)
                endpoint = parsed_url.path
                
                # 查找pins数据
                pins_data = self._extract_pins_data(json_data)
                if pins_data:
                    if endpoint not in self.data_structures:
                        self.data_structures[endpoint] = {
                            "pins_data_samples": [],
                            "pins_structure": set(),
                            "pins_count_range": {"min": float('inf'), "max": 0}
                        }
                    
                    structure_info = self.data_structures[endpoint]
                    
                    # 记录样本数据
                    if len(structure_info["pins_data_samples"]) < 3:
                        structure_info["pins_data_samples"].append(pins_data[:2])  # 只保存前2个样本
                    
                    # 分析pins结构
                    if pins_data and isinstance(pins_data[0], dict):
                        structure_info["pins_structure"].update(pins_data[0].keys())
                    
                    # 记录数量范围
                    pins_count = len(pins_data)
                    structure_info["pins_count_range"]["min"] = min(structure_info["pins_count_range"]["min"], pins_count)
                    structure_info["pins_count_range"]["max"] = max(structure_info["pins_count_range"]["max"], pins_count)
    
    def _extract_pins_data(self, json_data: Dict) -> Optional[List]:
        """从JSON数据中提取pins数据
        
        Args:
            json_data: JSON响应数据
            
        Returns:
            pins数据列表或None
        """
        if not isinstance(json_data, dict):
            return None
        
        # 尝试多种可能的pins数据位置
        possible_paths = [
            ["resource_response", "data"],
            ["results"],
            ["data", "results"],
            ["pins"],
            ["data", "pins"]
        ]
        
        for path in possible_paths:
            current_data = json_data
            for key in path:
                if isinstance(current_data, dict) and key in current_data:
                    current_data = current_data[key]
                else:
                    current_data = None
                    break
            
            if isinstance(current_data, list) and current_data:
                # 检查是否看起来像pins数据
                if isinstance(current_data[0], dict) and any(key in current_data[0] for key in ["id", "images", "url", "title"]):
                    return current_data
        
        return None
    
    def _generate_analysis_report(self) -> Dict:
        """生成分析报告
        
        Returns:
            分析报告
        """
        logger.info("生成分析报告...")
        
        # 转换set为list以便JSON序列化
        serializable_endpoints = {}
        for endpoint, info in self.api_endpoints.items():
            serializable_info = info.copy()
            serializable_info["query_parameters"] = list(info["query_parameters"])
            
            # 处理响应结构
            if "response_structure" in serializable_info:
                structure = serializable_info["response_structure"]
                for key, value in structure.items():
                    if isinstance(value, set):
                        structure[key] = list(value)
            
            serializable_endpoints[endpoint] = serializable_info
        
        # 处理分页信息
        serializable_pagination = {}
        for endpoint, info in self.pagination_info.items():
            serializable_info = info.copy()
            serializable_info["url_pagination_params"] = list(info["url_pagination_params"])
            serializable_pagination[endpoint] = serializable_info
        
        # 处理数据结构
        serializable_data_structures = {}
        for endpoint, info in self.data_structures.items():
            serializable_info = info.copy()
            serializable_info["pins_structure"] = list(info["pins_structure"])
            serializable_data_structures[endpoint] = serializable_info
        
        report = {
            "analysis_summary": {
                "total_endpoints": len(self.api_endpoints),
                "endpoints_with_pagination": len(self.pagination_info),
                "endpoints_with_pins_data": len(self.data_structures),
                "total_request_patterns": len(self.request_patterns)
            },
            "api_endpoints": serializable_endpoints,
            "pagination_info": serializable_pagination,
            "data_structures": serializable_data_structures,
            "request_patterns": dict(self.request_patterns),
            "recommendations": self._generate_recommendations()
        }
        
        return report
    
    def _generate_recommendations(self) -> List[str]:
        """生成使用建议
        
        Returns:
            建议列表
        """
        recommendations = []
        
        # 找出最有用的端点
        best_endpoints = []
        for endpoint, info in self.api_endpoints.items():
            if (info["success_count"] > 0 and 
                endpoint in self.data_structures and 
                len(self.data_structures[endpoint]["pins_structure"]) > 5):
                best_endpoints.append(endpoint)
        
        if best_endpoints:
            recommendations.append(f"推荐使用以下API端点进行数据抓取: {', '.join(best_endpoints)}")
        
        # 分页建议
        if self.pagination_info:
            recommendations.append("发现分页机制，建议实现基于bookmark的分页抓取")
        
        # 数据结构建议
        if self.data_structures:
            recommendations.append("已识别pins数据结构，可以直接解析JSON响应而无需HTML解析")
        
        return recommendations
    
    def _save_analysis_result(self, session_dir: str, analysis_result: Dict):
        """保存分析结果
        
        Args:
            session_dir: 会话目录
            analysis_result: 分析结果
        """
        output_file = os.path.join(session_dir, "analysis_report.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_result, f, indent=2, ensure_ascii=False)
        
        logger.info(f"分析报告已保存: {output_file}")
        
        # 生成简化的使用指南
        guide_file = os.path.join(session_dir, "api_usage_guide.md")
        self._generate_usage_guide(guide_file, analysis_result)
    
    def _generate_usage_guide(self, guide_file: str, analysis_result: Dict):
        """生成API使用指南
        
        Args:
            guide_file: 指南文件路径
            analysis_result: 分析结果
        """
        with open(guide_file, 'w', encoding='utf-8') as f:
            f.write("# Pinterest API使用指南\n\n")
            f.write("## 分析摘要\n")
            f.write(f"- 发现API端点: {analysis_result['analysis_summary']['total_endpoints']}个\n")
            f.write(f"- 支持分页的端点: {analysis_result['analysis_summary']['endpoints_with_pagination']}个\n")
            f.write(f"- 包含pins数据的端点: {analysis_result['analysis_summary']['endpoints_with_pins_data']}个\n\n")
            
            f.write("## 推荐的API端点\n")
            for endpoint, info in analysis_result['api_endpoints'].items():
                if info['success_count'] > 0:
                    f.write(f"### {endpoint}\n")
                    f.write(f"- 成功率: {info['success_count']}/{info['response_count']}\n")
                    f.write(f"- 查询参数: {', '.join(info['query_parameters'])}\n")
                    f.write(f"- 样本URL: {info['sample_urls'][0] if info['sample_urls'] else 'N/A'}\n\n")
            
            f.write("## 分页信息\n")
            for endpoint, info in analysis_result['pagination_info'].items():
                f.write(f"### {endpoint}\n")
                f.write(f"- URL分页参数: {', '.join(info['url_pagination_params'])}\n")
                if info['response_pagination']:
                    f.write(f"- 响应分页字段: {list(info['response_pagination'][0].keys())}\n")
                f.write("\n")
            
            f.write("## 使用建议\n")
            for recommendation in analysis_result['recommendations']:
                f.write(f"- {recommendation}\n")
        
        logger.info(f"API使用指南已生成: {guide_file}") 