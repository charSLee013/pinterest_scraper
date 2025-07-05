#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest混合动力爬虫测试脚本

第一阶段：通过Playwright浏览器模拟真实访问，捕获API认证凭证（如CSRF Token和Cookies）。
第二阶段：使用requests库，携带捕获的凭证，直接调用Pinterest API进行高速数据采集，突破滚动加载限制。
"""

import os
import sys
import json
import time
import requests
import argparse
import shutil
import random
from urllib.parse import urlparse, parse_qs
from loguru import logger
from typing import Dict, List, Set, Tuple

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from browser import Browser
from network_analysis.network_interceptor import NetworkInterceptor
from config import COOKIE_FILE_PATH, DEFAULT_TIMEOUT

logger.remove()
logger.add(sys.stdout, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}", level="DEBUG")
logger.debug("DEBUG logging is active, if you see this message!") # 验证日志级别是否激活

def get_auth_credentials(url: str) -> Dict:
    """
    通过Playwright浏览器模拟访问，捕获Pinterest API认证凭证和请求模板。

    Args:
        url: 初始访问的Pinterest URL，例如搜索页。

    Returns:
        包含API请求所需完整上下文的字典:
        {
            "api_url": str,
            "headers": Dict,
            "cookies": Dict,
            "data_template": Dict
        }
    """
    logger.info(f"阶段一：开始捕获认证凭证，访问URL: {url}")
    interceptor = None
    browser_instance = None
    session_output_dir = None
    
    try:
        interceptor = NetworkInterceptor()
        session_output_dir = interceptor.session_dir
        
        browser_instance = Browser(cookie_path=COOKIE_FILE_PATH, timeout=DEFAULT_TIMEOUT)
        
        if not browser_instance.start():
            raise Exception("浏览器启动失败")
        
        browser_instance.page.on("request", interceptor._handle_request)
        browser_instance.page.on("response", interceptor._handle_response)
        
        logger.info(f"导航到 {url} 以捕获网络请求...")
        if not browser_instance.get_url(url):
            raise Exception(f"无法访问URL: {url}")
        
        logger.info("执行滚动以触发API请求...")
        for _ in range(3):
            browser_instance.scroll_by(browser_instance.viewport_height / 2)
            time.sleep(1)

        logger.info("捕获网络活动完成，保存日志...")
        interceptor._save_results()

        requests_log_path = os.path.join(session_output_dir, "network_logs", "requests.json")
        if not os.path.exists(requests_log_path):
            raise FileNotFoundError(f"未找到请求日志文件: {requests_log_path}")

        with open(requests_log_path, 'r', encoding='utf-8') as f:
            network_requests = json.load(f)

        # 凭证初始化
        credentials = {
            "api_url": None,
            "headers": {},
            "cookies": {},
            "data_template": None
        }
        
        # 目标: 找到一个成功的搜索API请求(BaseSearchResource)，并将其作为模板
        target_api_request = None
        for req_log in reversed(network_requests):
            if req_log["type"] == "request":
                req_data = req_log["data"]
                # 优先寻找包含分页书签的搜索请求，这表明它是内容加载请求
                if "BaseSearchResource" in req_data["url"] and "bookmarks" in req_data["url"]:
                    # --- BEGIN VALIDATION BLOCK ---
                    query_params = req_data.get("query_params", {})
                    if "data" not in query_params:
                        continue
                    try:
                        data_payload = json.loads(query_params["data"][0])
                        if not isinstance(data_payload, dict):
                            continue
                        options = data_payload.get("options")
                        if not isinstance(options, dict):
                            continue
                        if "query" not in options or "scope" not in options:
                            continue
                    except (json.JSONDecodeError, IndexError):
                        continue
                    # --- END VALIDATION BLOCK ---
                    
                    target_api_request = req_data
                    logger.info(f"找到关键的API请求（带书签），从中提取模板: {target_api_request['url']}")
                    break
        
        # 如果没找到带书签的，就找任意一个BaseSearchResource请求
        if not target_api_request:
            for req_log in reversed(network_requests):
                if req_log["type"] == "request":
                    req_data = req_log["data"]
                    if "BaseSearchResource" in req_data["url"]:
                        # --- BEGIN VALIDATION BLOCK ---
                        query_params = req_data.get("query_params", {})
                        if "data" not in query_params:
                            continue
                        try:
                            data_payload = json.loads(query_params["data"][0])
                            if not isinstance(data_payload, dict):
                                continue
                            options = data_payload.get("options")
                            if not isinstance(options, dict):
                                continue
                            if "query" not in options or "scope" not in options:
                                continue
                        except (json.JSONDecodeError, IndexError):
                            continue
                        # --- END VALIDATION BLOCK ---

                        target_api_request = req_data
                        logger.info(f"未找到带书签的请求，使用第一个找到的搜索API请求作模板: {target_api_request['url']}")
                        break

        if not target_api_request:
            raise Exception("无法从网络日志中找到任何可用的 'BaseSearchResource' API请求来提取模板。")

        # 1. 提取API URL
        credentials["api_url"] = target_api_request["url"].split('?')[0]

        # 2. 提取并清理Headers
        excluded_headers = ['host', 'content-length', 'cookie'] # cookie单独处理
        for key, value in target_api_request["headers"].items():
            lower_key = key.lower()
            if lower_key not in excluded_headers and not lower_key.startswith(('x-b3-', 'sec-ch-')):
                credentials["headers"][key] = value

        # 3. 提取Cookies
        cookie_string = target_api_request["headers"].get("cookie", "")
        if cookie_string:
            for cookie_str in cookie_string.split(';'):
                parts = cookie_str.strip().split('=', 1)
                if len(parts) == 2:
                    credentials["cookies"][parts[0]] = parts[1]

        # 4. 提取Data模板
        data_param = target_api_request["query_params"].get("data", [None])[0]
        if data_param:
            credentials["data_template"] = json.loads(data_param)
        else:
            raise ValueError("在目标API请求中未找到 'data' 参数。")

        # 确保X-CSRFToken存在于headers中
        if 'x-csrftoken' not in (k.lower() for k in credentials["headers"]):
             if credentials["cookies"].get("csrftoken"):
                credentials["headers"]["X-CSRFToken"] = credentials["cookies"]["csrftoken"]
                logger.info("从Cookie中提取到X-CSRFToken并添加到请求头。")
             else:
                 # 尝试从POST请求中补充
                 for req_log in reversed(network_requests):
                     if req_log["type"] == "request" and req_log["data"]["method"] == "POST":
                         post_headers = req_log["data"]["headers"]
                         if 'x-csrftoken' in (k.lower() for k in post_headers):
                             for k, v in post_headers.items():
                                 if k.lower() == 'x-csrftoken':
                                     credentials["headers"]['X-CSRFToken'] = v
                                     logger.info(f"从POST请求 {req_log['data']['url']} 中补充了X-CSRFToken。")
                                     break
                         break
        
        if 'x-csrftoken' not in (k.lower() for k in credentials["headers"]):
            logger.warning("最终也未能找到X-CSRFToken，后续请求很可能失败。")

        logger.info(f"凭证捕获完成。User-Agent: {credentials['headers'].get('user-agent', 'N/A')}")
        logger.info(f"凭证捕获完成。获取的CSRFToken: {credentials['headers'].get('X-CSRFToken', 'N/A')}")
        logger.info(f"凭证捕获完成。获取的Cookie数量: {len(credentials['cookies'])}")

        return credentials

    except Exception as e:
        logger.error(f"凭证捕获失败: {e}")
        return {}
    finally:
        if browser_instance:
            browser_instance.stop()
        # 保留日志用于调试
        # if session_output_dir and os.path.exists(session_output_dir):
        #     shutil.rmtree(session_output_dir)

def fetch_all_pins_via_api(query: str, credentials: Dict, max_pins: int = 500) -> Tuple[List[Dict], int]:
    """
    使用requests库和捕获的凭证模板，通过Pinterest API高速采集pins数据。

    Args:
        query: 搜索关键词。
        credentials: 包含api_url, headers, cookies, data_template的字典。
        max_pins: 最大采集的pins数量。

    Returns:
        一个包含所有pins数据字典的列表和实际获取的数量。
    """
    logger.info(f"阶段二：开始高速API数据采集，关键词: {query}, 目标数量: {max_pins}")

    # 解包凭证模板
    api_url = credentials.get("api_url")
    headers = credentials.get("headers", {})
    cookies = credentials.get("cookies", {})
    data_template = credentials.get("data_template", {})

    if not all([api_url, headers, cookies, data_template]):
        logger.error("传入的凭证不完整，无法执行API采集。")
        return [], 0

    session = requests.Session()
    session.headers.update(headers)
    session.cookies.update(cookies)

    collected_pins = []
    collected_pin_ids = set() # 用于去重
    bookmarks = [] # bookmarks现在是一个列表
    page_count = 0

    try:
        debug_output_dir = "debug_responses"
        os.makedirs(debug_output_dir, exist_ok=True) # Create debug directory if it doesn't exist

        while len(collected_pins) < max_pins:
            page_count += 1
            logger.info(f"正在获取第 {page_count} 页数据，当前已获取 {len(collected_pins)}/{max_pins} pins...")

            # 使用模板构建请求数据
            current_data = data_template.copy() # 浅拷贝够用
            current_data['options']['bookmarks'] = bookmarks
            current_data['options']['query'] = query # 确保使用当前查询词
            
            # Pinterest的GET请求data参数是URL编码的JSON字符串
            params = {
                "source_url": f"/search/pins/?q={query}",
                "data": json.dumps(current_data),
                "_": int(time.time() * 1000) # 时间戳，防止缓存
            }

            response = session.get(api_url, params=params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status() # 检查HTTP错误

            # 新增：将原始响应文本写入文件，用于深度调试
            # debug_file_path = os.path.join(debug_output_dir, f"response_page_{page_count}.txt")
            # with open(debug_file_path, "w", encoding="utf-8") as f:
            #     f.write(response.text)
            # logger.debug(f"Raw API response for page {page_count} saved to: {debug_file_path}") # Log file path

            logger.debug(f"Received API raw text response: {response.text}") # Retain, but file is primary
            logger.debug("Attempting to parse API response JSON...") # 将INFO改为DEBUG
            json_data = response.json()
            logger.debug(f"Received API response JSON (raw): {json_data}")

            # 提取pins数据和新的bookmark
            new_pins = []
            next_bookmark = None

            # 根据实际API响应结构进行调整
            if "resource_response" in json_data and "data" in json_data["resource_response"]:
                response_data = json_data["resource_response"]
                if isinstance(response_data.get("data"), dict) and "results" in response_data["data"]:
                    new_pins = response_data["data"]["results"]
                # 新的bookmark在data的同级
                if "bookmark" in response_data:
                    next_bookmark = response_data["bookmark"]
            
            if not new_pins and page_count > 1:
                logger.info("未获取到新pins，可能已达末页，停止采集。")
                break

            for pin in new_pins:
                pin_id = pin.get("id") or pin.get("pin_id")
                if pin_id and pin_id not in collected_pin_ids:
                    collected_pins.append(pin)
                    collected_pin_ids.add(pin_id)

            if not next_bookmark:
                logger.info("已达到最后一页或无更多分页书签，停止采集。")
                break
            
            bookmarks = [next_bookmark] # Pinterest的bookmark似乎是单个值，但参数是列表

            # 频率控制
            time.sleep(1 + (random.random() * 0.5)) # 1到1.5秒随机延迟

    except requests.exceptions.RequestException as e:
        logger.error(f"API请求失败: {e}")
    except json.JSONDecodeError:
        logger.error("API响应不是有效的JSON格式。")
    except Exception as e:
        logger.error(f"高速采集过程中发生未知错误: {e}")

    logger.info(f"阶段二：数据采集完成。总共获取 {len(collected_pins)} 个不重复的pins。")
    return collected_pins, len(collected_pins)

def main():
    parser = argparse.ArgumentParser(description="Pinterest混合动力爬虫测试工具")
    parser.add_argument("query", help="要搜索的关键词")
    parser.add_argument("--max-pins", type=int, default=100, help="目标采集的最大pins数量 (默认: 100)")
    
    args = parser.parse_args()

    # 阶段一：获取认证凭证
    pinterest_search_url = f"https://www.pinterest.com/search/pins/?q={args.query}"
    credentials = get_auth_credentials(pinterest_search_url)

    if not credentials or not credentials.get("api_url"):
        logger.error("未能成功获取认证凭证模板，无法进行API采集。请检查网络或Cookie文件。")
        sys.exit(1)

    # 阶段二：高速API采集
    final_pins, total_count = fetch_all_pins_via_api(args.query, credentials, args.max_pins)

    logger.info(f"最终采集结果：成功获取 {total_count} 个 pins。")
    if total_count > 0:
        logger.info(f"示例Pins数据 (前3个):")
        for i, pin in enumerate(final_pins[:3]):
            logger.info(f"  Pin {i+1}: ID={pin.get('id') or pin.get('pin_id')}, Title={pin.get('title', 'N/A')}")
    
    # 你可以在这里进一步处理 final_pins 数据，例如保存到文件等

if __name__ == "__main__":
    main() 