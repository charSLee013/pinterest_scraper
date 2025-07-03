#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest HTML解析模块
"""

import json
import re
from typing import Dict, List

from bs4 import BeautifulSoup
from loguru import logger

import config


def extract_pin_id_from_html(html_element: str) -> str:
    """从HTML元素中提取Pinterest Pin ID

    尝试多种方式提取Pin ID

    Args:
        html_element: Pin的HTML内容

    Returns:
        Pin ID或空字符串
    """
    # 模式1: data-pin-id属性
    id_match = re.search(r'data-pin-id=[\'"](\d+)[\'"]', html_element)
    if id_match:
        return id_match.group(1)

    # 模式2: PIN URL模式 /pin/12345/
    url_match = re.search(r"/pin/(\d+)/?", html_element)
    if url_match:
        return url_match.group(1)

    # 模式3: 其他数字ID属性
    for attr in ["data-id", "data-item-id", "id"]:
        attr_match = re.search(rf'{attr}=[\'"]pin(\d+)[\'"]', html_element)
        if attr_match:
            return attr_match.group(1)

    # 其他可能的ID模式
    generic_id_match = re.search(
        r'pin_id[\'"]?\s*[:=]\s*[\'"]?(\d+)[\'"]?', html_element
    )
    if generic_id_match:
        return generic_id_match.group(1)

    return ""


def extract_image_urls_from_srcset(srcset: str) -> Dict[str, str]:
    """从srcset属性中提取图片URL和尺寸

    Args:
        srcset: HTML srcset属性值

    Returns:
        图片尺寸到URL的映射字典
    """
    if not srcset:
        return {}

    image_urls = {}
    for src_part in srcset.split(","):
        src_part = src_part.strip()
        if not src_part:
            continue

        parts = src_part.split(" ")
        if len(parts) >= 2:
            url = parts[0]
            size = parts[-1].replace("x", "")
            image_urls[size] = url

    return image_urls


def extract_image_urls_from_src(src: str) -> Dict[str, str]:
    """从单个src URL中提取并生成多种尺寸的URL

    Args:
        src: 图片src URL

    Returns:
        尺寸到URL的映射字典
    """
    if not src:
        return {}

    image_urls = {}

    # 从URL中提取尺寸
    size_patterns = [
        r"/(\d+)x/",  # 常规格式: /236x/
        r"_(\d+)\.jpg",  # 替代格式: _236.jpg
        r"-(\d+)\.jpg",  # 另一格式: -236.jpg
    ]

    size = "original"
    for pattern in size_patterns:
        size_match = re.search(pattern, src)
        if size_match:
            size = size_match.group(1)
            break

    # 添加当前URL
    image_urls[size] = src

    # 如果不是原始尺寸，试着构建原始尺寸URL
    if config.ORIGINAL_SIZE_MARKER not in src and size != "original":
        # 替换尺寸为originals
        original_url = re.sub(r"/\d+x/", f"/{config.ORIGINAL_SIZE_MARKER}/", src)
        if original_url != src:
            image_urls["original"] = original_url

        # 构建其他常见尺寸
        base_url_match = re.search(r"(https://i\.pinimg\.com)/\d+x/(.+)", src)
        if base_url_match:
            base_url = base_url_match.group(1)
            image_path = base_url_match.group(2)

            for s in config.IMAGE_SIZES:
                if str(s) != size:  # 避免重复
                    image_urls[str(s)] = f"{base_url}/{s}x/{image_path}"

    return image_urls


def find_largest_image_url(image_urls: Dict[str, str]) -> str:
    """查找最大尺寸的图片URL

    Args:
        image_urls: 尺寸到URL的映射字典

    Returns:
        最大尺寸的图片URL
    """
    if not image_urls:
        return ""

    # 优先返回原始尺寸
    if "original" in image_urls:
        return image_urls["original"]

    # 查找最大数字尺寸
    try:
        sizes = [int(s) for s in image_urls.keys() if s.isdigit()]
        if sizes:
            largest_size = str(max(sizes))
            return image_urls[largest_size]
    except Exception:
        pass

    # 如果上述方法失败，返回第一个URL
    return next(iter(image_urls.values()))


def parse_pin_from_html(html_element: str) -> Dict:
    """从HTML中解析单个Pin数据

    Args:
        html_element: Pin的HTML内容

    Returns:
        包含Pin数据的字典
    """
    soup = BeautifulSoup(html_element, "html.parser")

    # 提取Pin ID
    pin_id = extract_pin_id_from_html(html_element)

    # 查找图片元素
    img_element = soup.select_one(
        "img[srcset], img[src], [data-test-id='pin-image'] img"
    )

    # 初始化结果
    result = {
        "id": pin_id,
        "description": "",
        "image_urls": {},
        "largest_image_url": "",
        "title": "",
        "creator": {},
        "stats": {},
        "url": f"https://www.pinterest.com/pin/{pin_id}/" if pin_id else "",
    }

    # 如果找不到图片元素，尝试从JSON数据中提取
    if not img_element:
        json_data = extract_json_from_html(html_element)
        if json_data:
            return enrich_pin_data_from_json(result, json_data)
        return result

    # 从图片元素中提取信息
    image_urls = {}

    # 尝试从srcset提取
    if img_element.get("srcset"):
        image_urls.update(extract_image_urls_from_srcset(img_element["srcset"]))

    # 如果没有从srcset获取到URLs，尝试从src
    if not image_urls and img_element.get("src"):
        image_urls.update(extract_image_urls_from_src(img_element["src"]))

    # 更新结果
    result["image_urls"] = image_urls
    result["largest_image_url"] = find_largest_image_url(image_urls)

    # 提取描述
    description_selectors = [
        ".tBJ.dyH.iFc.MF7.pBj.DrD.IZT.mWe",
        "[data-test-id='pinTitle']",
        ".tBJ.dyH.iFc.MF7.pBj.DrD.IZT",
        ".lH1.dyH.iFc.MF7.pBj.IZT",
        "h1",
        "div[class*='title']",
    ]

    for selector in description_selectors:
        desc_element = soup.select_one(selector)
        if desc_element and desc_element.text.strip():
            result["description"] = desc_element.text.strip()
            break

    # 如果没有找到描述，尝试从图片属性中获取
    if not result["description"] and img_element:
        for attr in ["alt", "title", "aria-label"]:
            if img_element.get(attr):
                result["description"] = img_element.get(attr, "").strip()
                break

    # 尝试从JSON数据中丰富结果
    json_data = extract_json_from_html(html_element)
    if json_data:
        return enrich_pin_data_from_json(result, json_data)

    return result


def extract_json_from_html(html: str) -> Dict:
    """从HTML中提取JSON数据

    Args:
        html: HTML内容

    Returns:
        JSON数据字典
    """
    # 模式1: data-test-pin-info或data-pin-json属性
    json_matches = re.findall(r"data-test-pin-info=\'(.*?)\'", html)
    json_matches.extend(re.findall(r"data-pin-json=\'(.*?)\'", html))

    # 模式2: pin对象
    json_matches.extend(re.findall(r'"pin":\s*(\{.*?\})', html))

    # 尝试解析所有匹配
    for json_str in json_matches:
        try:
            return json.loads(json_str)
        except:
            continue

    return {}


def enrich_pin_data_from_json(pin_data: Dict, json_data: Dict) -> Dict:
    """使用JSON数据丰富Pin数据

    Args:
        pin_data: 初始Pin数据
        json_data: 从HTML中提取的JSON数据

    Returns:
        丰富后的Pin数据
    """
    # 复制原始数据，避免修改原始对象
    result = pin_data.copy()

    # 如果JSON中包含图片数据
    if "images" in json_data:
        image_urls = {}
        for size_key, img_data in json_data["images"].items():
            if not isinstance(img_data, dict):
                continue

            if "url" in img_data:
                if size_key == "orig":
                    image_urls["original"] = img_data["url"]
                else:
                    size = size_key.replace("x", "")
                    image_urls[size] = img_data["url"]

        # 只有在找到新图片URL时才更新
        if image_urls:
            result["image_urls"] = image_urls
            result["largest_image_url"] = find_largest_image_url(image_urls)

    # 基本信息
    if "id" in json_data and json_data["id"] and result["id"] == "":
        result["id"] = str(json_data["id"])

    if "url" in json_data:
        result["url"] = json_data["url"]

    if not result["description"] and "description" in json_data:
        result["description"] = json_data.get("description", "")

    if "title" in json_data:
        result["title"] = json_data.get("title", "")

    # 创建者信息
    creator = {}
    creator_source = None

    if "creator" in json_data:
        creator_source = json_data["creator"]
    elif "pinner" in json_data:
        creator_source = json_data["pinner"]

    if creator_source and isinstance(creator_source, dict):
        creator = {
            "name": creator_source.get("full_name", ""),
            "username": creator_source.get("username", ""),
            "id": creator_source.get("id", ""),
            "follower_count": creator_source.get("follower_count", 0),
            "url": f"https://www.pinterest.com/{creator_source.get('username', '')}/",
        }

        # 头像
        if "image_medium_url" in creator_source:
            creator["avatar_url"] = creator_source["image_medium_url"]

    if creator:
        result["creator"] = creator

    # 统计数据
    stats = {
        "likes": json_data.get("like_count", 0),
        "saves": json_data.get("repin_count", 0),
        "comments": json_data.get("comment_count", 0),
    }

    if any(stats.values()):
        result["stats"] = stats

    # 其他元数据
    if "created_at" in json_data:
        result["created_at"] = json_data["created_at"]

    if "link" in json_data:
        result["source_link"] = json_data["link"]

    # 分类信息
    if "board" in json_data and isinstance(json_data["board"], dict):
        board = json_data["board"]
        result["board"] = {
            "id": board.get("id", ""),
            "name": board.get("name", ""),
            "url": f"https://www.pinterest.com/{board.get('url', '')}",
        }

        # 使用board.name作为分类
        if board.get("name"):
            result["categories"] = [c.strip() for c in board["name"].split("/")]

    return result


def extract_pins_from_html(html: str) -> List[Dict]:
    """从Pinterest页面HTML中提取所有Pin数据

    Args:
        html: 完整的Pinterest页面HTML

    Returns:
        包含Pin数据的字典列表
    """
    soup = BeautifulSoup(html, "html.parser")
    pins = []

    # 尝试从 <script type="application/ld+json"> 标签中提取JSON-LD数据
    # 这通常在单个pin页面或富媒体页面中包含详细信息
    json_ld_scripts = soup.find_all("script", type="application/ld+json")
    for script in json_ld_scripts:
        try:
            json_data = json.loads(script.string)
            # 检查是否为单个Pin的JSON-LD数据
            if "@context" in json_data and "schema.org" in json_data["@context"]:
                # Pinterest的单个pin页面通常包含Graph数据
                if "@graph" in json_data:
                    for item in json_data["@graph"]:
                        if item.get("@type") == "ImageObject" and "mainEntityOfPage" in item:
                            pin_url = item["mainEntityOfPage"]
                            pin_id = re.search(r"/pin/(\d+)/", pin_url)
                            if pin_id:
                                # 提取图片URL
                                image_urls = {
                                    "original": item.get("contentUrl") or item.get("url"),
                                    "width": item.get("width"),
                                    "height": item.get("height")
                                }
                                # 尝试从thumbnailUrl获取不同尺寸
                                if "thumbnailUrl" in item and isinstance(item["thumbnailUrl"], list):
                                    for thumb_url in item["thumbnailUrl"]:
                                        size_match = re.search(r"/(\d+)x", thumb_url)
                                        if size_match:
                                            image_urls[size_match.group(1)] = thumb_url

                                pin_info = {
                                    "id": pin_id.group(1),
                                    "url": pin_url,
                                    "title": item.get("name") or "",
                                    "description": item.get("description") or "",
                                    "image_urls": image_urls,
                                    "largest_image_url": find_largest_image_url(image_urls),
                                    "creator": {
                                        "name": item["creator"].get("name") if "creator" in item and isinstance(item["creator"], dict) else "",
                                        "url": item["creator"].get("url") if "creator" in item and isinstance(item["creator"], dict) else ""
                                    },
                                    "tags": item.get("keywords", [])
                                }
                                # 进一步丰富数据，特别是处理嵌套结构
                                final_pin = enrich_pin_data_from_json({}, pin_info)
                                pins.append(final_pin)
                # 兼容直接的ImageObject或Product数据
                elif json_data.get("@type") == "ImageObject" or json_data.get("@type") == "Product":
                    pin_url = json_data.get("mainEntityOfPage") or json_data.get("url")
                    pin_id = re.search(r"/pin/(\d+)/", pin_url) if pin_url else ""
                    if pin_id:
                        image_urls = {
                            "original": json_data.get("contentUrl") or json_data.get("image") or json_data.get("url"),
                            "width": json_data.get("width"),
                            "height": json_data.get("height")
                        }
                        pin_info = {
                            "id": pin_id.group(1),
                            "url": pin_url,
                            "title": json_data.get("name") or "",
                            "description": json_data.get("description") or "",
                            "image_urls": image_urls,
                            "largest_image_url": find_largest_image_url(image_urls),
                            "creator": {
                                "name": json_data["creator"].get("name") if "creator" in json_data and isinstance(json_data["creator"], dict) else "",
                                "url": json_data["creator"].get("url") if "creator" in json_data and isinstance(json_data["creator"], dict) else ""
                            }
                        }
                        final_pin = enrich_pin_data_from_json({}, pin_info)
                        pins.append(final_pin)

        except Exception as e:
            logger.debug(f"解析JSON-LD数据失败: {e}")
            continue

    if pins:
        logger.info(f"从JSON-LD中提取到 {len(pins)} 个pin数据")
        return pins

    # 如果没有从JSON-LD中提取到，继续尝试从HTML元素中提取
    logger.info("未从JSON-LD中提取到pins，尝试从HTML元素中提取")
    # 尝试提取页面中的所有可能的pin元素
    for selector in config.PINTEREST_PIN_SELECTORS:
        pin_elements = soup.select(selector)
        if pin_elements:
            logger.debug(f"使用选择器 '{selector}' 找到 {len(pin_elements)} 个pin元素")
            break
    else:
        # 所有选择器都没有找到元素
        logger.warning("无法找到任何pin元素，尝试使用默认选择器")
        pin_elements = soup.select("div[role='listitem'], div[class*='Grid__Item']")

    # 处理所有找到的pin元素
    for pin_element in pin_elements:
        try:
            pin_data = parse_pin_from_html(str(pin_element))
            if pin_data["id"] and (
                pin_data["image_urls"] or pin_data["largest_image_url"]
            ):
                pins.append(pin_data)
        except Exception as e:
            logger.error(f"解析pin元素出错: {e}")
            continue

    # 如果通过常规方法找不到pins，尝试从全局JSON查找
    if not pins:
        logger.info("通过HTML选择器未找到pins，尝试从页面JSON提取")
        # 查找脚本中的初始状态数据
        script_tags = soup.find_all(
            "script", id=lambda x: x and ("__PWS_DATA__" in x or "initial-state" in x)
        )
        for script in script_tags:
            try:
                data = json.loads(script.string)
                # 在Redux状态中查找pins
                if "props" in data and "initialReduxState" in data["props"]:# {{ NEW_CODE }}
                    redux_state = data["props"]["initialReduxState"]
                    if "pins" in redux_state:
                        pin_items = redux_state["pins"]
                        for pin_id, pin_data in pin_items.items():
                            pins.append(
                                enrich_pin_data_from_json({"id": pin_id}, pin_data)
                            )
                        break
            except Exception as e:
                logger.debug(f"从脚本提取JSON数据失败: {e}")
                continue

    logger.info(f"从HTML中提取到 {len(pins)} 个pin数据")
    return pins
