#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
工具函数模块
"""

import hashlib
import json
import os
from typing import Any, Dict, List

from loguru import logger


def setup_directories(
    output_dir: str, search_term: str = "", create_debug_dirs: bool = False
) -> Dict[str, str]:
    """设置输出目录结构，如果提供了search_term，则为该搜索词创建独立目录

    Args:
        output_dir: 主输出目录路径
        search_term: 搜索关键词或URL标识符，为空时使用旧的目录结构
        create_debug_dirs: 是否创建调试目录

    Returns:
        目录路径的字典映射
    """
    # 创建主目录
    os.makedirs(output_dir, exist_ok=True)

    dirs = {"root": output_dir}

    if search_term:
        # 为搜索词创建独立目录
        safe_term = sanitize_filename(search_term)
        term_dir = os.path.join(output_dir, safe_term)
        dirs["term_root"] = term_dir
        dirs["images"] = os.path.join(term_dir, "images")
        dirs["json"] = os.path.join(term_dir, "json")
        dirs["cache"] = os.path.join(term_dir, "cache")
    else:
        # 旧的目录结构
        dirs["images"] = os.path.join(output_dir, "images")
        dirs["json"] = os.path.join(output_dir, "json")
        dirs["cache"] = os.path.join(output_dir, "cache")

    # 创建所有必要的子目录
    for path in dirs.values():
        os.makedirs(path, exist_ok=True)

    # 如果需要，创建调试目录
    if create_debug_dirs:
        debug_root = os.path.join(dirs.get("term_root", output_dir), "debug")
        dirs["debug"] = debug_root
        dirs["debug_screenshots"] = os.path.join(debug_root, "screenshots")
        dirs["debug_html"] = os.path.join(debug_root, "html")
        dirs["debug_network"] = os.path.join(debug_root, "network")

        for path in [
            debug_root,
            dirs["debug_screenshots"],
            dirs["debug_html"],
            dirs["debug_network"],
        ]:
            os.makedirs(path, exist_ok=True)

    return dirs


def save_json(data: Any, filepath: str, indent: int = 2) -> bool:
    """保存数据为JSON文件

    Args:
        data: 要保存的数据
        filepath: 文件路径
        indent: JSON缩进

    Returns:
        保存是否成功
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # 保存JSON
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)

        logger.debug(f"数据已保存到 {filepath}")
        return True
    except Exception as e:
        logger.error(f"保存JSON出错 {filepath}: {e}")
        return False


def load_json(filepath: str) -> Any:
    """从JSON文件加载数据

    Args:
        filepath: 文件路径

    Returns:
        加载的数据或None
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载JSON出错 {filepath}: {e}")
        return None


def load_url_list(filepath: str) -> List[str]:
    """从文件加载URL列表

    Args:
        filepath: 文件路径

    Returns:
        URL列表
    """
    urls = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)
        return urls
    except Exception as e:
        logger.error(f"加载URL列表出错 {filepath}: {e}")
        return []


def sanitize_filename(name: str) -> str:
    """生成安全的文件名

    Args:
        name: 原始文件名

    Returns:
        安全的文件名
    """
    import re

    # 首先去除URL中的任何参数和无关字符
    name = name.split("?")[0].split("#")[0]
    if "/" in name:
        # 如果是URL，只取最后一部分
        parts = [p for p in name.split("/") if p]
        if parts:
            name = parts[-1] or (parts[-2] if len(parts) > 1 else name)

    # 长度限制，避免文件名过长
    if len(name) > 50:
        name = name[:50]

    return re.sub(r'[\\/:*?"<>|]', "_", name)


def get_pin_hash(pin: Dict) -> str:
    """计算pin的哈希值，用于缓存识别

    Args:
        pin: pin数据字典

    Returns:
        哈希字符串
    """
    # 使用id和图片URL作为唯一标识符
    pin_id = pin.get("id", "")
    image_url = pin.get("largest_image_url", "")

    # 组合并计算MD5哈希
    hash_input = f"{pin_id}:{image_url}"
    return hashlib.md5(hash_input.encode()).hexdigest()


def load_cache(cache_file: str) -> Dict:
    """加载缓存数据

    Args:
        cache_file: 缓存文件路径

    Returns:
        缓存数据字典
    """
    cache = load_json(cache_file) or {"pins": {}, "downloaded_images": set()}

    # 确保downloaded_images是集合类型，因为JSON不支持集合
    if "downloaded_images" in cache:
        cache["downloaded_images"] = set(cache["downloaded_images"])
    else:
        cache["downloaded_images"] = set()

    return cache


def save_cache(cache_data: Dict, cache_file: str) -> bool:
    """保存缓存数据

    Args:
        cache_data: 缓存数据
        cache_file: 缓存文件路径

    Returns:
        是否保存成功
    """
    # 把set转成list再保存
    save_data = {
        "pins": cache_data["pins"],
        "downloaded_images": list(cache_data["downloaded_images"]),
    }
    return save_json(save_data, cache_file)


def update_cache_with_pins(pins: List[Dict], cache_file: str) -> Dict:
    """使用新的pin数据更新缓存

    Args:
        pins: pin数据列表
        cache_file: 缓存文件路径

    Returns:
        更新后的缓存数据
    """
    # 加载现有缓存
    cache = load_cache(cache_file)

    # 更新缓存中的pin数据
    for pin in pins:
        pin_hash = get_pin_hash(pin)
        cache["pins"][pin_hash] = pin

        # 如果pin已下载，记录到已下载集合
        if pin.get("downloaded", False) and pin.get("download_path"):
            cache["downloaded_images"].add(pin_hash)

    # 保存更新后的缓存
    save_cache(cache, cache_file)
    return cache


def get_cached_pins(cache_file: str) -> List[Dict]:
    """从缓存获取所有已缓存的pin数据

    Args:
        cache_file: 缓存文件路径

    Returns:
        缓存的pin数据列表
    """
    cache = load_cache(cache_file)
    return list(cache["pins"].values())
