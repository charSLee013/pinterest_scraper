#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
图片下载模块
"""

import concurrent.futures
import os
import random
import time
from typing import Dict, List, Optional

import requests
from loguru import logger
from tqdm import tqdm

import config
import utils


def generate_headers() -> Dict:
    """生成随机headers，减少被封的可能性

    Returns:
        随机生成的headers字典
    """
    # 常用浏览器UA
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    ]

    # 常见接受类型
    accept_types = [
        "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "image/webp,image/png,image/svg+xml,image/*;q=0.8,video/*;q=0.8,*/*;q=0.5",
        "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    ]

    # 常见语言设置
    languages = [
        "en-US,en;q=0.9",
        "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "en-GB,en;q=0.9",
        "en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7",
    ]

    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": random.choice(accept_types),
        "Accept-Language": random.choice(languages),
        "Referer": "https://www.pinterest.com/",
        "sec-ch-ua": '"Google Chrome";v="107", "Chromium";v="107"',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "image",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "cross-site",
    }

    return headers


def download_image_with_fallback(
    image_urls: Dict[str, str],
    filepath: str,
    headers: Optional[Dict] = None,
    timeout: int = config.DEFAULT_TIMEOUT,
    max_retries: int = config.MAX_RETRIES,
) -> bool:
    """尝试从多个图片URL下载，自动降级到较小尺寸

    Args:
        image_urls: 各种尺寸的图片URLs
        filepath: 保存路径
        headers: HTTP请求头
        timeout: 超时时间(秒)
        max_retries: 最大重试次数

    Returns:
        下载是否成功
    """
    # 检查文件是否已存在且有效
    if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
        logger.debug(f"图片已存在，跳过下载: {filepath}")
        return True

    # 如果没有提供headers，生成随机headers
    if headers is None:
        headers = generate_headers()

    # 按照优先级尝试不同尺寸
    size_priorities = ["original", "1200", "736", "564", "474", "236", "170"]

    # 尝试第一个可用的URL
    main_url = None
    for size in size_priorities:
        if size in image_urls:
            main_url = image_urls[size]
            break

    # 如果没有找到URL，从largest_image_url尝试
    if not main_url and "largest" in image_urls:
        main_url = image_urls["largest"]

    # 如果仍然没有URL
    if not main_url:
        logger.warning("没有可用的图片URL")
        return False

    # 尝试下载
    success = download_image(main_url, filepath, headers, timeout, max_retries)

    # 如果失败，尝试其他尺寸
    if not success:
        logger.info("主URL下载失败，尝试备用尺寸")

        for size in size_priorities:
            if size in image_urls and image_urls[size] != main_url:
                logger.debug(f"尝试下载尺寸 {size}: {image_urls[size]}")

                # 使用不同的headers，避免被识别为爬虫
                alt_headers = generate_headers()

                if download_image(image_urls[size], filepath, alt_headers, timeout, 1):
                    logger.debug(f"使用备用尺寸 {size} 下载成功")
                    return True

                # 避免请求过快
                time.sleep(0.5)

    return success


def download_image(
    url: str,
    filepath: str,
    headers: Optional[Dict] = None,
    timeout: int = config.DEFAULT_TIMEOUT,
    max_retries: int = config.MAX_RETRIES,
) -> bool:
    """下载图片

    Args:
        url: 图片URL
        filepath: 保存路径
        headers: HTTP请求头
        timeout: 超时时间(秒)
        max_retries: 最大重试次数

    Returns:
        下载是否成功
    """
    # 检查文件是否已存在且有效
    if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
        logger.debug(f"图片已存在，跳过下载: {filepath}")
        return True

    # 验证URL
    if not url.startswith(("http://", "https://")):
        logger.warning(f"无效的URL格式: {url}")
        return False

    # 确保目录存在
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
    except Exception as e:
        logger.error(f"创建目录失败: {e}")
        return False

    # 设置默认headers
    if headers is None:
        headers = generate_headers()

    # 下载图片，最多重试max_retries次
    for attempt in range(max_retries):
        try:
            # 添加随机延迟，减轻爬虫特征
            if attempt > 0:
                delay = random.uniform(0.5, 2.0) * attempt
                time.sleep(delay)

            # 使用会话
            with requests.Session() as session:
                # 增加尝试不同方法的断点续传逻辑
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    # 文件已经部分下载，尝试断点续传
                    file_size = os.path.getsize(filepath)
                    range_header = {"Range": f"bytes={file_size}-"}
                    headers.update(range_header)

                    # 用HEAD请求先检查支持
                    head_resp = session.head(url, headers=headers, timeout=timeout)
                    if (
                        head_resp.status_code == 206
                        or "Accept-Ranges" in head_resp.headers
                    ):
                        logger.debug(f"支持断点续传，继续下载: {filepath}")
                    else:
                        # 不支持断点续传，删除部分文件
                        os.remove(filepath)

                # 发送请求
                response = session.get(
                    url, headers=headers, timeout=timeout, stream=True
                )
                response.raise_for_status()

                # 验证内容类型
                content_type = response.headers.get("Content-Type", "")
                if not content_type.startswith("image/"):
                    logger.warning(f"内容类型不是图片: {content_type}")
                    # 如果是Pinterest的错误页面，直接失败
                    if (
                        "text/html" in content_type
                        and "pinterest" in response.text.lower()
                    ):
                        logger.warning("Pinterest返回了错误页面而不是图片")
                        if attempt == max_retries - 1:
                            return False
                        continue

                # 检查文件是否存在，决定是否追加
                mode = "ab" if os.path.exists(filepath) and "Range" in headers else "wb"

                # 保存图片
                with open(filepath, mode) as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                # 验证文件大小
                file_size = os.path.getsize(filepath)
                if file_size < 100:  # 太小的文件可能是错误的
                    logger.warning(f"下载的文件太小 ({file_size} 字节)")
                    if attempt < max_retries - 1:
                        continue

                logger.debug(f"成功下载图片: {filepath}")
                return True

        except requests.exceptions.Timeout:
            logger.warning(f"下载超时 {url} (尝试 {attempt + 1}/{max_retries})")
        except requests.exceptions.ConnectionError:
            logger.warning(f"连接错误 {url} (尝试 {attempt + 1}/{max_retries})")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP错误 {url}: {e} (尝试 {attempt + 1}/{max_retries})")

            # 专门处理403错误
            if hasattr(e, "response") and e.response.status_code == 403:
                logger.warning("服务器拒绝访问 (403 Forbidden)，可能是反爬虫机制")

                # 尝试修改细节来规避反爬
                headers = generate_headers()

                # 增加更长的延迟
                time.sleep(random.uniform(2.0, 5.0))
        except Exception as e:
            logger.error(f"下载出错 {url}: {e} (尝试 {attempt + 1}/{max_retries})")

    # 所有尝试都失败
    logger.error(f"下载失败 {url}")
    return False


def download_images_with_cache(
    pins: List[Dict],
    output_dir: str,
    prefix: str,
    cache_dir: str,
    max_workers: int = 0,
) -> List[Dict]:
    """使用缓存机制下载图片，避免重复下载

    Args:
        pins: Pin数据列表
        output_dir: 输出目录
        prefix: 文件名前缀
        cache_dir: 缓存目录
        max_workers: 最大并发数，0表示自动设置

    Returns:
        更新下载状态后的Pin数据列表
    """

    logger.info(f"准备下载 {len(pins)} 张图片")
    if not pins:
        logger.warning("没有图片可下载")
        return pins

    # 生成安全的文件名前缀
    safe_prefix = utils.sanitize_filename(prefix)

    # 加载缓存
    cache_file = os.path.join(cache_dir, f"{safe_prefix}_cache.json")
    cache = utils.load_cache(cache_file)

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 准备下载任务
    download_tasks = []
    cached_count = 0

    for i, pin in enumerate(pins):
        # 计算图片Hash
        pin_hash = utils.get_pin_hash(pin)

        # 检查是否已在缓存中
        if pin_hash in cache["downloaded_images"]:
            cached_pin = cache["pins"].get(pin_hash)
            if cached_pin and cached_pin.get("download_path"):
                # 检查文件是否存在
                if os.path.exists(cached_pin["download_path"]):
                    # 更新当前pin数据
                    pin["downloaded"] = True
                    pin["download_path"] = cached_pin["download_path"]
                    cached_count += 1
                    continue

        # 未缓存，需要下载
        pin_id = pin.get("id", "unknown")

        # 收集所有可用的图片URL
        image_urls = {}

        # 添加最大的图片URL
        largest_url = pin.get("largest_image_url")
        if largest_url:
            image_urls["largest"] = largest_url

        # 添加所有尺寸的URL
        pin_image_urls = pin.get("image_urls", {})
        if pin_image_urls:
            image_urls.update(pin_image_urls)

        # 如果没有图片URL，跳过
        if not image_urls:
            continue

        # 生成唯一文件名
        filename = f"{safe_prefix}_{pin_id}_{i}.jpg"
        filepath = os.path.join(output_dir, filename)

        # 添加到任务列表
        download_tasks.append(
            {
                "index": i,
                "pin": pin,
                "image_urls": image_urls,
                "filepath": filepath,
                "pin_hash": pin_hash,
            }
        )

    if cached_count > 0:
        logger.info(f"从缓存中找到 {cached_count} 张已下载的图片")

    if not download_tasks:
        logger.info("所有图片都已下载，无需重新下载")
        return pins

    logger.info(f"开始下载 {len(download_tasks)} 张新图片")

    # 设置并发数
    if max_workers <= 0:
        # 自动设置为CPU核心数的2倍或8，取小值，避免请求过多被封
        max_workers = min(8, os.cpu_count() * 2 or 4)

    logger.debug(f"使用 {max_workers} 个线程并发下载")

    # 下载函数
    def download_task(task):
        try:
            i = task["index"]
            image_urls = task["image_urls"]
            filepath = task["filepath"]
            pin_hash = task["pin_hash"]

            # 使用智能下载策略
            success = download_image_with_fallback(
                image_urls,
                filepath,
                headers=generate_headers(),
                timeout=config.DEFAULT_TIMEOUT,
                max_retries=config.MAX_RETRIES,
            )

            # 返回结果
            return {
                "index": i,
                "success": success,
                "filepath": filepath if success else "",
                "pin_hash": pin_hash,
            }
        except Exception as e:
            logger.error(f"下载任务出错: {e}")
            return {
                "index": task["index"],
                "success": False,
                "pin_hash": task["pin_hash"],
            }

    # 使用线程池并发下载，添加控制策略
    success_count = 0
    failure_count = 0
    max_failures = min(len(download_tasks) // 4, 10)  # 最多允许25%或10个失败

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交初始批次任务
        batch_size = min(max_workers * 2, len(download_tasks))
        pending_tasks = download_tasks[:batch_size]
        remaining_tasks = download_tasks[batch_size:]

        futures = {executor.submit(download_task, task): task for task in pending_tasks}

        with tqdm(total=len(download_tasks), desc="下载图片") as pbar:
            # 处理所有任务
            while futures:
                # 等待一个任务完成
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                # 处理完成的任务
                for future in done:
                    task = futures.pop(future)

                    try:
                        result = future.result()
                        i = result["index"]
                        pin_hash = result["pin_hash"]
                        pins[i]["downloaded"] = result["success"]

                        if result["success"]:
                            pins[i]["download_path"] = result["filepath"]
                            # 更新缓存
                            cache["pins"][pin_hash] = pins[i]
                            cache["downloaded_images"].add(pin_hash)
                            success_count += 1
                        else:
                            failure_count += 1

                        # 检查失败阈值
                        if failure_count >= max_failures:
                            logger.warning(
                                f"达到最大失败次数 ({failure_count})，可能遇到反爬限制，暂停下载"
                            )
                            # 增加延迟恢复时间
                            time.sleep(10)
                            # 重置计数器，继续尝试
                            failure_count = 0
                    except Exception as e:
                        logger.error(f"处理下载结果出错: {e}")
                        failure_count += 1
                    finally:
                        pbar.update(1)

                    # 添加新任务
                    if remaining_tasks:
                        next_task = remaining_tasks.pop(0)
                        futures[executor.submit(download_task, next_task)] = next_task

                        # 添加随机延迟，减轻爬虫特征
                        if random.random() < 0.3:  # 30%的概率
                            time.sleep(random.uniform(0.1, 0.5))

    # 保存更新后的缓存
    utils.save_cache(cache, cache_file)

    total_count = success_count + cached_count
    logger.info(
        f"图片下载完成，共 {total_count}/{len(pins)} 张图片 (新下载: {success_count}, 使用缓存: {cached_count})"
    )
    return pins


# 为了保持向后兼容
def download_images(
    pins: List[Dict], output_dir: str, prefix: str, max_workers: int = 0
) -> List[Dict]:
    """并发下载多个图片（旧接口，保持兼容性）

    Args:
        pins: Pin数据列表
        output_dir: 输出目录
        prefix: 文件名前缀
        max_workers: 最大并发数，0表示自动设置

    Returns:
        更新下载状态后的Pin数据列表
    """
    # 使用output_dir的cache子目录作为缓存目录
    cache_dir = os.path.join(output_dir, "cache")
    os.makedirs(cache_dir, exist_ok=True)

    images_dir = os.path.join(output_dir, "images")

    return download_images_with_cache(pins, images_dir, prefix, cache_dir, max_workers)
