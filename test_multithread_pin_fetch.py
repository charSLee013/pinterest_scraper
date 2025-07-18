#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest Pin详情页面多线程HTTP请求测试程序

测试目标：验证多线程环境下使用GlobalHeaderManager获取的headers
访问Pinterest Pin详情页面的实际效果，找出Pin详情获取失败的根本原因。

使用方法：
    python test_multithread_pin_fetch.py
"""

import asyncio
import json
import re
import time
import requests
import concurrent.futures
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from loguru import logger
from tabulate import tabulate

# 导入项目模块
from src.tools.global_header_manager import GlobalHeaderManager
from src.utils.improved_pin_detail_extractor import ImprovedPinDetailExtractor
from src.core.database.repository import SQLiteRepository


class PinFetchTester:
    """Pinterest Pin详情获取测试器"""
    
    def __init__(self):
        self.test_results = []
        self.headers = None
        
    async def initialize(self):
        """初始化测试环境"""
        logger.info("🔧 初始化测试环境...")
        
        # 获取全局headers
        header_manager = GlobalHeaderManager()
        success = await header_manager.ensure_headers_ready()
        
        if success:
            self.headers = header_manager.get_headers()
            logger.info(f"✅ 获取到headers，包含 {len(self.headers)} 个字段")
            logger.info(f"🔑 认证状态: _auth={self._extract_auth_status()}")
            return True
        else:
            logger.error("❌ 获取headers失败")
            return False
    
    def _extract_auth_status(self) -> str:
        """提取认证状态"""
        if not self.headers or 'Cookie' not in self.headers:
            return "无Cookie"
        
        cookie = self.headers['Cookie']
        auth_match = re.search(r'_auth=([^;]+)', cookie)
        return auth_match.group(1) if auth_match else "未找到"
    
    def get_test_pin_ids(self) -> List[str]:
        """获取测试用的Pin ID列表"""
        # 从现有数据库中获取一些Pin ID进行测试
        test_pins = []
        
        # 尝试从output目录中找到数据库文件
        output_dir = Path("output")
        if output_dir.exists():
            for db_dir in output_dir.iterdir():
                if db_dir.is_dir():
                    db_file = db_dir / "pinterest.db"
                    if db_file.exists():
                        try:
                            repo = SQLiteRepository(keyword=db_dir.name, output_dir="output")
                            with repo._get_session() as session:
                                from src.core.database.schema import Pin
                                pins = session.query(Pin).limit(5).all()
                                test_pins.extend([pin.id for pin in pins if pin.id])
                                if len(test_pins) >= 8:  # 获取8个测试Pin
                                    break
                        except Exception as e:
                            logger.debug(f"读取数据库 {db_file} 失败: {e}")
                            continue
        
        # 如果没有找到数据库中的Pin，使用一些示例Pin ID
        if not test_pins:
            test_pins = [
                "801077852519350337",  # 您curl测试中使用的Pin
                "bB3n6Tcs",
                "sa6tgU9C", 
                "lqypgVZ5",
                "5379106336332111288",
                "wF82nXE6",
                "b70fC3uk",
                "5379106335631667191"
            ]
        
        return test_pins[:8]  # 限制为8个测试Pin
    
    def fetch_pin_single_thread(self, pin_id: str) -> Dict:
        """单线程获取Pin详情"""
        start_time = time.time()
        result = {
            "pin_id": pin_id,
            "method": "single_thread",
            "status_code": None,
            "html_length": 0,
            "has_pws_data": False,
            "has_images_data": False,
            "extracted_images": 0,
            "success": False,
            "error": None,
            "response_time": 0
        }
        
        try:
            session = requests.Session()
            session.headers.update(self.headers)
            
            pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
            response = session.get(pin_url, timeout=30)
            
            result["status_code"] = response.status_code
            result["html_length"] = len(response.text)
            result["response_time"] = time.time() - start_time
            
            # 检查HTML内容
            html_content = response.text
            
            # 检查是否包含PWS数据
            result["has_pws_data"] = "window.__PWS_DATA__" in html_content
            
            # 检查是否包含images数据
            result["has_images_data"] = '"images":{' in html_content
            
            # 尝试提取Pin数据
            extractor = ImprovedPinDetailExtractor()
            pin_data = extractor._extract_pin_data_from_html(html_content, pin_id)
            
            if pin_data and pin_data.get('image_urls'):
                result["extracted_images"] = len(pin_data['image_urls'])
                result["success"] = True
            
            session.close()
            
        except Exception as e:
            result["error"] = str(e)
            result["response_time"] = time.time() - start_time
        
        return result
    
    def fetch_pin_multi_thread_worker(self, pin_id: str) -> Dict:
        """多线程工作函数"""
        start_time = time.time()
        result = {
            "pin_id": pin_id,
            "method": "multi_thread",
            "status_code": None,
            "html_length": 0,
            "has_pws_data": False,
            "has_images_data": False,
            "extracted_images": 0,
            "success": False,
            "error": None,
            "response_time": 0
        }
        
        try:
            # 复用项目中的fetch_pin_detail_with_headers函数逻辑
            session = requests.Session()
            session.headers.update(self.headers)
            
            pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
            response = session.get(pin_url, timeout=30)
            
            result["status_code"] = response.status_code
            result["html_length"] = len(response.text)
            result["response_time"] = time.time() - start_time
            
            # 检查HTML内容
            html_content = response.text
            
            # 检查是否包含PWS数据
            result["has_pws_data"] = "window.__PWS_DATA__" in html_content
            
            # 检查是否包含images数据
            result["has_images_data"] = '"images":{' in html_content
            
            # 尝试提取Pin数据
            extractor = ImprovedPinDetailExtractor()
            pin_data = extractor._extract_pin_data_from_html(html_content, pin_id)
            
            if pin_data and pin_data.get('image_urls'):
                result["extracted_images"] = len(pin_data['image_urls'])
                result["success"] = True
            
            session.close()
            
        except Exception as e:
            result["error"] = str(e)
            result["response_time"] = time.time() - start_time
        
        return result
    
    def test_single_thread(self, pin_ids: List[str]) -> List[Dict]:
        """测试单线程获取"""
        logger.info("🔄 开始单线程测试...")
        results = []
        
        for i, pin_id in enumerate(pin_ids, 1):
            logger.info(f"单线程测试 ({i}/{len(pin_ids)}): {pin_id}")
            result = self.fetch_pin_single_thread(pin_id)
            results.append(result)
            
            # 添加延迟避免被限制
            if i < len(pin_ids):
                time.sleep(1)
        
        return results
    
    def test_multi_thread(self, pin_ids: List[str], max_workers: int = 4) -> List[Dict]:
        """测试多线程获取"""
        logger.info(f"🔄 开始多线程测试 (并发数: {max_workers})...")
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_pin = {
                executor.submit(self.fetch_pin_multi_thread_worker, pin_id): pin_id
                for pin_id in pin_ids
            }
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_pin):
                pin_id = future_to_pin[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(f"多线程完成: {pin_id} - 状态: {result['status_code']}")
                except Exception as e:
                    logger.error(f"多线程异常: {pin_id} - {e}")
                    results.append({
                        "pin_id": pin_id,
                        "method": "multi_thread",
                        "error": str(e),
                        "success": False
                    })
        
        return results
    
    def analyze_results(self, single_results: List[Dict], multi_results: List[Dict]):
        """分析测试结果"""
        logger.info("📊 分析测试结果...")
        
        # 合并结果
        all_results = single_results + multi_results
        
        # 创建对比表格
        table_data = []
        for single, multi in zip(single_results, multi_results):
            table_data.append([
                single["pin_id"][:12] + "...",  # 截断Pin ID
                single["status_code"],
                multi["status_code"], 
                single["html_length"],
                multi["html_length"],
                "YES" if single["has_pws_data"] else "NO",
                "YES" if multi["has_pws_data"] else "NO",
                "YES" if single["has_images_data"] else "NO",
                "YES" if multi["has_images_data"] else "NO",
                single["extracted_images"],
                multi["extracted_images"],
                "SUCCESS" if single["success"] else "FAIL",
                "SUCCESS" if multi["success"] else "FAIL",
                f"{single['response_time']:.2f}s",
                f"{multi['response_time']:.2f}s"
            ])
        
        headers = [
            "Pin ID", "单线程状态", "多线程状态", "单线程HTML长度", "多线程HTML长度",
            "单线程PWS", "多线程PWS", "单线程Images", "多线程Images", 
            "单线程提取", "多线程提取", "单线程成功", "多线程成功",
            "单线程耗时", "多线程耗时"
        ]
        
        print("\n" + "="*120)
        print("Pinterest Pin详情获取测试结果对比")
        print("="*120)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        
        # 统计分析
        single_success = sum(1 for r in single_results if r["success"])
        multi_success = sum(1 for r in multi_results if r["success"])
        
        print(f"\n统计摘要:")
        print(f"单线程成功率: {single_success}/{len(single_results)} ({single_success/len(single_results)*100:.1f}%)")
        print(f"多线程成功率: {multi_success}/{len(multi_results)} ({multi_success/len(multi_results)*100:.1f}%)")
        
        # 分析失败原因
        self._analyze_failure_reasons(single_results, multi_results)
    
    def _analyze_failure_reasons(self, single_results: List[Dict], multi_results: List[Dict]):
        """分析失败原因"""
        print(f"\n失败原因分析:")
        
        for method, results in [("单线程", single_results), ("多线程", multi_results)]:
            failed_results = [r for r in results if not r["success"]]
            if failed_results:
                print(f"\n{method}失败情况:")
                for result in failed_results:
                    print(f"  Pin {result['pin_id'][:12]}...")
                    print(f"    状态码: {result['status_code']}")
                    print(f"    HTML长度: {result['html_length']}")
                    print(f"    包含PWS数据: {result['has_pws_data']}")
                    print(f"    包含Images数据: {result['has_images_data']}")
                    if result.get('error'):
                        print(f"    错误: {result['error']}")
    
    async def run_test(self):
        """运行完整测试"""
        logger.info("开始Pinterest Pin详情页面多线程HTTP请求测试")
        
        # 初始化
        if not await self.initialize():
            return
        
        # 获取测试Pin ID
        pin_ids = self.get_test_pin_ids()
        logger.info(f"测试Pin列表: {pin_ids}")
        
        # 单线程测试
        single_results = self.test_single_thread(pin_ids)
        
        # 多线程测试
        multi_results = self.test_multi_thread(pin_ids, max_workers=4)
        
        # 分析结果
        self.analyze_results(single_results, multi_results)
        
        logger.info("测试完成")

        # 保存HTML样本用于分析
        self.save_multiple_html_samples(pin_ids[:3], single_results[:3], multi_results[:3])

    def save_multiple_html_samples(self, pin_ids: List[str], single_results: List[Dict], multi_results: List[Dict]):
        """保存多个HTML样本用于分析对比"""
        logger.info("保存HTML样本到本地文件...")

        for i, pin_id in enumerate(pin_ids):
            try:
                # 保存单线程HTML样本
                self._save_single_html_sample(pin_id, "single_thread")

                # 保存多线程HTML样本
                self._save_single_html_sample(pin_id, "multi_thread")

                logger.info(f"Pin {pin_id} 的单线程和多线程HTML样本已保存")

            except Exception as e:
                logger.error(f"保存Pin {pin_id} HTML样本失败: {e}")

        logger.info("所有HTML样本保存完成！")
        logger.info("文件列表:")
        logger.info("- single_thread_pin_*.html (单线程获取的HTML)")
        logger.info("- multi_thread_pin_*.html (多线程获取的HTML)")

    def _save_single_html_sample(self, pin_id: str, method: str):
        """保存单个HTML样本"""
        session = requests.Session()
        session.headers.update(self.headers)

        pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
        response = session.get(pin_url, timeout=30)

        if response.status_code == 200:
            filename = f"{method}_pin_{pin_id}.html"

            # 保存HTML到文件
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)

            # 分析HTML内容
            html_content = response.text
            logger.info(f"[{method}] Pin {pin_id}:")
            logger.info(f"  - 文件: {filename}")
            logger.info(f"  - HTML长度: {len(html_content)}")
            logger.info(f"  - Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            logger.info(f"  - Content-Encoding: {response.headers.get('Content-Encoding', 'None')}")
            logger.info(f"  - 包含window.__PWS_DATA__: {'window.__PWS_DATA__' in html_content}")
            images_pattern = '"images":{'
            logger.info(f"  - 包含images字段: {images_pattern in html_content}")
            logger.info(f"  - 包含登录提示: {'Log in' in html_content}")
            logger.info(f"  - HTML前50字符: {repr(html_content[:50])}")

            # 检查特殊内容
            if 'pinterest.com/login' in html_content:
                logger.warning(f"  - ⚠️ [{method}] 页面被重定向到登录页面")

            if response.headers.get('Content-Encoding') in ['gzip', 'br']:
                logger.info(f"  - 响应内容被{response.headers.get('Content-Encoding')}压缩")

        session.close()


async def main():
    """主函数"""
    tester = PinFetchTester()
    await tester.run_test()


if __name__ == "__main__":
    asyncio.run(main())
