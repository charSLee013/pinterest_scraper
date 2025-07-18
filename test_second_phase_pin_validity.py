#!/usr/bin/env python3
"""
完整测试第二阶段Pin详情页深度扩展采集的有效性
验证第二阶段是否是数据库中无效Pin的根本原因
"""

import asyncio
import json
import os
import time
import random
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime

# 导入项目模块
import sys
import os

# 添加src目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

# 直接导入模块
sys.path.append(os.path.join(src_dir, 'core'))
sys.path.append(os.path.join(src_dir, 'utils'))

from browser_manager import BrowserManager
from network_interceptor import NetworkInterceptor
from logger import logger

@dataclass
class PinValidationResult:
    pin_id: str
    is_numeric: bool
    is_valid: bool
    has_login_page: bool
    has_pin_content: bool
    has_image_urls: bool
    image_url_count: int
    response_size: int
    error: str = None

class SecondPhaseValidator:
    def __init__(self):
        self.browser = None
        self.interceptor = None
        self.session_start_time = time.time()
        
    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.cleanup()
        
    async def initialize(self):
        """初始化浏览器和网络拦截器"""
        logger.info("初始化浏览器环境...")
        
        # 初始化浏览器管理器
        self.browser = BrowserManager()
        if not await self.browser.start():
            raise Exception("浏览器启动失败")
            
        # 初始化网络拦截器
        self.interceptor = NetworkInterceptor(
            output_dir="test_second_phase_results",
            verbose=True,
            target_count=100
        )
        
        # 启动网络拦截
        await self.interceptor.start_interception(self.browser.page)
        
        logger.info("浏览器环境初始化完成")
        
    async def cleanup(self):
        """清理资源"""
        if self.interceptor:
            await self.interceptor.stop_interception()
        if self.browser:
            await self.browser.stop()
            
    async def step1_collect_related_pins(self, seed_pin_id: str = "801077852519350337") -> List[Dict]:
        """第一步：模拟第二阶段采集逻辑，从已知有效Pin采集相关推荐"""
        logger.info(f"=== 第一步：从Pin {seed_pin_id} 采集相关推荐 ===")
        
        pin_url = f"https://www.pinterest.com/pin/{seed_pin_id}/"
        logger.info(f"访问Pin详情页: {pin_url}")
        
        # 导航到Pin详情页
        if not await self.browser.navigate(pin_url):
            logger.error("Pin页面导航失败")
            return []
            
        # 等待页面加载
        await asyncio.sleep(3)
        
        # 清空之前的数据
        self.interceptor.extracted_pins.clear()
        
        # 滚动采集策略（复制smart_scraper.py的逻辑）
        max_scrolls = 20
        consecutive_no_new = 0
        max_consecutive = 3
        scroll_count = 0
        
        logger.info(f"开始滚动采集，最大滚动次数: {max_scrolls}，连续无新数据限制: {max_consecutive}")
        
        while (consecutive_no_new < max_consecutive and scroll_count < max_scrolls):
            pins_before = len(self.interceptor.extracted_pins)
            
            # 滚动页面触发API请求
            await self.browser.page.evaluate("window.scrollBy(0, window.innerHeight)")
            await asyncio.sleep(random.uniform(1.5, 3.0))
            scroll_count += 1
            
            # 等待网络请求完成
            try:
                await self.browser.page.wait_for_load_state('networkidle', timeout=3000)
            except:
                pass
                
            pins_after = len(self.interceptor.extracted_pins)
            
            if pins_after > pins_before:
                consecutive_no_new = 0
                logger.info(f"滚动 {scroll_count}: 新增 {pins_after - pins_before} 个Pin，总计: {pins_after}")
            else:
                consecutive_no_new += 1
                logger.debug(f"滚动 {scroll_count}: 无新Pin，连续无新数据: {consecutive_no_new}")
                
        collected_pins = list(self.interceptor.extracted_pins)
        logger.info(f"第一步完成: 采集到 {len(collected_pins)} 个相关Pin (滚动 {scroll_count} 次)")
        
        # 保存采集结果
        self.save_collected_pins(collected_pins, seed_pin_id)
        
        return collected_pins
        
    def save_collected_pins(self, pins: List[Dict], seed_pin_id: str):
        """保存采集到的Pin数据"""
        try:
            os.makedirs("test_second_phase_results", exist_ok=True)
            
            result_data = {
                "seed_pin_id": seed_pin_id,
                "collection_timestamp": datetime.now().isoformat(),
                "total_pins_collected": len(pins),
                "pins": pins
            }
            
            filename = f"test_second_phase_results/collected_pins_{seed_pin_id}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"采集结果已保存到: {filename}")
            
        except Exception as e:
            logger.error(f"保存采集结果失败: {e}")
            
    async def step2_validate_collected_pins(self, collected_pins: List[Dict]) -> List[PinValidationResult]:
        """第二步：验证采集到的Pin有效性（使用相同浏览器会话）"""
        logger.info(f"=== 第二步：验证 {len(collected_pins)} 个采集Pin的有效性 ===")
        
        validation_results = []
        
        for i, pin_data in enumerate(collected_pins, 1):
            pin_id = pin_data.get('id', '')
            if not pin_id:
                continue
                
            logger.info(f"验证 {i}/{len(collected_pins)}: {pin_id}")
            
            result = await self.validate_single_pin(pin_id)
            validation_results.append(result)
            
            # 添加延迟避免过于频繁的请求
            await asyncio.sleep(random.uniform(1.0, 2.0))
            
        logger.info(f"第二步完成: 验证了 {len(validation_results)} 个Pin")
        return validation_results
        
    async def validate_single_pin(self, pin_id: str) -> PinValidationResult:
        """验证单个Pin的有效性"""
        result = PinValidationResult(
            pin_id=pin_id,
            is_numeric=pin_id.isdigit(),
            is_valid=False,
            has_login_page=True,
            has_pin_content=False,
            has_image_urls=False,
            image_url_count=0,
            response_size=0
        )
        
        try:
            pin_url = f"https://www.pinterest.com/pin/{pin_id}/"
            
            # 使用相同的浏览器会话访问Pin页面
            if not await self.browser.navigate(pin_url):
                result.error = "页面导航失败"
                return result
                
            # 等待页面加载
            await asyncio.sleep(2)
            
            # 获取页面内容
            html_content = await self.browser.page.content()
            result.response_size = len(html_content)
            
            # 分析页面内容
            html_lower = html_content.lower()
            
            # 检查是否是登录页面
            result.has_login_page = 'log in' in html_lower and 'sign up' in html_lower
            
            # 检查是否包含Pin内容
            result.has_pin_content = f'"{pin_id}"' in html_content or f"'{pin_id}'" in html_content
            
            # 检查图片URL
            import re
            image_patterns = [
                r'https://i\.pinimg\.com/[^"\']*\.jpg',
                r'https://i\.pinimg\.com/[^"\']*\.png',
                r'https://i\.pinimg\.com/[^"\']*\.webp',
            ]
            
            image_urls = set()
            for pattern in image_patterns:
                matches = re.findall(pattern, html_content)
                image_urls.update(matches)
                
            # 过滤有效图片
            valid_images = [url for url in image_urls 
                          if not any(skip in url.lower() for skip in ['favicon', 'logo', 'icon', 'avatar'])
                          and any(size in url for size in ['736x', '564x', '474x', '236x', 'originals'])]
            
            result.has_image_urls = len(valid_images) > 0
            result.image_url_count = len(valid_images)
            
            # 判断Pin是否有效
            result.is_valid = not result.has_login_page and result.has_image_urls
            
        except Exception as e:
            result.error = str(e)
            
        return result
        
    def step3_analyze_results(self, collected_pins: List[Dict], validation_results: List[PinValidationResult]):
        """第三步：对比分析结果"""
        logger.info("=== 第三步：对比分析结果 ===")
        
        total_collected = len(collected_pins)
        total_validated = len(validation_results)
        
        # 基本统计
        valid_pins = [r for r in validation_results if r.is_valid]
        invalid_pins = [r for r in validation_results if not r.is_valid]
        
        # 格式分析
        numeric_pins = [r for r in validation_results if r.is_numeric]
        alphanumeric_pins = [r for r in validation_results if not r.is_numeric]
        
        numeric_valid = [r for r in valid_pins if r.is_numeric]
        alphanumeric_valid = [r for r in valid_pins if not r.is_numeric]
        
        # 失败原因分析
        login_page_pins = [r for r in validation_results if r.has_login_page and not r.error]
        error_pins = [r for r in validation_results if r.error]
        no_content_pins = [r for r in validation_results if not r.has_pin_content and not r.has_login_page and not r.error]
        
        # 生成报告
        report = {
            "collection_summary": {
                "total_pins_collected": total_collected,
                "total_pins_validated": total_validated,
                "collection_success_rate": f"{total_validated/total_collected*100:.1f}%" if total_collected > 0 else "0%"
            },
            "validity_analysis": {
                "valid_pins": len(valid_pins),
                "invalid_pins": len(invalid_pins),
                "validity_rate": f"{len(valid_pins)/total_validated*100:.1f}%" if total_validated > 0 else "0%"
            },
            "format_analysis": {
                "numeric_pins": {
                    "total": len(numeric_pins),
                    "valid": len(numeric_valid),
                    "validity_rate": f"{len(numeric_valid)/len(numeric_pins)*100:.1f}%" if numeric_pins else "0%"
                },
                "alphanumeric_pins": {
                    "total": len(alphanumeric_pins),
                    "valid": len(alphanumeric_valid),
                    "validity_rate": f"{len(alphanumeric_valid)/len(alphanumeric_pins)*100:.1f}%" if alphanumeric_pins else "0%"
                }
            },
            "failure_analysis": {
                "login_page_redirects": len(login_page_pins),
                "network_errors": len(error_pins),
                "no_pin_content": len(no_content_pins)
            },
            "pin_examples": {
                "valid_pins": [{"pin_id": r.pin_id, "format": "numeric" if r.is_numeric else "alphanumeric", "images": r.image_url_count} 
                              for r in valid_pins[:10]],
                "invalid_numeric_pins": [r.pin_id for r in invalid_pins if r.is_numeric][:10],
                "invalid_alphanumeric_pins": [r.pin_id for r in invalid_pins if not r.is_numeric][:10]
            }
        }
        
        # 保存详细报告
        self.save_analysis_report(report, validation_results)
        
        # 打印摘要
        self.print_analysis_summary(report)
        
        return report
        
    def save_analysis_report(self, report: Dict, validation_results: List[PinValidationResult]):
        """保存分析报告"""
        try:
            os.makedirs("test_second_phase_results", exist_ok=True)
            
            # 保存摘要报告
            with open("test_second_phase_results/analysis_report.json", 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
                
            # 保存详细验证结果
            detailed_results = []
            for r in validation_results:
                detailed_results.append({
                    "pin_id": r.pin_id,
                    "is_numeric": r.is_numeric,
                    "is_valid": r.is_valid,
                    "has_login_page": r.has_login_page,
                    "has_pin_content": r.has_pin_content,
                    "has_image_urls": r.has_image_urls,
                    "image_url_count": r.image_url_count,
                    "response_size": r.response_size,
                    "error": r.error
                })
                
            with open("test_second_phase_results/detailed_validation_results.json", 'w', encoding='utf-8') as f:
                json.dump(detailed_results, f, indent=2, ensure_ascii=False)
                
            logger.info("分析报告已保存到: test_second_phase_results/")
            
        except Exception as e:
            logger.error(f"保存分析报告失败: {e}")
            
    def print_analysis_summary(self, report: Dict):
        """打印分析摘要"""
        print("\n" + "="*80)
        print("第二阶段Pin详情页深度扩展有效性测试报告")
        print("="*80)
        
        collection = report["collection_summary"]
        validity = report["validity_analysis"]
        format_analysis = report["format_analysis"]
        failure = report["failure_analysis"]
        
        print(f"\n📊 采集统计:")
        print(f"  采集到的Pin数量: {collection['total_pins_collected']}")
        print(f"  成功验证的Pin数量: {collection['total_pins_validated']}")
        print(f"  采集成功率: {collection['collection_success_rate']}")
        
        print(f"\n✅ 有效性分析:")
        print(f"  有效Pin: {validity['valid_pins']}")
        print(f"  无效Pin: {validity['invalid_pins']}")
        print(f"  有效率: {validity['validity_rate']}")
        
        print(f"\n🔢 格式分析:")
        numeric = format_analysis["numeric_pins"]
        alpha = format_analysis["alphanumeric_pins"]
        print(f"  纯数字Pin: {numeric['total']} (有效: {numeric['valid']}, 有效率: {numeric['validity_rate']})")
        print(f"  字母数字Pin: {alpha['total']} (有效: {alpha['valid']}, 有效率: {alpha['validity_rate']})")
        
        print(f"\n❌ 失败原因:")
        print(f"  登录页面重定向: {failure['login_page_redirects']}")
        print(f"  网络错误: {failure['network_errors']}")
        print(f"  无Pin内容: {failure['no_pin_content']}")
        
        examples = report["pin_examples"]
        if examples["valid_pins"]:
            print(f"\n✅ 有效Pin示例:")
            for pin in examples["valid_pins"][:5]:
                print(f"  {pin['pin_id']} ({pin['format']}) - {pin['images']}张图片")
        else:
            print(f"\n❌ 未找到有效Pin")
            
        print(f"\n🔍 结论:")
        validity_rate = float(validity['validity_rate'].rstrip('%'))
        if validity_rate < 10:
            print(f"  ⚠️  第二阶段采集的Pin有效率极低 ({validity['validity_rate']})，确认这是数据库无效Pin的主要来源")
        elif validity_rate < 50:
            print(f"  ⚠️  第二阶段采集的Pin有效率较低 ({validity['validity_rate']})，是数据库无效Pin的重要来源")
        else:
            print(f"  ✅ 第二阶段采集的Pin有效率正常 ({validity['validity_rate']})，不是数据库无效Pin的主要原因")

async def main():
    """主函数"""
    logger.info("开始第二阶段Pin详情页深度扩展有效性测试")
    
    async with SecondPhaseValidator() as validator:
        try:
            # 第一步：采集相关推荐Pin
            collected_pins = await validator.step1_collect_related_pins()
            
            if not collected_pins:
                logger.error("未采集到任何Pin，测试终止")
                return
                
            # 第二步：验证采集到的Pin有效性
            validation_results = await validator.step2_validate_collected_pins(collected_pins)
            
            # 第三步：分析结果
            validator.step3_analyze_results(collected_pins, validation_results)
            
        except KeyboardInterrupt:
            logger.warning("用户中断测试")
        except Exception as e:
            logger.error(f"测试过程中发生错误: {e}")
            import traceback
            logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())
