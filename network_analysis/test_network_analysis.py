#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest网络分析测试脚本

用于测试和验证网络拦截器和API分析器的功能
"""

import os
import sys
import argparse
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from network_analysis.network_interceptor import NetworkInterceptor
from network_analysis.api_analyzer import APIAnalyzer


def test_network_analysis(url: str, scroll_count: int = 10, analyze_results: bool = True):
    """测试网络分析功能
    
    Args:
        url: 要分析的Pinterest URL
        scroll_count: 滚动次数
        analyze_results: 是否分析结果
    """
    logger.info("开始Pinterest网络分析测试")
    logger.info(f"目标URL: {url}")
    logger.info(f"滚动次数: {scroll_count}")
    
    try:
        # 创建网络拦截器
        interceptor = NetworkInterceptor()
        
        # 开始网络分析
        logger.info("启动网络拦截器...")
        summary = interceptor.start_analysis(url, scroll_count)
        
        # 打印分析摘要
        logger.info("网络分析摘要:")
        logger.info(f"  会话ID: {summary['session_id']}")
        logger.info(f"  总请求数: {summary['total_requests']}")
        logger.info(f"  总响应数: {summary['total_responses']}")
        logger.info(f"  成功的API响应: {summary['successful_api_responses']}")
        logger.info(f"  请求方法: {summary['request_methods']}")
        logger.info(f"  唯一域名: {summary['unique_domains']}")
        logger.info(f"  唯一端点: {len(summary['unique_endpoints'])}")
        logger.info(f"  输出目录: {summary['output_directory']}")
        
        if analyze_results and summary['successful_api_responses'] > 0:
            logger.info("开始分析API响应...")
            
            # 创建API分析器
            analyzer = APIAnalyzer()
            
            # 分析会话数据
            analysis_result = analyzer.analyze_session(summary['output_directory'])
            
            # 打印分析结果
            logger.info("API分析结果:")
            logger.info(f"  发现API端点: {analysis_result['analysis_summary']['total_endpoints']}个")
            logger.info(f"  支持分页的端点: {analysis_result['analysis_summary']['endpoints_with_pagination']}个")
            logger.info(f"  包含pins数据的端点: {analysis_result['analysis_summary']['endpoints_with_pins_data']}个")
            
            # 打印使用建议
            if analysis_result['recommendations']:
                logger.info("使用建议:")
                for recommendation in analysis_result['recommendations']:
                    logger.info(f"  - {recommendation}")
            
            return summary['output_directory'], analysis_result
        else:
            logger.warning("未发现有效的API响应，跳过详细分析")
            return summary['output_directory'], None
            
    except Exception as e:
        logger.error(f"网络分析测试失败: {e}")
        raise


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Pinterest网络分析测试工具")
    parser.add_argument("url", help="要分析的Pinterest URL")
    parser.add_argument("--scroll-count", type=int, default=10, help="滚动次数 (默认: 10)")
    parser.add_argument("--no-analysis", action="store_true", help="仅进行网络拦截，不分析结果")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                       help="日志级别 (默认: INFO)")
    
    args = parser.parse_args()
    
    # 配置日志
    logger.remove()
    logger.add(sys.stdout, level=args.log_level, format="{time} | {level} | {message}")
    
    # 运行测试
    try:
        output_dir, analysis_result = test_network_analysis(
            url=args.url,
            scroll_count=args.scroll_count,
            analyze_results=not args.no_analysis
        )
        
        logger.success("网络分析测试完成!")
        logger.info(f"结果保存在: {output_dir}")
        
        if analysis_result:
            logger.info("查看详细分析报告:")
            logger.info(f"  - 分析报告: {os.path.join(output_dir, 'analysis_report.json')}")
            logger.info(f"  - 使用指南: {os.path.join(output_dir, 'api_usage_guide.md')}")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 