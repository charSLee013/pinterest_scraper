#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
网络连接诊断工具

用于诊断Pinterest爬虫的网络连接问题，包括DNS解析、连接测试、反爬虫检测等
"""

import asyncio
import socket
import ssl
import time
from typing import Dict, List, Optional, Tuple
import aiohttp
import requests
from loguru import logger
from patchright.async_api import async_playwright


class NetworkDiagnostics:
    """网络连接诊断器"""
    
    def __init__(self, proxy: Optional[str] = None):
        self.proxy = proxy
        self.results = {}
    
    async def run_full_diagnostics(self) -> Dict:
        """运行完整的网络诊断
        
        Returns:
            诊断结果字典
        """
        logger.info("🔍 开始网络连接诊断...")
        
        # 基础网络连接测试
        await self._test_basic_connectivity()
        
        # DNS解析测试
        await self._test_dns_resolution()
        
        # Pinterest连接测试
        await self._test_pinterest_connectivity()
        
        # 浏览器连接测试
        await self._test_browser_connectivity()
        
        # 反爬虫检测测试
        await self._test_anti_bot_detection()
        
        # 生成诊断报告
        self._generate_report()
        
        return self.results
    
    async def _test_basic_connectivity(self):
        """测试基础网络连接"""
        logger.info("📡 测试基础网络连接...")
        
        test_hosts = [
            ("google.com", 80),
            ("cloudflare.com", 443),
            ("github.com", 443)
        ]
        
        connectivity_results = []
        
        for host, port in test_hosts:
            try:
                start_time = time.time()
                
                # 测试TCP连接
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10)
                result = sock.connect_ex((host, port))
                sock.close()
                
                latency = (time.time() - start_time) * 1000
                
                if result == 0:
                    connectivity_results.append({
                        "host": host,
                        "port": port,
                        "status": "success",
                        "latency_ms": round(latency, 2)
                    })
                    logger.info(f"✅ {host}:{port} 连接成功 ({latency:.2f}ms)")
                else:
                    connectivity_results.append({
                        "host": host,
                        "port": port,
                        "status": "failed",
                        "error": f"Connection failed (code: {result})"
                    })
                    logger.warning(f"❌ {host}:{port} 连接失败")
                    
            except Exception as e:
                connectivity_results.append({
                    "host": host,
                    "port": port,
                    "status": "error",
                    "error": str(e)
                })
                logger.error(f"❌ {host}:{port} 连接错误: {e}")
        
        self.results["basic_connectivity"] = connectivity_results
    
    async def _test_dns_resolution(self):
        """测试DNS解析"""
        logger.info("🌐 测试DNS解析...")
        
        test_domains = [
            "pinterest.com",
            "www.pinterest.com",
            "i.pinimg.com",
            "api.pinterest.com"
        ]
        
        dns_results = []
        
        for domain in test_domains:
            try:
                start_time = time.time()
                ip_addresses = socket.gethostbyname_ex(domain)[2]
                resolution_time = (time.time() - start_time) * 1000
                
                dns_results.append({
                    "domain": domain,
                    "status": "success",
                    "ip_addresses": ip_addresses,
                    "resolution_time_ms": round(resolution_time, 2)
                })
                logger.info(f"✅ {domain} -> {ip_addresses[0]} ({resolution_time:.2f}ms)")
                
            except Exception as e:
                dns_results.append({
                    "domain": domain,
                    "status": "failed",
                    "error": str(e)
                })
                logger.error(f"❌ {domain} DNS解析失败: {e}")
        
        self.results["dns_resolution"] = dns_results
    
    async def _test_pinterest_connectivity(self):
        """测试Pinterest连接"""
        logger.info("📌 测试Pinterest连接...")
        
        test_urls = [
            "https://www.pinterest.com/",
            "https://www.pinterest.com/search/pins/?q=test",
            "https://api.pinterest.com/",
            "https://i.pinimg.com/"
        ]
        
        pinterest_results = []
        
        # 使用requests测试
        for url in test_urls:
            try:
                start_time = time.time()
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                }
                
                proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
                
                response = requests.get(
                    url, 
                    headers=headers, 
                    proxies=proxies,
                    timeout=30,
                    allow_redirects=True
                )
                
                response_time = (time.time() - start_time) * 1000
                
                pinterest_results.append({
                    "url": url,
                    "status": "success",
                    "status_code": response.status_code,
                    "response_time_ms": round(response_time, 2),
                    "content_length": len(response.content),
                    "headers": dict(response.headers)
                })
                
                logger.info(f"✅ {url} -> {response.status_code} ({response_time:.2f}ms)")
                
            except Exception as e:
                pinterest_results.append({
                    "url": url,
                    "status": "failed",
                    "error": str(e)
                })
                logger.error(f"❌ {url} 连接失败: {e}")
        
        self.results["pinterest_connectivity"] = pinterest_results
    
    async def _test_browser_connectivity(self):
        """测试浏览器连接"""
        logger.info("🌐 测试浏览器连接...")
        
        try:
            async with async_playwright() as p:
                # 启动浏览器
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-extensions",
                        "--disable-infobars",
                    ]
                )
                
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
                    viewport={"width": 1920, "height": 1080}
                )
                
                page = await context.new_page()
                
                # 测试访问Pinterest
                start_time = time.time()
                
                try:
                    await page.goto("https://www.pinterest.com/", timeout=30000, wait_until="domcontentloaded")
                    load_time = (time.time() - start_time) * 1000
                    
                    # 获取页面信息
                    title = await page.title()
                    url = page.url
                    
                    self.results["browser_connectivity"] = {
                        "status": "success",
                        "load_time_ms": round(load_time, 2),
                        "final_url": url,
                        "page_title": title
                    }
                    
                    logger.info(f"✅ 浏览器访问Pinterest成功 ({load_time:.2f}ms)")
                    
                except Exception as e:
                    self.results["browser_connectivity"] = {
                        "status": "failed",
                        "error": str(e)
                    }
                    logger.error(f"❌ 浏览器访问Pinterest失败: {e}")
                
                await browser.close()
                
        except Exception as e:
            self.results["browser_connectivity"] = {
                "status": "error",
                "error": str(e)
            }
            logger.error(f"❌ 浏览器启动失败: {e}")
    
    async def _test_anti_bot_detection(self):
        """测试反爬虫检测"""
        logger.info("🤖 测试反爬虫检测...")
        
        # 测试不同的User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        ]
        
        detection_results = []
        
        for ua in user_agents:
            try:
                headers = {
                    'User-Agent': ua,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                
                response = requests.get(
                    "https://www.pinterest.com/",
                    headers=headers,
                    timeout=30
                )
                
                # 检查是否被检测为机器人
                is_blocked = (
                    response.status_code in [403, 429] or
                    "blocked" in response.text.lower() or
                    "captcha" in response.text.lower() or
                    "robot" in response.text.lower()
                )
                
                detection_results.append({
                    "user_agent": ua,
                    "status_code": response.status_code,
                    "is_blocked": is_blocked,
                    "content_length": len(response.content)
                })
                
                if is_blocked:
                    logger.warning(f"⚠️  User-Agent可能被检测: {ua[:50]}...")
                else:
                    logger.info(f"✅ User-Agent正常: {ua[:50]}...")
                    
            except Exception as e:
                detection_results.append({
                    "user_agent": ua,
                    "error": str(e)
                })
                logger.error(f"❌ User-Agent测试失败: {e}")
        
        self.results["anti_bot_detection"] = detection_results
    
    def _generate_report(self):
        """生成诊断报告"""
        logger.info("📋 生成诊断报告...")
        
        # 统计成功率
        basic_success = sum(1 for r in self.results.get("basic_connectivity", []) if r.get("status") == "success")
        dns_success = sum(1 for r in self.results.get("dns_resolution", []) if r.get("status") == "success")
        pinterest_success = sum(1 for r in self.results.get("pinterest_connectivity", []) if r.get("status") == "success")
        
        report = {
            "summary": {
                "basic_connectivity_rate": f"{basic_success}/3",
                "dns_resolution_rate": f"{dns_success}/4", 
                "pinterest_connectivity_rate": f"{pinterest_success}/4",
                "browser_connectivity": self.results.get("browser_connectivity", {}).get("status", "unknown")
            },
            "recommendations": []
        }
        
        # 生成建议
        if basic_success < 3:
            report["recommendations"].append("基础网络连接存在问题，请检查网络配置")
        
        if dns_success < 4:
            report["recommendations"].append("DNS解析存在问题，建议使用公共DNS (8.8.8.8, 1.1.1.1)")
        
        if pinterest_success < 2:
            report["recommendations"].append("Pinterest连接存在问题，可能需要代理或反爬虫措施")
        
        if self.results.get("browser_connectivity", {}).get("status") != "success":
            report["recommendations"].append("浏览器连接失败，可能是反爬虫检测或网络限制")
        
        self.results["report"] = report
        
        # 输出报告
        logger.info("=" * 60)
        logger.info("📊 网络诊断报告")
        logger.info("=" * 60)
        logger.info(f"基础连接: {report['summary']['basic_connectivity_rate']}")
        logger.info(f"DNS解析: {report['summary']['dns_resolution_rate']}")
        logger.info(f"Pinterest连接: {report['summary']['pinterest_connectivity_rate']}")
        logger.info(f"浏览器连接: {report['summary']['browser_connectivity']}")
        
        if report["recommendations"]:
            logger.info("\n🔧 建议:")
            for rec in report["recommendations"]:
                logger.info(f"  • {rec}")
        
        logger.info("=" * 60)


async def run_diagnostics(proxy: Optional[str] = None) -> Dict:
    """运行网络诊断
    
    Args:
        proxy: 代理服务器地址
        
    Returns:
        诊断结果
    """
    diagnostics = NetworkDiagnostics(proxy)
    return await diagnostics.run_full_diagnostics()


if __name__ == "__main__":
    asyncio.run(run_diagnostics())
