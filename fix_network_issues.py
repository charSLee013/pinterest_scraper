#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest爬虫网络问题自动修复工具

自动检测和修复常见的网络连接问题
"""

import asyncio
import subprocess
import sys
import os
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger


class NetworkFixer:
    """网络问题修复器"""
    
    def __init__(self):
        self.fixes_applied = []
    
    async def run_all_fixes(self):
        """运行所有修复"""
        logger.info("🔧 开始自动修复网络问题...")
        
        # 1. 检查和修复DNS配置
        await self._fix_dns_configuration()
        
        # 2. 检查和安装系统依赖
        await self._fix_system_dependencies()
        
        # 3. 优化浏览器配置
        await self._fix_browser_configuration()
        
        # 4. 检查防火墙设置
        await self._check_firewall_settings()
        
        # 5. 测试修复效果
        await self._test_fixes()
        
        # 输出修复报告
        self._generate_fix_report()
    
    async def _fix_dns_configuration(self):
        """修复DNS配置"""
        logger.info("🌐 检查DNS配置...")
        
        try:
            # 检查当前DNS设置
            if os.name == 'posix':  # Linux/Unix
                # 检查/etc/resolv.conf
                resolv_conf = Path("/etc/resolv.conf")
                if resolv_conf.exists():
                    content = resolv_conf.read_text()
                    
                    # 检查是否有可靠的DNS服务器
                    reliable_dns = ["8.8.8.8", "1.1.1.1", "8.8.4.4", "1.0.0.1"]
                    has_reliable_dns = any(dns in content for dns in reliable_dns)
                    
                    if not has_reliable_dns:
                        logger.warning("⚠️  未检测到可靠的DNS服务器")
                        logger.info("建议手动添加以下DNS服务器到 /etc/resolv.conf:")
                        logger.info("  nameserver 8.8.8.8")
                        logger.info("  nameserver 1.1.1.1")
                        
                        self.fixes_applied.append("DNS配置建议")
                    else:
                        logger.info("✅ DNS配置正常")
                        
        except Exception as e:
            logger.error(f"❌ DNS配置检查失败: {e}")
    
    async def _fix_system_dependencies(self):
        """修复系统依赖"""
        logger.info("📦 检查系统依赖...")
        
        try:
            # 检查是否在Linux环境
            if os.name == 'posix':
                # 尝试安装浏览器依赖
                try:
                    result = subprocess.run([
                        "uv", "run", "python", "-m", "patchright", "install-deps"
                    ], capture_output=True, text=True, timeout=120)
                    
                    if result.returncode == 0:
                        logger.info("✅ 浏览器系统依赖安装成功")
                        self.fixes_applied.append("浏览器系统依赖")
                    else:
                        logger.warning(f"⚠️  浏览器依赖安装失败: {result.stderr}")
                        
                        # 尝试使用apt-get安装
                        logger.info("尝试使用apt-get安装依赖...")
                        apt_result = subprocess.run([
                            "sudo", "apt-get", "install", "-y",
                            "libnss3", "libnspr4", "libatk1.0-0", 
                            "libatk-bridge2.0-0", "libatspi2.0-0", "libxdamage1"
                        ], capture_output=True, text=True, timeout=300)
                        
                        if apt_result.returncode == 0:
                            logger.info("✅ 使用apt-get安装依赖成功")
                            self.fixes_applied.append("apt-get系统依赖")
                        else:
                            logger.error(f"❌ apt-get安装失败: {apt_result.stderr}")
                            
                except subprocess.TimeoutExpired:
                    logger.error("❌ 依赖安装超时")
                except Exception as e:
                    logger.error(f"❌ 依赖安装异常: {e}")
            else:
                logger.info("ℹ️  非Linux环境，跳过系统依赖检查")
                
        except Exception as e:
            logger.error(f"❌ 系统依赖检查失败: {e}")
    
    async def _fix_browser_configuration(self):
        """优化浏览器配置"""
        logger.info("🌐 优化浏览器配置...")
        
        try:
            # 重新安装浏览器
            result = subprocess.run([
                "uv", "run", "python", "-m", "patchright", "install"
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info("✅ 浏览器重新安装成功")
                self.fixes_applied.append("浏览器重新安装")
            else:
                logger.warning(f"⚠️  浏览器安装失败: {result.stderr}")
                
        except Exception as e:
            logger.error(f"❌ 浏览器配置失败: {e}")
    
    async def _check_firewall_settings(self):
        """检查防火墙设置"""
        logger.info("🔥 检查防火墙设置...")
        
        try:
            if os.name == 'posix':
                # 检查iptables规则
                try:
                    result = subprocess.run([
                        "iptables", "-L", "-n"
                    ], capture_output=True, text=True, timeout=10)
                    
                    if result.returncode == 0:
                        output = result.stdout
                        
                        # 检查是否有阻止HTTPS的规则
                        if "443" in output and "DROP" in output:
                            logger.warning("⚠️  检测到可能阻止HTTPS的防火墙规则")
                            logger.info("建议检查防火墙配置，确保允许HTTPS出站连接")
                        else:
                            logger.info("✅ 防火墙配置看起来正常")
                            
                except subprocess.CalledProcessError:
                    logger.info("ℹ️  无法检查iptables（可能需要root权限）")
                except FileNotFoundError:
                    logger.info("ℹ️  未找到iptables命令")
            else:
                logger.info("ℹ️  非Linux环境，跳过防火墙检查")
                
        except Exception as e:
            logger.error(f"❌ 防火墙检查失败: {e}")
    
    async def _test_fixes(self):
        """测试修复效果"""
        logger.info("🧪 测试修复效果...")
        
        try:
            # 运行网络诊断
            from src.utils.network_diagnostics import run_diagnostics
            
            results = await run_diagnostics()
            
            # 检查关键指标
            browser_status = results.get("browser_connectivity", {}).get("status")
            pinterest_results = results.get("pinterest_connectivity", [])
            
            success_count = sum(1 for r in pinterest_results if r.get("status") == "success")
            
            if browser_status == "success" and success_count >= 2:
                logger.info("✅ 修复效果良好，网络连接正常")
                self.fixes_applied.append("修复验证成功")
            else:
                logger.warning("⚠️  修复效果有限，可能需要手动干预")
                
        except Exception as e:
            logger.error(f"❌ 修复测试失败: {e}")
    
    def _generate_fix_report(self):
        """生成修复报告"""
        logger.info("=" * 60)
        logger.info("📋 修复报告")
        logger.info("=" * 60)
        
        if self.fixes_applied:
            logger.info("✅ 已应用的修复:")
            for fix in self.fixes_applied:
                logger.info(f"  • {fix}")
        else:
            logger.info("ℹ️  未应用任何修复")
        
        logger.info("\n🔧 手动修复建议:")
        logger.info("  1. 确保网络连接稳定")
        logger.info("  2. 检查代理设置（如果使用）")
        logger.info("  3. 尝试使用VPN或更换网络环境")
        logger.info("  4. 检查Pinterest是否在您的地区可访问")
        logger.info("  5. 尝试降低采集数量进行测试")
        
        logger.info("\n📞 获取帮助:")
        logger.info("  • 运行: python test_network.py --full")
        logger.info("  • 查看详细日志: --verbose 参数")
        logger.info("  • 检查GitHub Issues获取最新解决方案")
        
        logger.info("=" * 60)


async def main():
    """主函数"""
    logger.info("🚀 Pinterest爬虫网络问题自动修复工具")
    logger.info("=" * 60)
    
    fixer = NetworkFixer()
    await fixer.run_all_fixes()
    
    logger.info("🎉 修复完成！请运行以下命令测试:")
    logger.info("  python test_network.py --basic")
    logger.info("  uv run python main.py -q test -c 5 --no-images")


if __name__ == "__main__":
    asyncio.run(main())
