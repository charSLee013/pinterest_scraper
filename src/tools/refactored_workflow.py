#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
重构后的--only-images工作流程

实现完全独立的四阶段处理：
1. 数据库修复与检测阶段：自动检测并修复损坏的数据库文件
2. Base64编码Pin转换阶段：将base64编码转换为真实Pin ID
3. Pin详情数据补全阶段：批量获取缺失的Pin详情信息
4. 图片文件下载阶段：并发下载缺失的图片文件

每个阶段都有独立的数据库连接管理和优雅退出机制。
"""

import time
from typing import Optional, Dict
from loguru import logger

from .stage_manager import WorkflowManager
from .stage_implementations import (
    DatabaseRepairStage,
    Base64ConversionStage, 
    PinEnhancementStage,
    ImageDownloadStage
)


class RefactoredOnlyImagesWorkflow:
    """重构后的--only-images工作流程
    
    实现完全独立的四阶段处理逻辑，确保每个阶段都有独立的连接管理和优雅退出机制
    """
    
    def __init__(self, output_dir: str, max_concurrent: int = 15, proxy: Optional[str] = None):
        """初始化工作流程

        Args:
            output_dir: 输出目录
            max_concurrent: 最大并发数
            proxy: 代理设置
        """
        self.output_dir = output_dir
        self.max_concurrent = max_concurrent
        self.proxy = proxy
        
        # 创建工作流程管理器
        self.workflow_manager = WorkflowManager(output_dir)
        
        logger.info(f"🚀 初始化重构后的--only-images工作流程")
        logger.info(f"   - 输出目录: {output_dir}")
        logger.info(f"   - 最大并发: {max_concurrent}")
        logger.info(f"   - 代理设置: {proxy or '无'}")
        logger.info(f"   - 四阶段独立处理模式")
        
        # 工作流程统计
        self.workflow_stats = {
            "stage1_database_repair": {},
            "stage2_base64_conversion": {},
            "stage3_pin_enhancement": {},
            "stage4_image_download": {},
            "total_execution_time": 0
        }
    
    async def execute(self, target_keyword: Optional[str] = None) -> Dict:
        """执行重构后的四阶段工作流程 - 修复版本

        修复内容：
        1. 在每个阶段开始前强制检查中断状态
        2. 改进异常传播机制，确保KeyboardInterrupt立即终止工作流程
        3. 添加阶段间中断状态验证

        Args:
            target_keyword: 目标关键词，None表示处理所有关键词

        Returns:
            工作流程执行结果

        Raises:
            KeyboardInterrupt: 当工作流程被中断时立即抛出，不执行后续阶段
        """
        start_time = time.time()

        logger.info("🚀 开始执行重构后的--only-images工作流程")
        logger.info("=" * 80)
        logger.info("阶段1: 数据库修复与检测 - 自动检测并修复损坏的数据库文件")
        logger.info("阶段2: Base64编码Pin转换 - 将base64编码转换为真实Pin ID")
        logger.info("阶段3: Pin详情数据补全 - 批量获取缺失的Pin详情信息")
        logger.info("阶段4: 图片文件下载 - 并发下载缺失的图片文件")
        logger.info("=" * 80)

        try:
            # 【修复】导入全局中断管理器
            from .stage_manager import _global_interrupt_manager

            # 【修复】阶段1: 数据库修复与检测 - 执行前检查中断状态
            self._check_interrupt_before_stage("阶段1: 数据库修复与检测")
            logger.info("🔧 开始阶段1: 数据库修复与检测")
            stage1 = DatabaseRepairStage(self.output_dir)
            result1 = await self.workflow_manager.execute_stage(stage1, target_keyword=target_keyword)
            self.workflow_stats["stage1_database_repair"] = result1.get("repair_stats", {})

            if not result1.get("success"):
                logger.error("❌ 阶段1失败，但继续执行后续阶段")

            # 【修复】阶段2: Base64编码Pin转换 - 执行前检查中断状态
            self._check_interrupt_before_stage("阶段2: Base64编码Pin转换")
            logger.info("🔄 开始阶段2: Base64编码Pin转换")
            stage2 = Base64ConversionStage(self.output_dir)
            result2 = await self.workflow_manager.execute_stage(stage2, target_keyword=target_keyword)
            self.workflow_stats["stage2_base64_conversion"] = result2.get("conversion_stats", {})

            if not result2.get("success"):
                logger.error("❌ 阶段2失败，终止工作流程")
                return self._generate_failure_result("Base64转换失败")

            # 【修复】阶段3: Pin详情数据补全 - 执行前检查中断状态
            self._check_interrupt_before_stage("阶段3: Pin详情数据补全")
            logger.info("📥 开始阶段3: Pin详情数据补全")
            stage3 = PinEnhancementStage(self.output_dir)
            result3 = await self.workflow_manager.execute_stage(stage3, target_keyword=target_keyword)
            self.workflow_stats["stage3_pin_enhancement"] = result3.get("enhancement_stats", {})

            if not result3.get("success"):
                logger.warning("⚠️ 阶段3失败，但继续执行图片下载")

            # 【修复】阶段4: 图片文件下载 - 执行前检查中断状态
            self._check_interrupt_before_stage("阶段4: 图片文件下载")
            logger.info("📥 开始阶段4: 图片文件下载")
            stage4 = ImageDownloadStage(self.output_dir, self.max_concurrent)
            result4 = await self.workflow_manager.execute_stage(stage4, target_keyword=target_keyword)
            self.workflow_stats["stage4_image_download"] = result4.get("download_stats", {})

            if not result4.get("success"):
                logger.error("❌ 阶段4失败")
                return self._generate_failure_result("图片下载失败")

            # 计算总执行时间
            self.workflow_stats["total_execution_time"] = time.time() - start_time

            logger.info("=" * 80)
            logger.info("🎉 重构后的--only-images工作流程执行完成")

            return self._generate_success_result()

        except KeyboardInterrupt:
            # 【修复】立即处理工作流程中断，记录中断时间并立即终止
            logger.warning("🛑 工作流程被用户中断，立即停止所有后续阶段")
            self.workflow_stats["total_execution_time"] = time.time() - start_time

            # 【修复】返回中断结果而不是重新抛出异常，避免在main.py中被捕获后继续执行
            return self._generate_interrupted_result("用户中断工作流程")

        except Exception as e:
            logger.error(f"❌ 工作流程执行异常: {e}")
            return self._generate_failure_result(f"工作流程异常: {e}")

    def _check_interrupt_before_stage(self, stage_name: str):
        """【新增】在阶段开始前强制检查中断状态

        Args:
            stage_name: 阶段名称

        Raises:
            KeyboardInterrupt: 如果检测到中断状态
        """
        from .stage_manager import _global_interrupt_manager

        if _global_interrupt_manager.is_interrupted():
            logger.warning(f"🛑 {stage_name} 开始前检测到中断信号，立即终止工作流程")
            raise KeyboardInterrupt(f"{stage_name} 开始前被中断")
    
    def _generate_success_result(self) -> Dict:
        """生成成功结果"""
        return {
            "status": "success",  # 修复：使用main.py期望的字段名
            "message": "重构后的工作流程执行成功",
            "stats": self.workflow_stats,  # 修复：使用main.py期望的字段名
            "workflow_manager_stats": self.workflow_manager.get_workflow_stats()
        }

    def _generate_failure_result(self, error_message: str) -> Dict:
        """生成失败结果"""
        return {
            "status": "failed",  # 修复：使用main.py期望的字段名
            "message": error_message,
            "stats": self.workflow_stats,  # 修复：使用main.py期望的字段名
            "workflow_manager_stats": self.workflow_manager.get_workflow_stats()
        }

    def _generate_interrupted_result(self, error_message: str) -> Dict:
        """生成中断结果"""
        return {
            "status": "interrupted",  # 中断状态
            "message": error_message,
            "stats": self.workflow_stats,
            "workflow_manager_stats": self.workflow_manager.get_workflow_stats()
        }


# 为了向后兼容，创建别名
OptimizedOnlyImagesWorkflow = RefactoredOnlyImagesWorkflow
