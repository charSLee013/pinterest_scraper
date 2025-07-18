#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
独立阶段管理器 - 修复后的中断处理架构

为--only-images工作流程提供完全独立的阶段管理，确保：
1. 每个阶段使用独立的数据库连接
2. 阶段结束时完全关闭所有连接
3. 支持Ctrl+C优雅退出，退出后程序完全停止
4. 阶段完整性验证

修复内容：
- 移除了阶段特定的信号处理器，避免与主信号处理器冲突
- 实现了全局中断状态管理器，确保中断状态在所有组件间共享
- 添加了KeyboardInterrupt异常传播机制，确保中断能正确终止工作流程
- 保持了数据库连接独立性和事务完整性

架构原则：
- 信号处理器统一管理：只有主程序设置信号处理器
- 中断状态共享：通过GlobalInterruptionManager在所有阶段间共享中断状态
- 异常传播：中断时立即抛出KeyboardInterrupt，不继续执行后续阶段
- 资源清理：确保中断时数据库连接得到正确清理
"""

import asyncio
import threading
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
from loguru import logger

from ..core.database.manager_factory import DatabaseManagerFactory


class GlobalInterruptionManager:
    """全局中断状态管理器 - 增强线程安全版本

    修复内容：
    1. 改进双重检查锁定模式，避免竞态条件
    2. 添加中断状态持久性验证
    3. 增强线程安全性，使用更细粒度的锁控制
    4. 添加中断状态变更的原子性保证
    """

    _instance = None
    _creation_lock = threading.RLock()  # 使用可重入锁避免死锁

    def __new__(cls):
        # 【修复】改进双重检查锁定，避免竞态条件
        if cls._instance is None:
            with cls._creation_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    # 【修复】初始化实例变量，确保线程安全
                    cls._instance._interrupted = False
                    cls._instance._interrupt_lock = threading.RLock()  # 使用可重入锁
                    cls._instance._interrupt_count = 0  # 中断计数器，用于调试
                    cls._instance._last_interrupt_time = None  # 最后中断时间
        return cls._instance

    def set_interrupted(self):
        """【修复】设置中断状态 - 原子性操作"""
        with self._interrupt_lock:
            if not self._interrupted:  # 避免重复设置
                self._interrupted = True
                self._interrupt_count += 1
                self._last_interrupt_time = time.time()
                logger.warning(f"🛑 全局中断状态已设置 (第{self._interrupt_count}次)")
            else:
                logger.debug("🛑 中断状态已经设置，跳过重复设置")

    def is_interrupted(self) -> bool:
        """【修复】检查是否被中断 - 线程安全读取"""
        with self._interrupt_lock:
            return self._interrupted

    def reset(self):
        """【修复】重置中断状态 - 原子性操作"""
        with self._interrupt_lock:
            if self._interrupted:  # 只有在中断状态下才重置
                self._interrupted = False
                logger.debug(f"🔄 全局中断状态已重置 (之前有{self._interrupt_count}次中断)")
            else:
                logger.debug("🔄 中断状态已经是重置状态，跳过")

    def get_interrupt_info(self) -> dict:
        """【新增】获取中断状态详细信息 - 用于调试"""
        with self._interrupt_lock:
            return {
                "is_interrupted": self._interrupted,
                "interrupt_count": self._interrupt_count,
                "last_interrupt_time": self._last_interrupt_time
            }

    def force_interrupt_check(self) -> bool:
        """【新增】强制中断检查 - 确保状态一致性"""
        with self._interrupt_lock:
            # 双重验证中断状态
            current_state = self._interrupted
            if current_state:
                logger.debug("🔍 强制中断检查: 确认中断状态为True")
            return current_state


# 全局中断管理器实例
_global_interrupt_manager = GlobalInterruptionManager()


class StageManager(ABC):
    """抽象阶段管理器基类"""

    def __init__(self, stage_name: str, output_dir: str):
        """初始化阶段管理器

        Args:
            stage_name: 阶段名称
            output_dir: 输出目录
        """
        self.stage_name = stage_name
        self.output_dir = output_dir
        self.start_time = None
        self.end_time = None
        self._stop_event = asyncio.Event()

        # 使用全局中断管理器，不设置阶段特定信号处理器
        self.interrupt_manager = _global_interrupt_manager

        # 阶段统计
        self.stage_stats = {
            "stage_name": stage_name,
            "start_time": None,
            "end_time": None,
            "duration": 0,
            "success": False,
            "interrupted": False,
            "error_message": None
        }
    
    async def execute(self, **kwargs) -> Dict[str, Any]:
        """【修复版】执行阶段 - 增强中断检查机制

        修复内容：
        1. 多点中断状态检查，确保及时响应
        2. 强制中断验证，避免状态不一致
        3. 改进异常传播，确保KeyboardInterrupt立即终止工作流程

        Returns:
            阶段执行结果

        Raises:
            KeyboardInterrupt: 当阶段被用户中断时立即抛出
        """
        logger.info(f"🚀 开始执行 {self.stage_name}")
        self.start_time = time.time()
        self.stage_stats["start_time"] = self.start_time

        try:
            # 【修复】强制中断检查 - 阶段启动前
            self._force_interrupt_check("阶段启动前")

            # 执行阶段特定逻辑
            result = await self._execute_stage(**kwargs)

            # 【修复】强制中断检查 - 阶段执行后
            self._force_interrupt_check("阶段执行后")

            # 阶段完整性验证
            if not await self._verify_stage_completion():
                logger.error(f"❌ {self.stage_name}: 阶段完整性验证失败")
                return self._generate_failure_result("阶段完整性验证失败")

            # 【修复】强制中断检查 - 验证完成后
            self._force_interrupt_check("验证完成后")

            self.stage_stats["success"] = True
            logger.info(f"✅ {self.stage_name}: 阶段执行成功")
            return result

        except KeyboardInterrupt:
            # 【修复】立即处理中断，不允许继续执行
            self.stage_stats["interrupted"] = True
            logger.warning(f"🛑 {self.stage_name}: 阶段被用户中断，立即传播中断信号")

            # 【修复】确保中断状态被正确设置
            self.interrupt_manager.set_interrupted()

            # 【修复】立即重新抛出，不进行任何其他处理
            raise

        except Exception as e:
            logger.error(f"❌ {self.stage_name}: 阶段执行异常: {e}")
            self.stage_stats["error_message"] = str(e)
            return self._generate_failure_result(str(e))

        finally:
            # 强制清理所有连接
            await self._cleanup_connections()

            # 记录结束时间
            self.end_time = time.time()
            self.stage_stats["end_time"] = self.end_time
            self.stage_stats["duration"] = self.end_time - self.start_time

            logger.info(f"🏁 {self.stage_name}: 阶段结束，耗时 {self.stage_stats['duration']:.2f} 秒")

    def _force_interrupt_check(self, checkpoint: str):
        """【新增】强制中断检查 - 确保中断状态及时响应

        Args:
            checkpoint: 检查点名称

        Raises:
            KeyboardInterrupt: 如果检测到中断状态
        """
        if self.interrupt_manager.force_interrupt_check():
            logger.warning(f"🛑 {self.stage_name}: 在{checkpoint}检测到中断信号，立即终止")
            raise KeyboardInterrupt(f"{self.stage_name} 在{checkpoint}被中断")
    
    @abstractmethod
    async def _execute_stage(self, **kwargs) -> Dict[str, Any]:
        """执行阶段特定逻辑（子类实现）"""
        pass
    
    @abstractmethod
    async def _verify_stage_completion(self) -> bool:
        """验证阶段完整性（子类实现）"""
        pass
    
    async def _cleanup_connections(self):
        """清理所有数据库连接"""
        try:
            logger.debug(f"🔒 {self.stage_name}: 开始清理数据库连接")
            
            # 清理所有缓存的数据库管理器
            if hasattr(DatabaseManagerFactory, '_managers'):
                managers_to_cleanup = list(DatabaseManagerFactory._managers.keys())
                for cache_key in managers_to_cleanup:
                    try:
                        manager = DatabaseManagerFactory._managers.get(cache_key)
                        if manager and hasattr(manager, 'engine') and manager.engine:
                            manager.engine.dispose()
                            logger.debug(f"🔒 关闭数据库引擎: {cache_key}")
                        
                        # 从缓存中移除
                        if cache_key in DatabaseManagerFactory._managers:
                            del DatabaseManagerFactory._managers[cache_key]
                            
                    except Exception as e:
                        logger.debug(f"清理数据库管理器失败 {cache_key}: {e}")
                
                logger.debug(f"🔒 清理了 {len(managers_to_cleanup)} 个数据库连接")
            
            # 强制垃圾回收
            import gc
            gc.collect()
            
            # 等待连接完全释放
            await asyncio.sleep(0.5)
            
            logger.debug(f"✅ {self.stage_name}: 数据库连接清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ {self.stage_name}: 清理连接时出错: {e}")
    
    def _generate_success_result(self, data: Dict[str, Any] = None) -> Dict[str, Any]:
        """生成成功结果"""
        result = {
            "success": True,
            "stage": self.stage_name,
            "stats": self.stage_stats.copy()
        }
        if data:
            result.update(data)
        return result
    
    def _generate_failure_result(self, error_message: str) -> Dict[str, Any]:
        """生成失败结果"""
        return {
            "success": False,
            "stage": self.stage_name,
            "error": error_message,
            "stats": self.stage_stats.copy()
        }
    
    def _generate_interrupted_result(self) -> Dict[str, Any]:
        """生成中断结果"""
        return {
            "success": False,
            "stage": self.stage_name,
            "interrupted": True,
            "error": "用户中断操作",
            "stats": self.stage_stats.copy()
        }
    
    def is_interrupted(self) -> bool:
        """检查是否被中断"""
        return self.interrupt_manager.is_interrupted()

    def check_interruption_and_raise(self):
        """检查中断状态并在必要时抛出KeyboardInterrupt"""
        if self.interrupt_manager.is_interrupted():
            logger.debug(f"🛑 {self.stage_name}: 检测到中断信号，抛出KeyboardInterrupt")
            raise KeyboardInterrupt(f"{self.stage_name} 检测到中断信号")


class WorkflowManager:
    """工作流程管理器"""
    
    def __init__(self, output_dir: str):
        """初始化工作流程管理器
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        self.workflow_stats = {
            "total_stages": 0,
            "completed_stages": 0,
            "failed_stages": 0,
            "interrupted_stages": 0,
            "total_duration": 0,
            "stage_results": []
        }
    
    async def execute_stage(self, stage_manager: StageManager, **kwargs) -> Dict[str, Any]:
        """【修复版】执行单个阶段 - 增强中断传播机制

        修复内容：
        1. 阶段执行前强制检查中断状态
        2. 改进中断信号传播，确保立即终止工作流程
        3. 添加中断状态验证和调试信息

        Args:
            stage_manager: 阶段管理器
            **kwargs: 阶段参数

        Returns:
            阶段执行结果

        Raises:
            KeyboardInterrupt: 当阶段被中断时立即传播中断信号，终止整个工作流程
        """
        self.workflow_stats["total_stages"] += 1

        try:
            # 【修复】阶段执行前强制检查全局中断状态
            if _global_interrupt_manager.force_interrupt_check():
                logger.warning(f"🛑 阶段 {stage_manager.stage_name} 执行前检测到中断信号")
                self.workflow_stats["interrupted_stages"] += 1
                raise KeyboardInterrupt(f"阶段 {stage_manager.stage_name} 执行前被中断")

            # 执行阶段 - 如果被中断会抛出KeyboardInterrupt
            result = await stage_manager.execute(**kwargs)

            # 【修复】阶段执行后再次检查中断状态
            if _global_interrupt_manager.force_interrupt_check():
                logger.warning(f"🛑 阶段 {stage_manager.stage_name} 执行后检测到中断信号")
                self.workflow_stats["interrupted_stages"] += 1
                raise KeyboardInterrupt(f"阶段 {stage_manager.stage_name} 执行后被中断")

            # 记录结果
            self.workflow_stats["stage_results"].append(result)

            if result.get("success"):
                self.workflow_stats["completed_stages"] += 1
            else:
                self.workflow_stats["failed_stages"] += 1

            # 累计执行时间
            stage_duration = result.get("stats", {}).get("duration", 0)
            self.workflow_stats["total_duration"] += stage_duration

            return result

        except KeyboardInterrupt:
            # 【修复】立即传播中断信号，确保工作流程完全终止
            self.workflow_stats["interrupted_stages"] += 1

            # 【修复】确保全局中断状态被设置
            _global_interrupt_manager.set_interrupted()

            logger.warning(f"🛑 工作流程因阶段 {stage_manager.stage_name} 中断而立即停止")
            logger.debug(f"🔍 中断传播调试: 阶段={stage_manager.stage_name}, 中断状态={_global_interrupt_manager.get_interrupt_info()}")

            # 【修复】立即重新抛出KeyboardInterrupt，不允许工作流程继续
            raise  # 重新抛出KeyboardInterrupt以终止整个工作流程
    
    def get_workflow_stats(self) -> Dict[str, Any]:
        """获取工作流程统计信息"""
        return self.workflow_stats.copy()
