#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中断机制缺陷重现测试

专门用于重现和验证--only-images模式中断机制的缺陷：
1. 阶段间中断信号丢失问题
2. KeyboardInterrupt异常传播失效
3. 全局中断状态管理器竞态条件
4. 中断后程序继续执行下一阶段的核心bug
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
import threading
import time
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.tools.refactored_workflow import RefactoredOnlyImagesWorkflow
from src.tools.stage_manager import WorkflowManager, GlobalInterruptionManager, _global_interrupt_manager
from src.tools.stage_implementations import (
    DatabaseRepairStage, Base64ConversionStage, 
    PinEnhancementStage, ImageDownloadStage
)


class TestInterruptMechanismDefects:
    """中断机制缺陷重现测试类"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        
        # 重置全局中断状态
        _global_interrupt_manager.reset()
        
        # 创建工作流程实例
        self.workflow = RefactoredOnlyImagesWorkflow(
            output_dir=self.temp_dir,
            max_concurrent=2
        )

    def teardown_method(self):
        """测试后清理"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        # 确保重置中断状态
        _global_interrupt_manager.reset()

    @pytest.mark.asyncio
    async def test_interrupt_continues_to_next_stage_bug(self):
        """【核心Bug重现】测试中断后程序继续执行下一阶段的问题"""
        
        execution_log = []
        interrupt_triggered = False
        
        # Mock各个阶段，记录执行顺序
        async def mock_stage1_with_interrupt(**kwargs):
            execution_log.append("stage1_started")
            
            # 在阶段1中途设置中断
            nonlocal interrupt_triggered
            interrupt_triggered = True
            _global_interrupt_manager.set_interrupted()
            
            # 模拟阶段1被中断
            execution_log.append("stage1_interrupted")
            raise KeyboardInterrupt("Stage 1 interrupted by user")
        
        async def mock_stage2(**kwargs):
            execution_log.append("stage2_started")
            return {"success": True, "conversion_stats": {}}
        
        async def mock_stage3(**kwargs):
            execution_log.append("stage3_started") 
            return {"success": True, "enhancement_stats": {}}
        
        async def mock_stage4(**kwargs):
            execution_log.append("stage4_started")
            return {"success": True, "download_stats": {}}
        
        # 创建Mock阶段
        with patch('src.tools.stage_implementations.DatabaseRepairStage') as MockStage1:
            with patch('src.tools.stage_implementations.Base64ConversionStage') as MockStage2:
                with patch('src.tools.stage_implementations.PinEnhancementStage') as MockStage3:
                    with patch('src.tools.stage_implementations.ImageDownloadStage') as MockStage4:
                        
                        # 配置Mock阶段
                        mock_stage1_instance = Mock()
                        mock_stage1_instance._execute_stage = mock_stage1_with_interrupt
                        MockStage1.return_value = mock_stage1_instance
                        
                        mock_stage2_instance = Mock()
                        mock_stage2_instance._execute_stage = mock_stage2
                        MockStage2.return_value = mock_stage2_instance
                        
                        mock_stage3_instance = Mock()
                        mock_stage3_instance._execute_stage = mock_stage3
                        MockStage3.return_value = mock_stage3_instance
                        
                        mock_stage4_instance = Mock()
                        mock_stage4_instance._execute_stage = mock_stage4
                        MockStage4.return_value = mock_stage4_instance
                        
                        # 执行工作流程，期望在阶段1中断
                        try:
                            result = await self.workflow.execute()
                            
                            # 【核心Bug验证】：如果程序有bug，这里不会抛出异常
                            # 而是会继续执行后续阶段
                            pytest.fail("Expected KeyboardInterrupt but workflow completed normally - BUG CONFIRMED!")
                            
                        except KeyboardInterrupt:
                            # 这是期望的行为：中断应该立即终止工作流程
                            pass
                        
                        # 验证执行日志
                        print(f"Execution log: {execution_log}")
                        
                        # 【关键验证】：检查是否存在继续执行下一阶段的bug
                        assert "stage1_started" in execution_log, "阶段1应该开始执行"
                        assert "stage1_interrupted" in execution_log, "阶段1应该被中断"
                        
                        # 【Bug检测】：如果存在bug，这些阶段会被执行
                        bug_detected = any(stage in execution_log for stage in [
                            "stage2_started", "stage3_started", "stage4_started"
                        ])
                        
                        if bug_detected:
                            executed_after_interrupt = [stage for stage in execution_log 
                                                      if stage in ["stage2_started", "stage3_started", "stage4_started"]]
                            pytest.fail(f"BUG CONFIRMED: After interrupt in stage1, these stages still executed: {executed_after_interrupt}")

    @pytest.mark.asyncio
    async def test_global_interrupt_manager_race_condition(self):
        """测试全局中断状态管理器的竞态条件"""
        
        interrupt_manager = GlobalInterruptionManager()
        results = []
        
        def set_interrupt_worker():
            """工作线程：设置中断状态"""
            for i in range(100):
                interrupt_manager.set_interrupted()
                time.sleep(0.001)  # 模拟实际工作延迟
        
        def check_interrupt_worker():
            """工作线程：检查中断状态"""
            for i in range(100):
                is_interrupted = interrupt_manager.is_interrupted()
                results.append(is_interrupted)
                time.sleep(0.001)
        
        def reset_interrupt_worker():
            """工作线程：重置中断状态"""
            for i in range(50):
                interrupt_manager.reset()
                time.sleep(0.002)
        
        # 创建多个并发线程
        threads = [
            threading.Thread(target=set_interrupt_worker),
            threading.Thread(target=check_interrupt_worker),
            threading.Thread(target=reset_interrupt_worker),
        ]
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证结果：不应该有异常或不一致的状态
        assert len(results) == 100, "所有检查操作都应该完成"
        
        # 检查是否存在竞态条件导致的状态不一致
        # 注意：这个测试可能会暴露竞态条件问题
        print(f"Interrupt check results: True={results.count(True)}, False={results.count(False)}")

    @pytest.mark.asyncio 
    async def test_keyboard_interrupt_propagation_failure(self):
        """测试KeyboardInterrupt异常传播失效问题"""
        
        propagation_path = []
        
        class InterruptTrackingStage:
            """追踪中断传播的Mock阶段"""
            
            def __init__(self, stage_name):
                self.stage_name = stage_name
            
            async def _execute_stage(self, **kwargs):
                propagation_path.append(f"{self.stage_name}_started")
                
                # 在第一个阶段触发中断
                if self.stage_name == "stage1":
                    propagation_path.append(f"{self.stage_name}_triggering_interrupt")
                    _global_interrupt_manager.set_interrupted()
                    raise KeyboardInterrupt(f"{self.stage_name} interrupted")
                
                # 其他阶段不应该被执行
                propagation_path.append(f"{self.stage_name}_completed")
                return {"success": True}
        
        # 创建追踪阶段
        stage1 = InterruptTrackingStage("stage1")
        stage2 = InterruptTrackingStage("stage2") 
        stage3 = InterruptTrackingStage("stage3")
        stage4 = InterruptTrackingStage("stage4")
        
        workflow_manager = WorkflowManager(self.temp_dir)
        
        try:
            # 执行阶段1，应该中断
            await workflow_manager.execute_stage(stage1)
            
            # 如果到达这里，说明异常传播失效
            propagation_path.append("stage1_exception_not_propagated")
            
            # 继续执行后续阶段（这是bug行为）
            await workflow_manager.execute_stage(stage2)
            await workflow_manager.execute_stage(stage3) 
            await workflow_manager.execute_stage(stage4)
            
            # 如果所有阶段都执行完成，说明存在严重的异常传播问题
            pytest.fail(f"CRITICAL BUG: KeyboardInterrupt not propagated! Path: {propagation_path}")
            
        except KeyboardInterrupt:
            # 这是期望的行为
            propagation_path.append("keyboard_interrupt_properly_propagated")
        
        # 验证传播路径
        print(f"Interrupt propagation path: {propagation_path}")
        
        # 验证关键点
        assert "stage1_started" in propagation_path, "阶段1应该开始"
        assert "stage1_triggering_interrupt" in propagation_path, "阶段1应该触发中断"
        
        # 检查异常传播是否正确
        if "stage1_exception_not_propagated" in propagation_path:
            pytest.fail("KeyboardInterrupt exception propagation failed!")
        
        # 检查后续阶段是否被错误执行
        subsequent_stages = [item for item in propagation_path 
                           if any(stage in item for stage in ["stage2", "stage3", "stage4"])]
        
        if subsequent_stages:
            pytest.fail(f"BUG: Subsequent stages executed after interrupt: {subsequent_stages}")

    def test_interrupt_state_persistence_across_stages(self):
        """测试中断状态在阶段间的持久性"""
        
        # 重置状态
        _global_interrupt_manager.reset()
        assert not _global_interrupt_manager.is_interrupted(), "初始状态应该是未中断"
        
        # 设置中断状态
        _global_interrupt_manager.set_interrupted()
        assert _global_interrupt_manager.is_interrupted(), "中断状态应该被设置"
        
        # 模拟阶段切换过程中的状态检查
        stage_checks = []
        for i in range(5):
            is_interrupted = _global_interrupt_manager.is_interrupted()
            stage_checks.append(is_interrupted)
            time.sleep(0.1)  # 模拟阶段间的时间间隔
        
        # 验证中断状态在整个过程中保持一致
        assert all(stage_checks), f"中断状态应该在所有检查中保持True: {stage_checks}"
        
        # 测试重置功能
        _global_interrupt_manager.reset()
        assert not _global_interrupt_manager.is_interrupted(), "重置后状态应该是未中断"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
