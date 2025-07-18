#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中断机制修复验证测试

验证--only-images模式中断机制修复的有效性：
1. 验证阶段间中断检查机制正常工作
2. 验证KeyboardInterrupt异常正确传播并终止工作流程
3. 验证全局中断状态管理器线程安全性改进
4. 验证修复后的中断响应速度和准确性
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


class TestInterruptMechanismFixes:
    """中断机制修复验证测试类"""

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
    async def test_check_interrupt_before_stage_mechanism(self):
        """【修复验证】测试阶段前中断检查机制"""

        # 设置中断状态
        _global_interrupt_manager.set_interrupted()

        # 创建工作流程实例
        workflow = RefactoredOnlyImagesWorkflow(
            output_dir=self.temp_dir,
            max_concurrent=2
        )

        # 测试_check_interrupt_before_stage方法
        try:
            # 这应该立即抛出KeyboardInterrupt
            workflow._check_interrupt_before_stage("测试阶段")

            # 如果执行到这里，说明没有抛出异常，测试失败
            pytest.fail("_check_interrupt_before_stage应该抛出KeyboardInterrupt但没有")

        except KeyboardInterrupt:
            # 这是期望的行为
            print("SUCCESS: _check_interrupt_before_stage correctly throws KeyboardInterrupt")

        # 重置中断状态
        _global_interrupt_manager.reset()

        # 验证正常情况下不抛出异常
        try:
            workflow._check_interrupt_before_stage("测试阶段")
            print("SUCCESS: _check_interrupt_before_stage correctly does not throw exception when not interrupted")
        except KeyboardInterrupt:
            pytest.fail("未中断状态下_check_interrupt_before_stage不应该抛出KeyboardInterrupt")

    @pytest.mark.asyncio
    async def test_workflow_interrupt_handling(self):
        """【修复验证】测试工作流程中断处理机制"""

        # 创建工作流程实例
        workflow = RefactoredOnlyImagesWorkflow(
            output_dir=self.temp_dir,
            max_concurrent=2
        )

        # 模拟execute方法中的中断处理
        with patch.object(workflow, '_check_interrupt_before_stage') as mock_check:
            # 配置mock在第二次调用时抛出KeyboardInterrupt
            mock_check.side_effect = [None, KeyboardInterrupt("测试中断")]

            # 执行工作流程
            result = await workflow.execute()

            # 验证结果
            assert result["status"] == "interrupted", f"Expected interrupted status, got: {result.get('status')}"
            assert "用户中断工作流程" in result["message"], f"Expected interrupt message, got: {result.get('message')}"

            # 验证_check_interrupt_before_stage被调用
            assert mock_check.call_count >= 2, f"_check_interrupt_before_stage应该被调用至少2次，实际: {mock_check.call_count}"

    @pytest.mark.asyncio
    async def test_stage_manager_interrupt_propagation(self):
        """【修复验证】测试StageManager中断传播机制"""

        # 创建测试用的StageManager
        from src.tools.stage_manager import StageManager

        class TestStage(StageManager):
            def __init__(self, name, temp_dir):
                super().__init__(name, temp_dir)
                self.execute_called = False

            async def _execute_stage(self, **kwargs):
                self.execute_called = True
                # 设置中断并抛出KeyboardInterrupt
                _global_interrupt_manager.set_interrupted()
                raise KeyboardInterrupt("测试中断")

            async def _verify_stage_completion(self):
                return True

        # 创建工作流程管理器
        workflow_manager = WorkflowManager(self.temp_dir)

        # 创建测试阶段
        test_stage = TestStage("测试阶段", self.temp_dir)

        try:
            # 执行阶段，应该抛出KeyboardInterrupt
            await workflow_manager.execute_stage(test_stage)

            # 如果执行到这里，说明没有抛出异常，测试失败
            pytest.fail("execute_stage应该传播KeyboardInterrupt但没有")

        except KeyboardInterrupt:
            # 这是期望的行为
            print("SUCCESS: execute_stage correctly propagates KeyboardInterrupt")

            # 验证阶段被执行
            assert test_stage.execute_called, "阶段的_execute_stage方法应该被调用"

            # 验证中断状态
            assert _global_interrupt_manager.is_interrupted(), "全局中断状态应该被设置"

            # 验证工作流程统计
            assert workflow_manager.workflow_stats["interrupted_stages"] == 1, "中断阶段计数应该为1"

    @pytest.mark.asyncio
    async def test_stage_interrupt_checking_before_execution(self):
        """【修复验证】测试阶段执行前的中断检查机制"""
        
        execution_log = []
        
        # 在工作流程开始前就设置中断状态
        _global_interrupt_manager.set_interrupted()
        
        async def mock_stage1(**kwargs):
            execution_log.append("stage1_executed")
            return {"success": True, "repair_stats": {}}
        
        async def mock_stage2(**kwargs):
            execution_log.append("stage2_executed")
            return {"success": True, "conversion_stats": {}}
        
        # 创建Mock阶段
        with patch('src.tools.stage_implementations.DatabaseRepairStage') as MockStage1:
            with patch('src.tools.stage_implementations.Base64ConversionStage') as MockStage2:
                
                mock_stage1_instance = Mock()
                mock_stage1_instance._execute_stage = mock_stage1
                MockStage1.return_value = mock_stage1_instance
                
                mock_stage2_instance = Mock()
                mock_stage2_instance._execute_stage = mock_stage2
                MockStage2.return_value = mock_stage2_instance
                
                # 执行工作流程
                result = await self.workflow.execute()
                
                # 验证结果
                print(f"Execution log: {execution_log}")
                print(f"Result: {result}")
                
                # 【修复验证】：由于中断状态已设置，应该在第一个阶段开始前就被检测到
                assert result["status"] == "interrupted", "应该返回中断状态"
                
                # 【关键验证】：由于阶段开始前就检查中断，所以不应该执行任何阶段
                assert len(execution_log) == 0, f"阶段开始前检测到中断，不应该执行任何阶段，但执行了: {execution_log}"

    def test_global_interrupt_manager_thread_safety_improved(self):
        """【修复验证】测试全局中断状态管理器线程安全性改进"""
        
        interrupt_manager = GlobalInterruptionManager()
        results = []
        errors = []
        
        def concurrent_operations():
            """并发操作：设置、检查、重置中断状态"""
            try:
                for i in range(50):
                    # 设置中断
                    interrupt_manager.set_interrupted()
                    
                    # 检查中断状态
                    is_interrupted = interrupt_manager.is_interrupted()
                    results.append(is_interrupted)
                    
                    # 获取详细信息
                    info = interrupt_manager.get_interrupt_info()
                    
                    # 强制检查
                    force_check = interrupt_manager.force_interrupt_check()
                    
                    # 重置（偶尔）
                    if i % 10 == 0:
                        interrupt_manager.reset()
                    
                    time.sleep(0.001)  # 模拟实际工作延迟
                    
            except Exception as e:
                errors.append(str(e))
        
        # 创建多个并发线程
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=concurrent_operations)
            threads.append(thread)
        
        # 启动所有线程
        for thread in threads:
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证结果
        assert len(errors) == 0, f"不应该有线程安全错误: {errors}"
        assert len(results) > 0, "应该有检查结果"
        
        # 验证中断状态管理器的详细信息功能
        final_info = interrupt_manager.get_interrupt_info()
        assert "is_interrupted" in final_info, "应该包含中断状态信息"
        assert "interrupt_count" in final_info, "应该包含中断计数信息"
        
        print(f"Thread safety test completed. Results: {len(results)}, Errors: {len(errors)}")
        print(f"Final interrupt info: {final_info}")

    @pytest.mark.asyncio
    async def test_workflow_normal_execution_unaffected(self):
        """【回归测试】验证正常工作流程不受修复影响"""
        
        execution_log = []
        
        # Mock所有阶段正常执行
        async def mock_stage1(**kwargs):
            execution_log.append("stage1_completed")
            return {"success": True, "repair_stats": {"keywords_checked": 0}}
        
        async def mock_stage2(**kwargs):
            execution_log.append("stage2_completed")
            return {"success": True, "conversion_stats": {"total_converted": 0}}
        
        async def mock_stage3(**kwargs):
            execution_log.append("stage3_completed")
            return {"success": True, "enhancement_stats": {"total_pins_enhanced": 0}}
        
        async def mock_stage4(**kwargs):
            execution_log.append("stage4_completed")
            return {"success": True, "download_stats": {"total_downloaded": 0}}
        
        # 创建Mock阶段
        with patch('src.tools.stage_implementations.DatabaseRepairStage') as MockStage1:
            with patch('src.tools.stage_implementations.Base64ConversionStage') as MockStage2:
                with patch('src.tools.stage_implementations.PinEnhancementStage') as MockStage3:
                    with patch('src.tools.stage_implementations.ImageDownloadStage') as MockStage4:
                        
                        # 配置Mock阶段
                        mock_stage1_instance = Mock()
                        mock_stage1_instance._execute_stage = mock_stage1
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
                        
                        # 确保没有中断状态
                        _global_interrupt_manager.reset()
                        
                        # 执行工作流程
                        result = await self.workflow.execute()
                        
                        # 验证结果
                        print(f"Normal execution log: {execution_log}")
                        print(f"Normal execution result: {result}")
                        
                        # 【回归验证】：正常情况下应该成功完成
                        assert result["status"] == "success", f"正常执行应该成功，但得到: {result.get('status')}"
                        
                        # 验证所有阶段都被执行
                        expected_stages = ["stage1_completed", "stage2_completed", "stage3_completed", "stage4_completed"]
                        for stage in expected_stages:
                            assert stage in execution_log, f"阶段 {stage} 应该被执行"
                        
                        # 验证中断状态保持未设置
                        assert not _global_interrupt_manager.is_interrupted(), "正常执行后中断状态应该保持未设置"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
