#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
集成测试：验证中断机制修复的端到端效果

这个测试文件验证修复后的中断机制在真实场景下的表现：
1. 验证--only-images模式的中断机制修复
2. 测试实际的工作流程中断场景
3. 确保修复不影响正常的四阶段工作流程
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
import time
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.tools.refactored_workflow import RefactoredOnlyImagesWorkflow
from src.tools.stage_manager import _global_interrupt_manager


class TestIntegrationInterruptFixes:
    """集成测试：中断机制修复验证"""

    def setup_method(self):
        """测试前设置"""
        self.temp_dir = tempfile.mkdtemp()
        
        # 重置全局中断状态
        _global_interrupt_manager.reset()

    def teardown_method(self):
        """测试后清理"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        # 确保重置中断状态
        _global_interrupt_manager.reset()

    @pytest.mark.asyncio
    async def test_workflow_interrupt_before_stage1(self):
        """【集成测试】测试在阶段1开始前设置中断状态"""
        
        # 在工作流程开始前设置中断状态
        _global_interrupt_manager.set_interrupted()
        
        # 创建工作流程实例
        workflow = RefactoredOnlyImagesWorkflow(
            output_dir=self.temp_dir,
            max_concurrent=2
        )
        
        # 执行工作流程
        result = await workflow.execute()
        
        # 验证结果
        assert result["status"] == "interrupted", f"Expected interrupted status, got: {result.get('status')}"
        assert "用户中断工作流程" in result["message"], f"Expected interrupt message, got: {result.get('message')}"
        
        # 验证中断状态仍然设置
        assert _global_interrupt_manager.is_interrupted(), "全局中断状态应该保持设置"
        
        print("SUCCESS: Workflow correctly terminates when interrupt is set before execution")

    @pytest.mark.asyncio
    async def test_workflow_normal_execution_after_fixes(self):
        """【回归测试】验证修复后正常工作流程不受影响"""
        
        # 确保没有中断状态
        _global_interrupt_manager.reset()
        assert not _global_interrupt_manager.is_interrupted(), "初始状态应该是未中断"
        
        # 创建工作流程实例
        workflow = RefactoredOnlyImagesWorkflow(
            output_dir=self.temp_dir,
            max_concurrent=2
        )
        
        # 执行工作流程
        result = await workflow.execute()
        
        # 验证结果
        assert result["status"] == "success", f"正常执行应该成功，但得到: {result.get('status')}"
        
        # 验证中断状态保持未设置
        assert not _global_interrupt_manager.is_interrupted(), "正常执行后中断状态应该保持未设置"
        
        print("SUCCESS: Normal workflow execution works correctly after fixes")

    def test_interrupt_state_management_consistency(self):
        """【集成测试】测试中断状态管理的一致性"""
        
        # 初始状态验证
        assert not _global_interrupt_manager.is_interrupted(), "初始状态应该是未中断"
        
        # 设置中断状态
        _global_interrupt_manager.set_interrupted()
        assert _global_interrupt_manager.is_interrupted(), "中断状态应该被设置"
        
        # 获取详细信息
        info = _global_interrupt_manager.get_interrupt_info()
        assert info["is_interrupted"] == True, "详细信息应该显示中断状态为True"
        assert info["interrupt_count"] >= 1, "中断计数应该至少为1"
        assert info["last_interrupt_time"] is not None, "应该记录最后中断时间"
        
        # 强制检查
        assert _global_interrupt_manager.force_interrupt_check(), "强制检查应该返回True"
        
        # 重置状态
        _global_interrupt_manager.reset()
        assert not _global_interrupt_manager.is_interrupted(), "重置后状态应该是未中断"
        
        # 重置后的详细信息
        info_after_reset = _global_interrupt_manager.get_interrupt_info()
        assert info_after_reset["is_interrupted"] == False, "重置后详细信息应该显示未中断"
        
        print("SUCCESS: Interrupt state management is consistent and reliable")

    @pytest.mark.asyncio
    async def test_interrupt_mechanism_performance_impact(self):
        """【性能测试】验证中断机制修复对性能的影响"""
        
        # 测试正常执行的性能
        start_time = time.time()
        
        workflow = RefactoredOnlyImagesWorkflow(
            output_dir=self.temp_dir,
            max_concurrent=2
        )
        
        result = await workflow.execute()
        
        execution_time = time.time() - start_time
        
        # 验证结果
        assert result["status"] == "success", "正常执行应该成功"
        
        # 验证性能影响（应该在合理范围内，比如不超过60秒）
        assert execution_time < 60, f"执行时间过长: {execution_time:.2f}秒"
        
        print(f"SUCCESS: Workflow execution completed in {execution_time:.2f} seconds")
        print("Performance impact of interrupt mechanism fixes is acceptable")

    def test_multiple_interrupt_operations(self):
        """【压力测试】测试多次中断操作的稳定性"""
        
        # 执行多次中断操作
        for i in range(10):
            # 设置中断
            _global_interrupt_manager.set_interrupted()
            assert _global_interrupt_manager.is_interrupted(), f"第{i+1}次设置中断失败"
            
            # 强制检查
            assert _global_interrupt_manager.force_interrupt_check(), f"第{i+1}次强制检查失败"
            
            # 重置
            _global_interrupt_manager.reset()
            assert not _global_interrupt_manager.is_interrupted(), f"第{i+1}次重置失败"
        
        # 最终状态验证
        final_info = _global_interrupt_manager.get_interrupt_info()
        assert not final_info["is_interrupted"], "最终状态应该是未中断"
        assert final_info["interrupt_count"] >= 10, f"中断计数应该至少为10，实际: {final_info['interrupt_count']}"
        
        print("SUCCESS: Multiple interrupt operations are stable and reliable")

    @pytest.mark.asyncio
    async def test_interrupt_during_workflow_execution(self):
        """【模拟测试】模拟工作流程执行过程中的中断"""
        
        workflow = RefactoredOnlyImagesWorkflow(
            output_dir=self.temp_dir,
            max_concurrent=2
        )
        
        # 创建一个异步任务来执行工作流程
        async def execute_workflow():
            return await workflow.execute()
        
        # 创建一个异步任务来模拟中断
        async def simulate_interrupt():
            # 等待一小段时间让工作流程开始
            await asyncio.sleep(0.1)
            
            # 设置中断状态
            _global_interrupt_manager.set_interrupted()
            print("Interrupt signal sent during workflow execution")
        
        # 同时运行两个任务
        workflow_task = asyncio.create_task(execute_workflow())
        interrupt_task = asyncio.create_task(simulate_interrupt())
        
        # 等待两个任务完成
        await asyncio.gather(interrupt_task)
        result = await workflow_task
        
        # 验证结果
        assert result["status"] == "interrupted", f"Expected interrupted status, got: {result.get('status')}"
        assert _global_interrupt_manager.is_interrupted(), "中断状态应该被设置"
        
        print("SUCCESS: Workflow correctly handles interrupt during execution")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
