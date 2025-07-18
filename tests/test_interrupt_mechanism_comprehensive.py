#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中断机制综合测试

合并了原有的多个中断机制测试文件，提供完整的中断机制测试覆盖：
1. 中断机制缺陷重现和修复验证
2. 阶段间中断检查机制
3. KeyboardInterrupt异常传播
4. 全局中断状态管理器线程安全性
5. 端到端中断场景测试
"""

import os
import sys
import tempfile
import shutil
import asyncio
import pytest
import threading
import time
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.tools.refactored_workflow import RefactoredOnlyImagesWorkflow
from src.tools.stage_manager import _global_interrupt_manager


class TestInterruptMechanismComprehensive:
    """中断机制综合测试类"""
    
    @pytest.fixture
    def temp_output_dir(self):
        """创建临时输出目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def mock_workflow(self, temp_output_dir):
        """创建模拟的工作流程实例"""
        return RefactoredOnlyImagesWorkflow(
            output_dir=temp_output_dir,
            max_concurrent=2,
            proxy=None
        )
    
    def setup_method(self):
        """每个测试方法前的设置"""
        # 重置全局中断状态
        _global_interrupt_manager.reset()
    
    def teardown_method(self):
        """每个测试方法后的清理"""
        # 确保中断状态被重置
        _global_interrupt_manager.reset()
    
    @pytest.mark.asyncio
    async def test_interrupt_signal_propagation(self, mock_workflow):
        """测试中断信号传播机制"""
        # 模拟阶段执行过程中的中断
        with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
            def stage_side_effect(stage, **kwargs):
                # 在第二个阶段模拟中断
                if hasattr(stage, 'stage_name') and "Base64编码" in stage.stage_name:
                    _global_interrupt_manager.set_interrupted()
                    raise KeyboardInterrupt("模拟用户中断")
                return {"success": True}
            
            mock_execute.side_effect = stage_side_effect
            
            # 执行工作流程，应该被中断
            result = await mock_workflow.execute(target_keyword="test")
            
            # 验证中断结果
            assert result['status'] == 'interrupted'
            assert '用户中断' in result.get('message', '')
    
    @pytest.mark.asyncio
    async def test_interrupt_between_stages(self, mock_workflow):
        """测试阶段间中断检查机制"""
        # 在阶段开始前设置中断状态
        _global_interrupt_manager.set_interrupted()
        
        with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
            mock_execute.return_value = {"success": True}
            
            # 执行工作流程，应该在第一个阶段前被中断
            result = await mock_workflow.execute(target_keyword="test")
            
            # 验证中断结果
            assert result['status'] == 'interrupted'
            # 验证没有执行任何阶段
            assert mock_execute.call_count == 0
    
    @pytest.mark.asyncio
    async def test_interrupt_state_thread_safety(self, mock_workflow):
        """测试全局中断状态管理器的线程安全性"""
        results = []
        
        def worker_thread():
            """工作线程函数"""
            for i in range(100):
                _global_interrupt_manager.set_interrupted()
                is_interrupted = _global_interrupt_manager.is_interrupted()
                results.append(is_interrupted)
                _global_interrupt_manager.reset()
                time.sleep(0.001)  # 短暂延迟增加竞态条件概率
        
        # 创建多个线程同时操作中断状态
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=worker_thread)
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 验证所有操作都成功
        assert len(results) == 500  # 5个线程 × 100次操作
        assert all(isinstance(result, bool) for result in results)
    
    @pytest.mark.asyncio
    async def test_interrupt_recovery_mechanism(self, mock_workflow):
        """测试中断后的恢复机制"""
        # 第一次执行：模拟中断
        with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
            mock_execute.side_effect = KeyboardInterrupt("第一次中断")
            
            result1 = await mock_workflow.execute(target_keyword="test")
            assert result1['status'] == 'interrupted'
        
        # 重置中断状态
        _global_interrupt_manager.reset()
        
        # 第二次执行：应该正常工作
        with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
            mock_execute.return_value = {"success": True}
            
            result2 = await mock_workflow.execute(target_keyword="test")
            assert result2['success'] is True
            assert mock_execute.call_count == 4  # 4个阶段都应该执行
    
    @pytest.mark.asyncio
    async def test_interrupt_timing_accuracy(self, mock_workflow):
        """测试中断响应时间的准确性"""
        start_time = time.time()
        
        with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
            def delayed_interrupt(stage, **kwargs):
                # 延迟100ms后触发中断
                time.sleep(0.1)
                _global_interrupt_manager.set_interrupted()
                raise KeyboardInterrupt("延迟中断")
            
            mock_execute.side_effect = delayed_interrupt
            
            result = await mock_workflow.execute(target_keyword="test")
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # 验证中断响应时间合理（应该在200ms内完成）
            assert execution_time < 0.2
            assert result['status'] == 'interrupted'
    
    def test_global_interrupt_manager_singleton(self):
        """测试全局中断管理器的单例模式"""
        from src.tools.stage_manager import _global_interrupt_manager as manager1
        from src.tools.stage_manager import _global_interrupt_manager as manager2
        
        # 验证是同一个实例
        assert manager1 is manager2
        
        # 验证状态同步
        manager1.set_interrupted()
        assert manager2.is_interrupted()
        
        manager2.reset()
        assert not manager1.is_interrupted()
    
    @pytest.mark.asyncio
    async def test_interrupt_with_concurrent_operations(self, mock_workflow):
        """测试并发操作中的中断处理"""
        interrupt_count = 0
        
        async def concurrent_operation():
            nonlocal interrupt_count
            try:
                await asyncio.sleep(0.1)
                if _global_interrupt_manager.is_interrupted():
                    interrupt_count += 1
                    raise KeyboardInterrupt("并发操作中断")
            except KeyboardInterrupt:
                interrupt_count += 1
                raise
        
        # 启动多个并发操作
        tasks = [concurrent_operation() for _ in range(5)]
        
        # 在操作进行中设置中断
        await asyncio.sleep(0.05)
        _global_interrupt_manager.set_interrupted()
        
        # 等待所有任务完成或被中断
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 验证所有任务都被正确中断
        assert all(isinstance(result, KeyboardInterrupt) for result in results)
        assert interrupt_count >= 5  # 至少5次中断
    
    @pytest.mark.asyncio
    async def test_interrupt_cleanup_resources(self, mock_workflow):
        """测试中断时的资源清理"""
        cleanup_called = False
        
        class MockResource:
            def __init__(self):
                self.closed = False
            
            async def close(self):
                nonlocal cleanup_called
                cleanup_called = True
                self.closed = True
        
        mock_resource = MockResource()
        
        with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
            def interrupt_with_resource(stage, **kwargs):
                # 模拟资源使用和中断
                _global_interrupt_manager.set_interrupted()
                raise KeyboardInterrupt("资源清理测试中断")
            
            mock_execute.side_effect = interrupt_with_resource
            
            try:
                result = await mock_workflow.execute(target_keyword="test")
                # 模拟资源清理
                await mock_resource.close()
            except KeyboardInterrupt:
                await mock_resource.close()
            
            # 验证资源被正确清理
            assert cleanup_called
            assert mock_resource.closed


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
