#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
下载工作流程端到端测试

测试完整的--only-images工作流程中的图片下载阶段，确保：
1. RefactoredOnlyImagesWorkflow能正确调用ImageDownloadStage
2. 整个工作流程的数据流正确
3. 错误传播和处理机制正常
4. 统计信息的汇总和报告正确
"""

import pytest
import asyncio
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path

# 添加项目根目录到路径
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.tools.refactored_workflow import RefactoredOnlyImagesWorkflow
from src.tools.stage_implementations import ImageDownloadStage


class TestDownloadWorkflowE2E:
    """下载工作流程端到端测试类"""
    
    @pytest.fixture
    def temp_output_dir(self):
        """创建临时输出目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def mock_workflow(self, temp_output_dir):
        """创建模拟的RefactoredOnlyImagesWorkflow实例"""
        return RefactoredOnlyImagesWorkflow(
            output_dir=temp_output_dir,
            max_concurrent=2,
            proxy=None
        )
    
    @pytest.fixture
    def sample_workflow_context(self, temp_output_dir):
        """创建示例工作流程上下文"""
        # 创建多个关键词目录
        keywords = ["test1", "test2"]
        for keyword in keywords:
            keyword_dir = Path(temp_output_dir) / keyword
            keyword_dir.mkdir(exist_ok=True)
            
            # 创建数据库文件
            db_path = keyword_dir / "pinterest.db"
            db_path.touch()
            
            # 创建图片目录
            images_dir = keyword_dir / "images"
            images_dir.mkdir(exist_ok=True)
        
        return keywords
    
    @pytest.mark.asyncio
    async def test_workflow_stage4_execution(self, mock_workflow, sample_workflow_context):
        """测试工作流程中阶段4的执行"""
        target_keyword = "test1"
        
        # 模拟前面阶段的成功执行
        mock_stage_results = {
            "stage1": {"success": True},
            "stage2": {"success": True}, 
            "stage3": {"success": True}
        }
        
        # 模拟阶段4的下载结果
        mock_download_stats = {
            "keywords_processed": 1,
            "total_downloaded": 5,
            "total_failed": 2,
            "keyword_details": {
                target_keyword: {
                    "downloaded": 5,
                    "failed": 2,
                    "total": 7
                }
            }
        }
        
        with patch.object(mock_workflow, '_check_interrupt_before_stage'):
            with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
                # 模拟前面阶段的执行
                def stage_side_effect(stage, **kwargs):
                    if isinstance(stage, ImageDownloadStage):
                        return {
                            "success": True,
                            "download_stats": mock_download_stats
                        }
                    else:
                        return {"success": True}
                
                mock_execute.side_effect = stage_side_effect
                
                # 执行工作流程
                result = await mock_workflow.execute(target_keyword=target_keyword)
        
        # 验证结果
        assert result['success'] is True
        assert 'stage4_image_download' in mock_workflow.workflow_stats
        
        stage4_stats = mock_workflow.workflow_stats['stage4_image_download']
        assert stage4_stats == mock_download_stats
        
        # 验证阶段4被正确调用
        assert mock_execute.call_count == 4  # 4个阶段
        
        # 检查最后一次调用（阶段4）
        last_call = mock_execute.call_args_list[-1]
        stage_instance = last_call[0][0]
        assert isinstance(stage_instance, ImageDownloadStage)
        assert last_call[1]['target_keyword'] == target_keyword
    
    @pytest.mark.asyncio
    async def test_workflow_all_keywords(self, mock_workflow, sample_workflow_context):
        """测试处理所有关键词的工作流程"""
        keywords = sample_workflow_context
        
        # 模拟阶段4的下载结果（所有关键词）
        mock_download_stats = {
            "keywords_processed": len(keywords),
            "total_downloaded": 10,
            "total_failed": 3,
            "keyword_details": {
                "test1": {"downloaded": 6, "failed": 1, "total": 7},
                "test2": {"downloaded": 4, "failed": 2, "total": 6}
            }
        }
        
        with patch.object(mock_workflow, '_check_interrupt_before_stage'):
            with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
                def stage_side_effect(stage, **kwargs):
                    if isinstance(stage, ImageDownloadStage):
                        return {
                            "success": True,
                            "download_stats": mock_download_stats
                        }
                    else:
                        return {"success": True}
                
                mock_execute.side_effect = stage_side_effect
                
                # 执行工作流程（不指定关键词）
                result = await mock_workflow.execute(target_keyword=None)
        
        # 验证结果
        assert result['success'] is True
        
        stage4_stats = mock_workflow.workflow_stats['stage4_image_download']
        assert stage4_stats['keywords_processed'] == len(keywords)
        assert stage4_stats['total_downloaded'] == 10
        assert stage4_stats['total_failed'] == 3
        
        # 验证所有关键词都被处理
        for keyword in keywords:
            assert keyword in stage4_stats['keyword_details']
    
    @pytest.mark.asyncio
    async def test_workflow_stage4_failure(self, mock_workflow):
        """测试阶段4失败时的工作流程处理"""
        target_keyword = "fail_test"
        
        with patch.object(mock_workflow, '_check_interrupt_before_stage'):
            with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
                def stage_side_effect(stage, **kwargs):
                    if isinstance(stage, ImageDownloadStage):
                        return {"success": False, "error": "下载失败"}
                    else:
                        return {"success": True}
                
                mock_execute.side_effect = stage_side_effect
                
                # 执行工作流程
                result = await mock_workflow.execute(target_keyword=target_keyword)
        
        # 验证工作流程失败
        assert result['success'] is False
        assert "图片下载失败" in result.get('error', '')
    
    @pytest.mark.asyncio
    async def test_workflow_interruption_during_stage4(self, mock_workflow):
        """测试阶段4执行期间的中断处理"""
        target_keyword = "interrupt_test"
        
        with patch.object(mock_workflow, '_check_interrupt_before_stage') as mock_check:
            # 模拟在阶段4之前发生中断
            def interrupt_side_effect(stage_name):
                if "阶段4" in stage_name:
                    raise KeyboardInterrupt("用户中断")
            
            mock_check.side_effect = interrupt_side_effect
            
            with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
                mock_execute.return_value = {"success": True}
                
                # 执行工作流程应该被中断
                with pytest.raises(KeyboardInterrupt):
                    await mock_workflow.execute(target_keyword=target_keyword)
    
    @pytest.mark.asyncio
    async def test_workflow_statistics_aggregation(self, mock_workflow, sample_workflow_context):
        """测试工作流程统计信息的聚合"""
        target_keyword = "stats_test"
        
        # 模拟各阶段的统计信息
        stage_stats = {
            "stage1": {"repair_count": 2},
            "stage2": {"converted_count": 100},
            "stage3": {"enhanced_count": 95},
            "stage4": {
                "keywords_processed": 1,
                "total_downloaded": 90,
                "total_failed": 5,
                "keyword_details": {
                    target_keyword: {"downloaded": 90, "failed": 5, "total": 95}
                }
            }
        }
        
        with patch.object(mock_workflow, '_check_interrupt_before_stage'):
            with patch.object(mock_workflow.workflow_manager, 'execute_stage') as mock_execute:
                def stage_side_effect(stage, **kwargs):
                    if hasattr(stage, 'stage_name'):
                        stage_name = stage.stage_name
                        if "数据库修复" in stage_name:
                            return {"success": True, "repair_stats": stage_stats["stage1"]}
                        elif "Base64编码" in stage_name:
                            return {"success": True, "conversion_stats": stage_stats["stage2"]}
                        elif "Pin详情" in stage_name:
                            return {"success": True, "enhancement_stats": stage_stats["stage3"]}
                        elif "图片文件下载" in stage_name:
                            return {"success": True, "download_stats": stage_stats["stage4"]}
                    return {"success": True}
                
                mock_execute.side_effect = stage_side_effect
                
                # 执行工作流程
                result = await mock_workflow.execute(target_keyword=target_keyword)
        
        # 验证统计信息被正确聚合
        assert result['success'] is True
        
        workflow_stats = mock_workflow.workflow_stats
        assert 'stage1_database_repair' in workflow_stats
        assert 'stage2_base64_conversion' in workflow_stats
        assert 'stage3_pin_enhancement' in workflow_stats
        assert 'stage4_image_download' in workflow_stats
        
        # 验证阶段4的统计信息
        stage4_stats = workflow_stats['stage4_image_download']
        assert stage4_stats['total_downloaded'] == 90
        assert stage4_stats['total_failed'] == 5
        assert target_keyword in stage4_stats['keyword_details']
    
    def test_workflow_initialization(self, temp_output_dir):
        """测试工作流程初始化"""
        workflow = RefactoredOnlyImagesWorkflow(
            output_dir=temp_output_dir,
            max_concurrent=5,
            proxy="http://proxy:8080"
        )
        
        assert workflow.output_dir == temp_output_dir
        assert workflow.max_concurrent == 5
        assert workflow.proxy == "http://proxy:8080"
        assert hasattr(workflow, 'workflow_manager')
        assert hasattr(workflow, 'workflow_stats')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
