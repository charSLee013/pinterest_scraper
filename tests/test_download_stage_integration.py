#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ImageDownloadStage集成测试

测试ImageDownloadStage与ImageDownloader的集成，确保：
1. 阶段能正确调用ImageDownloader的兼容性接口
2. 结果统计和处理逻辑正确
3. 错误处理和中断机制正常
4. 多关键词处理逻辑正确
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

from src.tools.stage_implementations import ImageDownloadStage
from src.tools.image_downloader import ImageDownloader


class TestImageDownloadStageIntegration:
    """ImageDownloadStage集成测试类"""
    
    @pytest.fixture
    def temp_output_dir(self):
        """创建临时输出目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def mock_stage(self, temp_output_dir):
        """创建模拟的ImageDownloadStage实例"""
        return ImageDownloadStage(
            output_dir=temp_output_dir,
            max_concurrent=2
        )
    
    @pytest.fixture
    def sample_keywords_structure(self, temp_output_dir):
        """创建多个关键词的目录结构"""
        keywords = ["keyword1", "keyword2", "keyword3"]
        structures = []
        
        for keyword in keywords:
            keyword_dir = Path(temp_output_dir) / keyword
            keyword_dir.mkdir(exist_ok=True)
            
            # 创建数据库文件
            db_path = keyword_dir / "pinterest.db"
            db_path.touch()
            
            # 创建图片目录
            images_dir = keyword_dir / "images"
            images_dir.mkdir(exist_ok=True)
            
            structures.append({
                'keyword': keyword,
                'keyword_dir': str(keyword_dir),
                'db_path': str(db_path),
                'images_dir': str(images_dir)
            })
        
        return structures
    
    @pytest.mark.asyncio
    async def test_single_keyword_download(self, mock_stage, sample_keywords_structure):
        """测试单个关键词下载"""
        target_keyword = "keyword1"
        
        # 模拟ImageDownloader的行为
        mock_results = [
            (True, "下载成功"),
            (True, "下载成功"),
            (False, "下载失败")
        ]
        
        with patch('src.tools.image_downloader.ImageDownloader') as MockDownloader:
            mock_downloader_instance = Mock()
            mock_downloader_instance.download_keyword_images = AsyncMock(return_value=mock_results)
            mock_downloader_instance.close = AsyncMock()
            MockDownloader.return_value = mock_downloader_instance
            
            # 模拟中断检查
            with patch.object(mock_stage, 'check_interruption_and_raise'):
                result = await mock_stage._execute_stage(target_keyword=target_keyword)
        
        # 验证结果
        assert result['success'] is True
        assert 'download_stats' in result
        
        download_stats = result['download_stats']
        assert download_stats['keywords_processed'] == 1
        assert download_stats['total_downloaded'] == 2
        assert download_stats['total_failed'] == 1
        assert target_keyword in download_stats['keyword_details']
        
        keyword_detail = download_stats['keyword_details'][target_keyword]
        assert keyword_detail['downloaded'] == 2
        assert keyword_detail['failed'] == 1
        assert keyword_detail['total'] == 3
        
        # 验证ImageDownloader被正确调用
        MockDownloader.assert_called_once_with(
            output_dir=mock_stage.output_dir,
            max_concurrent=mock_stage.max_concurrent,
            prefer_requests=True
        )
        mock_downloader_instance.download_keyword_images.assert_called_once_with(target_keyword)
        mock_downloader_instance.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_multiple_keywords_download(self, mock_stage, sample_keywords_structure):
        """测试多个关键词下载"""
        # 模拟发现关键词
        keywords = [struct['keyword'] for struct in sample_keywords_structure]
        
        with patch.object(mock_stage, '_discover_all_keywords') as mock_discover:
            mock_discover.return_value = keywords
            
            # 模拟ImageDownloader的行为
            mock_results_per_keyword = {
                "keyword1": [(True, "成功"), (False, "失败")],
                "keyword2": [(True, "成功"), (True, "成功"), (True, "成功")],
                "keyword3": [(False, "失败")]
            }
            
            with patch('src.tools.image_downloader.ImageDownloader') as MockDownloader:
                mock_downloader_instance = Mock()
                
                def side_effect(keyword):
                    return mock_results_per_keyword.get(keyword, [])
                
                mock_downloader_instance.download_keyword_images = AsyncMock(side_effect=side_effect)
                mock_downloader_instance.close = AsyncMock()
                MockDownloader.return_value = mock_downloader_instance
                
                # 模拟中断检查
                with patch.object(mock_stage, 'check_interruption_and_raise'):
                    result = await mock_stage._execute_stage(target_keyword=None)
        
        # 验证结果
        assert result['success'] is True
        download_stats = result['download_stats']
        
        assert download_stats['keywords_processed'] == 3
        assert download_stats['total_downloaded'] == 4  # 1 + 3 + 0
        assert download_stats['total_failed'] == 2      # 1 + 0 + 1
        
        # 验证每个关键词的详细信息
        for keyword in keywords:
            assert keyword in download_stats['keyword_details']
            detail = download_stats['keyword_details'][keyword]
            expected_results = mock_results_per_keyword[keyword]
            expected_downloaded = sum(1 for success, _ in expected_results if success)
            expected_failed = sum(1 for success, _ in expected_results if not success)
            
            assert detail['downloaded'] == expected_downloaded
            assert detail['failed'] == expected_failed
            assert detail['total'] == len(expected_results)
    
    @pytest.mark.asyncio
    async def test_download_error_handling(self, mock_stage):
        """测试下载过程中的错误处理"""
        target_keyword = "error_keyword"
        
        with patch('src.tools.image_downloader.ImageDownloader') as MockDownloader:
            mock_downloader_instance = Mock()
            mock_downloader_instance.download_keyword_images = AsyncMock(
                side_effect=Exception("下载器异常")
            )
            mock_downloader_instance.close = AsyncMock()
            MockDownloader.return_value = mock_downloader_instance
            
            with patch.object(mock_stage, 'check_interruption_and_raise'):
                result = await mock_stage._execute_stage(target_keyword=target_keyword)
        
        # 验证错误被正确处理
        assert result['success'] is True  # 阶段本身不应该失败
        download_stats = result['download_stats']
        
        assert download_stats['keywords_processed'] == 1
        assert download_stats['total_downloaded'] == 0
        assert download_stats['total_failed'] == 0
        
        # 验证错误信息被记录
        assert target_keyword in download_stats['keyword_details']
        keyword_detail = download_stats['keyword_details'][target_keyword]
        assert 'error' in keyword_detail
        assert "下载器异常" in keyword_detail['error']
    
    @pytest.mark.asyncio
    async def test_interruption_handling(self, mock_stage):
        """测试中断处理"""
        target_keyword = "interrupt_test"
        
        with patch('src.tools.image_downloader.ImageDownloader') as MockDownloader:
            mock_downloader_instance = Mock()
            mock_downloader_instance.close = AsyncMock()
            MockDownloader.return_value = mock_downloader_instance
            
            # 模拟中断
            with patch.object(mock_stage, 'check_interruption_and_raise') as mock_interrupt:
                mock_interrupt.side_effect = KeyboardInterrupt("用户中断")
                
                with pytest.raises(KeyboardInterrupt):
                    await mock_stage._execute_stage(target_keyword=target_keyword)
        
        # 验证资源被清理
        mock_downloader_instance.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_no_keywords_found(self, mock_stage):
        """测试没有找到关键词的情况"""
        with patch.object(mock_stage, '_discover_all_keywords') as mock_discover:
            mock_discover.return_value = []
            
            with patch('src.tools.image_downloader.ImageDownloader') as MockDownloader:
                mock_downloader_instance = Mock()
                mock_downloader_instance.close = AsyncMock()
                MockDownloader.return_value = mock_downloader_instance
                
                result = await mock_stage._execute_stage(target_keyword=None)
        
        # 验证结果
        assert result['success'] is True
        download_stats = result['download_stats']
        
        assert download_stats['keywords_processed'] == 0
        assert download_stats['total_downloaded'] == 0
        assert download_stats['total_failed'] == 0
        assert len(download_stats['keyword_details']) == 0
    
    @pytest.mark.asyncio
    async def test_stage_completion_verification(self, mock_stage):
        """测试阶段完成验证"""
        # 这个测试验证 _verify_stage_completion 方法
        result = await mock_stage._verify_stage_completion()
        assert result is True  # 当前实现总是返回True
    
    def test_stage_initialization(self, temp_output_dir):
        """测试阶段初始化"""
        stage = ImageDownloadStage(temp_output_dir, max_concurrent=10)
        
        assert stage.output_dir == temp_output_dir
        assert stage.max_concurrent == 10
        assert stage.stage_name == "图片文件下载"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
