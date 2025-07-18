#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ImageDownloader统一接口测试

测试重构后的ImageDownloader类的统一接口，确保：
1. 核心方法 download_missing_images_for_keyword 正常工作
2. 兼容性方法 download_keyword_images 返回正确格式
3. 两个方法的结果一致性
4. 错误处理和边界情况
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

from src.tools.image_downloader import ImageDownloader
from src.core.database.repository import SQLiteRepository


class TestImageDownloaderUnified:
    """ImageDownloader统一接口测试类"""
    
    @pytest.fixture
    def temp_output_dir(self):
        """创建临时输出目录"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def mock_downloader(self, temp_output_dir):
        """创建模拟的ImageDownloader实例"""
        return ImageDownloader(
            output_dir=temp_output_dir,
            max_concurrent=2,
            prefer_requests=True
        )
    
    @pytest.fixture
    def sample_keyword_structure(self, temp_output_dir):
        """创建示例关键词目录结构"""
        keyword = "test_keyword"
        keyword_dir = Path(temp_output_dir) / keyword
        keyword_dir.mkdir(exist_ok=True)
        
        # 创建数据库文件（空文件用于测试）
        db_path = keyword_dir / "pinterest.db"
        db_path.touch()
        
        # 创建图片目录
        images_dir = keyword_dir / "images"
        images_dir.mkdir(exist_ok=True)
        
        return {
            'keyword': keyword,
            'keyword_dir': str(keyword_dir),
            'db_path': str(db_path),
            'images_dir': str(images_dir)
        }
    
    @pytest.mark.asyncio
    async def test_download_missing_images_for_keyword_no_database(self, mock_downloader):
        """测试没有数据库时的行为"""
        result = await mock_downloader.download_missing_images_for_keyword("nonexistent")
        
        assert isinstance(result, dict)
        assert result == {'downloaded': 0, 'failed': 0, 'skipped': 0}
    
    @pytest.mark.asyncio
    async def test_download_keyword_images_no_database(self, mock_downloader):
        """测试兼容性接口在没有数据库时的行为"""
        result = await mock_downloader.download_keyword_images("nonexistent")
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_download_missing_images_for_keyword_empty_database(self, mock_downloader, sample_keyword_structure):
        """测试空数据库的处理"""
        keyword = sample_keyword_structure['keyword']
        
        # 模拟空数据库
        with patch.object(mock_downloader, 'discover_keyword_databases') as mock_discover:
            mock_discover.return_value = [sample_keyword_structure]
            
            with patch.object(mock_downloader, 'find_missing_images') as mock_find:
                mock_find.return_value = []
                
                result = await mock_downloader.download_missing_images_for_keyword(keyword)
                
                assert result == {'downloaded': 0, 'failed': 0, 'skipped': 0}
    
    @pytest.mark.asyncio
    async def test_download_keyword_images_empty_database(self, mock_downloader, sample_keyword_structure):
        """测试兼容性接口处理空数据库"""
        keyword = sample_keyword_structure['keyword']
        
        # 模拟空数据库
        with patch.object(mock_downloader, 'discover_keyword_databases') as mock_discover:
            mock_discover.return_value = [sample_keyword_structure]
            
            with patch.object(mock_downloader, 'find_missing_images') as mock_find:
                mock_find.return_value = []
                
                result = await mock_downloader.download_keyword_images(keyword)
                
                assert isinstance(result, list)
                assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_interface_consistency(self, mock_downloader, sample_keyword_structure):
        """测试两个接口的一致性"""
        keyword = sample_keyword_structure['keyword']
        
        # 模拟下载结果
        mock_results = [
            (True, "下载成功"),
            (True, "下载成功"), 
            (False, "下载失败")
        ]
        
        with patch.object(mock_downloader, 'discover_keyword_databases') as mock_discover:
            mock_discover.return_value = [sample_keyword_structure]
            
            with patch.object(mock_downloader, 'find_missing_images') as mock_find:
                mock_find.return_value = [{'pin_id': f'pin_{i}'} for i in range(3)]
                
                with patch.object(mock_downloader, '_ensure_browser_session') as mock_browser:
                    mock_browser.return_value = True
                    
                    with patch.object(mock_downloader, '_download_images_concurrently') as mock_download:
                        mock_download.return_value = mock_results
                        
                        with patch.object(mock_downloader, '_analyze_download_results') as mock_analyze:
                            mock_analyze.return_value = {'success': 2, 'failed': 1}
                            
                            # 测试核心方法
                            core_result = await mock_downloader.download_missing_images_for_keyword(keyword)
                            
                            # 测试兼容性方法
                            compat_result = await mock_downloader.download_keyword_images(keyword)
                            
                            # 验证核心方法结果
                            assert core_result == {'downloaded': 2, 'failed': 1, 'skipped': 0}
                            
                            # 验证兼容性方法结果
                            assert isinstance(compat_result, list)
                            assert len(compat_result) == 3  # 2 success + 1 failed
                            
                            success_count = sum(1 for success, _ in compat_result if success)
                            failed_count = sum(1 for success, _ in compat_result if not success)
                            
                            assert success_count == 2
                            assert failed_count == 1
    
    @pytest.mark.asyncio
    async def test_download_keyword_images_return_format(self, mock_downloader):
        """测试兼容性接口的返回格式"""
        # 直接测试返回格式转换逻辑
        with patch.object(mock_downloader, 'download_missing_images_for_keyword') as mock_core:
            mock_core.return_value = {'downloaded': 3, 'failed': 2, 'skipped': 0}
            
            result = await mock_downloader.download_keyword_images("test")
            
            assert isinstance(result, list)
            assert len(result) == 5  # 3 + 2
            
            # 检查结果格式
            for success, message in result:
                assert isinstance(success, bool)
                assert isinstance(message, str)
                assert message in ["下载成功", "下载失败"]
            
            # 统计成功和失败数量
            success_count = sum(1 for success, _ in result if success)
            failed_count = sum(1 for success, _ in result if not success)
            
            assert success_count == 3
            assert failed_count == 2
    
    @pytest.mark.asyncio
    async def test_error_handling(self, mock_downloader):
        """测试错误处理"""
        with patch.object(mock_downloader, 'download_missing_images_for_keyword') as mock_core:
            mock_core.side_effect = Exception("测试异常")
            
            # 兼容性接口应该传播异常
            with pytest.raises(Exception, match="测试异常"):
                await mock_downloader.download_keyword_images("test")
    
    def test_method_signatures(self, mock_downloader):
        """测试方法签名的正确性"""
        import inspect
        
        # 检查核心方法签名
        core_sig = inspect.signature(mock_downloader.download_missing_images_for_keyword)
        assert 'keyword' in core_sig.parameters
        
        # 检查兼容性方法签名
        compat_sig = inspect.signature(mock_downloader.download_keyword_images)
        assert 'keyword' in compat_sig.parameters
        
        # 两个方法都应该是异步的
        assert asyncio.iscoroutinefunction(mock_downloader.download_missing_images_for_keyword)
        assert asyncio.iscoroutinefunction(mock_downloader.download_keyword_images)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
