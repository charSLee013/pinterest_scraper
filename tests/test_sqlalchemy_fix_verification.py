"""
SQLAlchemy UPSERT错误修复验证测试

验证原子化保存功能和DownloadTask修复是否完全解决了原始的SQLAlchemy UPSERT错误问题。
包含Pin数据和DownloadTask的完整测试套件。
"""

import pytest
import tempfile
import os
from datetime import datetime

from src.core.database.base import initialize_database
from src.core.database.repository import SQLiteRepository


class TestSQLAlchemyFixVerification:
    """SQLAlchemy错误修复验证测试类"""
    
    @pytest.fixture
    def temp_db(self):
        """创建临时数据库"""
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_file.close()
        
        db_manager = initialize_database(temp_file.name)
        yield db_manager
        
        # 清理
        try:
            os.unlink(temp_file.name)
        except:
            pass
    
    @pytest.fixture
    def repository(self, temp_db):
        """创建Repository实例"""
        return SQLiteRepository()
    
    def test_original_error_data_structure(self, repository):
        """测试原始错误中的数据结构"""
        # 这是原始错误日志中的数据结构
        problematic_pin = {
            'id': '76561262410443623',
            'title': '',  # 空字符串标题
            'description': "an artist's rendering of a house with a swimming pool in the foreground",
            'creator': {},  # 空创作者对象
            'board': None,  # None板块
            'stats': {},   # 空统计对象
            'image_urls': {
                "1": "https://i.pinimg.com/236x/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg",
                "2": "https://i.pinimg.com/474x/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg",
                "3": "https://i.pinimg.com/736x/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg",
                "4": "https://i.pinimg.com/originals/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg"
            },
            'largest_image_url': 'https://i.pinimg.com/originals/2a/71/dd/2a71dd763197e1bbfceda6f1104ea7d4.jpg'
        }
        
        # 这种数据结构之前会导致SQLAlchemy UPSERT错误
        # 现在应该能够正常保存
        success = repository.save_pin_immediately(problematic_pin, 'building')
        assert success == True
        
        # 验证数据已正确保存
        saved_pins = repository.load_pins_by_query('building')
        assert len(saved_pins) == 1
        
        saved_pin = saved_pins[0]
        assert saved_pin['id'] == '76561262410443623'
        assert saved_pin['title'] == ''  # 空标题应该被保留
        assert saved_pin['description'] == "an artist's rendering of a house with a swimming pool in the foreground"
    
    def test_multiple_problematic_pins_batch(self, repository):
        """测试批量保存多个问题Pin"""
        problematic_pins = [
            {
                'id': 'pin_1',
                'title': '',
                'description': 'Description 1',
                'creator': {},
                'board': None,
                'stats': {}
            },
            {
                'id': 'pin_2',
                'title': None,  # None标题
                'description': '',
                'creator': None,
                'board': {},
                'stats': None
            },
            {
                'id': 'pin_3',
                'title': 'Normal Title',
                'description': 'Normal Description'
                # 缺少creator, board, stats字段
            }
        ]
        
        # 批量保存应该成功
        success = repository.save_pins_batch(problematic_pins, 'batch_test')
        assert success == True
        
        # 验证所有Pin都已保存
        saved_pins = repository.load_pins_by_query('batch_test')
        assert len(saved_pins) == 3
        
        # 验证每个Pin的ID都正确
        saved_ids = {pin['id'] for pin in saved_pins}
        expected_ids = {'pin_1', 'pin_2', 'pin_3'}
        assert saved_ids == expected_ids
    
    def test_concurrent_save_simulation(self, repository):
        """模拟并发保存场景"""
        # 模拟多次中断重启时的数据保存场景
        base_pin = {
            'id': 'concurrent_pin',
            'title': 'Original Title',
            'description': 'Original Description',
            'creator': {'name': 'Original Creator'},
            'stats': {'saves': 100}
        }
        
        # 第一次保存
        success1 = repository.save_pin_immediately(base_pin, 'concurrent_test')
        assert success1 == True
        
        # 模拟重启后的更新数据（最新数据应该覆盖）
        updated_pin = {
            'id': 'concurrent_pin',
            'title': 'Updated Title',
            'description': 'Updated Description',
            'creator': {},  # 空创作者
            'stats': None   # None统计
        }
        
        # 第二次保存（应该覆盖原数据）
        success2 = repository.save_pin_immediately(updated_pin, 'concurrent_test')
        assert success2 == True
        
        # 验证最新数据已覆盖
        saved_pins = repository.load_pins_by_query('concurrent_test')
        assert len(saved_pins) == 1
        
        saved_pin = saved_pins[0]
        assert saved_pin['id'] == 'concurrent_pin'
        assert saved_pin['title'] == 'Updated Title'
        assert saved_pin['description'] == 'Updated Description'
    
    def test_edge_case_data_types(self, repository):
        """测试边缘情况的数据类型"""
        edge_case_pins = [
            {
                'id': 'edge_1',
                'title': 0,  # 数字标题
                'description': False,  # 布尔描述
                'creator': {'name': None, 'id': ''},
                'stats': {'saves': 0, 'likes': None}
            },
            {
                'id': 'edge_2',
                'title': [],  # 列表标题
                'description': {},  # 字典描述
                'creator': {'name': 123, 'id': True},
                'stats': {'saves': 'invalid', 'likes': []}
            }
        ]
        
        # 这些边缘情况应该被正确处理（转换为字符串或None）
        success = repository.save_pins_batch(edge_case_pins, 'edge_test')
        assert success == True
        
        # 验证数据已保存
        saved_pins = repository.load_pins_by_query('edge_test')
        assert len(saved_pins) == 2
    
    def test_large_batch_save(self, repository):
        """测试大批量保存"""
        # 生成100个Pin，包含各种问题数据
        large_batch = []
        for i in range(100):
            pin = {
                'id': f'batch_pin_{i}',
                'title': '' if i % 3 == 0 else f'Title {i}',
                'description': None if i % 5 == 0 else f'Description {i}',
                'creator': {} if i % 7 == 0 else {'name': f'Creator {i}'},
                'board': None if i % 11 == 0 else {'name': f'Board {i}'},
                'stats': {} if i % 13 == 0 else {'saves': i * 10}
            }
            large_batch.append(pin)
        
        # 大批量保存应该成功
        success = repository.save_pins_batch(large_batch, 'large_batch_test')
        assert success == True
        
        # 验证所有Pin都已保存
        saved_pins = repository.load_pins_by_query('large_batch_test')
        assert len(saved_pins) == 100
    
    def test_no_sqlalchemy_errors(self, repository):
        """确保不再出现SQLAlchemy相关错误"""
        # 这个测试专门验证不会出现原始的SQLAlchemy错误
        # 包括: CompileError, IntegrityError, 以及 "gkpj" 错误代码
        
        # 使用最容易触发原始错误的数据结构
        trigger_data = [
            {
                'id': 'trigger_1',
                'title': '',
                'creator_name': None,
                'creator_id': None,
                'board_name': None,
                'board_id': None,
                'stats': None
            },
            {
                'id': 'trigger_2',
                # 完全缺少可选字段
            },
            {
                'id': 'trigger_3',
                'title': None,
                'description': '',
                'creator': {},
                'board': {},
                'stats': {},
                'image_urls': {}
            }
        ]
        
        # 这些操作之前会触发SQLAlchemy错误，现在应该正常工作
        try:
            success = repository.save_pins_batch(trigger_data, 'no_error_test')
            assert success == True
            
            # 验证数据保存成功
            saved_pins = repository.load_pins_by_query('no_error_test')
            assert len(saved_pins) == 3
            
        except Exception as e:
            # 如果出现任何SQLAlchemy相关错误，测试失败
            error_msg = str(e).lower()
            assert 'sqlalchemy' not in error_msg, f"SQLAlchemy错误仍然存在: {e}"
            assert 'gkpj' not in error_msg, f"原始gkpj错误仍然存在: {e}"
            assert 'compile' not in error_msg, f"编译错误仍然存在: {e}"
            assert 'upsert' not in error_msg, f"UPSERT错误仍然存在: {e}"
            
            # 如果是其他类型的错误，重新抛出以便调试
            raise


class TestDownloadTaskFixVerification:
    """DownloadTask UPSERT错误修复验证测试类"""

    @pytest.fixture
    def temp_db(self):
        """创建临时数据库"""
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_file.close()

        db_manager = initialize_database(temp_file.name)
        yield db_manager

        # 清理
        try:
            os.unlink(temp_file.name)
        except:
            pass

    @pytest.fixture
    def repository(self, temp_db):
        """创建Repository实例"""
        return SQLiteRepository()

    def test_download_task_upsert_no_errors(self, repository):
        """测试DownloadTask UPSERT操作不再出现SQLAlchemy错误"""
        # 创建测试Pin数据
        pin_data = {
            'id': 'download_test_pin_1',
            'title': 'Download Test Pin',
            'description': 'Test pin for download task UPSERT',
            'largest_image_url': 'https://example.com/test_image.jpg',
            'creator': {},  # 空创作者
            'board': None,  # None板块
            'stats': {}     # 空统计
        }

        # 第一次保存 - 应该创建Pin和DownloadTask
        success1 = repository.save_pin_immediately(pin_data, 'download_test')
        assert success1 == True

        # 第二次保存相同Pin - 应该更新Pin和DownloadTask，不出现错误
        updated_pin_data = {
            'id': 'download_test_pin_1',
            'title': 'Updated Download Test Pin',
            'description': 'Updated test pin for download task UPSERT',
            'largest_image_url': 'https://example.com/updated_test_image.jpg',
            'creator': {'name': 'Test Creator'},
            'board': {'name': 'Test Board'},
            'stats': {'saves': 100}
        }

        success2 = repository.save_pin_immediately(updated_pin_data, 'download_test')
        assert success2 == True

        # 验证数据已正确保存和更新
        saved_pins = repository.load_pins_by_query('download_test')
        assert len(saved_pins) == 1

        saved_pin = saved_pins[0]
        assert saved_pin['id'] == 'download_test_pin_1'
        assert saved_pin['title'] == 'Updated Download Test Pin'

    def test_no_sqlalchemy_download_task_errors(self, repository):
        """确保不再出现SQLAlchemy DownloadTask相关错误"""
        # 使用最容易触发原始错误的数据结构
        problematic_pins = [
            {
                'id': '377317275052370516',  # 原始错误中的Pin ID
                'title': '',
                'largest_image_url': 'https://i.pinimg.com/originals/6f/d5/2a/6fd52a2bc35f99f4b7bb384dbfd0a377.jpg',
                'creator': {},
                'board': None,
                'stats': {}
            },
            {
                'id': '774124930403473',  # 另一个原始错误中的Pin ID
                'title': None,
                'largest_image_url': 'https://i.pinimg.com/originals/b8/91/c5/b891c5599fb44688ef09303a21104d14.jpg',
                'creator': None,
                'board': {},
                'stats': None
            }
        ]

        # 这些操作之前会触发SQLAlchemy DownloadTask错误，现在应该正常工作
        try:
            success = repository.save_pins_batch(problematic_pins, 'no_download_error_test')
            assert success == True

            # 验证数据保存成功
            saved_pins = repository.load_pins_by_query('no_download_error_test')
            assert len(saved_pins) == 2

            # 验证Pin ID正确
            saved_ids = {pin['id'] for pin in saved_pins}
            expected_ids = {'377317275052370516', '774124930403473'}
            assert saved_ids == expected_ids

        except Exception as e:
            # 如果出现任何SQLAlchemy相关错误，测试失败
            error_msg = str(e).lower()
            assert 'sqlalchemy' not in error_msg, f"SQLAlchemy错误仍然存在: {e}"
            assert 'on conflict' not in error_msg, f"ON CONFLICT错误仍然存在: {e}"
            assert 'e3q8' not in error_msg, f"原始e3q8错误仍然存在: {e}"
            assert 'unique constraint' not in error_msg, f"唯一约束错误仍然存在: {e}"

            # 如果是其他类型的错误，重新抛出以便调试
            raise
