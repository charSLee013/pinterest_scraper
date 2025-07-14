"""
原子化数据保存器模块

提供Pin数据的原子化保存功能，确保每个Pin的保存操作要么完全成功要么完全失败。
采用INSERT OR REPLACE策略，避免复杂的UPSERT操作。
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from contextlib import contextmanager
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger

from .schema import Pin, DownloadTask
from .normalizer import PinDataNormalizer


class AtomicPinSaver:
    """原子化Pin保存器
    
    负责Pin数据的原子化保存，每个Pin独立事务处理。
    采用INSERT OR REPLACE策略，确保最新数据覆盖旧数据。
    """
    
    def __init__(self, session_factory):
        """初始化原子化保存器
        
        Args:
            session_factory: 数据库会话工厂函数
        """
        self.session_factory = session_factory
        self.stats = {
            'total_processed': 0,
            'successful_saves': 0,
            'failed_saves': 0,
            'skipped_saves': 0
        }
    
    def save_pin_atomic(self, pin_data: Dict[str, Any], query: str, 
                       session_id: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """原子化保存单个Pin
        
        Args:
            pin_data: Pin数据字典
            query: 搜索关键词
            session_id: 会话ID（可选）
            
        Returns:
            (是否成功, 错误信息)
        """
        self.stats['total_processed'] += 1
        
        try:
            # 1. 数据标准化
            normalized_data = PinDataNormalizer.normalize_pin_data(pin_data, query)
            
            # 2. 数据验证
            if not PinDataNormalizer.validate_normalized_data(normalized_data):
                error_msg = f"Pin数据验证失败: {normalized_data.get('id')}"
                logger.error(error_msg)
                self.stats['failed_saves'] += 1
                return False, error_msg
            
            # 3. 原子化保存
            with self._get_atomic_session() as session:
                success = self._save_pin_to_database(session, normalized_data)
                if success:
                    self._create_download_task_if_needed(session, normalized_data)
                    self.stats['successful_saves'] += 1
                    logger.debug(f"原子化保存Pin成功: {normalized_data.get('id')}")
                    return True, None
                else:
                    self.stats['failed_saves'] += 1
                    return False, "数据库保存失败"
                    
        except Exception as e:
            # 安全地获取pin_id，处理非字典类型的pin_data
            pin_id = 'unknown'
            if isinstance(pin_data, dict):
                pin_id = pin_data.get('id', 'unknown')
            elif hasattr(pin_data, '__str__'):
                pin_id = str(pin_data)[:50]  # 限制长度避免日志过长

            error_msg = f"原子化保存Pin异常: {pin_id}, 错误: {e}"
            logger.error(error_msg)
            self.stats['failed_saves'] += 1
            return False, error_msg
    
    def save_pins_batch_atomic(self, pins: List[Dict[str, Any]], query: str, 
                              session_id: Optional[str] = None) -> Dict[str, Any]:
        """批量原子化保存Pin数据
        
        Args:
            pins: Pin数据列表
            query: 搜索关键词
            session_id: 会话ID（可选）
            
        Returns:
            保存结果统计
        """
        if not pins:
            return self._get_batch_result(0, 0, 0, [])
        
        successful_count = 0
        failed_count = 0
        skipped_count = 0
        errors = []
        
        logger.info(f"开始批量原子化保存: {len(pins)} 个Pin")
        
        for i, pin_data in enumerate(pins):
            try:
                # 检查是否需要跳过（基于时间戳的最新数据策略）
                # 只对字典类型的数据进行跳过检查
                if isinstance(pin_data, dict) and self._should_skip_pin(pin_data, query):
                    skipped_count += 1
                    self.stats['skipped_saves'] += 1
                    continue
                
                # 原子化保存单个Pin
                success, error_msg = self.save_pin_atomic(pin_data, query, session_id)
                
                if success:
                    successful_count += 1
                else:
                    failed_count += 1
                    if error_msg:
                        # 安全地获取pin_id
                        pin_id = f'index_{i}'
                        if isinstance(pin_data, dict):
                            pin_id = pin_data.get('id', f'index_{i}')

                        errors.append({
                            'pin_id': pin_id,
                            'error': error_msg
                        })

            except Exception as e:
                failed_count += 1
                error_msg = f"批量保存异常: {e}"
                logger.error(error_msg)

                # 安全地获取pin_id
                pin_id = f'index_{i}'
                if isinstance(pin_data, dict):
                    pin_id = pin_data.get('id', f'index_{i}')

                errors.append({
                    'pin_id': pin_id,
                    'error': error_msg
                })
        
        result = self._get_batch_result(successful_count, failed_count, skipped_count, errors)
        logger.info(f"批量原子化保存完成: 成功{successful_count}, 失败{failed_count}, 跳过{skipped_count}")
        
        return result
    
    @contextmanager
    def _get_atomic_session(self):
        """获取原子化数据库会话"""
        # 如果session_factory返回的是上下文管理器，直接使用
        if hasattr(self.session_factory, '__call__'):
            session_context = self.session_factory()
            if hasattr(session_context, '__enter__'):
                # 这是一个上下文管理器
                with session_context as session:
                    yield session
            else:
                # 这是一个普通的session对象
                session = session_context
                try:
                    yield session
                    session.commit()
                except Exception as e:
                    session.rollback()
                    logger.error(f"原子化会话回滚: {e}")
                    raise
                finally:
                    session.close()
        else:
            raise ValueError("session_factory必须是可调用对象")
    
    def _save_pin_to_database(self, session, normalized_data: Dict[str, Any]) -> bool:
        """保存Pin到数据库 - 使用INSERT OR REPLACE策略

        Args:
            session: 数据库会话
            normalized_data: 标准化后的Pin数据

        Returns:
            是否保存成功
        """
        try:
            # 准备数据库保存的数据，处理datetime序列化
            db_data = normalized_data.copy()

            # 将datetime对象转换为字符串
            if isinstance(db_data.get('created_at'), datetime):
                db_data['created_at'] = db_data['created_at'].isoformat()
            if isinstance(db_data.get('updated_at'), datetime):
                db_data['updated_at'] = db_data['updated_at'].isoformat()

            # 使用INSERT OR REPLACE避免UPSERT复杂性
            sql = text("""
                INSERT OR REPLACE INTO pins (
                    id, pin_hash, query, title, description,
                    creator_name, creator_id, board_name, board_id,
                    image_urls, largest_image_url, stats, raw_data,
                    created_at, updated_at
                ) VALUES (
                    :id, :pin_hash, :query, :title, :description,
                    :creator_name, :creator_id, :board_name, :board_id,
                    :image_urls, :largest_image_url, :stats, :raw_data,
                    :created_at, :updated_at
                )
            """)

            session.execute(sql, db_data)
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"数据库保存失败: {normalized_data.get('id')}, 错误: {e}")
            return False
    
    def _create_download_task_if_needed(self, session, normalized_data: Dict[str, Any]):
        """如果需要，创建下载任务"""
        image_url = normalized_data.get('largest_image_url')
        if not image_url:
            return
        
        try:
            # 使用INSERT OR REPLACE确保下载任务的原子性
            sql = text("""
                INSERT OR REPLACE INTO download_tasks (
                    pin_id, pin_hash, image_url, status, retry_count,
                    created_at, updated_at
                ) VALUES (
                    :pin_id, :pin_hash, :image_url, 'pending', 0,
                    :created_at, :updated_at
                )
            """)
            
            # 准备下载任务数据，处理datetime序列化
            created_at = normalized_data['created_at']
            updated_at = normalized_data['updated_at']

            if isinstance(created_at, datetime):
                created_at = created_at.isoformat()
            if isinstance(updated_at, datetime):
                updated_at = updated_at.isoformat()

            task_data = {
                'pin_id': normalized_data['id'],
                'pin_hash': normalized_data['pin_hash'],
                'image_url': image_url,
                'created_at': created_at,
                'updated_at': updated_at
            }
            
            session.execute(sql, task_data)
            logger.debug(f"创建下载任务: {normalized_data['id']}")
            
        except SQLAlchemyError as e:
            logger.warning(f"创建下载任务失败: {normalized_data.get('id')}, 错误: {e}")
    
    def _should_skip_pin(self, pin_data: Dict[str, Any], query: str) -> bool:
        """判断是否应该跳过Pin保存（基于最新数据策略）
        
        Args:
            pin_data: Pin数据
            query: 查询关键词
            
        Returns:
            是否应该跳过
        """
        # 这里可以实现基于时间戳的最新数据判断逻辑
        # 当前简化实现：不跳过任何Pin，让INSERT OR REPLACE处理覆盖
        return False
    
    def _get_batch_result(self, successful: int, failed: int, skipped: int, 
                         errors: List[Dict]) -> Dict[str, Any]:
        """构建批量保存结果"""
        return {
            'successful_count': successful,
            'failed_count': failed,
            'skipped_count': skipped,
            'total_count': successful + failed + skipped,
            'success_rate': successful / (successful + failed) if (successful + failed) > 0 else 0,
            'errors': errors,
            'stats': self.stats.copy()
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """获取保存统计信息"""
        return self.stats.copy()
    
    def reset_stats(self):
        """重置统计信息"""
        self.stats = {
            'total_processed': 0,
            'successful_saves': 0,
            'failed_saves': 0,
            'skipped_saves': 0
        }
