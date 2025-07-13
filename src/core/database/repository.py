"""
SQLite Repository数据持久化层实现
"""

import hashlib
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy import and_, or_, func, desc
from sqlalchemy.exc import IntegrityError
from loguru import logger

from .base import get_database_session
from .schema import Pin, DownloadTask, ScrapingSession, CacheMetadata


class SQLiteRepository:
    """SQLite数据库Repository实现"""

    def __init__(self, keyword: str = None, output_dir: str = None):
        """初始化Repository

        Args:
            keyword: 搜索关键词，用于关键词特定的数据库路由
            output_dir: 输出目录，用于关键词特定的数据库路由
        """
        self.keyword = keyword
        self.output_dir = output_dir

    def _get_session(self):
        """获取数据库会话

        根据是否提供关键词上下文，路由到对应的数据库管理器：
        - 有关键词：使用关键词特定的数据库管理器
        - 无关键词：回退到全局数据库管理器（向后兼容）

        Returns:
            数据库会话上下文管理器
        """
        if self.keyword and self.output_dir:
            # 使用关键词特定的数据库管理器
            from .manager_factory import DatabaseManagerFactory
            manager = DatabaseManagerFactory.get_manager(self.keyword, self.output_dir)
            return manager.get_session()
        else:
            # 向后兼容：回退到全局管理器
            return get_database_session()
    
    def save_pins_batch(self, pins: List[Dict[str, Any]], query: str, session_id: Optional[str] = None) -> bool:
        """批量保存Pin数据
        
        Args:
            pins: Pin数据列表
            query: 搜索关键词
            session_id: 会话ID（可选）
            
        Returns:
            保存是否成功
        """
        if not pins:
            return True
            
        try:
            with self._get_session() as session:
                saved_count = 0
                
                for pin_data in pins:
                    # 计算Pin哈希值
                    pin_hash = self._calculate_pin_hash(pin_data)
                    pin_id = pin_data.get('id', str(uuid.uuid4()))
                    
                    # 检查Pin是否已存在
                    existing_pin = session.query(Pin).filter_by(pin_hash=pin_hash).first()
                    
                    if existing_pin:
                        # 更新现有Pin
                        self._update_pin_from_dict(existing_pin, pin_data, query)
                        logger.debug(f"更新现有Pin: {pin_id}")
                    else:
                        # 创建新Pin
                        new_pin = self._create_pin_from_dict(pin_data, query, pin_hash)
                        session.add(new_pin)
                        saved_count += 1
                        logger.debug(f"保存新Pin: {pin_id}")
                        
                        # 创建下载任务（如果有图片URL）
                        self._create_download_task(session, new_pin, pin_data)
                
                # 先提交Pin数据
                session.flush()  # 确保Pin数据已写入数据库

                # 更新缓存元数据
                self._update_cache_metadata(session, query)

                # 更新会话信息
                if session_id:
                    self._update_session_stats(session, session_id, len(pins))
                
                logger.info(f"批量保存完成: {saved_count} 个新Pin，查询: {query}")
                return True
                
        except Exception as e:
            logger.error(f"批量保存Pin失败: {e}")
            return False
    
    def load_pins_by_query(self, query: str, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """根据查询关键词加载Pin数据
        
        Args:
            query: 搜索关键词
            limit: 限制数量
            offset: 偏移量
            
        Returns:
            Pin数据列表
        """
        try:
            with self._get_session() as session:
                query_obj = session.query(Pin).filter_by(query=query).order_by(desc(Pin.created_at))
                
                if limit:
                    query_obj = query_obj.limit(limit)
                if offset:
                    query_obj = query_obj.offset(offset)
                
                pins = query_obj.all()
                
                # 转换为字典格式
                result = [pin.to_dict() for pin in pins]
                logger.debug(f"加载Pin数据: {len(result)} 个，查询: {query}")
                return result
                
        except Exception as e:
            logger.error(f"加载Pin数据失败: {e}")
            return []
    
    def get_pin_count_by_query(self, query: str) -> int:
        """获取指定查询的Pin数量
        
        Args:
            query: 搜索关键词
            
        Returns:
            Pin数量
        """
        try:
            with self._get_session() as session:
                count = session.query(func.count(Pin.id)).filter_by(query=query).scalar()
                return count or 0
        except Exception as e:
            logger.error(f"获取Pin数量失败: {e}")
            return 0
    
    def create_scraping_session(self, query: str, target_count: int, output_dir: str, download_images: bool = True) -> str:
        """创建采集会话
        
        Args:
            query: 搜索关键词
            target_count: 目标数量
            output_dir: 输出目录
            download_images: 是否下载图片
            
        Returns:
            会话ID
        """
        session_id = str(uuid.uuid4())
        
        try:
            with self._get_session() as session:
                scraping_session = ScrapingSession(
                    id=session_id,
                    query=query,
                    target_count=target_count,
                    output_dir=output_dir,
                    download_images=download_images,
                    status='running'
                )
                session.add(scraping_session)
                
                logger.debug(f"创建采集会话: {session_id}, 查询: {query}")
                return session_id
                
        except Exception as e:
            logger.error(f"创建采集会话失败: {e}")
            return session_id
    
    def update_session_status(self, session_id: str, status: str, actual_count: Optional[int] = None, stats: Optional[Dict] = None):
        """更新会话状态
        
        Args:
            session_id: 会话ID
            status: 新状态
            actual_count: 实际采集数量
            stats: 统计信息
        """
        try:
            with self._get_session() as session:
                scraping_session = session.query(ScrapingSession).filter_by(id=session_id).first()
                
                if scraping_session:
                    scraping_session.status = status
                    if actual_count is not None:
                        scraping_session.actual_count = actual_count
                    if stats:
                        scraping_session.stats_dict = stats
                    if status in ['completed', 'failed']:
                        scraping_session.completed_at = datetime.utcnow()
                    
                    logger.debug(f"更新会话状态: {session_id} -> {status}")
                    
        except Exception as e:
            logger.error(f"更新会话状态失败: {e}")
    
    def get_pending_download_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取待下载的任务

        Args:
            limit: 限制数量

        Returns:
            下载任务字典列表
        """
        try:
            with self._get_session() as session:
                tasks = session.query(DownloadTask).filter_by(status='pending').limit(limit).all()
                # 转换为字典以避免DetachedInstanceError
                return [
                    {
                        'id': task.id,
                        'pin_id': task.pin_id,
                        'pin_hash': task.pin_hash,
                        'image_url': task.image_url,
                        'status': task.status,
                        'retry_count': task.retry_count
                    }
                    for task in tasks
                ]
        except Exception as e:
            logger.error(f"获取待下载任务失败: {e}")
            return []
    
    def update_download_task_status(self, task_id: int, status: str, local_path: Optional[str] = None, 
                                  error_message: Optional[str] = None, file_size: Optional[int] = None):
        """更新下载任务状态
        
        Args:
            task_id: 任务ID
            status: 新状态
            local_path: 本地路径
            error_message: 错误信息
            file_size: 文件大小
        """
        try:
            with self._get_session() as session:
                task = session.query(DownloadTask).filter_by(id=task_id).first()
                
                if task:
                    task.status = status
                    if local_path:
                        task.local_path = local_path
                    if error_message:
                        task.error_message = error_message
                    if file_size:
                        task.file_size = file_size
                    if status == 'failed':
                        task.retry_count += 1
                    
                    logger.debug(f"更新下载任务状态: {task_id} -> {status}")
                    
        except Exception as e:
            logger.error(f"更新下载任务状态失败: {e}")

    def get_download_task_by_pin_and_url(self, pin_id: str, image_url: str) -> Optional[Dict]:
        """根据Pin ID和图片URL获取下载任务

        Args:
            pin_id: Pin ID
            image_url: 图片URL

        Returns:
            任务信息字典，如果不存在则返回None
        """
        try:
            with self._get_session() as session:
                task = session.query(DownloadTask).filter_by(
                    pin_id=pin_id,
                    image_url=image_url
                ).first()

                if task:
                    return {
                        'id': task.id,
                        'pin_id': task.pin_id,
                        'pin_hash': task.pin_hash,
                        'image_url': task.image_url,
                        'status': task.status,
                        'retry_count': task.retry_count,
                        'local_path': task.local_path,
                        'file_size': task.file_size
                    }
                return None
        except Exception as e:
            logger.error(f"获取下载任务失败: {e}")
            return None

    def create_download_task(self, pin_id: str, image_url: str) -> str:
        """创建新的下载任务

        Args:
            pin_id: Pin ID
            image_url: 图片URL

        Returns:
            任务ID
        """
        try:
            with self._get_session() as session:
                task = DownloadTask(
                    pin_id=pin_id,
                    pin_hash=hashlib.md5(pin_id.encode()).hexdigest(),
                    image_url=image_url,
                    status='pending',
                    retry_count=0
                )
                session.add(task)
                session.flush()  # 获取ID
                task_id = task.id
                logger.debug(f"创建下载任务: {pin_id} -> {task_id}")
                return task_id
        except Exception as e:
            logger.error(f"创建下载任务失败: {e}")
            return None

    def _calculate_pin_hash(self, pin_data: Dict[str, Any]) -> str:
        """计算Pin的哈希值"""
        pin_id = pin_data.get("id", "")
        image_url = pin_data.get("largest_image_url", "")
        hash_input = f"{pin_id}:{image_url}"
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def _create_pin_from_dict(self, pin_data: Dict[str, Any], query: str, pin_hash: str) -> Pin:
        """从字典创建Pin对象"""
        creator = pin_data.get('creator', {})
        board = pin_data.get('board', {})
        
        pin = Pin(
            id=pin_data.get('id', str(uuid.uuid4())),
            pin_hash=pin_hash,
            query=query,
            title=pin_data.get('title'),
            description=pin_data.get('description'),
            creator_name=creator.get('name') if creator else None,
            creator_id=creator.get('id') if creator else None,
            board_name=board.get('name') if board else None,
            board_id=board.get('id') if board else None,
            largest_image_url=pin_data.get('largest_image_url')
        )

        # 使用属性设置器来正确处理JSON数据
        pin.raw_data_dict = pin_data
        
        # 设置图片URLs和统计信息
        if 'image_urls' in pin_data:
            pin.image_urls_dict = pin_data['image_urls']
        if 'stats' in pin_data:
            pin.stats_dict = pin_data['stats']
            
        return pin
    
    def _update_pin_from_dict(self, pin: Pin, pin_data: Dict[str, Any], query: str):
        """从字典更新Pin对象"""
        pin.updated_at = datetime.utcnow()
        pin.raw_data_dict = pin_data  # 更新原始数据
        
        # 更新其他字段（如果有新数据）
        if 'title' in pin_data and pin_data['title']:
            pin.title = pin_data['title']
        if 'description' in pin_data and pin_data['description']:
            pin.description = pin_data['description']
    
    def _create_download_task(self, session, pin: Pin, pin_data: Dict[str, Any]):
        """创建下载任务"""
        image_url = pin_data.get('largest_image_url') or pin_data.get('image_urls', {}).get('original')
        
        if image_url:
            download_task = DownloadTask(
                pin_id=pin.id,
                pin_hash=pin.pin_hash,
                image_url=image_url,
                status='pending'
            )
            session.add(download_task)
    
    def _update_cache_metadata(self, session, query: str):
        """更新缓存元数据"""
        try:
            # 先尝试获取现有记录
            cache_meta = session.query(CacheMetadata).filter_by(query=query).first()

            # 计算当前Pin数量
            pin_count = session.query(func.count(Pin.id)).filter_by(query=query).scalar() or 0

            if cache_meta:
                # 更新现有记录
                cache_meta.pin_count = pin_count
                cache_meta.last_updated = datetime.utcnow()
            else:
                # 创建新记录，使用merge来处理并发插入
                cache_meta = CacheMetadata(
                    query=query,
                    pin_count=pin_count
                )
                session.merge(cache_meta)  # 使用merge而不是add来处理并发
        except Exception as e:
            # 忽略缓存元数据更新错误，不影响主要功能
            logger.warning(f"更新缓存元数据失败: {e}")
            pass
    
    def _update_session_stats(self, session, session_id: str, new_pins_count: int):
        """更新会话统计信息"""
        scraping_session = session.query(ScrapingSession).filter_by(id=session_id).first()
        
        if scraping_session:
            scraping_session.actual_count = (scraping_session.actual_count or 0) + new_pins_count
