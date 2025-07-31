"""
SQLite Repository数据持久化层实现
"""

import hashlib
import uuid
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy import and_, or_, func, desc, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.sqlite import insert
from loguru import logger

from .base import get_database_session
from .schema import Pin, DownloadTask, ScrapingSession, CacheMetadata
from .atomic_saver import AtomicPinSaver
from .normalizer import PinDataNormalizer


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
        # 初始化原子化保存器
        self.atomic_saver = AtomicPinSaver(self._get_session)

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
            try:
                manager = DatabaseManagerFactory.get_manager(self.keyword, self.output_dir)
                return manager.get_session()
            except Exception as e:
                logger.error(f"获取关键词特定数据库管理器失败: {e}")
                raise RuntimeError(f"数据库管理器初始化失败: {e}")
        else:
            # 向后兼容：回退到全局管理器
            try:
                return get_database_session()
            except RuntimeError as e:
                logger.error(f"全局数据库管理器未初始化: {e}")
                # 尝试自动初始化默认数据库
                from .base import initialize_database
                import tempfile
                import os
                default_db_path = os.path.join(tempfile.gettempdir(), "pinterest_default.db")
                logger.warning(f"尝试初始化默认数据库: {default_db_path}")
                initialize_database(default_db_path)
                return get_database_session()
    
    def save_pins_batch(self, pins: List[Dict[str, Any]], query: str, session_id: Optional[str] = None, force_overwrite: bool = False) -> bool:
        """批量保存Pin数据 - 使用原子化保存策略

        Args:
            pins: Pin数据列表
            query: 搜索关键词
            session_id: 会话ID（可选）
            force_overwrite: 是否强制覆盖现有数据（已废弃，始终以最新数据为准）

        Returns:
            保存是否成功
        """
        if not pins:
            return True

        # 使用原子化保存器进行批量保存
        result = self.atomic_saver.save_pins_batch_atomic(pins, query, session_id)

        # 更新元数据（如果有成功保存的Pin）
        if result['successful_count'] > 0:
            try:
                with self._get_session() as session:
                    self._update_cache_metadata(session, query)
                    if session_id:
                        self._update_session_stats(session, session_id, result['successful_count'])
            except Exception as e:
                logger.warning(f"更新元数据失败: {e}")

        # 记录保存结果
        logger.info(f"批量原子化保存完成: 成功{result['successful_count']}, 失败{result['failed_count']}, 跳过{result['skipped_count']}, 查询: {query}")

        # 如果有错误，记录详细信息
        if result['errors']:
            for error in result['errors'][:5]:  # 只记录前5个错误
                logger.error(f"保存Pin失败: {error['pin_id']}, 错误: {error['error']}")

        # 只要有成功保存的Pin就返回True
        return result['successful_count'] > 0

    def save_pins_and_get_new_ids(self, pins: List[Dict[str, Any]], query: str, session_id: Optional[str] = None) -> List[str]:
        """保存Pin数据并返回新增的Pin ID列表

        Args:
            pins: Pin数据列表
            query: 搜索关键词
            session_id: 会话ID

        Returns:
            新增的Pin ID列表
        """
        if not pins:
            return []

        new_pin_ids = []

        try:
            with self._get_session() as session:
                for pin_data in pins:
                    pin_id = pin_data.get('id')
                    if not pin_id:
                        continue

                    # 检查Pin是否已存在
                    existing_pin = session.query(Pin).filter(Pin.id == pin_id).first()
                    if existing_pin:
                        continue  # 跳过已存在的Pin

                    # 标准化Pin数据
                    normalized_data = PinDataNormalizer.normalize(pin_data, query)
                    if not normalized_data:
                        continue

                    # 创建新Pin记录
                    pin_hash = hashlib.md5(json.dumps(normalized_data, sort_keys=True).encode()).hexdigest()

                    new_pin = Pin(
                        id=pin_id,
                        hash=pin_hash,
                        query=query,
                        title=normalized_data.get('title', ''),
                        description=normalized_data.get('description', ''),
                        image_url=normalized_data.get('image_url', ''),
                        largest_image_url=normalized_data.get('largest_image_url', ''),
                        pin_url=normalized_data.get('pin_url', ''),
                        board_name=normalized_data.get('board_name', ''),
                        user_name=normalized_data.get('user_name', ''),
                        save_count=normalized_data.get('save_count', 0),
                        stats=json.dumps(normalized_data.get('stats', {})),
                        raw_data=json.dumps(pin_data),
                        session_id=session_id,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )

                    session.add(new_pin)
                    new_pin_ids.append(pin_id)

                    # 处理下载任务
                    self._upsert_download_task(session, pin_id, pin_hash, normalized_data)

                session.commit()

        except Exception as e:
            logger.error(f"保存Pin并获取新ID失败: {e}")

        return new_pin_ids

    def save_pin_immediately(self, pin_data: Dict[str, Any], query: str, session_id: Optional[str] = None) -> bool:
        """立即保存单个Pin到数据库（原子化）

        Args:
            pin_data: Pin数据字典
            query: 搜索关键词
            session_id: 会话ID（可选）

        Returns:
            保存是否成功
        """
        if not pin_data:
            return True

        # 使用原子化保存器直接保存单个Pin
        success, error_msg = self.atomic_saver.save_pin_atomic(pin_data, query, session_id)

        if success:
            logger.debug(f"立即原子化保存Pin成功: {pin_data.get('id')}")

            # 更新元数据
            try:
                with self._get_session() as session:
                    self._update_cache_metadata(session, query)
                    if session_id:
                        self._update_session_stats(session, session_id, 1)
            except Exception as e:
                logger.warning(f"更新元数据失败: {e}")
        else:
            logger.error(f"立即原子化保存Pin失败: {pin_data.get('id')}, 错误: {error_msg}")

        return success



    def _upsert_pin(self, session, pin_data: Dict[str, Any], query: str, pin_hash: str, pin_id: str) -> bool:
        """原子性UPSERT操作，避免并发冲突

        Args:
            session: 数据库会话
            pin_data: Pin数据字典
            query: 搜索关键词
            pin_hash: Pin哈希值
            pin_id: Pin ID

        Returns:
            操作是否成功
        """
        try:
            # 准备数据
            creator = pin_data.get('creator', {})
            board = pin_data.get('board', {})

            pin_dict = {
                'id': pin_id,
                'pin_hash': pin_hash,
                'query': query,
                'title': pin_data.get('title'),
                'description': pin_data.get('description'),
                'creator_name': creator.get('name') if creator else None,
                'creator_id': creator.get('id') if creator else None,
                'board_name': board.get('name') if board else None,
                'board_id': board.get('id') if board else None,
                'largest_image_url': pin_data.get('largest_image_url'),
                'image_urls': json.dumps(pin_data.get('image_urls', {})) if pin_data.get('image_urls') else None,
                'stats': json.dumps(pin_data.get('stats', {})) if pin_data.get('stats') else None,
                'raw_data': json.dumps(pin_data),
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }

            # SQLite UPSERT语法
            stmt = insert(Pin).values(**pin_dict)

            # 在pin_hash冲突时更新（以最新数据为准）
            stmt = stmt.on_conflict_do_update(
                index_elements=['pin_hash'],
                set_={
                    'title': stmt.excluded.title,
                    'description': stmt.excluded.description,
                    'creator_name': stmt.excluded.creator_name,
                    'creator_id': stmt.excluded.creator_id,
                    'board_name': stmt.excluded.board_name,
                    'board_id': stmt.excluded.board_id,
                    'largest_image_url': stmt.excluded.largest_image_url,
                    'image_urls': stmt.excluded.image_urls,
                    'stats': stmt.excluded.stats,
                    'raw_data': stmt.excluded.raw_data,
                    'updated_at': datetime.utcnow()
                }
            )

            session.execute(stmt)

            # 处理下载任务
            self._upsert_download_task(session, pin_id, pin_hash, pin_data)

            logger.debug(f"UPSERT Pin成功: {pin_id}")
            return True

        except Exception as e:
            logger.error(f"UPSERT Pin失败: {pin_id}, 错误: {e}")
            return False

    def _upsert_download_task(self, session, pin_id: str, pin_hash: str, pin_data: Dict[str, Any]):
        """UPSERT下载任务，避免重复创建 - 使用INSERT OR REPLACE策略

        Args:
            session: 数据库会话
            pin_id: Pin ID
            pin_hash: Pin哈希值
            pin_data: Pin数据字典
        """
        image_url = pin_data.get('largest_image_url') or pin_data.get('image_urls', {}).get('original')

        if not image_url:
            return

        try:
            # 使用INSERT OR REPLACE避免SQLAlchemy UPSERT复杂性
            sql = text("""
                INSERT OR REPLACE INTO download_tasks (
                    pin_id, pin_hash, image_url, status, retry_count,
                    created_at, updated_at
                ) VALUES (
                    :pin_id, :pin_hash, :image_url, 'pending', 0,
                    :created_at, :updated_at
                )
            """)

            task_data = {
                'pin_id': pin_id,
                'pin_hash': pin_hash,
                'image_url': image_url,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }

            session.execute(sql, task_data)
            logger.debug(f"UPSERT DownloadTask成功: {pin_id}")

        except Exception as e:
            logger.error(f"UPSERT DownloadTask失败: {pin_id}, 错误: {e}")

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

    def load_pins_with_images(self, query: str, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """根据查询关键词加载有图片链接的Pin数据（按创建时间倒序）

        Args:
            query: 搜索关键词
            limit: 限制数量
            offset: 偏移量

        Returns:
            有图片链接的Pin数据列表（从新到旧排序）
        """
        try:
            with self._get_session() as session:
                # 查询条件：largest_image_url不为空 OR image_urls不为空
                query_obj = session.query(Pin).filter(
                    and_(
                        Pin.query == query,
                        or_(
                            and_(Pin.largest_image_url.isnot(None), Pin.largest_image_url != ''),
                            and_(Pin.image_urls.isnot(None), Pin.image_urls != '', Pin.image_urls != '[]')
                        )
                    )
                ).order_by(desc(Pin.created_at))

                if offset:
                    query_obj = query_obj.offset(offset)
                if limit:
                    query_obj = query_obj.limit(limit)

                pins = query_obj.all()

                # 转换为字典格式
                result = [pin.to_dict() for pin in pins]
                logger.debug(f"加载有图片Pin数据: {len(result)} 个，查询: {query}, offset: {offset}")
                return result

        except Exception as e:
            logger.error(f"加载有图片Pin数据失败: {e}")
            return []

    def save_pins_and_get_new_ids(self, pins: List[Dict[str, Any]], query: str, session_id: str) -> List[str]:
        """批量保存pins并返回真正新增的pin id列表 - 使用标准化流程

        Args:
            pins: Pin数据列表
            query: 搜索关键词
            session_id: 会话ID

        Returns:
            真正新增到数据库的pin id列表
        """
        if not pins:
            return []

        new_pin_ids = []

        try:
            # 使用标准化流程批量处理
            for pin_data in pins:
                pin_id = pin_data.get('id')
                if not pin_id:
                    continue

                # 使用标准化器生成一致的hash
                try:
                    normalized_data = PinDataNormalizer.normalize_pin_data(pin_data, query)
                    pin_hash = normalized_data['pin_hash']

                    # 检查是否已存在（基于标准化hash）
                    if self._pin_exists_by_hash(pin_hash):
                        logger.debug(f"Pin已存在，跳过: {pin_id}")
                        continue

                    # 使用原子化保存器保存
                    success, error_msg = self.atomic_saver.save_pin_atomic(pin_data, query, session_id)

                    if success:
                        new_pin_ids.append(pin_id)
                        logger.debug(f"新增Pin保存成功: {pin_id}")
                    else:
                        logger.warning(f"Pin保存失败: {pin_id}, 错误: {error_msg}")

                except Exception as e:
                    logger.error(f"处理Pin失败: {pin_id}, 错误: {e}")
                    continue

            logger.debug(f"批量保存Pin完成: {len(new_pin_ids)} 个新增，查询: {query}")
            return new_pin_ids

        except Exception as e:
            logger.error(f"批量保存Pin失败: {e}")
            return []

    def _pin_exists_by_hash(self, pin_hash: str) -> bool:
        """检查Pin是否已存在（基于hash）

        Args:
            pin_hash: Pin的hash值

        Returns:
            是否已存在
        """
        try:
            with self._get_session() as session:
                existing_pin = session.query(Pin).filter_by(pin_hash=pin_hash).first()
                return existing_pin is not None
        except Exception as e:
            logger.error(f"检查Pin存在性失败: {e}")
            return False

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

    def create_pin_expansion_session(
        self,
        start_pin_id: str,
        target_count: int,
        output_dir: str,
        download_images: bool = True
    ) -> str:
        """创建Pin扩展会话

        Args:
            start_pin_id: 起始Pin ID
            target_count: 目标采集数量
            output_dir: 输出目录
            download_images: 是否下载图片

        Returns:
            会话ID
        """
        session_id = str(uuid.uuid4())
        query = f"pin_expansion_{start_pin_id}"

        try:
            with self._get_session() as session:
                new_session = ScrapingSession(
                    id=session_id,
                    query=query,
                    target_count=target_count,
                    status='running',
                    output_dir=output_dir,
                    download_images=download_images
                    # started_at has default=datetime.utcnow, no need to set explicitly
                )
                session.add(new_session)
                session.commit()

            logger.info(f"创建Pin扩展会话: {session_id} (起始Pin: {start_pin_id})")

        except Exception as e:
            logger.error(f"创建Pin扩展会话失败: {e}")

        return session_id

    def get_incomplete_pin_expansion_sessions(self, start_pin_id: str) -> List[Dict]:
        """获取未完成的Pin扩展会话

        Args:
            start_pin_id: 起始Pin ID

        Returns:
            未完成会话列表
        """
        query = f"pin_expansion_{start_pin_id}"

        try:
            with self._get_session() as session:
                incomplete_sessions = session.query(ScrapingSession).filter(
                    and_(
                        ScrapingSession.query == query,
                        ScrapingSession.status.in_(['running', 'interrupted'])
                    )
                ).order_by(desc(ScrapingSession.started_at)).all()

                return [
                    {
                        'id': s.id,
                        'query': s.query,
                        'target_count': s.target_count,
                        'status': s.status,
                        'started_at': s.started_at
                    }
                    for s in incomplete_sessions
                ]
        except Exception as e:
            logger.error(f"获取Pin扩展会话失败: {e}")
            return []

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
                    if status in ['completed', 'failed', 'interrupted']:
                        scraping_session.completed_at = datetime.utcnow()

                    logger.debug(f"更新会话状态: {session_id} -> {status}")
                    
        except Exception as e:
            logger.error(f"更新会话状态失败: {e}")

    def get_incomplete_sessions(self, query: str) -> List[Dict[str, Any]]:
        """获取未完成的会话

        Args:
            query: 搜索关键词

        Returns:
            未完成的会话字典列表
        """
        try:
            with self._get_session() as session:
                incomplete_sessions = session.query(ScrapingSession).filter(
                    ScrapingSession.query == query,
                    ScrapingSession.status.in_(['running', 'interrupted'])
                ).order_by(desc(ScrapingSession.started_at)).all()

                # 转换为字典格式，避免会话绑定问题
                result = []
                for sess in incomplete_sessions:
                    result.append({
                        'id': sess.id,
                        'query': sess.query,
                        'target_count': sess.target_count,
                        'actual_count': sess.actual_count,
                        'status': sess.status,
                        'output_dir': sess.output_dir,
                        'download_images': sess.download_images,
                        'started_at': sess.started_at,
                        'completed_at': sess.completed_at
                    })

                logger.debug(f"查询到 {len(result)} 个未完成会话: {query}")
                return result

        except Exception as e:
            logger.error(f"查询未完成会话失败: {e}")
            return []

    def update_session_progress(self, session_id: str, current_count: int):
        """实时更新会话进度

        Args:
            session_id: 会话ID
            current_count: 当前采集数量
        """
        try:
            with self._get_session() as session:
                scraping_session = session.query(ScrapingSession).filter_by(id=session_id).first()

                if scraping_session:
                    scraping_session.actual_count = current_count
                    logger.debug(f"更新会话进度: {session_id} -> {current_count}")

        except Exception as e:
            logger.error(f"更新会话进度失败: {e}")

    def resume_session(self, session_id: str) -> bool:
        """恢复中断的会话

        Args:
            session_id: 会话ID

        Returns:
            恢复是否成功
        """
        try:
            with self._get_session() as session:
                scraping_session = session.query(ScrapingSession).filter_by(id=session_id).first()

                if scraping_session and scraping_session.status in ['interrupted', 'running']:
                    scraping_session.status = 'running'
                    scraping_session.completed_at = None  # 清除完成时间
                    logger.info(f"恢复会话: {session_id}")
                    return True
                else:
                    logger.warning(f"无法恢复会话: {session_id}")
                    return False

        except Exception as e:
            logger.error(f"恢复会话失败: {e}")
            return False

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
    
    def _force_overwrite_pin(self, session, existing_pin: Pin, pin_data: Dict[str, Any], query: str, pin_hash: str):
        """强制覆盖现有Pin数据

        Args:
            session: 数据库会话
            existing_pin: 现有Pin对象
            pin_data: 新Pin数据
            query: 搜索关键词
            pin_hash: Pin哈希值
        """
        try:
            pin_id = existing_pin.id
            logger.debug(f"开始强制覆盖Pin: {pin_id}")

            # 1. 删除现有的下载任务
            existing_tasks = session.query(DownloadTask).filter_by(pin_id=pin_id).all()
            for task in existing_tasks:
                session.delete(task)
            logger.debug(f"删除了 {len(existing_tasks)} 个关联下载任务")

            # 2. 删除现有Pin
            session.delete(existing_pin)
            session.flush()  # 确保删除操作完成

            # 3. 创建新Pin（使用相同的ID和hash）
            new_pin = self._create_pin_from_dict(pin_data, query, pin_hash)
            # 保持原有的ID以维持引用一致性
            new_pin.id = pin_id
            session.add(new_pin)
            session.flush()  # 确保新Pin已创建

            # 4. 创建新的下载任务
            self._create_download_task(session, new_pin, pin_data)

            logger.info(f"强制覆盖完成: {pin_id}")

        except Exception as e:
            logger.error(f"强制覆盖Pin失败: {pin_id}, 错误: {e}")
            raise

    def _update_pin_from_dict(self, pin: Pin, pin_data: Dict[str, Any], query: str, force_overwrite: bool = False):
        """从字典更新Pin对象

        Args:
            pin: Pin对象
            pin_data: Pin数据字典
            query: 搜索关键词
            force_overwrite: 是否强制覆盖所有字段
        """
        pin.updated_at = datetime.utcnow()

        if force_overwrite:
            # 强制覆盖所有字段
            logger.debug(f"强制覆盖Pin所有字段: {pin.id}")
            creator = pin_data.get('creator', {})
            board = pin_data.get('board', {})

            pin.title = pin_data.get('title')
            pin.description = pin_data.get('description')
            pin.creator_name = creator.get('name') if creator else None
            pin.creator_id = creator.get('id') if creator else None
            pin.board_name = board.get('name') if board else None
            pin.board_id = board.get('id') if board else None
            pin.largest_image_url = pin_data.get('largest_image_url')

            # 强制更新JSON字段
            pin.raw_data_dict = pin_data
            if 'image_urls' in pin_data:
                pin.image_urls_dict = pin_data['image_urls']
            if 'stats' in pin_data:
                pin.stats_dict = pin_data['stats']
        else:
            # 选择性更新（原有逻辑）
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


