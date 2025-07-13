"""
SQLAlchemy数据库模型定义
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional

from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property

from .base import Base


class Pin(Base):
    """Pin数据表模型"""
    
    __tablename__ = 'pins'
    
    # 主键和唯一标识
    id = Column(String(50), primary_key=True, comment='Pin的唯一ID')
    pin_hash = Column(String(32), unique=True, nullable=False, index=True, comment='Pin的MD5哈希值，用于去重')
    
    # 查询信息
    query = Column(String(200), nullable=False, index=True, comment='搜索关键词')
    
    # Pin基本信息
    title = Column(Text, comment='Pin标题')
    description = Column(Text, comment='Pin描述')
    
    # 创作者信息
    creator_name = Column(String(100), comment='创作者名称')
    creator_id = Column(String(50), comment='创作者ID')
    
    # 板块信息
    board_name = Column(String(200), comment='板块名称')
    board_id = Column(String(50), comment='板块ID')
    
    # 图片信息
    image_urls = Column(Text, comment='图片URLs的JSON字符串')
    largest_image_url = Column(Text, comment='最大尺寸图片URL')
    
    # 统计信息
    stats = Column(Text, comment='统计信息的JSON字符串（saves, comments等）')
    
    # 原始数据
    raw_data = Column(Text, comment='完整的Pin数据JSON字符串')
    
    # 时间戳
    created_at = Column(DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 关联关系
    download_tasks = relationship("DownloadTask", back_populates="pin", cascade="all, delete-orphan")
    
    # 索引定义
    __table_args__ = (
        Index('idx_pin_query_created', 'query', 'created_at'),
        Index('idx_pin_creator', 'creator_id'),
        Index('idx_pin_board', 'board_id'),
    )
    
    @hybrid_property
    def image_urls_dict(self) -> Dict[str, str]:
        """获取图片URLs字典"""
        if self.image_urls:
            try:
                return json.loads(self.image_urls)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}
    
    @image_urls_dict.setter
    def image_urls_dict(self, value: Dict[str, str]):
        """设置图片URLs字典"""
        self.image_urls = json.dumps(value) if value else None
    
    @hybrid_property
    def stats_dict(self) -> Dict[str, Any]:
        """获取统计信息字典"""
        if self.stats:
            try:
                return json.loads(self.stats)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}
    
    @stats_dict.setter
    def stats_dict(self, value: Dict[str, Any]):
        """设置统计信息字典"""
        self.stats = json.dumps(value) if value else None
    
    @hybrid_property
    def raw_data_dict(self) -> Dict[str, Any]:
        """获取原始数据字典"""
        if self.raw_data:
            try:
                return json.loads(self.raw_data)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}
    
    @raw_data_dict.setter
    def raw_data_dict(self, value: Dict[str, Any]):
        """设置原始数据字典"""
        self.raw_data = json.dumps(value) if value else None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，兼容原有的Pin数据格式"""
        # 优先使用原始数据
        if self.raw_data:
            try:
                data = json.loads(self.raw_data)
                # 确保包含下载状态信息
                data['downloaded'] = any(task.status == 'completed' for task in self.download_tasks)
                if data['downloaded']:
                    completed_task = next((task for task in self.download_tasks if task.status == 'completed'), None)
                    if completed_task:
                        data['download_path'] = completed_task.local_path
                return data
            except (json.JSONDecodeError, TypeError):
                pass
        
        # 构建基础数据结构
        return {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'creator': {
                'name': self.creator_name,
                'id': self.creator_id
            } if self.creator_name or self.creator_id else {},
            'board': {
                'name': self.board_name,
                'id': self.board_id
            } if self.board_name or self.board_id else {},
            'image_urls': self.image_urls_dict,
            'largest_image_url': self.largest_image_url,
            'stats': self.stats_dict,
            'downloaded': any(task.status == 'completed' for task in self.download_tasks),
            'download_path': next((task.local_path for task in self.download_tasks if task.status == 'completed'), None)
        }
    
    def __repr__(self):
        return f"<Pin(id='{self.id}', title='{self.title[:50]}...', query='{self.query}')>"


class DownloadTask(Base):
    """图片下载任务表模型"""
    
    __tablename__ = 'download_tasks'
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True, comment='下载任务ID')
    
    # 关联的Pin
    pin_id = Column(String(50), ForeignKey('pins.id'), nullable=False, index=True, comment='关联的Pin ID')
    pin_hash = Column(String(32), nullable=False, index=True, comment='Pin哈希值，用于快速查找')
    
    # 下载信息
    image_url = Column(Text, nullable=False, comment='图片URL')
    local_path = Column(Text, comment='本地保存路径')
    
    # 任务状态
    status = Column(String(20), default='pending', index=True, comment='下载状态：pending/downloading/completed/failed')
    retry_count = Column(Integer, default=0, comment='重试次数')
    error_message = Column(Text, comment='错误信息')
    
    # 文件信息
    file_size = Column(Integer, comment='文件大小（字节）')
    
    # 时间戳
    created_at = Column(DateTime, default=datetime.utcnow, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='更新时间')
    
    # 关联关系
    pin = relationship("Pin", back_populates="download_tasks")
    
    # 索引定义
    __table_args__ = (
        Index('idx_download_status_created', 'status', 'created_at'),
        Index('idx_download_pin_status', 'pin_id', 'status'),
    )
    
    def __repr__(self):
        return f"<DownloadTask(id={self.id}, pin_id='{self.pin_id}', status='{self.status}')>"


class ScrapingSession(Base):
    """采集会话表模型"""
    
    __tablename__ = 'scraping_sessions'
    
    # 主键
    id = Column(String(50), primary_key=True, comment='会话ID')
    
    # 采集参数
    query = Column(String(200), nullable=False, index=True, comment='搜索关键词')
    target_count = Column(Integer, comment='目标采集数量')
    actual_count = Column(Integer, default=0, comment='实际采集数量')
    
    # 会话状态
    status = Column(String(20), default='running', index=True, comment='会话状态：running/completed/failed')
    
    # 配置信息
    output_dir = Column(Text, comment='输出目录')
    download_images = Column(Boolean, default=True, comment='是否下载图片')
    
    # 统计信息
    stats = Column(Text, comment='会话统计信息的JSON字符串')
    
    # 时间戳
    started_at = Column(DateTime, default=datetime.utcnow, comment='开始时间')
    completed_at = Column(DateTime, comment='完成时间')
    
    # 索引定义
    __table_args__ = (
        Index('idx_session_query_started', 'query', 'started_at'),
        Index('idx_session_status', 'status'),
    )
    
    @hybrid_property
    def stats_dict(self) -> Dict[str, Any]:
        """获取统计信息字典"""
        if self.stats:
            try:
                return json.loads(self.stats)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}
    
    @stats_dict.setter
    def stats_dict(self, value: Dict[str, Any]):
        """设置统计信息字典"""
        self.stats = json.dumps(value) if value else None
    
    def __repr__(self):
        return f"<ScrapingSession(id='{self.id}', query='{self.query}', status='{self.status}')>"


class CacheMetadata(Base):
    """缓存元数据表模型"""
    
    __tablename__ = 'cache_metadata'
    
    # 主键
    query = Column(String(200), primary_key=True, comment='搜索关键词')
    
    # 缓存信息
    pin_count = Column(Integer, default=0, comment='缓存的Pin数量')
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment='最后更新时间')
    cache_version = Column(String(10), default='1.0', comment='缓存版本')
    
    def __repr__(self):
        return f"<CacheMetadata(query='{self.query}', pin_count={self.pin_count})>"
