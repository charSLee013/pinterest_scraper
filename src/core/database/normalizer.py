"""
Pin数据标准化器模块

提供Pin数据的清理、标准化和验证功能，确保数据库保存的原子性和一致性。
采用鲁棒性设计：核心字段严格验证，可选字段宽松处理。
"""

import json
import hashlib
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from loguru import logger


class PinDataNormalizer:
    """Pin数据标准化器
    
    负责清理和标准化Pin数据，确保数据库保存的一致性。
    采用"核心字段严格，可选字段宽松"的策略。
    """
    
    # 核心必需字段 - 这些字段必须存在且有效
    CORE_REQUIRED_FIELDS = {'id', 'query'}
    
    # 核心可选字段 - 这些字段重要但可以为空
    CORE_OPTIONAL_FIELDS = {'title', 'description', 'largest_image_url', 'image_urls'}
    
    # 扩展可选字段 - 这些字段可以完全缺失
    EXTENDED_OPTIONAL_FIELDS = {'creator', 'board', 'stats', 'url', 'created_at', 'source_link', 'categories'}
    
    @classmethod
    def normalize_pin_data(cls, pin_data: Dict[str, Any], query: str) -> Dict[str, Any]:
        """标准化Pin数据
        
        Args:
            pin_data: 原始Pin数据
            query: 搜索关键词
            
        Returns:
            标准化后的Pin数据
            
        Raises:
            ValueError: 核心字段验证失败时抛出
        """
        if not isinstance(pin_data, dict):
            raise ValueError("Pin数据必须是字典格式")
        
        # 创建标准化结果
        normalized = {}
        
        # 1. 处理核心必需字段
        cls._process_core_required_fields(normalized, pin_data, query)
        
        # 2. 处理核心可选字段
        cls._process_core_optional_fields(normalized, pin_data)
        
        # 3. 处理扩展可选字段
        cls._process_extended_optional_fields(normalized, pin_data)
        
        # 4. 生成Pin哈希值
        normalized['pin_hash'] = cls._calculate_pin_hash(normalized)
        
        # 5. 添加时间戳
        normalized['created_at'] = datetime.utcnow()
        normalized['updated_at'] = datetime.utcnow()
        
        logger.debug(f"Pin数据标准化完成: {normalized.get('id')}")
        return normalized
    
    @classmethod
    def _process_core_required_fields(cls, normalized: Dict, pin_data: Dict, query: str):
        """处理核心必需字段"""
        # Pin ID - 必须存在
        pin_id = pin_data.get('id')
        if not pin_id:
            pin_id = str(uuid.uuid4())
            logger.warning("Pin缺少ID，自动生成UUID")
        normalized['id'] = str(pin_id)
        
        # 查询关键词 - 必须存在
        if not query:
            raise ValueError("查询关键词不能为空")
        normalized['query'] = str(query)
    
    @classmethod
    def _process_core_optional_fields(cls, normalized: Dict, pin_data: Dict):
        """处理核心可选字段"""
        # 标题
        title = pin_data.get('title')
        normalized['title'] = str(title) if title else None

        # 描述
        description = pin_data.get('description')
        normalized['description'] = str(description) if description else None

        # 图片URL处理 - 增强版
        cls._process_image_urls(normalized, pin_data)

    @classmethod
    def _process_image_urls(cls, normalized: Dict, pin_data: Dict):
        """处理图片URL字段 - 增强版

        策略:
        1. 优先使用已有的largest_image_url和image_urls字段
        2. 如果没有，尝试从raw_data中提取
        """
        # 策略1: 使用已有字段
        largest_image_url = pin_data.get('largest_image_url')
        image_urls = pin_data.get('image_urls', {})

        # 策略2: 如果没有图片URL，尝试从其他字段提取
        if not largest_image_url and not image_urls:
            extracted_largest, extracted_urls = cls._extract_image_urls_from_pin_data(pin_data)
            if extracted_largest:
                largest_image_url = extracted_largest
            if extracted_urls:
                image_urls = extracted_urls

        # 设置normalized字段
        normalized['largest_image_url'] = str(largest_image_url) if largest_image_url else None

        # 图片URLs - 处理为JSON字符串
        if isinstance(image_urls, dict) and image_urls:
            normalized['image_urls'] = json.dumps(image_urls)
        elif isinstance(image_urls, str):
            # 如果已经是JSON字符串，直接使用
            normalized['image_urls'] = image_urls
        else:
            normalized['image_urls'] = None

    @classmethod
    def _extract_image_urls_from_pin_data(cls, pin_data: Dict) -> Tuple[Optional[str], Optional[Dict]]:
        """从Pin数据中提取图片URL

        Args:
            pin_data: Pin数据字典

        Returns:
            (largest_image_url, image_urls_dict)
        """
        try:
            # 尝试从raw_data中提取
            raw_data = pin_data.get('raw_data')
            if not raw_data:
                return None, None

            # 如果raw_data是字符串，解析为字典
            if isinstance(raw_data, str):
                try:
                    raw_data = json.loads(raw_data)
                except json.JSONDecodeError:
                    return None, None

            if not isinstance(raw_data, dict):
                return None, None

            largest_url = None
            image_urls = {}

            # 从images字段提取
            if 'images' in raw_data and isinstance(raw_data['images'], dict):
                images = raw_data['images']

                # 优先使用orig（原图）
                if 'orig' in images and isinstance(images['orig'], dict) and 'url' in images['orig']:
                    largest_url = images['orig']['url']

                # 收集所有尺寸
                for size, img_data in images.items():
                    if isinstance(img_data, dict) and 'url' in img_data:
                        image_urls[size] = img_data['url']

            # 如果没有找到orig，尝试其他字段
            if not largest_url and image_urls:
                # 使用最大尺寸作为largest_url
                largest_size = max(image_urls.items(), key=lambda x: len(x[1]))
                largest_url = largest_size[1]

            # 如果还是没有找到，尝试image字段
            if not largest_url and 'image' in raw_data:
                image_data = raw_data['image']
                if isinstance(image_data, dict) and 'url' in image_data:
                    largest_url = image_data['url']
                    image_urls['image'] = image_data['url']

            return largest_url, image_urls if image_urls else None

        except Exception as e:
            logger.debug(f"从Pin数据提取图片URL失败: {e}")
            return None, None

    @classmethod
    def _process_extended_optional_fields(cls, normalized: Dict, pin_data: Dict):
        """处理扩展可选字段 - 采用宽松策略"""
        # 创作者信息
        creator = pin_data.get('creator')
        if isinstance(creator, dict) and (creator.get('name') or creator.get('id')):
            normalized['creator_name'] = creator.get('name')
            normalized['creator_id'] = creator.get('id')
        else:
            normalized['creator_name'] = None
            normalized['creator_id'] = None
        
        # 板块信息
        board = pin_data.get('board')
        if isinstance(board, dict) and (board.get('name') or board.get('id')):
            normalized['board_name'] = board.get('name')
            normalized['board_id'] = board.get('id')
        else:
            normalized['board_name'] = None
            normalized['board_id'] = None
        
        # 统计信息 - 处理为JSON字符串
        stats = pin_data.get('stats')
        if isinstance(stats, dict) and stats:
            normalized['stats'] = json.dumps(stats)
        else:
            normalized['stats'] = None
        
        # 原始数据 - 完整保存，处理datetime序列化
        try:
            normalized['raw_data'] = json.dumps(pin_data, default=cls._json_serializer)
        except Exception as e:
            logger.warning(f"原始数据JSON序列化失败: {e}")
            # 如果序列化失败，保存一个简化版本
            simplified_data = {
                'id': pin_data.get('id'),
                'title': pin_data.get('title'),
                'description': pin_data.get('description')
            }
            normalized['raw_data'] = json.dumps(simplified_data)
    
    @classmethod
    def _json_serializer(cls, obj):
        """JSON序列化器，处理datetime等特殊对象"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    @classmethod
    def _calculate_pin_hash(cls, normalized_data: Dict[str, Any]) -> str:
        """计算Pin的MD5哈希值用于去重
        
        Args:
            normalized_data: 标准化后的Pin数据
            
        Returns:
            32位MD5哈希字符串
        """
        # 使用核心字段计算哈希，确保去重的准确性
        hash_fields = {
            'id': normalized_data.get('id', ''),
            'title': normalized_data.get('title', ''),
            'description': normalized_data.get('description', ''),
            'largest_image_url': normalized_data.get('largest_image_url', '')
        }
        
        hash_string = json.dumps(hash_fields, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
    
    @classmethod
    def validate_normalized_data(cls, normalized_data: Dict[str, Any]) -> bool:
        """验证标准化后的数据完整性
        
        Args:
            normalized_data: 标准化后的Pin数据
            
        Returns:
            验证是否通过
        """
        try:
            # 检查核心必需字段
            if not normalized_data.get('id'):
                logger.error("验证失败: 缺少Pin ID")
                return False
            
            if not normalized_data.get('query'):
                logger.error("验证失败: 缺少查询关键词")
                return False
            
            if not normalized_data.get('pin_hash'):
                logger.error("验证失败: 缺少Pin哈希值")
                return False
            
            # 检查时间戳
            if not normalized_data.get('created_at'):
                logger.error("验证失败: 缺少创建时间")
                return False
            
            logger.debug(f"Pin数据验证通过: {normalized_data.get('id')}")
            return True
            
        except Exception as e:
            logger.error(f"Pin数据验证异常: {e}")
            return False
