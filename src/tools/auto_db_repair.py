#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动数据库修复模块

集成到--only-images模式中，自动检测和修复数据库中的图片URL问题
"""

import os
import sqlite3
import json
import time
import asyncio
import base64
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path
import logging

from loguru import logger
from ..core.database.repository import SQLiteRepository
# 使用新的批量原子Base64转换器
from .realtime_base64_converter import BatchAtomicBase64Converter

class AutoDatabaseRepairer:
    """自动数据库修复器"""
    
    def __init__(self, backup: bool = True):
        """初始化自动修复器

        Args:
            backup: 是否备份原始数据库
        """
        self.backup = backup
        self.repair_stats = {}

    async def repair_base64_pins(self, keyword: str, output_dir: str = "output") -> Dict[str, int]:
        """修复关键词数据库中的base64编码Pin

        Args:
            keyword: 关键词
            output_dir: 输出目录

        Returns:
            修复统计信息
        """
        logger.info(f"开始修复关键词 '{keyword}' 的base64编码Pin")

        try:
            # 创建repository
            repository = SQLiteRepository(keyword=keyword, output_dir=output_dir)

            # 创建批量原子Base64转换器
            converter = BatchAtomicBase64Converter(output_dir)

            # 执行base64 Pin转换
            stats = await converter.process_all_databases(target_keyword=keyword)

            logger.info(f"base64 Pin修复完成: {stats}")
            return stats

        except Exception as e:
            logger.error(f"修复base64 Pin时发生异常: {e}")
            return {"error": str(e)}

    def detect_base64_pins(self, db_path: str) -> Dict[str, Any]:
        """检测数据库中的base64编码Pin

        Args:
            db_path: 数据库路径

        Returns:
            检测结果
        """
        logger.debug(f"检测base64编码Pin: {db_path}")

        if not os.path.exists(db_path):
            return {'status': 'error', 'message': '数据库文件不存在'}

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # 查询所有Pin ID
            cursor.execute("SELECT id, title FROM pins LIMIT 1000")
            pins = cursor.fetchall()

            base64_count = 0
            normal_count = 0
            base64_samples = []

            for pin_id, title in pins:
                if self._is_base64_pin_id(pin_id):
                    base64_count += 1
                    if len(base64_samples) < 5:  # 保存前5个样本
                        decoded_id = self._decode_pin_id(pin_id)
                        base64_samples.append({
                            'encoded': pin_id,
                            'decoded': decoded_id,
                            'title': title[:50] if title else 'No title'
                        })
                else:
                    normal_count += 1

            conn.close()

            total_pins = base64_count + normal_count
            needs_repair = base64_count > 0

            return {
                'status': 'success',
                'total_pins': total_pins,
                'base64_pins': base64_count,
                'normal_pins': normal_count,
                'base64_percentage': (base64_count / total_pins * 100) if total_pins > 0 else 0,
                'needs_repair': needs_repair,
                'base64_samples': base64_samples
            }

        except Exception as e:
            logger.error(f"检测base64 Pin时发生异常: {e}")
            return {'status': 'error', 'message': str(e)}

    def _is_base64_pin_id(self, pin_id: str) -> bool:
        """判断是否是base64编码的Pin ID

        Args:
            pin_id: Pin ID

        Returns:
            是否是base64编码
        """
        try:
            if pin_id and pin_id.startswith('UGlu'):  # Base64编码的"Pin:"前缀
                decoded = base64.b64decode(pin_id).decode('utf-8')
                return decoded.startswith('Pin:')
            return False
        except Exception:
            return False

    def _decode_pin_id(self, encoded_pin_id: str) -> Optional[str]:
        """解码base64编码的Pin ID

        Args:
            encoded_pin_id: base64编码的Pin ID

        Returns:
            解码后的数字Pin ID，失败返回None
        """
        try:
            if encoded_pin_id.startswith('UGlu'):
                decoded = base64.b64decode(encoded_pin_id).decode('utf-8')
                if decoded.startswith('Pin:'):
                    return decoded[4:]  # 移除"Pin:"前缀
            return None
        except Exception:
            return None
    
    def check_database_health(self, db_path: str) -> Dict:
        """检查数据库健康状态
        
        Args:
            db_path: 数据库路径
            
        Returns:
            健康状态报告
        """
        logger.debug(f"检查数据库健康状态: {db_path}")
        
        if not os.path.exists(db_path):
            return {'status': 'error', 'message': '数据库文件不存在', 'needs_repair': False}
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 检查数据库完整性
            cursor.execute("PRAGMA integrity_check")
            integrity = cursor.fetchone()[0]
            if integrity != 'ok':
                conn.close()
                return {'status': 'error', 'message': f'数据库完整性检查失败: {integrity}', 'needs_repair': False}
            
            # 检查pins表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pins'")
            if not cursor.fetchone():
                conn.close()
                return {'status': 'error', 'message': '数据库中没有pins表', 'needs_repair': False}
            
            # 统计Pin数量
            cursor.execute("SELECT COUNT(*) FROM pins")
            total_pins = cursor.fetchone()[0]
            
            if total_pins == 0:
                conn.close()
                return {'status': 'healthy', 'message': '数据库为空', 'needs_repair': False}
            
            # 统计有raw_data的Pin数量
            cursor.execute("SELECT COUNT(*) FROM pins WHERE raw_data IS NOT NULL AND raw_data != ''")
            pins_with_raw_data = cursor.fetchone()[0]
            
            # 统计有largest_image_url的Pin数量
            cursor.execute("SELECT COUNT(*) FROM pins WHERE largest_image_url IS NOT NULL AND largest_image_url != ''")
            pins_with_image_urls = cursor.fetchone()[0]
            
            conn.close()
            
            # 计算需要修复的Pin数量
            pins_need_fix = pins_with_raw_data - pins_with_image_urls
            
            # 判断是否需要修复
            if pins_need_fix > 0:
                repair_ratio = pins_need_fix / total_pins
                # 如果超过10%的Pin需要修复，则认为需要修复
                needs_repair = repair_ratio > 0.1
                
                return {
                    'status': 'need_fix' if needs_repair else 'minor_issues',
                    'message': f'需要修复 {pins_need_fix} 个Pin ({repair_ratio*100:.1f}%)',
                    'needs_repair': needs_repair,
                    'total_pins': total_pins,
                    'pins_with_raw_data': pins_with_raw_data,
                    'pins_with_image_urls': pins_with_image_urls,
                    'pins_need_fix': pins_need_fix,
                    'repair_ratio': repair_ratio
                }
            else:
                return {
                    'status': 'healthy',
                    'message': '数据库健康',
                    'needs_repair': False,
                    'total_pins': total_pins,
                    'pins_with_raw_data': pins_with_raw_data,
                    'pins_with_image_urls': pins_with_image_urls,
                    'pins_need_fix': 0,
                    'repair_ratio': 0.0
                }
            
        except Exception as e:
            logger.error(f"检查数据库健康状态失败: {e}")
            return {'status': 'error', 'message': f'检查数据库健康状态失败: {e}', 'needs_repair': False}
    
    def backup_database(self, db_path: str) -> bool:
        """备份数据库
        
        Args:
            db_path: 数据库路径
            
        Returns:
            备份是否成功
        """
        if not self.backup:
            return True
        
        try:
            backup_path = f"{db_path}.auto_repair_backup_{int(time.time())}"
            import shutil
            shutil.copy2(db_path, backup_path)
            logger.debug(f"数据库备份成功: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"数据库备份失败: {e}")
            return False
    
    def extract_image_urls_from_raw_data(self, raw_data: str) -> Tuple[Optional[str], Optional[str]]:
        """从raw_data中提取图片URL
        
        Args:
            raw_data: 原始数据JSON字符串
            
        Returns:
            (largest_image_url, image_urls_json)
        """
        try:
            if not raw_data:
                return None, None
            
            data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
            
            largest_url = None
            image_urls = {}
            
            # 从images字段提取
            if 'images' in data and isinstance(data['images'], dict):
                images = data['images']
                
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
            if not largest_url and 'image' in data:
                image_data = data['image']
                if isinstance(image_data, dict) and 'url' in image_data:
                    largest_url = image_data['url']
                    image_urls['image'] = image_data['url']
            
            # 序列化image_urls
            image_urls_json = json.dumps(image_urls) if image_urls else None
            
            return largest_url, image_urls_json
            
        except Exception as e:
            logger.debug(f"提取图片URL失败: {e}")
            return None, None
    
    def repair_database(self, db_path: str) -> Dict:
        """修复数据库
        
        Args:
            db_path: 数据库路径
            
        Returns:
            修复结果
        """
        logger.info(f"开始自动修复数据库: {db_path}")
        
        # 检查数据库健康状态
        health = self.check_database_health(db_path)
        if health['status'] == 'error':
            return {'status': 'error', 'message': health['message']}
        
        if not health['needs_repair']:
            return {'status': 'success', 'message': '数据库不需要修复', 'stats': health}
        
        # 备份数据库
        if not self.backup_database(db_path):
            return {'status': 'error', 'message': '数据库备份失败'}
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 查询需要修复的Pin
            cursor.execute("""
                SELECT id, raw_data 
                FROM pins 
                WHERE (largest_image_url IS NULL OR largest_image_url = '')
                AND raw_data IS NOT NULL AND raw_data != ''
            """)
            
            pins_to_fix = cursor.fetchall()
            total_to_fix = len(pins_to_fix)
            
            if total_to_fix == 0:
                conn.close()
                return {'status': 'success', 'message': '没有需要修复的Pin', 'stats': health}
            
            logger.info(f"找到 {total_to_fix} 个需要修复的Pin")
            
            # 修复Pin
            fixed_count = 0
            failed_count = 0
            skipped_count = 0
            
            for pin_id, raw_data in pins_to_fix:
                try:
                    # 提取图片URL
                    largest_url, image_urls_json = self.extract_image_urls_from_raw_data(raw_data)
                    
                    if largest_url:
                        # 更新数据库
                        cursor.execute("""
                            UPDATE pins 
                            SET largest_image_url = ?, image_urls = ?
                            WHERE id = ?
                        """, (largest_url, image_urls_json, pin_id))
                        
                        fixed_count += 1
                        logger.debug(f"修复成功: {pin_id}")
                    else:
                        skipped_count += 1
                        logger.debug(f"跳过Pin: {pin_id} (无法提取图片URL)")
                
                except Exception as e:
                    failed_count += 1
                    logger.debug(f"修复失败: {pin_id}, 错误: {e}")
                
                # 每100条提交一次，避免事务过大
                if (fixed_count + failed_count + skipped_count) % 100 == 0:
                    conn.commit()
            
            # 最终提交
            conn.commit()
            conn.close()
            
            # 记录修复统计
            repair_stats = {
                'total_to_fix': total_to_fix,
                'fixed_count': fixed_count,
                'failed_count': failed_count,
                'skipped_count': skipped_count,
                'success_rate': fixed_count / total_to_fix * 100 if total_to_fix > 0 else 0
            }
            
            self.repair_stats[db_path] = repair_stats
            
            logger.info(f"数据库修复完成: 成功 {fixed_count}, 失败 {failed_count}, 跳过 {skipped_count}")
            
            return {
                'status': 'success',
                'message': f'数据库修复完成: 成功 {fixed_count}, 失败 {failed_count}, 跳过 {skipped_count}',
                'stats': repair_stats
            }
            
        except Exception as e:
            logger.error(f"修复数据库失败: {e}")
            return {'status': 'error', 'message': f'修复数据库失败: {e}'}
    
    async def auto_repair_if_needed(self, db_path: str, keyword: str = None, output_dir: str = "output") -> Dict:
        """如果需要则自动修复数据库（包括base64 Pin修复）

        Args:
            db_path: 数据库路径
            keyword: 关键词（用于base64 Pin修复）
            output_dir: 输出目录

        Returns:
            修复结果
        """
        repair_results = {
            'status': 'success',
            'repaired': False,
            'basic_repair': False,
            'base64_repair': False,
            'messages': [],
            'stats': {}
        }

        # 1. 检查基础健康状态
        health = self.check_database_health(db_path)

        if health['status'] == 'error':
            return {'status': 'error', 'message': health['message'], 'repaired': False}

        # 2. 执行基础修复（如果需要）
        if health['needs_repair']:
            logger.info(f"检测到数据库需要基础修复: {health['message']}")
            repair_result = self.repair_database(db_path)

            if repair_result['status'] == 'success':
                repair_results['basic_repair'] = True
                repair_results['repaired'] = True
                repair_results['messages'].append(f"基础修复完成: {repair_result['message']}")
                repair_results['stats']['basic_repair'] = repair_result.get('stats', {})
            else:
                repair_results['status'] = 'error'
                repair_results['messages'].append(f"基础修复失败: {repair_result['message']}")
                return repair_results

        # 3. 检测和修复base64编码Pin（如果提供了关键词）
        if keyword:
            base64_detection = self.detect_base64_pins(db_path)

            if base64_detection['status'] == 'success' and base64_detection['needs_repair']:
                logger.info(f"检测到 {base64_detection['base64_pins']} 个base64编码Pin需要修复")

                # 显示base64 Pin样本
                if base64_detection['base64_samples']:
                    logger.info("base64 Pin样本:")
                    for sample in base64_detection['base64_samples']:
                        logger.info(f"  {sample['encoded']} -> {sample['decoded']} ({sample['title']})")

                # 执行base64 Pin修复
                base64_repair_result = await self.repair_base64_pins(keyword, output_dir)

                if 'error' not in base64_repair_result:
                    repair_results['base64_repair'] = True
                    repair_results['repaired'] = True
                    repair_results['messages'].append(
                        f"base64 Pin修复完成: {base64_repair_result['converted_pins']}/{base64_repair_result['base64_pins']} 成功"
                    )
                    repair_results['stats']['base64_repair'] = base64_repair_result
                else:
                    repair_results['messages'].append(f"base64 Pin修复失败: {base64_repair_result['error']}")
            else:
                repair_results['messages'].append("未发现需要修复的base64编码Pin")

        # 4. 生成最终结果
        if repair_results['repaired']:
            repair_results['message'] = "; ".join(repair_results['messages'])
        else:
            repair_results['message'] = "数据库健康，无需修复"
            repair_results['status'] = 'healthy'

        return repair_results

    def auto_repair_if_needed_sync(self, db_path: str) -> Dict:
        """同步版本的自动修复（保持向后兼容）

        Args:
            db_path: 数据库路径

        Returns:
            修复结果
        """
        # 检查健康状态
        health = self.check_database_health(db_path)

        if health['status'] == 'error':
            return {'status': 'error', 'message': health['message'], 'repaired': False}

        if not health['needs_repair']:
            return {'status': 'healthy', 'message': '数据库健康，无需修复', 'repaired': False}

        # 执行修复
        logger.info(f"检测到数据库需要修复: {health['message']}")
        repair_result = self.repair_database(db_path)

        if repair_result['status'] == 'success':
            return {
                'status': 'success',
                'message': f"自动修复完成: {repair_result['message']}",
                'repaired': True,
                'repair_stats': repair_result.get('stats', {})
            }
        else:
            return {
                'status': 'error',
                'message': f"自动修复失败: {repair_result['message']}",
                'repaired': False
            }

# 便捷函数
def auto_repair_database_if_needed(db_path: str, backup: bool = True) -> Dict:
    """自动修复数据库的便捷函数
    
    Args:
        db_path: 数据库路径
        backup: 是否备份原始数据库
        
    Returns:
        修复结果
    """
    repairer = AutoDatabaseRepairer(backup=backup)
    return repairer.auto_repair_if_needed(db_path)
