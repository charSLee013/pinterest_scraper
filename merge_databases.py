#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库合并工具

将源output目录中的数据库合并到目标output目录中。
支持相同关键词的数据库合并，采用目标数据库优先策略。
"""

import os
import sys
import argparse
import asyncio
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict

# 添加src目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from loguru import logger
from tqdm import tqdm

from src.core.database.repository import SQLiteRepository
from src.utils.utils import sanitize_filename


def setup_logger(debug: bool = False, verbose: bool = False):
    """设置日志系统"""
    logger.remove()
    
    if debug:
        logger.add(sys.stderr, level="DEBUG", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")
    elif verbose:
        logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")
    else:
        logger.add(sys.stderr, level="WARNING", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")


def create_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="Pinterest数据库合并工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python merge_databases.py --source main_output --target backup_output
  python merge_databases.py --source current_output --target additional_data --verbose
  python merge_databases.py --source primary_data --target secondary_data --debug

注意:
  --source 是主数据库目录（合并后的最终位置）
  --target 是要合并进来的数据库目录（数据来源）
        """
    )
    
    # 必需参数
    parser.add_argument(
        "--source",
        required=True,
        help="主数据库目录路径（合并后的最终位置）"
    )
    parser.add_argument(
        "--target",
        required=True,
        help="要合并的数据库目录路径（数据来源）"
    )
    
    # 可选参数
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="启用详细输出"
    )
    parser.add_argument(
        "--debug",
        action="store_true", 
        help="启用调试模式"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式，不实际执行合并"
    )
    
    return parser


class DatabaseMerger:
    """数据库合并器"""
    
    def __init__(self, source_dir: str, target_dir: str, dry_run: bool = False):
        """初始化合并器

        Args:
            source_dir: 主数据库目录（合并后的最终位置）
            target_dir: 要合并的数据库目录（数据来源）
            dry_run: 是否为预览模式
        """
        self.source_dir = Path(source_dir)  # 主数据库目录
        self.target_dir = Path(target_dir)  # 要合并的数据库目录
        self.dry_run = dry_run

        logger.debug(f"数据库合并器初始化: 将 {target_dir} 合并到 {source_dir}")
    
    def discover_database_pairs(self) -> List[Dict]:
        """发现需要合并的数据库对
        
        Returns:
            数据库对列表
        """
        if not self.target_dir.exists():
            logger.error(f"要合并的数据库目录不存在: {self.target_dir}")
            return []

        # 发现主数据库（source）
        source_dbs = self._discover_databases(self.source_dir)
        logger.info(f"发现主数据库: {len(source_dbs)} 个")

        # 发现要合并的数据库（target）
        target_dbs = self._discover_databases(self.target_dir)
        logger.info(f"发现要合并的数据库: {len(target_dbs)} 个")

        # 创建主数据库的关键词映射
        source_map = {db['keyword']: db for db in source_dbs}

        merge_pairs = []
        for target_db in target_dbs:
            keyword = target_db['keyword']

            if keyword in source_map:
                # 主数据库中存在对应关键词
                merge_pairs.append({
                    'keyword': keyword,
                    'source': source_map[keyword],  # 主数据库（目标位置）
                    'target': target_db,            # 要合并的数据库（数据来源）
                    'operation': 'merge'
                })
                logger.debug(f"合并对: {keyword} (合并到现有)")
            else:
                # 主数据库中不存在，需要在主数据库中创建
                source_path = self.source_dir / keyword
                merge_pairs.append({
                    'keyword': keyword,
                    'source': {
                        'keyword': keyword,
                        'db_path': str(source_path / 'pinterest.db'),
                        'images_dir': str(source_path / 'images'),
                        'keyword_dir': str(source_path)
                    },
                    'target': target_db,            # 要合并的数据库（数据来源）
                    'operation': 'create_and_merge'
                })
                logger.debug(f"合并对: {keyword} (新建到主数据库)")
        
        return merge_pairs
    
    def _discover_databases(self, base_dir: Path) -> List[Dict]:
        """发现目录中的数据库
        
        Args:
            base_dir: 基础目录
            
        Returns:
            数据库信息列表
        """
        databases = []
        
        if not base_dir.exists():
            return databases
        
        for item in base_dir.iterdir():
            if not item.is_dir():
                continue
                
            keyword = item.name
            db_path = item / 'pinterest.db'
            images_dir = item / 'images'
            
            # 检查数据库文件是否存在
            if not db_path.exists():
                logger.debug(f"跳过目录 {keyword}: 数据库文件不存在")
                continue
            
            databases.append({
                'keyword': keyword,
                'db_path': str(db_path),
                'images_dir': str(images_dir),
                'keyword_dir': str(item)
            })
            
            logger.debug(f"发现数据库: {keyword} -> {db_path}")
        
        return databases
    
    def preview_merge(self, merge_pairs: List[Dict]) -> bool:
        """预览合并操作
        
        Args:
            merge_pairs: 合并对列表
            
        Returns:
            用户是否确认继续
        """
        if not merge_pairs:
            logger.warning("没有发现需要合并的数据库")
            return False
        
        print("\n" + "="*60)
        print("数据库合并预览")
        print("="*60)
        
        total_source_pins = 0
        total_target_pins = 0
        
        for pair in merge_pairs:
            keyword = pair['keyword']
            operation = pair['operation']
            source_db = pair['source']  # 主数据库（目标位置）
            target_db = pair['target']  # 要合并的数据库（数据来源）

            # 获取要合并的数据库的Pin数量
            target_count = self._get_pin_count(target_db['db_path'])
            total_target_pins += target_count

            if operation == 'merge':
                # 主数据库中已存在该关键词
                source_count = self._get_pin_count(source_db['db_path'])
                total_source_pins += source_count
                print(f"{keyword}: {target_count} pins -> {source_count} pins (合并到现有)")
            else:
                # 主数据库中不存在，将新建
                print(f"{keyword}: {target_count} pins (新建到主数据库)")
        
        print("="*60)
        print(f"总计: {len(merge_pairs)} 个关键词")
        print(f"主数据库: {total_source_pins} pins")
        print(f"要合并的数据: {total_target_pins} pins")
        print("注意: 主数据库中的数据将优先保留（相同Pin ID时）")
        print("="*60)
        
        if self.dry_run:
            print("预览模式，不会实际执行合并")
            return True
        
        response = input("是否继续合并? (y/n): ").lower().strip()
        return response in ['y', 'yes', '是']
    
    def _get_pin_count(self, db_path: str) -> int:
        """获取数据库中的Pin数量
        
        Args:
            db_path: 数据库路径
            
        Returns:
            Pin数量
        """
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM pins")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            logger.error(f"获取Pin数量失败: {db_path}, 错误: {e}")
            return 0
    
    async def merge_all_databases(self, merge_pairs: List[Dict]) -> Dict:
        """合并所有数据库对
        
        Args:
            merge_pairs: 合并对列表
            
        Returns:
            合并统计结果
        """
        if self.dry_run:
            logger.info("预览模式，跳过实际合并")
            return {'keywords': len(merge_pairs), 'pins_merged': 0, 'pins_skipped': 0, 'errors': 0, 'error_details': []}
        
        total_stats = {
            'keywords': 0,
            'pins_merged': 0,
            'pins_skipped': 0,
            'errors': 0,
            'error_details': []
        }
        
        logger.info(f"开始合并 {len(merge_pairs)} 个数据库对")
        
        for pair in merge_pairs:
            keyword = pair['keyword']
            logger.info(f"处理关键词: {keyword}")
            
            try:
                stats = await self.merge_database_pair(pair)
                
                total_stats['keywords'] += 1
                total_stats['pins_merged'] += stats['pins_merged']
                total_stats['pins_skipped'] += stats['pins_skipped']
                
                logger.info(f"关键词 {keyword} 完成: 合并 {stats['pins_merged']}, 跳过 {stats['pins_skipped']}")
                
            except Exception as e:
                error_msg = f"处理关键词 {keyword} 时出错: {e}"
                logger.error(error_msg)
                total_stats['errors'] += 1
                total_stats['error_details'].append(error_msg)
        
        return total_stats

    async def merge_database_pair(self, merge_info: Dict) -> Dict:
        """合并单个数据库对

        Args:
            merge_info: 合并信息

        Returns:
            合并统计结果
        """
        keyword = merge_info['keyword']
        source_db = merge_info['source']  # 主数据库（目标位置）
        target_db = merge_info['target']  # 要合并的数据库（数据来源）
        operation = merge_info['operation']

        # 创建主数据库目录（如果不存在）
        source_dir = Path(source_db['keyword_dir'])
        source_dir.mkdir(parents=True, exist_ok=True)

        images_dir = Path(source_db['images_dir'])
        images_dir.mkdir(parents=True, exist_ok=True)

        # 初始化repository
        # source_repo: 主数据库（接收数据）
        # target_repo: 要合并的数据库（提供数据）
        source_repo = SQLiteRepository(keyword=keyword, output_dir=str(self.source_dir))
        target_repo = SQLiteRepository(keyword=keyword, output_dir=str(self.target_dir))

        merge_stats = {
            'pins_processed': 0,
            'pins_merged': 0,
            'pins_skipped': 0,
            'tasks_merged': 0,
            'sessions_merged': 0,
            'errors': []
        }

        try:
            # 批量读取要合并的数据库中的数据
            target_pins = target_repo.load_pins_by_query(keyword)
            logger.debug(f"从要合并的数据库加载 {len(target_pins)} 个Pin: {keyword}")

            if not target_pins:
                logger.info(f"要合并的数据库为空: {keyword}")
                return merge_stats

            # 获取主数据库中已存在的Pin ID集合
            existing_pin_ids = set()
            try:
                source_pins = source_repo.load_pins_by_query(keyword)
                existing_pin_ids = {pin.get('id') for pin in source_pins if pin.get('id')}
                logger.debug(f"主数据库已有 {len(existing_pin_ids)} 个Pin: {keyword}")
            except Exception as e:
                logger.debug(f"主数据库为空或不存在: {keyword}, {e}")

            # 过滤需要合并的Pin（主数据库优先策略）
            pins_to_merge = []
            for pin in target_pins:
                pin_id = pin.get('id')
                if not pin_id:
                    continue

                merge_stats['pins_processed'] += 1

                if pin_id in existing_pin_ids:
                    merge_stats['pins_skipped'] += 1
                    logger.debug(f"跳过已存在Pin: {pin_id}")
                else:
                    pins_to_merge.append(pin)
                    merge_stats['pins_merged'] += 1

            # 批量保存新Pin到主数据库
            if pins_to_merge:
                logger.info(f"开始合并 {len(pins_to_merge)} 个新Pin到主数据库: {keyword}")

                # 使用进度条显示合并进度
                with tqdm(total=len(pins_to_merge), desc=f"合并 {keyword}", unit="pins") as pbar:
                    # 分批处理，避免内存问题
                    batch_size = 100
                    for i in range(0, len(pins_to_merge), batch_size):
                        batch = pins_to_merge[i:i + batch_size]

                        # 转换为保存格式
                        pins_to_save = []
                        for pin in batch:
                            # 直接使用原始Pin数据，保持完整性
                            pins_to_save.append(pin)

                        # 批量保存到主数据库
                        success = source_repo.save_pins_batch(pins_to_save, keyword)
                        if not success:
                            error_msg = f"批量保存失败: {len(batch)} pins"
                            merge_stats['errors'].append(error_msg)
                            logger.error(error_msg)

                        pbar.update(len(batch))

            logger.info(f"合并完成: {keyword}, 新增 {merge_stats['pins_merged']} pins到主数据库")

        except Exception as e:
            error_msg = f"合并数据库对失败: {keyword}, 错误: {e}"
            merge_stats['errors'].append(error_msg)
            logger.error(error_msg)

        return merge_stats


async def async_main():
    """异步主函数"""
    parser = create_parser()
    args = parser.parse_args()

    # 设置日志
    setup_logger(args.debug, args.verbose)

    # 验证参数
    if not os.path.exists(args.source):
        logger.error(f"源目录不存在: {args.source}")
        return 1

    # 创建合并器
    merger = DatabaseMerger(
        source_dir=args.source,
        target_dir=args.target,
        dry_run=args.dry_run
    )

    try:
        # 发现数据库对
        merge_pairs = merger.discover_database_pairs()

        # 预览合并操作
        if not merger.preview_merge(merge_pairs):
            logger.info("用户取消合并操作")
            return 0

        # 执行合并
        stats = await merger.merge_all_databases(merge_pairs)

        # 显示最终报告
        print("\n" + "="*60)
        print("数据库合并完成!")
        print("="*60)
        print(f"处理关键词: {stats['keywords']} 个")
        print(f"合并Pin: {stats['pins_merged']} 个")
        print(f"跳过Pin: {stats['pins_skipped']} 个")
        print(f"错误: {stats['errors']} 个")

        if stats['error_details']:
            print("\n错误详情:")
            for error in stats['error_details']:
                print(f"  - {error}")

        print("="*60)

        return 0 if stats['errors'] == 0 else 1

    except Exception as e:
        logger.error(f"合并过程中出现错误: {e}")
        if args.debug:
            import traceback
            logger.error(traceback.format_exc())
        return 1


def main():
    """主函数"""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    return asyncio.run(async_main())


if __name__ == "__main__":
    sys.exit(main())
