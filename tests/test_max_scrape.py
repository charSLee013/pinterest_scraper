import unittest
import sys
import os

# 将项目根目录添加到Python路径中，以便导入模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pinterest import PinterestScraper
from loguru import logger

class TestMaxScrape(unittest.TestCase):
    """
    一个专门的测试用例，用于验证在新的、自定义的终止条件下，
    针对单一主题能抓取的最大数据量。
    """

    def test_scrape_cat_with_custom_termination(self):
        """
        测试 'cat' 主题的抓取，使用以下终止条件：
        1. 连续3次没有新数据。
        2. 滚动3次后，最近3次滚动的新增数据总和小于5。
        """
        # 强制将日志级别设置为INFO，以便获得干净的报告
        logger.remove()
        logger.add(sys.stderr, level="INFO")

        logger.info("--- 开始最大数量抓取测试 (主题: cat) ---")
        
        # 初始化爬虫，并禁用图片下载
        scraper = PinterestScraper(
            output_dir="output/test_max_scrape",
            download_images=False
        )

        # 开始搜索
        final_count = scraper.search(
            query="cat",
            count=9999,  # 设置一个非常高的目标值
        )

        logger.info(f"--- 测试完成 ---")
        logger.info(f"最终捕获的Pin数量: {len(final_count)}")

        # 断言结果
        print(f"测试结束，查询 'cat' 共捕获 {len(final_count)} 个pins。")
        self.assertTrue(len(final_count) > 0, "未能收集到任何pin")

if __name__ == '__main__':
    unittest.main() 