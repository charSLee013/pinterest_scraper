
import unittest
import subprocess
import os
import shutil
import json
import time

class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.output_dir = "test_output"
        # 确保测试前清理环境
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

    def tearDown(self):
        # 测试后清理环境
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

    def test_single_keyword_search(self):
        """
        测试单个关键词搜索的端到端流程
        """
        keyword = "cat"
        count = 5
        
        # 1. 运行主程序
        command = [
            "python", "main.py",
            "-s", keyword,
            "-c", str(count),
            "-o", self.output_dir,
            "--log-level", "DEBUG" # 方便调试
        ]
        
        result = subprocess.run(command, capture_output=True, text=True)
        
        # 打印子进程的输出，方便调试
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        
        # 2. 验证退出码
        self.assertEqual(result.returncode, 0, "脚本执行失败")
        
        # 3. 验证输出目录和文件结构
        expected_keyword_dir = os.path.join(self.output_dir, keyword)
        self.assertTrue(os.path.isdir(expected_keyword_dir), "关键词目录未创建")
        
        json_dir = os.path.join(expected_keyword_dir, "json")
        metadata_files = [f for f in os.listdir(json_dir) if f.startswith("pinterest_search_") and f.endswith(".json")]
        self.assertGreater(len(metadata_files), 0, "pinterest_search_*.json 文件未创建")
        metadata_path = os.path.join(expected_keyword_dir, "json", metadata_files[0])
        self.assertTrue(os.path.isfile(metadata_path), "metadata.json 文件未创建")
        
        # 4. 验证下载的图片数量
        image_dir = os.path.join(expected_keyword_dir, "images")
        files = os.listdir(image_dir)
        image_files = [f for f in files if f.endswith(('.jpg', '.png', '.gif'))]
        # 实际下载数量可能小于请求数量，但应该大于0
        self.assertGreater(len(image_files), 0, "没有下载任何图片")
        
        # 5. 验证metadata.json内容
        with open(metadata_path, 'r', encoding='utf-8') as f:
            try:
                metadata = json.load(f)
                self.assertIsInstance(metadata, list, "元数据不是一个列表")
                self.assertGreater(len(metadata), 0, "元数据文件为空")
                self.assertEqual(len(metadata), len(image_files), "元数据条目数与图片数不匹配")
                
                # 检查第一个条目的结构
                first_item = metadata[0]
                self.assertIn("id", first_item)
                self.assertIn("url", first_item)
                self.assertIn("title", first_item)
                
            except json.JSONDecodeError:
                self.fail("metadata.json 文件不是有效的JSON")


    def test_scrape_url(self):
        """
        测试单个URL爬取的端到端流程
        """
        url = "https://www.pinterest.com/pin/544322987541295328/" # Example Pinterest pin URL
        count = 5
        
        # 1. 运行主程序
        command = [
            "python", "main.py",
            "-u", url,
            "-c", str(count),
            "-o", self.output_dir,
            "--log-level", "DEBUG" # 方便调试
        ]
        
        result = subprocess.run(command, capture_output=True, text=True)
        
        # 打印子进程的输出，方便调试
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        
        # 2. 验证退出码
        self.assertEqual(result.returncode, 0, "脚本执行失败")
        
        # 3. 验证输出目录和文件结构
        safe_url_term = self.sanitize_filename(url)
        print(f"DEBUG: safe_url_term in test: {safe_url_term}")
        expected_url_dir = os.path.join(self.output_dir, safe_url_term)
        self.assertTrue(os.path.isdir(expected_url_dir), "URL目录未创建")
        
        json_dir = os.path.join(expected_url_dir, "json")
        metadata_files = [f for f in os.listdir(json_dir) if f.startswith("pinterest_url_") and f.endswith(".json")]
        self.assertGreater(len(metadata_files), 0, "pinterest_url_*.json 文件未创建")
        metadata_path = os.path.join(json_dir, metadata_files[0])
        self.assertTrue(os.path.isfile(metadata_path), "metadata.json 文件未创建")
        
        # 4. 验证下载的图片数量
        image_dir = os.path.join(expected_url_dir, "images")
        files = os.listdir(image_dir)
        image_files = [f for f in files if f.endswith(('.jpg', '.png', '.gif'))]
        # 实际下载数量可能小于请求数量，但应该大于0
        self.assertGreater(len(image_files), 0, "没有下载任何图片")
        
        # 5. 验证metadata.json内容
        with open(metadata_path, 'r', encoding='utf-8') as f:
            try:
                metadata = json.load(f)
                self.assertIsInstance(metadata, list, "元数据不是一个列表")
                self.assertGreater(len(metadata), 0, "元数据文件为空")
                self.assertEqual(len(metadata), len(image_files), "元数据条目数与图片数不匹配")
                
                # 检查第一个条目的结构
                first_item = metadata[0]
                self.assertIn("id", first_item)
                self.assertIn("url", first_item)
                self.assertIn("title", first_item)
                
            except json.JSONDecodeError:
                self.fail("metadata.json 文件不是有效的JSON")

    def sanitize_filename(self, name: str) -> str:
        """生成安全的文件名，拷贝自utils.py，为了测试独立性"""
        import re
        # 首先去除URL中的任何参数和无关字符
        name = name.split("?")[0].split("#")[0]
        if "/" in name:
            # 如果是URL，只取最后一部分
            parts = [p for p in name.split("/") if p]
            if parts:
                name = parts[-1] or (parts[-2] if len(parts) > 1 else name)
        
        # 长度限制，避免文件名过长
        if len(name) > 50:
            name = name[:50]
        
        return re.sub(r'[\/:*?"<>|]', "_", name)

if __name__ == '__main__':
    unittest.main()
