#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pinterest网络分析模块测试套件

包含单元测试和集成测试，验证网络拦截器和API分析器的功能
"""

import os
import sys
import unittest
import tempfile
import shutil
import json
from unittest.mock import Mock, patch, MagicMock

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from network_analysis.network_interceptor import NetworkInterceptor
from network_analysis.api_analyzer import APIAnalyzer


class TestNetworkInterceptor(unittest.TestCase):
    """网络拦截器单元测试"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.interceptor = NetworkInterceptor(output_dir=self.temp_dir)
    
    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_init(self):
        """测试初始化"""
        self.assertIsNotNone(self.interceptor.session_id)
        self.assertEqual(self.interceptor.output_dir, self.temp_dir)
        self.assertTrue(os.path.exists(self.interceptor.session_dir))
    
    def test_is_pinterest_api_request(self):
        """测试Pinterest API请求判断"""
        # 测试正面案例
        api_urls = [
            "https://api.pinterest.com/v3/search/pins",
            "https://www.pinterest.com/resource/BaseSearchResource/get/",
            "https://pinterest.com/resource/UserPinsResource/get/",
            "https://www.pinterest.com/v3/pins/123456/"
        ]
        
        for url in api_urls:
            with self.subTest(url=url):
                self.assertTrue(self.interceptor._is_pinterest_api_request(url))
        
        # 测试负面案例
        non_api_urls = [
            "https://www.google.com/search",
            "https://facebook.com/api/test",
            "https://www.pinterest.com/static/images/logo.png"
        ]
        
        for url in non_api_urls:
            with self.subTest(url=url):
                self.assertFalse(self.interceptor._is_pinterest_api_request(url))
    
    def test_extract_request_info(self):
        """测试请求信息提取"""
        # 创建模拟请求对象
        mock_request = Mock()
        mock_request.url = "https://www.pinterest.com/resource/BaseSearchResource/get/?q=test"
        mock_request.method = "GET"
        mock_request.headers = {"User-Agent": "test-agent"}
        mock_request.resource_type = "xhr"
        mock_request.post_data = None
        
        request_info = self.interceptor._extract_request_info(mock_request)
        
        self.assertEqual(request_info["method"], "GET")
        self.assertEqual(request_info["url"], mock_request.url)
        self.assertEqual(request_info["domain"], "www.pinterest.com")
        self.assertEqual(request_info["path"], "/resource/BaseSearchResource/get/")
        self.assertEqual(request_info["resource_type"], "xhr")
    
    def test_extract_response_info(self):
        """测试响应信息提取"""
        # 创建模拟响应对象
        mock_response = Mock()
        mock_response.url = "https://www.pinterest.com/resource/BaseSearchResource/get/"
        mock_response.status = 200
        mock_response.status_text = "OK"
        mock_response.headers = {"content-type": "application/json"}
        test_data = b'{"test": "data"}'
        mock_response.body.return_value = test_data
        mock_response.json.return_value = {"test": "data"}
        
        response_info = self.interceptor._extract_response_info(mock_response)
        
        self.assertEqual(response_info["status"], 200)
        self.assertEqual(response_info["url"], mock_response.url)
        self.assertEqual(response_info["content_type"], "application/json")
        self.assertEqual(response_info["size"], len(test_data))
        self.assertEqual(response_info["json_data"], {"test": "data"})
    
    def test_save_results(self):
        """测试结果保存"""
        # 添加测试数据
        self.interceptor.network_logs = [
            {"type": "request", "data": {"url": "test", "method": "GET"}}
        ]
        self.interceptor.api_responses = [
            {"url": "test", "status": 200, "json_data": {"test": "data"}}
        ]
        
        self.interceptor._save_results()
        
        # 验证文件是否创建
        network_log_file = os.path.join(self.interceptor.session_dir, "network_logs", "requests.json")
        api_response_file = os.path.join(self.interceptor.session_dir, "api_responses", "responses.json")
        
        self.assertTrue(os.path.exists(network_log_file))
        self.assertTrue(os.path.exists(api_response_file))
        
        # 验证文件内容
        with open(network_log_file, 'r', encoding='utf-8') as f:
            network_data = json.load(f)
        self.assertEqual(len(network_data), 1)
        
        with open(api_response_file, 'r', encoding='utf-8') as f:
            api_data = json.load(f)
        self.assertEqual(len(api_data), 1)
    
    def test_generate_summary(self):
        """测试摘要生成"""
        # 添加测试数据
        self.interceptor.network_logs = [
            {"type": "request", "data": {"url": "https://www.pinterest.com/resource/test", "method": "GET"}},
            {"type": "response", "data": {"url": "https://www.pinterest.com/resource/test", "status": 200}}
        ]
        self.interceptor.api_responses = [
            {"url": "https://www.pinterest.com/resource/test", "status": 200}
        ]
        
        summary = self.interceptor._generate_summary()
        
        self.assertEqual(summary["total_requests"], 1)
        self.assertEqual(summary["total_responses"], 1)
        self.assertEqual(summary["successful_api_responses"], 1)
        self.assertIn("www.pinterest.com", summary["unique_domains"])
        self.assertIn("/resource/test", summary["unique_endpoints"])


class TestAPIAnalyzer(unittest.TestCase):
    """API分析器单元测试"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.analyzer = APIAnalyzer()
        
        # 创建测试数据目录结构
        self.session_dir = os.path.join(self.temp_dir, "test_session")
        os.makedirs(os.path.join(self.session_dir, "network_logs"), exist_ok=True)
        os.makedirs(os.path.join(self.session_dir, "api_responses"), exist_ok=True)
        
        # 创建测试数据文件
        self.create_test_data()
    
    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_test_data(self):
        """创建测试数据"""
        # 网络日志数据
        network_logs = [
            {
                "type": "request",
                "data": {
                    "url": "https://www.pinterest.com/resource/BaseSearchResource/get/?q=nature",
                    "method": "GET",
                    "query_params": {"q": ["nature"]},
                    "headers": {"User-Agent": "test"}
                }
            }
        ]
        
        # API响应数据
        api_responses = [
            {
                "url": "https://www.pinterest.com/resource/BaseSearchResource/get/?q=nature",
                "status": 200,
                "content_type": "application/json",
                "json_data": {
                    "resource_response": {
                        "data": [
                            {"id": "123", "title": "Nature Pin", "images": {"orig": {"url": "test.jpg"}}}
                        ],
                        "bookmark": "next_page_token"
                    }
                }
            }
        ]
        
        # 保存测试数据
        with open(os.path.join(self.session_dir, "network_logs", "requests.json"), 'w') as f:
            json.dump(network_logs, f)
        
        with open(os.path.join(self.session_dir, "api_responses", "responses.json"), 'w') as f:
            json.dump(api_responses, f)
    
    def test_analyze_session(self):
        """测试会话分析"""
        result = self.analyzer.analyze_session(self.session_dir)
        
        self.assertIn("analysis_summary", result)
        self.assertIn("api_endpoints", result)
        self.assertIn("pagination_info", result)
        self.assertIn("data_structures", result)
        self.assertIn("recommendations", result)
        
        # 验证分析摘要
        summary = result["analysis_summary"]
        self.assertEqual(summary["total_endpoints"], 1)
        self.assertEqual(summary["endpoints_with_pagination"], 1)
        self.assertEqual(summary["endpoints_with_pins_data"], 1)
    
    def test_analyze_api_endpoints(self):
        """测试API端点分析"""
        api_responses = [
            {
                "url": "https://www.pinterest.com/resource/BaseSearchResource/get/?q=nature",
                "status": 200,
                "json_data": {"resource_response": {"data": []}}
            }
        ]
        
        self.analyzer._analyze_api_endpoints(api_responses)
        
        endpoint = "/resource/BaseSearchResource/get/"
        self.assertIn(endpoint, self.analyzer.api_endpoints)
        self.assertEqual(self.analyzer.api_endpoints[endpoint]["success_count"], 1)
        self.assertIn("q", self.analyzer.api_endpoints[endpoint]["query_parameters"])
    
    def test_extract_pins_data(self):
        """测试pins数据提取"""
        # 测试resource_response结构
        json_data1 = {
            "resource_response": {
                "data": [
                    {"id": "123", "title": "Test Pin", "images": {"orig": {"url": "test.jpg"}}}
                ]
            }
        }
        pins_data1 = self.analyzer._extract_pins_data(json_data1)
        self.assertIsNotNone(pins_data1)
        self.assertEqual(len(pins_data1), 1)
        self.assertEqual(pins_data1[0]["id"], "123")
        
        # 测试results结构
        json_data2 = {
            "results": [
                {"id": "456", "title": "Another Pin", "url": "test2.jpg"}
            ]
        }
        pins_data2 = self.analyzer._extract_pins_data(json_data2)
        self.assertIsNotNone(pins_data2)
        self.assertEqual(len(pins_data2), 1)
        self.assertEqual(pins_data2[0]["id"], "456")
        
        # 测试无效数据
        json_data3 = {"invalid": "data"}
        pins_data3 = self.analyzer._extract_pins_data(json_data3)
        self.assertIsNone(pins_data3)
    
    def test_generate_recommendations(self):
        """测试建议生成"""
        # 设置测试数据
        self.analyzer.api_endpoints = {
            "/resource/BaseSearchResource/get/": {
                "success_count": 5,
                "response_count": 5
            }
        }
        self.analyzer.pagination_info = {
            "/resource/BaseSearchResource/get/": {
                "response_pagination": [{"bookmark": "test"}]
            }
        }
        self.analyzer.data_structures = {
            "/resource/BaseSearchResource/get/": {
                "pins_structure": {"id", "title", "images", "url", "description", "board"}
            }
        }
        
        recommendations = self.analyzer._generate_recommendations()
        
        self.assertTrue(len(recommendations) > 0)
        self.assertTrue(any("API端点" in rec for rec in recommendations))
        self.assertTrue(any("分页" in rec for rec in recommendations))
        self.assertTrue(any("pins数据结构" in rec for rec in recommendations))


class TestIntegration(unittest.TestCase):
    """集成测试"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('network_analysis.network_interceptor.Browser')
    def test_full_analysis_workflow(self, mock_browser_class):
        """测试完整的分析工作流程"""
        # 模拟浏览器行为
        mock_browser = Mock()
        mock_browser.start.return_value = True
        mock_browser.get_url.return_value = True
        mock_browser.viewport_height = 800
        mock_browser.page = Mock()
        mock_browser_class.return_value = mock_browser
        
        # 创建拦截器
        interceptor = NetworkInterceptor(output_dir=self.temp_dir)
        
        # 模拟网络请求和响应
        def simulate_network_activity():
            # 模拟请求
            mock_request = Mock()
            mock_request.url = "https://www.pinterest.com/resource/BaseSearchResource/get/?q=test"
            mock_request.method = "GET"
            mock_request.headers = {"User-Agent": "test"}
            mock_request.resource_type = "xhr"
            mock_request.post_data = None
            interceptor._handle_request(mock_request)
            
            # 模拟响应
            mock_response = Mock()
            mock_response.url = "https://www.pinterest.com/resource/BaseSearchResource/get/?q=test"
            mock_response.status = 200
            mock_response.status_text = "OK"
            mock_response.headers = {"content-type": "application/json"}
            mock_response.body.return_value = b'{"resource_response": {"data": [{"id": "123"}]}}'
            mock_response.json.return_value = {
                "resource_response": {
                    "data": [{"id": "123", "title": "Test Pin"}],
                    "bookmark": "next_token"
                }
            }
            interceptor._handle_response(mock_response)
        
        # 模拟网络活动
        simulate_network_activity()
        
        # 执行分析
        with patch.object(interceptor, 'start_analysis') as mock_start:
            mock_start.return_value = {
                "session_id": interceptor.session_id,
                "total_requests": 1,
                "total_responses": 1,
                "successful_api_responses": 1,
                "output_directory": interceptor.session_dir
            }
            
            summary = mock_start.return_value
            
            # 验证摘要
            self.assertEqual(summary["total_requests"], 1)
            self.assertEqual(summary["successful_api_responses"], 1)
            
            # 创建API分析器并分析
            analyzer = APIAnalyzer()
            
            # 创建测试数据用于分析
            interceptor._save_results()
            
            # 验证文件创建
            self.assertTrue(os.path.exists(os.path.join(interceptor.session_dir, "network_logs", "requests.json")))
            self.assertTrue(os.path.exists(os.path.join(interceptor.session_dir, "api_responses", "responses.json")))


def run_test_suite():
    """运行完整的测试套件"""
    # 创建测试套件
    suite = unittest.TestSuite()
    
    # 添加单元测试
    suite.addTest(unittest.makeSuite(TestNetworkInterceptor))
    suite.addTest(unittest.makeSuite(TestAPIAnalyzer))
    
    # 添加集成测试
    suite.addTest(unittest.makeSuite(TestIntegration))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # 返回测试结果
    return result.wasSuccessful(), result.testsRun, len(result.failures), len(result.errors)


if __name__ == "__main__":
    print("Pinterest网络分析模块测试套件")
    print("=" * 50)
    
    success, total_tests, failures, errors = run_test_suite()
    
    print("\n" + "=" * 50)
    print(f"测试结果: {'通过' if success else '失败'}")
    print(f"总测试数: {total_tests}")
    print(f"失败数: {failures}")
    print(f"错误数: {errors}")
    
    if success:
        print("\n✅ 所有测试通过！网络分析模块功能正常。")
    else:
        print("\n❌ 部分测试失败，请检查错误信息。")
    
    sys.exit(0 if success else 1) 