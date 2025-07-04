import unittest
from unittest.mock import MagicMock
import os
import sys

# Add project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from browser import Browser
import parser
import config

class TestBrowserIntegration(unittest.TestCase):

    def setUp(self):
        """Set up for the integration test."""
        self.browser = Browser(timeout=30)
        self.assertTrue(self.browser.start(), "浏览器启动失败")

    def tearDown(self):
        """Tear down the test."""
        self.browser.stop()

    def test_scroll_and_extract_integration(self):
        """
        Integration test for the simple_scroll_and_extract method.
        This test navigates to a real Pinterest search page and tries to scrape a small number of pins.
        """
        # A simple search URL
        test_url = "https://www.pinterest.com/search/pins/?q=nature"
        target_count = 10  # A small target to keep the test fast
        
        # Navigate to the URL
        self.assertTrue(self.browser.get_url(test_url), f"无法访问URL: {test_url}")
        
        # Wait for initial content to load
        load_success = self.browser.wait_for_element("div[data-test-id='pin-card']", timeout=20)
        
        if not load_success:
            # Save debug info if loading fails
            debug_dir = os.path.join(os.path.dirname(__file__), 'debug')
            os.makedirs(debug_dir, exist_ok=True)
            screenshot_path = os.path.join(debug_dir, 'test_failure_screenshot.png')
            html_path = os.path.join(debug_dir, 'test_failure_page.html')
            
            self.browser.take_screenshot(screenshot_path)
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(self.browser.get_page_source())
            
            self.fail(f"页面初始内容加载超时。调试截图和HTML已保存到: {debug_dir}")

        # Call the function to be tested
        pins = self.browser.simple_scroll_and_extract(
            target_count=target_count,
            extract_func=parser.extract_pins_from_html,
            new_item_selector="div[data-test-id='pin-card']"
        )

        # Assertions
        self.assertIsNotNone(pins, "提取结果不应为 None")
        self.assertIsInstance(pins, list, "提取结果应为列表")
        self.assertGreaterEqual(len(pins), target_count, f"获取的pin数量不足, 预期: {target_count}, 实际: {len(pins)}")
        
        # Check a few pins for basic structure
        if pins:
            for i in range(min(3, len(pins))):
                pin = pins[i]
                self.assertIn("id", pin)
                self.assertIn("url", pin)
                self.assertTrue(pin["id"], f"第 {i+1} 个pin缺少ID")

if __name__ == '__main__':
    unittest.main() 