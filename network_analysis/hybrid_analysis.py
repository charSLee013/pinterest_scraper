import logging

logger = logging.getLogger(__name__)

def fetch_all_pins_via_api(self):
    response = self.session.get(self.api_url)
    response.raise_for_status() # 检查HTTP错误

    logger.info("Attempting to parse API response JSON...") # 新增日志
    json_data = response.json()
    logger.debug(f"Received API response JSON (raw): {json_data}") # 简化日志

    # 提取pins数据和新的bookmark
    new_pins = []

    # ... existing code ...

    return new_pins 