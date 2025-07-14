# Pinterest Scraper 技术架构

## 系统架构概览

Pinterest Scraper v4.0 采用现代化的异步架构，以SQLite为存储基础，实现了真正的断点续传功能。

```
┌─────────────────────────────────────────────────────────────┐
│                    Pinterest Scraper v4.0                   │
├─────────────────────────────────────────────────────────────┤
│  Command Line Interface (main.py)                          │
├─────────────────────────────────────────────────────────────┤
│  PinterestScraper (核心协调器)                              │
│  ├── 会话恢复检查                                           │
│  ├── 增量采集逻辑                                           │
│  └── 断点续传管理                                           │
├─────────────────────────────────────────────────────────────┤
│  SmartScraper (智能采集引擎)                                │
│  ├── 基于数据库的状态管理                                   │
│  ├── 实时保存机制                                           │
│  └── 混合采集策略                                           │
├─────────────────────────────────────────────────────────────┤
│  数据层                                                     │
│  ├── SQLiteRepository (数据访问层)                         │
│  ├── 实时UPSERT操作                                        │
│  └── 会话状态管理                                           │
├─────────────────────────────────────────────────────────────┤
│  SQLite Database (存储基础)                                │
│  ├── Pins Table (Pin数据)                                  │
│  ├── ScrapingSessions Table (会话管理)                     │
│  └── DownloadTasks Table (下载任务)                        │
└─────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. PinterestScraper (核心协调器)
- **职责**: 整体流程控制、会话管理、断点续传
- **关键功能**:
  - 会话恢复检查 (`_check_and_resume_session`)
  - 增量采集逻辑
  - 用户交互界面
  - 资源管理和清理

### 2. SmartScraper (智能采集引擎)
- **职责**: 数据采集、实时保存、去重处理
- **关键功能**:
  - 基于数据库的状态管理
  - 实时Pin保存 (`_save_pins_to_db`)
  - 基准数量跟踪 (`_baseline_count`)
  - 混合采集策略

### 3. SQLiteRepository (数据访问层)
- **职责**: 数据库操作、原子化保存、会话管理
- **关键功能**:
  - 原子化Pin保存 (`AtomicPinSaver`)
  - 数据标准化 (`PinDataNormalizer`)
  - 会话状态管理 (`get_incomplete_sessions`, `resume_session`)
  - 数据去重 (基于INSERT OR REPLACE)
  - 并发安全保证
  - SQLAlchemy错误修复

## 断点续传机制

### 1. 会话生命周期
```
创建会话 → 运行中 → [中断] → 恢复检查 → 继续/新建
   ↓         ↓        ↓         ↓         ↓
running → running → interrupted → running → completed
```

### 2. 数据流程（修复后）
```
第一阶段: Pin采集 → 数据标准化 → 原子化保存 → 状态更新
第二阶段: Pin采集 → 数据标准化 → 原子化保存 → 状态更新
   ↓         ↓           ↓           ↓
Browser → Normalizer → AtomicSaver → SQLite
```

**🔥 重要修复**: 第二阶段现在也启用了实时保存机制，确保数据零丢失。

### 3. 恢复逻辑
1. **检测未完成会话**: 查询status='interrupted'或'running'的会话
2. **计算已有数据**: 统计数据库中已保存的Pin数量
3. **用户确认**: 显示进度信息，询问是否继续
4. **增量采集**: 只采集剩余需要的数量

## 内存优化架构

### v4.0 vs v3.x 对比

| 方面 | v3.x | v4.0 |
|------|------|------|
| 数据存储 | 内存累积 + 批量保存 | 实时数据库保存 |
| 内存使用 | O(n) 线性增长 | O(1) 常量级别 |
| 数据安全 | 中断时丢失 | 零数据丢失 |
| 去重机制 | 内存Set检查 | 原子化INSERT OR REPLACE |
| 断点续传 | 不支持 | 完全支持 |
| 数据一致性 | 无保证 | 原子化操作保证 |

### 内存优化策略
1. **移除内存累积**: 不再保存`collected_pins`列表
2. **实时数据库操作**: 每个Pin立即保存到数据库
3. **基准数量跟踪**: 只记录采集开始时的基准数量
4. **状态查询**: 通过数据库查询获取当前状态

## 数据库设计

### 核心表结构

#### Pins表
```sql
CREATE TABLE pins (
    id TEXT PRIMARY KEY,
    title TEXT,
    description TEXT,
    largest_image_url TEXT,
    query TEXT,
    -- ... 其他字段
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### ScrapingSessions表
```sql
CREATE TABLE scraping_sessions (
    id TEXT PRIMARY KEY,
    query TEXT,
    target_count INTEGER,
    actual_count INTEGER,
    status TEXT, -- 'running', 'completed', 'failed', 'interrupted'
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);
```

### UPSERT机制
```sql
INSERT OR REPLACE INTO pins (id, title, ...) VALUES (?, ?, ...);
```

## 性能特性

### 1. 并发安全
- 进程锁机制防止多进程冲突
- SQLite WAL模式支持并发读写
- 事务边界优化

### 2. 内存效率
- 常量级内存使用
- 支持大规模数据采集
- 垃圾回收友好

### 3. 数据安全
- 实时保存，零数据丢失
- 事务保证数据一致性
- 自动错误恢复

## 扩展性设计

### 1. 模块化架构
- 清晰的职责分离
- 接口抽象和实现分离
- 易于测试和维护

### 2. 配置化设计
- 数据库连接可配置
- 采集策略可调整
- 性能参数可优化

### 3. 向后兼容
- API接口保持稳定
- 数据库结构兼容
- 配置文件兼容

## 最佳实践

### 1. 大规模采集
```python
# 支持采集数万个Pin
scraper = PinterestScraper()
pins = await scraper.scrape(query="nature", count=50000)
```

### 2. 断点续传
```python
# 中断后继续
# 系统会自动检测并询问是否继续
pins = await scraper.scrape(query="nature", count=1000)
```

### 3. 增量采集
```python
# 逐步增加目标数量
await scraper.scrape(query="cats", count=100)  # 第一次
await scraper.scrape(query="cats", count=500)  # 增量采集400个
```

## 故障排除

### 1. 常见问题
- **内存不足**: v4.0已解决，支持大规模采集
- **数据丢失**: 实时保存机制确保零数据丢失
- **重复数据**: 数据库UPSERT机制自动去重

### 2. 调试模式
```bash
uv run python main.py -q "test" -c 10 --debug
```

### 3. 数据库检查
```python
from src.core.database.repository import SQLiteRepository
repo = SQLiteRepository()
pins = repo.load_pins_by_query("your_query")
print(f"已采集: {len(pins)} 个Pin")
```
