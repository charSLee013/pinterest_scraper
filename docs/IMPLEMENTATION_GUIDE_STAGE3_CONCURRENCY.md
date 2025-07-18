# Stage 3 Concurrent Processing Implementation Guide

## Overview
This guide provides step-by-step instructions for implementing concurrent processing in Stage 3 (Pin Detail Enhancement) of the Pinterest Scraper --only-images workflow.

## Current Problem
- **Stage 3 Performance**: 48,382 pins processed sequentially in ~67 hours
- **Bottleneck**: Single-threaded Pin detail fetching with 2-second delays
- **Target**: Reduce processing time to 4-8 hours (8-16x improvement)

## Architecture Solution
```
Browser Instance (Headers) → Shared Headers → Multi-threaded Requests → Queue → Single-threaded DB
     (One-time)                 (Thread-safe)      (Concurrent)         (Atomic)    (Transactions)
```

## Implementation Steps

### Step 1: Create SharedHeadersManager
**File**: `src/tools/shared_headers_manager.py` (NEW)

```python
import threading
import asyncio
from typing import Dict, Optional
from loguru import logger

class SharedHeadersManager:
    def __init__(self):
        self.headers = None
        self.headers_lock = threading.Lock()
        self.headers_ready = threading.Event()
    
    async def initialize_headers(self, header_manager):
        """Extract headers from browser session once"""
        try:
            headers_ready = await header_manager.ensure_headers_ready()
            if headers_ready:
                self.headers = self._extract_headers_from_session()
                logger.info(f"Headers extracted: {len(self.headers)} fields")
            else:
                self.headers = self._get_default_headers()
                logger.warning("Using default headers")
            
            self.headers_ready.set()
        except Exception as e:
            logger.error(f"Header extraction failed: {e}")
            self.headers = self._get_default_headers()
            self.headers_ready.set()
    
    def get_headers(self) -> Dict[str, str]:
        """Thread-safe header access"""
        self.headers_ready.wait()
        with self.headers_lock:
            return self.headers.copy()
```

### Step 2: Create ConcurrentPinDetailFetcher
**File**: `src/tools/concurrent_pin_fetcher.py` (NEW)

```python
import requests
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from tqdm import tqdm
from loguru import logger

class ConcurrentPinDetailFetcher:
    def __init__(self, max_concurrent: int, shared_headers):
        self.max_concurrent = max_concurrent
        self.shared_headers = shared_headers
        self.session_pool = self._create_session_pool()
    
    def _create_session_pool(self):
        """Create requests session pool"""
        sessions = []
        for i in range(self.max_concurrent):
            session = requests.Session()
            session.timeout = 30
            sessions.append(session)
        return sessions
    
    def fetch_pin_details_concurrent(self, pin_ids: List[str]) -> List[Dict]:
        """Concurrent Pin detail fetching"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            future_to_pin = {
                executor.submit(
                    self._fetch_single_pin, 
                    pin_id, 
                    self.session_pool[i % len(self.session_pool)]
                ): pin_id
                for i, pin_id in enumerate(pin_ids)
            }
            
            with tqdm(total=len(pin_ids), desc="Fetching Pin details", unit="pin") as pbar:
                for future in as_completed(future_to_pin):
                    pin_id = future_to_pin[future]
                    try:
                        pin_data = future.result()
                        if pin_data:
                            results.append(pin_data)
                    except Exception as e:
                        logger.debug(f"Pin {pin_id} fetch failed: {e}")
                    finally:
                        pbar.update(1)
        
        return results
    
    def _fetch_single_pin(self, pin_id: str, session: requests.Session) -> Optional[Dict]:
        """Fetch single Pin using requests"""
        headers = self.shared_headers.get_headers()
        
        try:
            # Pinterest API request
            url = "https://www.pinterest.com/resource/PinResource/get/"
            params = {
                'source_url': f'/pin/{pin_id}/',
                'data': json.dumps({
                    'options': {
                        'field_set_key': 'detailed',
                        'id': pin_id
                    }
                })
            }
            
            response = session.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            return self._parse_pin_data(data, pin_id)
            
        except Exception as e:
            logger.debug(f"Request failed for Pin {pin_id}: {e}")
            return None
```

### Step 3: Create AtomicDatabaseWriter
**File**: `src/tools/atomic_database_writer.py` (NEW)

```python
import queue
import threading
import time
from typing import Dict
from loguru import logger
from ..core.database.repository import SQLiteRepository

class AtomicDatabaseWriter:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.write_queue = queue.Queue()
        self.writer_thread = None
        self.stop_event = threading.Event()
        self.stats = {"written": 0, "failed": 0}
        self.stats_lock = threading.Lock()
    
    def start_writer_thread(self):
        """Start single-threaded database writer"""
        self.writer_thread = threading.Thread(target=self._database_writer_loop)
        self.writer_thread.daemon = True
        self.writer_thread.start()
    
    def stop_writer_thread(self):
        """Stop database writer"""
        self.stop_event.set()
        if self.writer_thread:
            self.writer_thread.join(timeout=30)
    
    def queue_pin_update(self, pin_data: Dict, keyword: str):
        """Queue Pin data for atomic database update"""
        self.write_queue.put((pin_data, keyword))
    
    def _database_writer_loop(self):
        """Single-threaded database writer loop"""
        while not self.stop_event.is_set():
            try:
                pin_data, keyword = self.write_queue.get(timeout=1.0)
                success = self._write_pin_atomic(pin_data, keyword)
                
                with self.stats_lock:
                    if success:
                        self.stats["written"] += 1
                    else:
                        self.stats["failed"] += 1
                
                self.write_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Database writer error: {e}")
    
    def _write_pin_atomic(self, pin_data: Dict, keyword: str) -> bool:
        """Atomic Pin data update"""
        try:
            repository = SQLiteRepository(keyword=keyword, output_dir=self.output_dir)
            
            with repository._get_session() as session:
                from ..core.database.schema import Pin
                
                session.begin()
                try:
                    existing_pin = session.query(Pin).filter_by(id=pin_data['id']).first()
                    
                    if existing_pin:
                        for key, value in pin_data.items():
                            if hasattr(existing_pin, key) and value:
                                setattr(existing_pin, key, value)
                        
                        session.commit()
                        return True
                    else:
                        session.rollback()
                        return False
                        
                except Exception as e:
                    session.rollback()
                    logger.error(f"Transaction failed for Pin {pin_data['id']}: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Database operation failed: {e}")
            return False
```

### Step 4: Modify PinEnhancementStage
**File**: `src/tools/stage_implementations.py` (MODIFY)

```python
# Add imports
from .shared_headers_manager import SharedHeadersManager
from .concurrent_pin_fetcher import ConcurrentPinDetailFetcher
from .atomic_database_writer import AtomicDatabaseWriter

class PinEnhancementStage(StageManager):
    def __init__(self, output_dir: str, max_concurrent: int = 8):
        super().__init__("Pin详情数据补全", output_dir)
        self.max_concurrent = max(1, min(max_concurrent, 20))
        logger.info(f"Pin详情补全并发数: {self.max_concurrent}")
    
    async def _execute_stage(self, target_keyword: Optional[str] = None) -> Dict[str, Any]:
        """Execute concurrent Pin detail enhancement"""
        logger.info("📥 开始Pin详情数据补全阶段（并发模式）")
        
        # Initialize components
        shared_headers = SharedHeadersManager()
        header_manager = GlobalHeaderManager(self.output_dir)
        await shared_headers.initialize_headers(header_manager)
        
        fetcher = ConcurrentPinDetailFetcher(self.max_concurrent, shared_headers)
        db_writer = AtomicDatabaseWriter(self.output_dir)
        db_writer.start_writer_thread()
        
        try:
            # Process keywords
            keywords = [target_keyword] if target_keyword else self._discover_all_keywords()
            
            for keyword in keywords:
                pins_to_enhance = await self._get_pins_needing_enhancement(keyword)
                
                if not pins_to_enhance:
                    continue
                
                logger.info(f"📊 关键词 {keyword}: 发现 {len(pins_to_enhance)} 个需要增强的Pin")
                
                # Concurrent processing
                await self._enhance_pins_concurrent(pins_to_enhance, keyword, fetcher, db_writer)
            
            # Wait for database writes to complete
            db_writer.write_queue.join()
            
        finally:
            db_writer.stop_writer_thread()
    
    async def _enhance_pins_concurrent(self, pins: List[Dict], keyword: str,
                                     fetcher: ConcurrentPinDetailFetcher,
                                     db_writer: AtomicDatabaseWriter):
        """Concurrent Pin enhancement"""
        pin_ids = [pin['id'] for pin in pins if not self._has_valid_image_urls(pin)]
        
        if not pin_ids:
            return
        
        logger.info(f"开始并发获取 {len(pin_ids)} 个Pin的详情数据")
        
        # Concurrent fetch
        enhanced_pins = fetcher.fetch_pin_details_concurrent(pin_ids)
        
        # Queue for atomic database updates
        for pin_data in enhanced_pins:
            if pin_data and pin_data.get('image_urls'):
                db_writer.queue_pin_update(pin_data, keyword)
```

### Step 5: Integration with main.py
**File**: `main.py` (MODIFY)

```python
# In the --only-images workflow section
pin_enhancement_stage = PinEnhancementStage(
    output_dir=args.output_dir,
    max_concurrent=args.max_concurrent  # Pass concurrent parameter
)
```

## Testing and Validation

### Performance Testing
1. **Baseline Measurement**: Test current single-threaded performance
2. **Concurrent Testing**: Test with different concurrent values (4, 8, 16)
3. **Resource Monitoring**: Monitor memory and CPU usage
4. **Data Integrity**: Verify all Pin data is correctly updated

### Integration Testing
1. **Complete Workflow**: Test full --only-images workflow
2. **Stage Transitions**: Verify Stage 3 → Stage 4 transition
3. **Interrupt Handling**: Test Ctrl+C during concurrent processing
4. **Error Recovery**: Test handling of network errors and failures

## Expected Results
- **Processing Time**: 67 hours → 4-8 hours
- **Performance Improvement**: 8-16x faster
- **Concurrency**: 1-20 threads supported
- **Data Integrity**: 100% maintained
- **Resource Usage**: <2GB memory, reasonable CPU

## Rollback Plan
If issues occur:
1. Revert to single-threaded processing in `PinEnhancementStage`
2. Use existing `SmartPinEnhancer` implementation
3. Maintain current performance until issues resolved

This implementation guide provides the complete roadmap for implementing concurrent Pin detail enhancement while maintaining data integrity and system stability.
