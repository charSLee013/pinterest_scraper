#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Windows兼容的单行进度显示器

专门为Windows PowerShell环境设计的进度显示组件，
完全避免使用logging系统，确保真正的单行更新。
"""

import sys
import time
import threading
from typing import Optional, Dict, Any


class WindowsProgressDisplay:
    """Windows兼容的单行进度显示器
    
    特点：
    1. 使用stdout直接输出，不依赖logging系统
    2. 使用\r回车符实现单行更新
    3. 针对Windows PowerShell优化
    4. 线程安全的进度更新
    """
    
    def __init__(self, total: int, desc: str = "进度", unit: str = "item"):
        """初始化进度显示器
        
        Args:
            total: 总数量
            desc: 描述文本
            unit: 单位名称
        """
        self.total = total
        self.desc = desc
        self.unit = unit
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.lock = threading.Lock()
        self._closed = False
        
        # Windows PowerShell优化设置
        self.bar_length = 25  # 较短的进度条，适合PowerShell
        self.update_interval = 0.5  # 最小更新间隔（秒）
        
        # 显示初始进度
        self._display_progress()
    
    def update(self, n: int = 1):
        """更新进度
        
        Args:
            n: 增加的数量
        """
        if self._closed:
            return
            
        with self.lock:
            self.current = min(self.current + n, self.total)
            current_time = time.time()
            
            # 控制更新频率，避免过于频繁的刷新
            if (current_time - self.last_update_time) >= self.update_interval or self.current >= self.total:
                self._display_progress()
                self.last_update_time = current_time
    
    def set_current(self, value: int):
        """设置当前进度值
        
        Args:
            value: 当前进度值
        """
        if self._closed:
            return
            
        with self.lock:
            self.current = min(max(value, 0), self.total)
            self._display_progress()
    
    def _display_progress(self):
        """显示进度条（内部方法）"""
        if self._closed:
            return
            
        # 计算进度百分比
        if self.total > 0:
            progress = self.current / self.total
            percentage = progress * 100
        else:
            progress = 0
            percentage = 0
        
        # 创建进度条
        filled_length = int(self.bar_length * progress)
        bar = '█' * filled_length + '░' * (self.bar_length - filled_length)
        
        # 计算速度和剩余时间
        elapsed_time = time.time() - self.start_time
        if elapsed_time > 0 and self.current > 0:
            rate = self.current / elapsed_time
            if rate > 0 and self.current < self.total:
                remaining_items = self.total - self.current
                eta_seconds = remaining_items / rate
                eta_str = self._format_time(eta_seconds)
            else:
                eta_str = "00:00"
        else:
            rate = 0
            eta_str = "计算中"
        
        # 格式化速度
        if rate >= 1:
            rate_str = f"{rate:.1f}{self.unit}/s"
        else:
            rate_str = f"{rate:.2f}{self.unit}/s"
        
        # 构建进度字符串
        progress_str = (
            f"\r{self.desc}: {percentage:5.1f}% |{bar}| "
            f"{self.current:,}/{self.total:,} [{rate_str}, 剩余:{eta_str}]"
        )
        
        # 确保字符串不会太长，避免换行
        max_width = 100  # PowerShell窗口通常较窄
        if len(progress_str) > max_width:
            # 截断描述部分
            desc_limit = max(10, max_width - 70)  # 为其他部分预留70个字符
            short_desc = self.desc[:desc_limit] + "..." if len(self.desc) > desc_limit else self.desc
            progress_str = (
                f"\r{short_desc}: {percentage:4.0f}% |{bar}| "
                f"{self.current:,}/{self.total:,} [{rate_str}]"
            )
        
        # 输出进度条（不换行）
        try:
            sys.stdout.write(progress_str)
            sys.stdout.flush()
        except Exception:
            # 如果输出失败，静默忽略
            pass
    
    def _format_time(self, seconds: float) -> str:
        """格式化时间显示
        
        Args:
            seconds: 秒数
            
        Returns:
            格式化的时间字符串
        """
        if seconds < 60:
            return f"{int(seconds):02d}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes:02d}:{secs:02d}"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h{minutes:02d}m"
    
    def close(self):
        """关闭进度显示器"""
        if self._closed:
            return
            
        with self.lock:
            self._closed = True
            # 确保进度条显示100%
            if self.current < self.total:
                self.current = self.total
                self._display_progress()
            
            # 换行，结束进度条显示
            try:
                sys.stdout.write('\n')
                sys.stdout.flush()
            except Exception:
                pass
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()


class SimpleProgressLogger:
    """简单的进度日志记录器
    
    当不需要实时进度条时，使用这个类进行阶段性进度报告
    """
    
    def __init__(self, total: int, desc: str = "处理", report_interval: int = 100):
        """初始化进度记录器
        
        Args:
            total: 总数量
            desc: 描述文本
            report_interval: 报告间隔
        """
        self.total = total
        self.desc = desc
        self.report_interval = report_interval
        self.current = 0
        self.start_time = time.time()
        self.last_report = 0
    
    def update(self, n: int = 1):
        """更新进度并在需要时输出日志
        
        Args:
            n: 增加的数量
        """
        self.current += n
        
        # 检查是否需要报告进度
        if (self.current - self.last_report >= self.report_interval or 
            self.current >= self.total):
            
            self._report_progress()
            self.last_report = self.current
    
    def _report_progress(self):
        """报告当前进度"""
        if self.total > 0:
            percentage = (self.current / self.total) * 100
            elapsed_time = time.time() - self.start_time
            
            if elapsed_time > 0:
                rate = self.current / elapsed_time
                remaining = self.total - self.current
                eta = remaining / rate if rate > 0 else 0
                eta_str = f"{eta/60:.1f}分钟" if eta > 60 else f"{eta:.0f}秒"
            else:
                rate = 0
                eta_str = "计算中"
            
            # 使用print而不是logger，避免日志格式化
            print(f"📊 {self.desc}: {percentage:.1f}% ({self.current:,}/{self.total:,}) "
                  f"速度: {rate:.1f}/s 预计剩余: {eta_str}")


def create_progress_display(total: int, desc: str = "进度", use_bar: bool = True) -> Any:
    """创建适合的进度显示器
    
    Args:
        total: 总数量
        desc: 描述文本
        use_bar: 是否使用进度条
        
    Returns:
        进度显示器实例
    """
    if use_bar and total > 0:
        return WindowsProgressDisplay(total, desc)
    else:
        return SimpleProgressLogger(total, desc)
