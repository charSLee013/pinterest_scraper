#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
进程管理器 - 防止多实例同时运行
"""

import os
import atexit
import time
from pathlib import Path
from typing import Optional

from loguru import logger

# Windows和Unix系统的文件锁实现
try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    # Windows系统没有fcntl，使用msvcrt
    try:
        import msvcrt
        HAS_MSVCRT = True
        HAS_FCNTL = False
    except ImportError:
        HAS_FCNTL = False
        HAS_MSVCRT = False


class ProcessManager:
    """进程管理器，防止多实例同时运行"""
    
    def __init__(self, work_name: str, output_dir: str):
        """初始化进程管理器
        
        Args:
            work_name: 工作名称（通常是关键词）
            output_dir: 输出目录
        """
        self.work_name = work_name
        self.output_dir = Path(output_dir)
        self.lock_file_path = self.output_dir / f".{work_name}.lock"
        self.lock_file: Optional[object] = None
        self.is_locked = False
        
        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def acquire_lock(self) -> bool:
        """获取进程锁
        
        Returns:
            是否成功获取锁
        """
        try:
            # 检查是否已经有锁
            if self.is_locked:
                logger.warning(f"进程锁已经被当前实例持有: {self.work_name}")
                return True
            
            # 尝试获取锁
            if HAS_FCNTL:
                return self._acquire_lock_unix()
            elif HAS_MSVCRT:
                return self._acquire_lock_windows()
            else:
                # 降级到简单的文件存在检查
                return self._acquire_lock_simple()
                
        except Exception as e:
            logger.error(f"获取进程锁时发生错误: {e}")
            return False
    
    def _acquire_lock_unix(self) -> bool:
        """Unix系统的文件锁实现"""
        try:
            self.lock_file = open(self.lock_file_path, 'w')
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # 写入进程信息
            self.lock_file.write(f"{os.getpid()}\n")
            self.lock_file.write(f"{time.time()}\n")
            self.lock_file.flush()
            
            self.is_locked = True
            
            # 注册退出时清理
            atexit.register(self.release_lock)
            
            logger.info(f"获取进程锁成功 (Unix): {self.work_name}")
            return True
            
        except (IOError, OSError) as e:
            if self.lock_file:
                self.lock_file.close()
                self.lock_file = None
            
            # 检查是否是锁被占用的错误
            if "Resource temporarily unavailable" in str(e) or "already locked" in str(e):
                logger.warning(f"无法获取进程锁，可能有其他实例正在运行: {self.work_name}")
            else:
                logger.error(f"获取进程锁失败: {e}")
            return False
    
    def _acquire_lock_windows(self) -> bool:
        """Windows系统的文件锁实现"""
        try:
            # 检查锁文件是否已存在
            if self.lock_file_path.exists():
                # 尝试读取并验证进程是否还在运行
                try:
                    with open(self.lock_file_path, 'r') as f:
                        lines = f.readlines()
                        if len(lines) >= 2:
                            pid = int(lines[0].strip())
                            if self._is_process_running(pid):
                                logger.warning(f"检测到其他实例正在运行: PID={pid}, 工作名称={self.work_name}")
                                return False
                            else:
                                # 进程已不存在，删除僵尸锁文件
                                logger.warning(f"发现僵尸锁文件，删除: {self.lock_file_path}")
                                self.lock_file_path.unlink()
                except Exception:
                    # 如果读取失败，删除损坏的锁文件
                    try:
                        self.lock_file_path.unlink()
                    except:
                        pass

            # 创建新的锁文件
            self.lock_file = open(self.lock_file_path, 'w')

            # 写入进程信息
            self.lock_file.write(f"{os.getpid()}\n")
            self.lock_file.write(f"{time.time()}\n")
            self.lock_file.flush()

            self.is_locked = True

            # 注册退出时清理
            atexit.register(self.release_lock)

            logger.info(f"获取进程锁成功 (Windows): {self.work_name}")
            return True

        except (IOError, OSError) as e:
            if self.lock_file:
                try:
                    self.lock_file.close()
                except:
                    pass
                self.lock_file = None

            logger.warning(f"无法获取进程锁，可能有其他实例正在运行: {self.work_name}")
            return False
    
    def _acquire_lock_simple(self) -> bool:
        """简单的文件存在检查（降级方案）"""
        try:
            if self.lock_file_path.exists():
                # 检查锁文件是否过期（超过1小时认为是僵尸锁）
                if time.time() - self.lock_file_path.stat().st_mtime > 3600:
                    logger.warning(f"发现过期锁文件，删除: {self.lock_file_path}")
                    self.lock_file_path.unlink()
                else:
                    logger.warning(f"发现锁文件，可能有其他实例正在运行: {self.work_name}")
                    return False
            
            # 创建锁文件
            with open(self.lock_file_path, 'w') as f:
                f.write(f"{os.getpid()}\n")
                f.write(f"{time.time()}\n")
            
            self.is_locked = True
            
            # 注册退出时清理
            atexit.register(self.release_lock)
            
            logger.info(f"获取进程锁成功 (Simple): {self.work_name}")
            return True
            
        except Exception as e:
            logger.error(f"简单锁获取失败: {e}")
            return False
    
    def release_lock(self):
        """释放进程锁"""
        if not self.is_locked:
            return

        try:
            # 先关闭文件句柄
            if self.lock_file:
                try:
                    if HAS_FCNTL:
                        fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                    # Windows不需要显式解锁，关闭文件即可
                    self.lock_file.close()
                except Exception as e:
                    logger.debug(f"关闭锁文件时出错: {e}")
                finally:
                    self.lock_file = None

            # 删除锁文件
            try:
                if self.lock_file_path.exists():
                    self.lock_file_path.unlink()
            except Exception as e:
                logger.debug(f"删除锁文件时出错: {e}")

            self.is_locked = False
            logger.debug(f"释放进程锁: {self.work_name}")

        except Exception as e:
            logger.error(f"释放进程锁失败: {e}")
    
    def is_another_instance_running(self) -> bool:
        """检查是否有其他实例正在运行
        
        Returns:
            是否有其他实例运行
        """
        if not self.lock_file_path.exists():
            return False
        
        try:
            # 尝试读取锁文件信息
            with open(self.lock_file_path, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    pid = int(lines[0].strip())
                    timestamp = float(lines[1].strip())
                    
                    # 检查进程是否还在运行
                    if self._is_process_running(pid):
                        logger.info(f"检测到其他实例正在运行: PID={pid}, 工作名称={self.work_name}")
                        return True
                    else:
                        # 进程已经不存在，删除僵尸锁文件
                        logger.warning(f"发现僵尸锁文件，删除: {self.lock_file_path}")
                        self.lock_file_path.unlink()
                        return False
        
        except Exception as e:
            logger.error(f"检查其他实例时发生错误: {e}")
            return False
        
        return False
    
    def _is_process_running(self, pid: int) -> bool:
        """检查指定PID的进程是否还在运行
        
        Args:
            pid: 进程ID
            
        Returns:
            进程是否在运行
        """
        try:
            if os.name == 'nt':  # Windows
                import subprocess
                result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'], 
                                      capture_output=True, text=True)
                return str(pid) in result.stdout
            else:  # Unix/Linux
                os.kill(pid, 0)  # 发送信号0检查进程是否存在
                return True
        except (OSError, subprocess.SubprocessError):
            return False
    
    def __enter__(self):
        """上下文管理器入口"""
        if self.acquire_lock():
            return self
        else:
            raise RuntimeError(f"无法获取进程锁: {self.work_name}")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.release_lock()
