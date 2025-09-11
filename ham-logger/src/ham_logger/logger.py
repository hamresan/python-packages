import logging
import os
from datetime import datetime
from typing import Any


class Logger:
    """Console + daily-file logger with duplicate-handler protection.
    
    This logger creates both console and file handlers, writing to daily log files.
    It includes protection against duplicate handlers when multiple instances
    are created with the same name.
    
    Args:
        name: Logger name (used in log messages and handler identification)
        log_dir: Directory where log files will be stored
        console_level: Minimum level for console output
        file_level: Minimum level for file output
        fmt: Log message format string
        enable_console: Whether to enable console output
    """
    def __init__(
        self,
        name: str = "Logger",
        log_dir: str = "logs",
        console_level: int = logging.INFO,
        file_level: int = logging.DEBUG,
        fmt: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        enable_console: bool = True
    ) -> None:
        try:
            os.makedirs(log_dir, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise RuntimeError(f"Failed to create log directory '{log_dir}': {e}")
            
        log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

        self.logger = logging.getLogger(name)
        self.logger.setLevel(min(console_level, file_level))

        # Prevent adding handlers multiple times for the same logger name
        self.logger.propagate = False

        # console handler (idempotent)
        if enable_console:
            console_handler_name = f"{name}-console"
            if not any(h.get_name() == console_handler_name for h in self.logger.handlers):
                try:
                    ch = logging.StreamHandler()
                    ch.set_name(console_handler_name)
                    ch.setLevel(console_level)
                    ch.setFormatter(logging.Formatter(fmt))
                    self.logger.addHandler(ch)
                except Exception as e:
                    raise RuntimeError(f"Failed to create console handler: {e}")

        # file handler (idempotent)
        file_handler_name = f"{name}-file"
        if not any(h.get_name() == file_handler_name for h in self.logger.handlers):
            try:
                fh = logging.FileHandler(log_file, encoding="utf-8")
                fh.set_name(file_handler_name)
                fh.setLevel(file_level)
                fh.setFormatter(logging.Formatter(fmt))
                self.logger.addHandler(fh)
            except (OSError, PermissionError) as e:
                raise RuntimeError(f"Failed to create file handler for '{log_file}': {e}")

    # convenience passthroughs
    def debug(self, msg: str, *args, **kwargs) -> None:
        """Log a debug message."""
        self.logger.debug(msg, *args, **kwargs)
        
    def info(self, msg: str, *args, **kwargs) -> None:
        """Log an info message."""
        self.logger.info(msg, *args, **kwargs)
        
    def warning(self, msg: str, *args, **kwargs) -> None:
        """Log a warning message."""
        self.logger.warning(msg, *args, **kwargs)
        
    def error(self, msg: str, *args, **kwargs) -> None:
        """Log an error message."""
        self.logger.error(msg, *args, **kwargs)
        
    def critical(self, msg: str, *args, **kwargs) -> None:
        """Log a critical message."""
        self.logger.critical(msg, *args, **kwargs)

    def get_log_file_path(self) -> str:
        """Get the current log file path.
        
        Returns:
            Path to the current day's log file
        """
        for handler in self.logger.handlers:
            if isinstance(handler, logging.FileHandler):
                return handler.baseFilename
        return ""

    def set_level(self, level: int) -> None:
        """Set the logger level.
        
        Args:
            level: Logging level (e.g., logging.DEBUG, logging.INFO)
        """
        self.logger.setLevel(level)