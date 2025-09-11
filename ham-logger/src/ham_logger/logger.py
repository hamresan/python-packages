import logging
import os
from datetime import datetime

class Logger:
    """Console + daily-file logger with duplicate-handler protection."""
    def __init__(
        self,
        name: str = "Logger",
        log_dir: str = "logs",
        console_level: int = logging.INFO,
        file_level: int = logging.DEBUG,
        fmt: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        enable_console: bool = True,  

    ) -> None:
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

        self.logger = logging.getLogger(name)
        self.logger.setLevel(min(console_level, file_level))

        # console handler (idempotent)
        if enable_console:
            if not any(h.get_name() == f"{name}-console" for h in self.logger.handlers):
                ch = logging.StreamHandler()
                ch.set_name(f"{name}-console")
                ch.setLevel(console_level)
                ch.setFormatter(logging.Formatter(fmt))
                self.logger.addHandler(ch)


        # file handler (idempotent)
        if not any(h.get_name() == f"{name}-file" for h in self.logger.handlers):
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.set_name(f"{name}-file")
            fh.setLevel(file_level)
            fh.setFormatter(logging.Formatter(fmt))
            self.logger.addHandler(fh)

    # convenience passthroughs
    def debug(self, msg: str, *args, **kwargs) -> None: self.logger.debug(msg, *args, **kwargs)
    def info(self, msg: str, *args, **kwargs) -> None: self.logger.info(msg, *args, **kwargs)
    def warning(self, msg: str, *args, **kwargs) -> None: self.logger.warning(msg, *args, **kwargs)
    def error(self, msg: str, *args, **kwargs) -> None: self.logger.error(msg, *args, **kwargs)
    def critical(self, msg: str, *args, **kwargs) -> None: self.logger.critical(msg, *args, **kwargs)