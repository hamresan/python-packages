"""Comprehensive tests for ham-logger package."""

import logging
import os
import tempfile
import shutil
from unittest.mock import patch
import pytest
from ham_logger import Logger


def test_import():
    """Test that Logger can be imported."""
    from ham_logger import Logger  # noqa: F401


class TestLogger:
    """Test cases for the Logger class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if hasattr(self, 'temp_dir'):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_basic_logging(self):
        """Test basic logging functionality."""
        logger = Logger(name="test", log_dir=self.temp_dir)
        
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")
        
        # Check if log file exists and contains messages
        log_file_path = logger.get_log_file_path()
        assert os.path.exists(log_file_path)
        
        with open(log_file_path, 'r') as f:
            content = f.read()
            assert "Info message" in content
            assert "Warning message" in content
            assert "Error message" in content
            assert "Debug message" in content  # File level is DEBUG by default
            assert "Critical message" in content

    def test_console_disabled(self):
        """Test logger with console output disabled."""
        logger = Logger(name="no_console", log_dir=self.temp_dir, enable_console=False)
        
        # Should have only file handler, no console handler
        console_handlers = [
            h for h in logger.logger.handlers 
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        ]
        assert len(console_handlers) == 0
        
        # Should still have file handler
        file_handlers = [
            h for h in logger.logger.handlers 
            if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 1

    def test_custom_levels(self):
        """Test logger with custom console and file levels."""
        logger = Logger(
            name="custom_levels", 
            log_dir=self.temp_dir,
            console_level=logging.ERROR,
            file_level=logging.WARNING
        )
        
        logger.debug("Debug message")
        logger.info("Info message") 
        logger.warning("Warning message")
        logger.error("Error message")
        
        # Check file contains warning and above
        log_file_path = logger.get_log_file_path()
        with open(log_file_path, 'r') as f:
            content = f.read()
            assert "Warning message" in content
            assert "Error message" in content
            assert "Debug message" not in content
            assert "Info message" not in content

    def test_duplicate_handler_protection(self):
        """Test that creating multiple loggers with same name doesn't duplicate handlers."""
        logger1 = Logger(name="same_name", log_dir=self.temp_dir)
        initial_handler_count = len(logger1.logger.handlers)
        
        logger2 = Logger(name="same_name", log_dir=self.temp_dir)
        
        # Should not add duplicate handlers
        assert len(logger2.logger.handlers) == initial_handler_count

    def test_different_names_different_handlers(self):
        """Test that loggers with different names have separate handlers."""
        logger1 = Logger(name="logger1", log_dir=self.temp_dir)
        logger2 = Logger(name="logger2", log_dir=self.temp_dir)
        
        # Both should have their own handlers
        assert len(logger1.logger.handlers) >= 1
        assert len(logger2.logger.handlers) >= 1
        
        # Handler names should be different
        logger1_handler_names = [h.get_name() for h in logger1.logger.handlers]
        logger2_handler_names = [h.get_name() for h in logger2.logger.handlers]
        
        assert set(logger1_handler_names).isdisjoint(set(logger2_handler_names))

    def test_invalid_log_directory(self):
        """Test error handling for invalid log directory."""
        with patch('os.makedirs', side_effect=PermissionError("Permission denied")):
            with pytest.raises(RuntimeError, match="Failed to create log directory"):
                Logger(name="test", log_dir="/invalid/directory")

    def test_file_handler_creation_failure(self):
        """Test error handling for directory creation failure."""
        # Test with an invalid path to trigger directory creation error
        with pytest.raises(RuntimeError, match="Failed to create log directory"):
            # Use a path that will cause permission issues
            Logger(name="test", log_dir="/root/cannot_create_here")

    def test_custom_format(self):
        """Test logger with custom format string."""
        custom_format = "%(levelname)s: %(message)s"
        logger = Logger(name="custom_format", log_dir=self.temp_dir, fmt=custom_format)
        
        logger.info("Test message")
        
        log_file_path = logger.get_log_file_path()
        with open(log_file_path, 'r') as f:
            content = f.read()
            # Should not contain timestamp (not in custom format)
            assert "INFO: Test message" in content

    def test_get_log_file_path(self):
        """Test getting the log file path."""
        # Use a fresh temp directory for this specific test
        test_temp_dir = tempfile.mkdtemp()
        try:
            logger = Logger(name="test_get_path", log_dir=test_temp_dir)  # Use unique name
            log_file_path = logger.get_log_file_path()
            
            assert log_file_path != ""
            assert log_file_path.startswith(test_temp_dir)
            assert ".log" in log_file_path
        finally:
            shutil.rmtree(test_temp_dir, ignore_errors=True)

    def test_set_level(self):
        """Test setting logger level."""
        logger = Logger(name="test", log_dir=self.temp_dir)
        logger.set_level(logging.ERROR)
        
        assert logger.logger.level == logging.ERROR

    def test_log_file_naming(self):
        """Test that log files are named with current date."""
        logger = Logger(name="test", log_dir=self.temp_dir)
        log_file_path = logger.get_log_file_path()
        
        from datetime import datetime
        today = datetime.now().strftime('%Y-%m-%d')
        assert today in log_file_path

    def test_utf8_encoding(self):
        """Test that log files handle UTF-8 characters properly."""
        logger = Logger(name="utf8_test", log_dir=self.temp_dir)
        
        # Test with various UTF-8 characters
        test_messages = [
            "Hello 世界",
            "Café ☕",
            "🚀 Rocket",
            "Ñoño piña"
        ]
        
        for msg in test_messages:
            logger.info(msg)
        
        # Read the file and verify UTF-8 content
        log_file_path = logger.get_log_file_path()
        with open(log_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            for msg in test_messages:
                assert msg in content
