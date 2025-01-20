"""
General utility functions for pgshift.

Copyright (c) 2025 Jinesh Nagori. All rights reserved.
This software is licensed under the MIT License. See LICENSE file for details.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler

from pythonjsonlogger import json


def setup_logging(level=logging.INFO, log_file=None, json_format=False):
    """
    Sets up logging for the application.

    Args:
        level: The logging level (e.g., logging.DEBUG, logging.INFO).
        log_file: Optional path to a log file. If None, logs to console only.
        json_format: Whether to use JSON format for logs in file.
    """
    root_logger = logging.getLogger("pgshift")
    root_logger.setLevel(level)

    # Console handler (single-line format)
    console_formatter = logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler (JSON format, with rotation)
    if log_file:
        if json_format:
            file_formatter = json.JsonFormatter(
                "%(asctime)s %(levelname)s %(name)s %(filename)s %(lineno)d %(funcName)s %(message)s"
            )
        else:
            file_formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)-8s %(name)s(%(filename)s:%(lineno)d):%(funcName)s - %(message)s"
            )
        # Use RotatingFileHandler for log rotation
        file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 5,
                                           backupCount=5)  # 5 MB per file, keep 5 backups
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
