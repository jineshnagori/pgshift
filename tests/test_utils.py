"""
Tests for utility functions in pgshift.

Copyright (c) [Year] [Your Name or Company Name]. All rights reserved.
This software is licensed under the MIT License. See LICENSE file for details.
"""

import logging
import os
from pgshift.utils import setup_logging

def test_setup_logging_console(capsys):
    setup_logging(level=logging.INFO)
    log = logging.getLogger("pgshift")
    log.info("Test message")

    captured = capsys.readouterr()
    assert "Test message" in captured.out

def test_setup_logging_file(tmp_path):
    log_file = tmp_path / "test.log"
    setup_logging(level=logging.DEBUG, log_file=str(log_file), json_format=False)
    log = logging.getLogger("pgshift")
    log.debug("Test message")

    assert log_file.exists()
    with open(log_file, "r") as f:
        content = f.read()
        assert "Test message" in content
        assert "DEBUG" in content

def test_setup_logging_file_json(tmp_path):
    log_file = tmp_path / "test.log"
    setup_logging(level=logging.INFO, log_file=str(log_file), json_format=True)
    log = logging.getLogger("pgshift")
    log.info("Test message")

    assert log_file.exists()
    with open(log_file, "r") as f:
        content = f.read()
        assert "Test message" in content
        assert '"levelname": "INFO"' in content

def test_setup_logging_file_rotation(tmp_path):
    log_file = tmp_path / "test.log"
    setup_logging(level=logging.INFO, log_file=str(log_file), json_format=False)
    log = logging.getLogger("pgshift")

    # Write enough messages to exceed the maxBytes limit and trigger rotation
    for i in range(10000):
        log.info(f"Test message {i}" * 5000)

    assert log_file.exists()
    # Check that backup files exist
    assert os.path.exists(str(log_file) + ".1")