#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Unit tests for the StreamCaptureManager class used in Airflow CLI tests."""

from __future__ import annotations

import logging
import sys

import pytest


def test_stdout_only(stdout_capture):
    """Test capturing stdout only."""
    with stdout_capture as capture:
        print("Hello stdout")
        print("Error message", file=sys.stderr)

        # Access during context
        assert "Hello stdout" in capture.getvalue()
        assert "Error message" not in capture.getvalue()


def test_stderr_only(stderr_capture):
    """Test capturing stderr only."""
    with stderr_capture as capture:
        print("Hello stdout")
        print("Error message", file=sys.stderr)

        assert "Error message" in capture.getvalue()
        assert "Hello stdout" not in capture.getvalue()


def test_combined(combined_capture):
    """Test capturing both streams."""
    with combined_capture as capture:
        print("Hello stdout")
        print("Error message", file=sys.stderr)

        assert "Hello stdout" in capture.stdout
        assert "Error message" in capture.stderr
        assert "Hello stdout" in capture.get_combined()
        assert "Error message" in capture.get_combined()


def test_configurable(stream_capture):
    """Test with configurable capture."""
    # Capture both
    with stream_capture(stdout=True, stderr=True) as capture:
        print("stdout message")
        print("stderr message", file=sys.stderr)

        assert "stdout message" in capture.stdout
        assert "stderr message" in capture.stderr

    # Capture stderr only
    with stream_capture(stdout=False, stderr=True) as capture:
        print("stdout message")
        print("stderr message", file=sys.stderr)

        assert capture.stdout == ""
        assert "stderr message" in capture.stderr


# ============== Tests for Logging Isolation ==============


def test_stdout_logging_isolation(stdout_capture):
    """Test that logging to stdout is isolated from captured output."""
    # Set up a logger that writes to stdout
    logger = logging.getLogger("test_stdout_logger")
    logger.setLevel(logging.INFO)

    # Create handler that writes to stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logger.addHandler(stdout_handler)

    try:
        with stdout_capture as capture:
            # Regular print should be captured
            print("Regular print to stdout")

            # Logging should NOT be captured (isolated)
            logger.info("This is a log message to stdout")
            logger.warning("This is a warning to stdout")

            # Another regular print
            print("Another regular print")

            output = capture.getvalue()

            # Regular prints should be in output
            assert "Regular print to stdout" in output
            assert "Another regular print" in output

            # Log messages should NOT be in output
            assert "This is a log message to stdout" not in output
            assert "This is a warning to stdout" not in output
            assert "INFO" not in output
            assert "WARNING" not in output
    finally:
        # Clean up
        logger.removeHandler(stdout_handler)


def test_stderr_logging_isolation(stderr_capture):
    """Test that logging to stderr is isolated from captured output."""
    # Set up a logger that writes to stderr
    logger = logging.getLogger("test_stderr_logger")
    logger.setLevel(logging.INFO)

    # Create handler that writes to stderr
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logger.addHandler(stderr_handler)

    try:
        with stderr_capture as capture:
            # Regular print to stderr should be captured
            print("Regular print to stderr", file=sys.stderr)

            # Logging should NOT be captured (isolated)
            logger.error("This is an error log to stderr")
            logger.critical("This is a critical log to stderr")

            # Another regular print
            print("Another stderr print", file=sys.stderr)

            output = capture.getvalue()

            # Regular prints should be in output
            assert "Regular print to stderr" in output
            assert "Another stderr print" in output

            # Log messages should NOT be in output
            assert "This is an error log to stderr" not in output
            assert "This is a critical log to stderr" not in output
            assert "ERROR" not in output
            assert "CRITICAL" not in output
    finally:
        # Clean up
        logger.removeHandler(stderr_handler)


def test_combined_logging_isolation(combined_capture):
    """Test that logging is isolated when capturing both stdout and stderr."""
    # Set up loggers for both streams
    stdout_logger = logging.getLogger("test_combined_stdout")
    stderr_logger = logging.getLogger("test_combined_stderr")

    stdout_logger.setLevel(logging.INFO)
    stderr_logger.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stderr_handler = logging.StreamHandler(sys.stderr)

    stdout_handler.setFormatter(logging.Formatter("[STDOUT LOG] %(message)s"))
    stderr_handler.setFormatter(logging.Formatter("[STDERR LOG] %(message)s"))

    stdout_logger.addHandler(stdout_handler)
    stderr_logger.addHandler(stderr_handler)

    try:
        with combined_capture as capture:
            # Regular prints
            print("Regular stdout")
            print("Regular stderr", file=sys.stderr)

            # Logging (should be isolated)
            stdout_logger.info("Log to stdout")
            stderr_logger.error("Log to stderr")

            # Check stdout
            assert "Regular stdout" in capture.stdout
            assert "Log to stdout" not in capture.stdout
            assert "[STDOUT LOG]" not in capture.stdout

            # Check stderr
            assert "Regular stderr" in capture.stderr
            assert "Log to stderr" not in capture.stderr
            assert "[STDERR LOG]" not in capture.stderr

            # Combined should have regular prints but not logs
            combined = capture.get_combined()
            assert "Regular stdout" in combined
            assert "Regular stderr" in combined
            assert "Log to stdout" not in combined
            assert "Log to stderr" not in combined
    finally:
        # Clean up
        stdout_logger.removeHandler(stdout_handler)
        stderr_logger.removeHandler(stderr_handler)


def test_root_logger_isolation(stdout_capture):
    """Test that root logger messages are isolated from captured output."""
    # Configure root logger to output to stdout
    root_logger = logging.getLogger()
    original_level = root_logger.level
    root_logger.setLevel(logging.DEBUG)

    # Add a stdout handler to root logger
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    root_logger.addHandler(handler)

    try:
        with stdout_capture as capture:
            # Regular print
            print("Before logging")

            # Various log levels using root logger explicitly
            root_logger.debug("Debug message")
            root_logger.info("Info message")
            root_logger.warning("Warning message")
            root_logger.error("Error message")

            # Regular print
            print("After logging")

            output = capture.getvalue()

            # Only regular prints should be captured
            assert "Before logging" in output
            assert "After logging" in output

            # No log messages should be captured
            assert "Debug message" not in output
            assert "Info message" not in output
            assert "Warning message" not in output
            assert "Error message" not in output

            # No log formatting should be present
            assert "DEBUG" not in output
            assert "INFO" not in output
            assert "WARNING" not in output
            assert "ERROR" not in output
            assert "%(asctime)s" not in output
    finally:
        # Clean up
        root_logger.removeHandler(handler)
        root_logger.setLevel(original_level)


def test_mixed_output_ordering(stdout_capture):
    """Test that the order of regular prints is preserved when logging is mixed in."""
    logger = logging.getLogger("test_ordering")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("LOG: %(message)s"))
    logger.addHandler(handler)

    try:
        with stdout_capture as capture:
            print("1. First print")
            logger.info("Should not appear 1")
            print("2. Second print")
            logger.info("Should not appear 2")
            print("3. Third print")

            output = capture.getvalue()
            lines = output.strip().split("\n")
    finally:
        logger.removeHandler(handler)

    # Should have exactly 3 lines (only the prints)
    assert len(lines) == 3
    assert lines[0] == "1. First print"
    assert lines[1] == "2. Second print"
    assert lines[2] == "3. Third print"

    # No log messages
    assert "Should not appear" not in output
    assert "LOG:" not in output


def test_handler_restoration(stdout_capture):
    """Test that logging handlers are properly restored after capture."""
    root_logger = logging.getLogger()

    # Add a test handler to root logger to ensure we have something to test
    test_root_handler = logging.StreamHandler(sys.stdout)
    test_root_handler.setFormatter(logging.Formatter("ROOT: %(message)s"))
    root_logger.addHandler(test_root_handler)

    # Also create a non-root logger with its own handler
    logger = logging.getLogger("test_restoration")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("TEST: %(message)s"))
    logger.addHandler(handler)

    try:
        # Record initial state
        initial_root_handlers = list(root_logger.handlers)
        assert test_root_handler in initial_root_handlers

        # Use the capture context
        with stdout_capture:
            print("Inside capture")
            logger.info("Log inside capture")

            # During capture, our test handler should be removed from root
            current_root_handlers = list(root_logger.handlers)
            assert test_root_handler not in current_root_handlers, (
                "Test handler should be removed during capture"
            )

            # The non-root logger's handler should still exist
            assert handler in logger.handlers

        # After capture, root logger handlers should be restored
        final_root_handlers = list(root_logger.handlers)
        assert test_root_handler in final_root_handlers, "Test handler should be restored after capture"
        assert len(final_root_handlers) == len(initial_root_handlers), (
            f"Handler count mismatch. Initial: {len(initial_root_handlers)}, Final: {len(final_root_handlers)}"
        )

    finally:
        # Clean up
        root_logger.removeHandler(test_root_handler)
        logger.removeHandler(handler)


def test_multiple_loggers_isolation(stream_capture):
    """Test isolation works with multiple loggers writing to different streams."""
    # Create multiple loggers
    app_logger = logging.getLogger("app")
    db_logger = logging.getLogger("database")
    api_logger = logging.getLogger("api")

    # Set up handlers
    app_handler = logging.StreamHandler(sys.stdout)
    db_handler = logging.StreamHandler(sys.stderr)
    api_handler = logging.StreamHandler(sys.stdout)

    app_handler.setFormatter(logging.Formatter("[APP] %(message)s"))
    db_handler.setFormatter(logging.Formatter("[DB] %(message)s"))
    api_handler.setFormatter(logging.Formatter("[API] %(message)s"))

    app_logger.addHandler(app_handler)
    db_logger.addHandler(db_handler)
    api_logger.addHandler(api_handler)

    app_logger.setLevel(logging.INFO)
    db_logger.setLevel(logging.INFO)
    api_logger.setLevel(logging.INFO)

    try:
        with stream_capture(stdout=True, stderr=True) as capture:
            # Regular prints
            print("Starting application")
            print("Database connection error", file=sys.stderr)

            # Logs (should be isolated)
            app_logger.info("App initialized")
            db_logger.error("Connection failed")
            api_logger.info("API server started")

            # More regular prints
            print("Application ready")
            print("Check error log", file=sys.stderr)

            # Verify stdout
            assert "Starting application" in capture.stdout
            assert "Application ready" in capture.stdout
            assert "[APP]" not in capture.stdout
            assert "[API]" not in capture.stdout
            assert "App initialized" not in capture.stdout
            assert "API server started" not in capture.stdout

            # Verify stderr
            assert "Database connection error" in capture.stderr
            assert "Check error log" in capture.stderr
            assert "[DB]" not in capture.stderr
            assert "Connection failed" not in capture.stderr
    finally:
        # Clean up
        app_logger.removeHandler(app_handler)
        db_logger.removeHandler(db_handler)
        api_logger.removeHandler(api_handler)


def test_exception_during_capture_preserves_isolation(stdout_capture):
    """Test that logging isolation is maintained even when an exception occurs."""
    logger = logging.getLogger("test_exception")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("LOG: %(message)s"))
    logger.addHandler(handler)

    # Test setup and isolation check before exception
    captured_output = None

    try:
        with stdout_capture as capture:
            print("Before exception")
            logger.info("Log before exception")

            # Check isolation before exception
            captured_output = capture.getvalue()
            assert "Before exception" in captured_output
            assert "Log before exception" not in captured_output

            # This will raise an exception
            raise ValueError("Test exception")
    except ValueError:
        # Expected exception
        pass

    # Verify captured output was correct
    assert captured_output is not None
    assert "Before exception" in captured_output
    assert "Log before exception" not in captured_output

    # Verify handlers are restored after exception
    root_logger = logging.getLogger()
    # Just verify no crash when accessing handlers
    restored_handlers = list(root_logger.handlers)
    assert isinstance(restored_handlers, list)

    # Clean up
    logger.removeHandler(handler)


def test_exception_during_capture_with_pytest_raises(stdout_capture):
    """Test exception handling with proper pytest.raises usage."""
    logger = logging.getLogger("test_exception_pytest")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("LOG: %(message)s"))
    logger.addHandler(handler)

    try:
        # Capture and verify before the exception
        with stdout_capture as capture:
            print("Before exception")
            logger.info("Log before exception")

            # Verify isolation works
            output = capture.getvalue()
            assert "Before exception" in output
            assert "Log before exception" not in output

        # Now test that exception in capture context still works
        with pytest.raises(ValueError, match="Test exception"):
            with stdout_capture:
                raise ValueError("Test exception")

        # Verify we can still use capture after exception
        with stdout_capture as capture:
            print("After exception test")
            output = capture.getvalue()
            assert "After exception test" in output

    finally:
        logger.removeHandler(handler)
