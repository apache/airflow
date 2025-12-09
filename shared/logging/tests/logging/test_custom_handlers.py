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
from __future__ import annotations

import io
import logging
from unittest import mock

import pytest
import structlog

from airflow_shared.logging.structlog import configure_logging


class CustomTestHandler(logging.Handler):
    """A custom handler for testing purposes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.messages = []

    def emit(self, record):
        """Store the log record."""
        self.messages.append(self.format(record))


import sys

_handler_module = type(sys)("custom_test_handler")
_handler_module.CustomTestHandler = CustomTestHandler
sys.modules["custom_test_handler"] = _handler_module


@pytest.fixture
def reset_logging():
    """Reset logging configuration after each test."""
    prev_config = structlog.get_config()
    yield
    structlog.configure(**prev_config)
    logging.getLogger().handlers.clear()
    for logger_name in list(logging.Logger.manager.loggerDict.keys()):
        if logger_name.startswith("airflow"):
            logging.getLogger(logger_name).handlers.clear()


class TestCustomHandlers:
    """Test that custom handlers are properly configured and preserved."""

    def test_custom_handler_with_custom_formatter(self, reset_logging):
        """Test that custom handlers with custom formatters are preserved."""
        # Create a custom logging config with a custom handler
        custom_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "custom": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
                "airflow": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "custom_handler": {
                    "()": "custom_test_handler.CustomTestHandler",
                    "formatter": "custom",
                    "level": "INFO",
                },
                "task": {
                    "class": "logging.StreamHandler",
                    "formatter": "airflow",
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                "airflow.task": {
                    "handlers": ["custom_handler", "task"],
                    "level": "INFO",
                    "propagate": True,
                },
            },
            "root": {
                "handlers": [],
                "level": "INFO",
            },
        }

        # Configure logging with the custom config
        with mock.patch("sys.stdout") as mock_stdout:
            mock_stdout.isatty.return_value = True
            buff = io.StringIO()
            configure_logging(stdlib_config=custom_config, output=buff)

        # Verify that the custom handler is attached to the logger
        logger = logging.getLogger("airflow.task")
        handler_names = [h.name for h in logger.handlers if hasattr(h, "name")]

        # The custom handler should be present
        # Note: The handler name might be different, so we check by class
        custom_handlers = [h for h in logger.handlers if isinstance(h, CustomTestHandler)]
        assert len(custom_handlers) > 0, "Custom handler should be attached to logger"

        # Verify that the custom handler has its formatter preserved
        custom_handler = custom_handlers[0]
        assert custom_handler.formatter is not None, "Custom handler should have a formatter"

    def test_custom_handler_without_formatter(self, reset_logging):
        """Test that custom handlers without formatters get structlog formatter."""
        # Create a custom logging config with a custom handler without formatter
        custom_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "airflow": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "custom_handler": {
                    "()": "custom_test_handler.CustomTestHandler",
                    # No formatter specified
                    "level": "INFO",
                },
                "task": {
                    "class": "logging.StreamHandler",
                    "formatter": "airflow",
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                "airflow.task": {
                    "handlers": ["custom_handler", "task"],
                    "level": "INFO",
                    "propagate": True,
                },
            },
            "root": {
                "handlers": [],
                "level": "INFO",
            },
        }

        # Configure logging with the custom config
        with mock.patch("sys.stdout") as mock_stdout:
            mock_stdout.isatty.return_value = True
            buff = io.StringIO()
            configure_logging(stdlib_config=custom_config, output=buff)

        # Verify that the custom handler is attached to the logger
        logger = logging.getLogger("airflow.task")
        custom_handlers = [h for h in logger.handlers if isinstance(h, CustomTestHandler)]
        assert len(custom_handlers) > 0, "Custom handler should be attached to logger"

        # Verify that the custom handler has structlog formatter
        custom_handler = custom_handlers[0]
        assert custom_handler.formatter is not None, "Custom handler should have a formatter"
        # The formatter should be a ProcessorFormatter (structlog formatter)
        assert hasattr(custom_handler.formatter, "processors"), "Formatter should be structlog ProcessorFormatter"

    def test_custom_handler_preserves_formatter(self, reset_logging):
        """Test that custom handlers preserve their specified formatter."""
        # Create a custom logging config with a custom handler with custom formatter
        custom_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "my_custom_formatter": {
                    "format": "[CUSTOM] %(message)s",
                },
                "airflow": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "custom_handler": {
                    "()": "custom_test_handler.CustomTestHandler",
                    "formatter": "my_custom_formatter",
                    "level": "INFO",
                },
            },
            "loggers": {
                "airflow.task": {
                    "handlers": ["custom_handler"],
                    "level": "INFO",
                    "propagate": True,
                },
            },
            "root": {
                "handlers": [],
                "level": "INFO",
            },
        }

        # Configure logging with the custom config
        with mock.patch("sys.stdout") as mock_stdout:
            mock_stdout.isatty.return_value = True
            buff = io.StringIO()
            configure_logging(stdlib_config=custom_config, output=buff)

        # Verify that the custom handler is attached and can log
        logger = logging.getLogger("airflow.task")
        custom_handlers = [h for h in logger.handlers if isinstance(h, CustomTestHandler)]
        assert len(custom_handlers) > 0, "Custom handler should be attached to logger"

        # Test that the handler can actually log
        logger.info("Test message")
        custom_handler = custom_handlers[0]
        assert len(custom_handler.messages) > 0, "Custom handler should have received log messages"

    def test_multiple_custom_handlers(self, reset_logging):
        """Test that multiple custom handlers can be configured."""
        # Create a custom logging config with multiple custom handlers
        custom_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "airflow": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "custom_handler_1": {
                    "()": "custom_test_handler.CustomTestHandler",
                    "formatter": "airflow",
                    "level": "INFO",
                },
                "custom_handler_2": {
                    "()": "custom_test_handler.CustomTestHandler",
                    # No formatter - should get structlog formatter
                    "level": "INFO",
                },
            },
            "loggers": {
                "airflow.task": {
                    "handlers": ["custom_handler_1", "custom_handler_2"],
                    "level": "INFO",
                    "propagate": True,
                },
            },
            "root": {
                "handlers": [],
                "level": "INFO",
            },
        }

        # Configure logging with the custom config
        with mock.patch("sys.stdout") as mock_stdout:
            mock_stdout.isatty.return_value = True
            buff = io.StringIO()
            configure_logging(stdlib_config=custom_config, output=buff)

        # Verify that both custom handlers are attached to the logger
        logger = logging.getLogger("airflow.task")
        custom_handlers = [h for h in logger.handlers if isinstance(h, CustomTestHandler)]
        assert len(custom_handlers) >= 2, "Both custom handlers should be attached to logger"

        # Verify that both handlers have formatters
        for handler in custom_handlers:
            assert handler.formatter is not None, "Each custom handler should have a formatter"

