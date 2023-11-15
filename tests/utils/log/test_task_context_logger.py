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

import logging
from unittest.mock import Mock, patch

import pytest

from airflow.utils.log.task_context_logger import TaskContextLogger

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_task_handler():
    return Mock(supports_task_context_logging=True)


@pytest.fixture
def task_context_logger(mock_task_handler):
    logger = logging.getLogger("airflow.task")
    logger.handlers = [mock_task_handler]
    return TaskContextLogger(component_name="test_component")


@pytest.fixture
def task_context_logger_does_not_support_task_context_logging():
    logger = logging.getLogger("airflow.task")
    logger.handlers = [Mock(supports_task_context_logging=False)]
    return TaskContextLogger(component_name="test_component")


def test_task_context_logger_enabled_by_default(task_context_logger):
    assert task_context_logger.should_log is True


def test_task_handler_not_supports_task_context_logging(
    task_context_logger_does_not_support_task_context_logging,
):
    assert task_context_logger_does_not_support_task_context_logging._should_log() is False


def test_task_context_log_with_correct_arguments(task_context_logger, mock_task_handler):
    ti = Mock()
    task_context_logger.info(ti, logger, "test message with args %s, %s", "a", "b")
    mock_task_handler.set_context.assert_called_once_with(ti, identifier="test_component")
    mock_task_handler.emit.assert_called_once()


def test_task_context_log_closes_task_handler(task_context_logger, mock_task_handler):
    ti = Mock()
    task_context_logger.info(ti, logger, "test message")
    mock_task_handler.close.assert_called_once()


@patch("airflow.utils.log.task_context_logger.TASK_CONTEXT_LOGGER_ENABLED", False)
def test_task_context_logger_disabled(task_context_logger, mock_task_handler):
    ti = Mock()
    task_context_logger.info(ti, logger, "test message")
    mock_task_handler.assert_not_called()


def test_invalid_log_level_attribute_raises_exception(task_context_logger):
    ti = Mock()
    with pytest.raises(AttributeError):
        task_context_logger.invalid_log_level(ti, logger, "test")
