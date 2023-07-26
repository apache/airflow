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

from airflow.utils.log.task_log_shipper import TaskLogShipper

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_task_handler():
    return Mock(supports_task_log_ship=True)


@pytest.fixture
def task_log_shipper(mock_task_handler):
    logger = logging.getLogger("airflow.task")
    logger.handlers = [mock_task_handler]
    return TaskLogShipper("test_component")


@pytest.fixture
def task_log_shipper_does_not_support_task_log_ship():
    logger = logging.getLogger("airflow.task")
    logger.handlers = [Mock(supports_task_log_ship=False)]
    return TaskLogShipper("test_component")


def test_log_shipper_enabled_by_default(task_log_shipper):
    assert task_log_shipper.task_handler_can_ship_logs is True


@patch("airflow.utils.log.task_log_shipper.TASK_LOG_SHIPPER_ENABLED", False)
def test_log_shipper_disabled(task_log_shipper):
    assert task_log_shipper.task_handler_can_ship_logs is True


def test_task_handler_not_supports_task_log_shipping(task_log_shipper_does_not_support_task_log_ship):
    assert task_log_shipper_does_not_support_task_log_ship._can_ship_logs() is False


def test_ship_task_message_with_correct_arguments(task_log_shipper, mock_task_handler):
    ti = Mock()
    task_log_shipper.info(ti, "test message", logger)
    mock_task_handler.set_context.assert_called_once_with(ti, identifier="test_component")
    mock_task_handler.emit.assert_called_once()


def test_ship_task_message_closes_task_handler(task_log_shipper, mock_task_handler):
    ti = Mock()
    task_log_shipper.info(ti, "test message", logger)
    mock_task_handler.close.assert_called_once()


@patch("airflow.utils.log.task_log_shipper.TASK_LOG_SHIPPER_ENABLED", False)
def test_ship_task_message_not_shipped_when_task_log_shipper_disabled(task_log_shipper, mock_task_handler):
    ti = Mock()
    task_log_shipper.info(ti, "test message", logger)
    mock_task_handler.assert_not_called()


def test_invalid_log_level_attribute_raises_exception(task_log_shipper):
    ti = Mock()
    logger = Mock()
    with pytest.raises(AttributeError):
        task_log_shipper.invalid_log_level(ti, "test", logger)
