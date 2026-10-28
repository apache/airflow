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
from unittest.mock import patch

import pytest

from airflow.providers.common.ai.utils import task_logger
from airflow.providers.common.ai.utils.task_logger import get_task_logger


class _RecordingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def airflow2_task_log():
    """Route ``get_task_logger`` down its Airflow 2 path and capture the ``airflow.task`` records."""
    airflow_task = logging.getLogger("airflow.task")
    handler = _RecordingHandler()
    previous_level = airflow_task.level
    airflow_task.addHandler(handler)
    airflow_task.setLevel(logging.INFO)
    try:
        with patch.object(task_logger, "AIRFLOW_V_3_0_PLUS", False):
            yield handler.records
    finally:
        airflow_task.removeHandler(handler)
        airflow_task.setLevel(previous_level)


class TestGetTaskLoggerOnAirflow2:
    def test_applies_the_task_log_level(self, airflow2_task_log):
        get_task_logger().debug("Durable: cached model response", step=0)

        assert airflow2_task_log == []

    def test_folds_fields_into_the_message(self, airflow2_task_log):
        get_task_logger().warning("Durable: cache miss", step=2, tool="get_weather")

        (record,) = airflow2_task_log
        assert record.levelno == logging.WARNING
        assert record.getMessage() == "Durable: cache miss step=2 tool='get_weather'"

    def test_names_the_calling_line_not_structlog(self, airflow2_task_log):
        get_task_logger().warning("from the caller")

        (record,) = airflow2_task_log
        assert record.pathname == __file__
        assert record.funcName == "test_names_the_calling_line_not_structlog"

    def test_hands_exc_info_to_stdlib(self, airflow2_task_log):
        try:
            raise ValueError("boom")
        except ValueError:
            get_task_logger().warning("Failed to write the cache", exc_info=True)

        (record,) = airflow2_task_log
        assert record.getMessage() == "Failed to write the cache"
        assert record.exc_info is not None
        assert record.exc_info[0] is ValueError
