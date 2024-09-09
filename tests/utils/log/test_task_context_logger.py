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
from unittest import mock
from unittest.mock import Mock

import pytest

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.log.task_context_logger import TaskContextLogger
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests.test_utils.config import conf_vars

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.skip_if_database_isolation_mode


@pytest.fixture
def mock_handler():
    logger = logging.getLogger("airflow.task")
    old = logger.handlers[:]
    h = Mock()
    logger.handlers[:] = [h]
    yield h
    logger.handlers[:] = old


@pytest.fixture
def ti(dag_maker):
    with dag_maker() as dag:

        @dag.task()
        def nothing():
            return None

        nothing()

    triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
    dr = dag.create_dagrun("running", run_id="abc", **triggered_by_kwargs)
    ti = dr.get_task_instances()[0]
    return ti


def test_task_context_logger_enabled_by_default():
    t = TaskContextLogger(component_name="test_component")
    assert t.enabled is True


@pytest.mark.parametrize("supported", [True, False])
def test_task_handler_not_supports_task_context_logging(mock_handler, supported):
    mock_handler.supports_task_context_logging = supported
    t = TaskContextLogger(component_name="test_component")
    assert t.enabled is supported


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
@pytest.mark.parametrize("supported", [True, False])
def test_task_context_log_with_correct_arguments(ti, mock_handler, supported):
    mock_handler.supports_task_context_logging = supported
    t = TaskContextLogger(component_name="test_component")
    t.info("test message with args %s, %s", "a", "b", ti=ti)
    if supported:
        mock_handler.set_context.assert_called_once_with(ti, identifier="test_component")
        mock_handler.emit.assert_called_once()
    else:
        mock_handler.set_context.assert_not_called()
        mock_handler.emit.assert_not_called()


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
@mock.patch("airflow.utils.log.task_context_logger._ensure_ti")
@pytest.mark.parametrize("supported", [True, False])
def test_task_context_log_with_task_instance_key(mock_ensure_ti, ti, mock_handler, supported):
    mock_handler.supports_task_context_logging = supported
    mock_ensure_ti.return_value = ti
    task_instance_key = TaskInstanceKey(ti.dag_id, ti.task_id, ti.run_id, ti.try_number, ti.map_index)
    t = TaskContextLogger(component_name="test_component")
    t.info("test message with args %s, %s", "a", "b", ti=task_instance_key)
    if supported:
        mock_handler.set_context.assert_called_once_with(ti, identifier="test_component")
        mock_handler.emit.assert_called_once()
    else:
        mock_handler.set_context.assert_not_called()
        mock_handler.emit.assert_not_called()


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
def test_task_context_log_closes_task_handler(ti, mock_handler):
    t = TaskContextLogger("blah")
    t.info("test message", ti=ti)
    mock_handler.close.assert_called_once()


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
def test_task_context_log_also_emits_to_call_site_logger(ti):
    logger = logging.getLogger("abc123567")
    logger.setLevel(logging.INFO)
    logger.log = Mock()
    t = TaskContextLogger("blah", call_site_logger=logger)
    t.info("test message", ti=ti)
    logger.log.assert_called_once_with(logging.INFO, "test message")


@pytest.mark.db_test
@pytest.mark.parametrize("val, expected", [("true", True), ("false", False)])
def test_task_context_logger_config_works(ti, mock_handler, val, expected):
    with conf_vars({("logging", "enable_task_context_logger"): val}):
        t = TaskContextLogger("abc")
        t.info("test message", ti=ti)
        if expected:
            mock_handler.emit.assert_called()
        else:
            mock_handler.emit.assert_not_called()
