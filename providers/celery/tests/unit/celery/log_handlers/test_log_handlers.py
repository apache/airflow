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

import logging
import logging.config
from importlib import reload
from unittest import mock

import pytest

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.executors import executor_loader
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.log.file_task_handler import (
    FileTaskHandler,
)
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.file_task_handler import (
    log_str_to_parsed_log_stream,
    mark_test_for_old_read_log_method,
    mark_test_for_stream_based_read_log_method,
)

pytestmark = pytest.mark.db_test

DEFAULT_DATE = datetime(2016, 1, 1)
TASK_LOGGER = "airflow.task"
FILE_TASK_HANDLER = "task"


class TestFileTaskLogHandler:
    def clean_up(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TaskInstance).delete()

    def setup_method(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        logging.root.disabled = False
        self.clean_up()
        # We use file task handler by default.

    def teardown_method(self):
        self.clean_up()

    @mark_test_for_old_read_log_method
    def test__read_for_celery_executor_fallbacks_to_worker(self, create_task_instance):
        """Test for executors which do not have `get_task_log` method, it fallbacks to reading
        log from worker"""
        executor_name = "CeleryExecutor"
        ti = create_task_instance(
            dag_id="dag_for_testing_celery_executor_log_read",
            task_id="task_for_testing_celery_executor_log_read",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
        )
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 1
        with conf_vars({("core", "executor"): executor_name}):
            reload(executor_loader)
            fth = FileTaskHandler("")

            fth._read_from_logs_server = mock.Mock()
            fth._read_from_logs_server.return_value = ["this message"], ["this\nlog\ncontent"]
            actual = fth._read(ti=ti, try_number=1)
            fth._read_from_logs_server.assert_called_once()
        assert "*** this message\n" in actual[0]
        assert actual[0].endswith("this\nlog\ncontent")
        assert actual[1] == {"end_of_log": False, "log_pos": 16}

    @mark_test_for_stream_based_read_log_method
    def test_stream_based__read_for_celery_executor_fallbacks_to_worker(self, create_task_instance):
        """Test for executors which do not have `get_task_log` method, it fallbacks to reading
        log from worker"""
        executor_name = "CeleryExecutor"
        ti = create_task_instance(
            dag_id="dag_for_testing_celery_executor_log_read",
            task_id="task_for_testing_celery_executor_log_read",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
        )
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 1
        with conf_vars({("core", "executor"): executor_name}):
            reload(executor_loader)
            fth = FileTaskHandler("")

            fth._read_from_logs_server = mock.Mock()
            fth._read_from_logs_server.return_value = (
                ["this message"],
                [log_str_to_parsed_log_stream("this\nlog\ncontent")],
                len("this\nlog\ncontent"),
            )
            log_stream, metadata = fth._read(ti=ti, try_number=1)
            log_str = "\n".join(line for line in log_stream)
            fth._read_from_logs_server.assert_called_once()
        assert "*** this message\n" in log_str
        assert log_str.endswith("this\nlog\ncontent")
        assert metadata == {"end_of_log": False, "log_pos": 16}
