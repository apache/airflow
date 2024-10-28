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

import copy
import datetime
import logging
import os
import sys
import tempfile
from typing import TYPE_CHECKING
from unittest import mock

import pendulum
import pytest

from airflow import settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models.tasklog import LogTemplate
from airflow.operators.python import PythonOperator
from airflow.timetables.base import DataInterval
from airflow.utils import timezone
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.state import TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


if TYPE_CHECKING:
    from airflow.models import DagRun


class TestLogView:
    DAG_ID = "dag_log_reader"
    TASK_ID = "task_log_reader"
    DEFAULT_DATE = timezone.datetime(2017, 9, 1)
    FILENAME_TEMPLATE = "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts | replace(':', '.') }}/{{ try_number }}.log"

    @pytest.fixture(autouse=True)
    def log_dir(self):
        with tempfile.TemporaryDirectory() as log_dir:
            self.log_dir = log_dir
            yield log_dir
        del self.log_dir

    @pytest.fixture(autouse=True)
    def settings_folder(self):
        old_modules = dict(sys.modules)
        with tempfile.TemporaryDirectory() as settings_folder:
            self.settings_folder = settings_folder
            sys.path.append(settings_folder)
            yield settings_folder
        sys.path.remove(settings_folder)
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in old_modules]:
            del sys.modules[mod]
        del self.settings_folder

    @pytest.fixture(autouse=True)
    def configure_loggers(self, log_dir, settings_folder):
        logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        logging_config["handlers"]["task"]["base_log_folder"] = log_dir
        settings_file = os.path.join(settings_folder, "airflow_local_settings_test.py")
        with open(settings_file, "w") as handle:
            new_logging_file = f"LOGGING_CONFIG = {logging_config}"
            handle.writelines(new_logging_file)
        with conf_vars(
            {
                (
                    "logging",
                    "logging_config_class",
                ): "airflow_local_settings_test.LOGGING_CONFIG"
            }
        ):
            settings.configure_logging()
        yield
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)

    @pytest.fixture(autouse=True)
    def prepare_log_files(self, log_dir):
        dir_path = f"{log_dir}/{self.DAG_ID}/{self.TASK_ID}/2017-09-01T00.00.00+00.00/"
        os.makedirs(dir_path)
        for try_number in range(1, 4):
            with open(f"{dir_path}/{try_number}.log", "w+") as f:
                f.write(f"try_number={try_number}.\n")
                f.flush()

    @pytest.fixture(autouse=True)
    def prepare_db(self, create_task_instance):
        session = settings.Session()
        log_template = LogTemplate(filename=self.FILENAME_TEMPLATE, elasticsearch_id="")
        session.add(log_template)
        session.commit()
        ti = create_task_instance(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            start_date=self.DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            execution_date=self.DEFAULT_DATE,
            state=TaskInstanceState.RUNNING,
        )
        ti.try_number = 3
        ti.hostname = "localhost"
        self.ti = ti
        yield
        clear_db_runs()
        clear_db_dags()
        session.delete(log_template)
        session.commit()

    def test_test_read_log_chunks_should_read_one_try(self):
        task_log_reader = TaskLogReader()
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        logs, metadatas = task_log_reader.read_log_chunks(
            ti=ti, try_number=1, metadata={}
        )
        assert logs[0] == [
            (
                "localhost",
                "*** Found local files:\n"
                f"***   * {self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/1.log\n"
                "try_number=1.",
            )
        ]
        assert metadatas == {"end_of_log": True, "log_pos": 13}

    def test_test_read_log_chunks_should_read_all_files(self):
        task_log_reader = TaskLogReader()
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        logs, metadatas = task_log_reader.read_log_chunks(
            ti=ti, try_number=None, metadata={}
        )

        assert logs == [
            [
                (
                    "localhost",
                    "*** Found local files:\n"
                    f"***   * {self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/1.log\n"
                    "try_number=1.",
                )
            ],
            [
                (
                    "localhost",
                    "*** Found local files:\n"
                    f"***   * {self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/2.log\n"
                    f"try_number=2.",
                )
            ],
            [
                (
                    "localhost",
                    "*** Found local files:\n"
                    f"***   * {self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/3.log\n"
                    f"try_number=3.",
                )
            ],
        ]
        assert metadatas == {"end_of_log": True, "log_pos": 13}

    def test_test_test_read_log_stream_should_read_one_try(self):
        task_log_reader = TaskLogReader()
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        stream = task_log_reader.read_log_stream(ti=ti, try_number=1, metadata={})
        assert list(stream) == [
            "localhost\n*** Found local files:\n"
            f"***   * {self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/1.log\n"
            "try_number=1.\n"
        ]

    def test_test_test_read_log_stream_should_read_all_logs(self):
        task_log_reader = TaskLogReader()
        self.ti.state = (
            TaskInstanceState.SUCCESS
        )  # Ensure mocked instance is completed to return stream
        stream = task_log_reader.read_log_stream(ti=self.ti, try_number=None, metadata={})
        assert list(stream) == [
            "localhost\n*** Found local files:\n"
            f"***   * {self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/1.log\n"
            "try_number=1."
            "\n",
            "localhost\n*** Found local files:\n"
            f"***   * {self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/2.log\n"
            "try_number=2."
            "\n",
            "localhost\n*** Found local files:\n"
            f"***   * {self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/3.log\n"
            "try_number=3."
            "\n",
        ]

    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_read_log_stream_should_support_multiple_chunks(self, mock_read):
        first_return = ([[("", "1st line")]], [{}])
        second_return = ([[("", "2nd line")]], [{"end_of_log": False}])
        third_return = ([[("", "3rd line")]], [{"end_of_log": True}])
        fourth_return = ([[("", "should never be read")]], [{"end_of_log": True}])
        mock_read.side_effect = [first_return, second_return, third_return, fourth_return]

        task_log_reader = TaskLogReader()
        self.ti.state = TaskInstanceState.SUCCESS
        log_stream = task_log_reader.read_log_stream(
            ti=self.ti, try_number=1, metadata={}
        )
        assert ["\n1st line\n", "\n2nd line\n", "\n3rd line\n"] == list(log_stream)

        mock_read.assert_has_calls(
            [
                mock.call(self.ti, 1, metadata={}),
                mock.call(self.ti, 1, metadata={}),
                mock.call(self.ti, 1, metadata={"end_of_log": False}),
            ],
            any_order=False,
        )

    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_read_log_stream_should_read_each_try_in_turn(self, mock_read):
        first_return = ([[("", "try_number=1.")]], [{"end_of_log": True}])
        second_return = ([[("", "try_number=2.")]], [{"end_of_log": True}])
        third_return = ([[("", "try_number=3.")]], [{"end_of_log": True}])
        fourth_return = ([[("", "should never be read")]], [{"end_of_log": True}])
        mock_read.side_effect = [first_return, second_return, third_return, fourth_return]

        task_log_reader = TaskLogReader()
        log_stream = task_log_reader.read_log_stream(
            ti=self.ti, try_number=None, metadata={}
        )
        assert ["\ntry_number=1.\n", "\ntry_number=2.\n", "\ntry_number=3.\n"] == list(
            log_stream
        )

        mock_read.assert_has_calls(
            [
                mock.call(self.ti, 1, metadata={}),
                mock.call(self.ti, 2, metadata={}),
                mock.call(self.ti, 3, metadata={}),
            ],
            any_order=False,
        )

    def test_supports_external_link(self):
        task_log_reader = TaskLogReader()

        # Short circuit if log_handler doesn't include ExternalLoggingMixin
        task_log_reader.log_handler = mock.MagicMock()
        mock_prop = mock.PropertyMock()
        mock_prop.return_value = False
        type(task_log_reader.log_handler).supports_external_link = mock_prop
        assert not task_log_reader.supports_external_link
        mock_prop.assert_not_called()

        # Otherwise, defer to the log_handlers supports_external_link
        task_log_reader.log_handler = mock.MagicMock(spec=ExternalLoggingMixin)
        type(task_log_reader.log_handler).supports_external_link = mock_prop
        assert not task_log_reader.supports_external_link
        mock_prop.assert_called_once()

        mock_prop.return_value = True
        assert task_log_reader.supports_external_link

    def test_task_log_filename_unique(self, dag_maker):
        """Ensure the default log_filename_template produces a unique filename.

        See discussion in apache/airflow#19058 [1]_ for how uniqueness may
        change in a future Airflow release. For now, the logical date is used
        to distinguish DAG runs. This test should be modified when the logical
        date is no longer used to ensure uniqueness.

        [1]: https://github.com/apache/airflow/issues/19058
        """
        dag_id = "test_task_log_filename_ts_corresponds_to_logical_date"
        task_id = "echo_run_type"

        def echo_run_type(dag_run: DagRun, **kwargs):
            print(dag_run.run_type)

        with dag_maker(dag_id, start_date=self.DEFAULT_DATE, schedule="@daily") as dag:
            PythonOperator(task_id=task_id, python_callable=echo_run_type)

        start = pendulum.datetime(2021, 1, 1)
        end = start + datetime.timedelta(days=1)
        trigger_time = end + datetime.timedelta(hours=4, minutes=29)  # Arbitrary.

        # Create two DAG runs that have the same data interval, but not the same
        # execution date, to check if they correctly use different log files.
        scheduled_dagrun: DagRun = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=start,
            data_interval=DataInterval(start, end),
        )
        manual_dagrun: DagRun = dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=trigger_time,
            data_interval=DataInterval(start, end),
        )

        scheduled_ti = scheduled_dagrun.get_task_instance(task_id)
        manual_ti = manual_dagrun.get_task_instance(task_id)
        assert scheduled_ti is not None
        assert manual_ti is not None

        scheduled_ti.refresh_from_task(dag.get_task(task_id))
        manual_ti.refresh_from_task(dag.get_task(task_id))

        reader = TaskLogReader()
        assert reader.render_log_filename(scheduled_ti, 1) != reader.render_log_filename(
            manual_ti, 1
        )
