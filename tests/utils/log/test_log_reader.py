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

import copy
import logging
import os
import sys
import tempfile
from unittest import mock

import pytest

from airflow import settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.utils import timezone
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.state import TaskInstanceState
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs


class TestLogView:
    DAG_ID = "dag_log_reader"
    TASK_ID = "task_log_reader"
    DEFAULT_DATE = timezone.datetime(2017, 9, 1)

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
        logging_config["handlers"]["task"][
            "filename_template"
        ] = "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts | replace(':', '.') }}/{{ try_number }}.log"
        settings_file = os.path.join(settings_folder, "airflow_local_settings.py")
        with open(settings_file, "w") as handle:
            new_logging_file = f"LOGGING_CONFIG = {logging_config}"
            handle.writelines(new_logging_file)
        with conf_vars({("logging", "logging_config_class"): "airflow_local_settings.LOGGING_CONFIG"}):
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
    def prepare_db(self, session, create_task_instance):
        ti = create_task_instance(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            start_date=self.DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            execution_date=self.DEFAULT_DATE,
            state=TaskInstanceState.RUNNING,
        )
        ti.try_number = 3
        self.ti = ti
        yield
        clear_db_runs()
        clear_db_dags()

    def test_test_read_log_chunks_should_read_one_try(self):
        task_log_reader = TaskLogReader()
        logs, metadatas = task_log_reader.read_log_chunks(ti=self.ti, try_number=1, metadata={})

        assert [
            (
                '',
                f"*** Reading local file: "
                f"{self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/1.log\n"
                f"try_number=1.\n",
            )
        ] == logs[0]
        assert {"end_of_log": True} == metadatas

    def test_test_read_log_chunks_should_read_all_files(self):
        task_log_reader = TaskLogReader()
        logs, metadatas = task_log_reader.read_log_chunks(ti=self.ti, try_number=None, metadata={})

        assert [
            [
                (
                    '',
                    "*** Reading local file: "
                    f"{self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/1.log\n"
                    "try_number=1.\n",
                )
            ],
            [
                (
                    '',
                    f"*** Reading local file: "
                    f"{self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/2.log\n"
                    f"try_number=2.\n",
                )
            ],
            [
                (
                    '',
                    f"*** Reading local file: "
                    f"{self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/3.log\n"
                    f"try_number=3.\n",
                )
            ],
        ] == logs
        assert {"end_of_log": True} == metadatas

    def test_test_test_read_log_stream_should_read_one_try(self):
        task_log_reader = TaskLogReader()
        stream = task_log_reader.read_log_stream(ti=self.ti, try_number=1, metadata={})

        assert [
            "\n*** Reading local file: "
            f"{self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/1.log\n"
            "try_number=1.\n"
            "\n"
        ] == list(stream)

    def test_test_test_read_log_stream_should_read_all_logs(self):
        task_log_reader = TaskLogReader()
        stream = task_log_reader.read_log_stream(ti=self.ti, try_number=None, metadata={})
        assert [
            "\n*** Reading local file: "
            f"{self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/1.log\n"
            "try_number=1.\n"
            "\n",
            "\n*** Reading local file: "
            f"{self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/2.log\n"
            "try_number=2.\n"
            "\n",
            "\n*** Reading local file: "
            f"{self.log_dir}/dag_log_reader/task_log_reader/2017-09-01T00.00.00+00.00/3.log\n"
            "try_number=3.\n"
            "\n",
        ] == list(stream)

    @mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read")
    def test_read_log_stream_should_support_multiple_chunks(self, mock_read):
        first_return = ([[('', "1st line")]], [{}])
        second_return = ([[('', "2nd line")]], [{"end_of_log": False}])
        third_return = ([[('', "3rd line")]], [{"end_of_log": True}])
        fourth_return = ([[('', "should never be read")]], [{"end_of_log": True}])
        mock_read.side_effect = [first_return, second_return, third_return, fourth_return]

        task_log_reader = TaskLogReader()
        log_stream = task_log_reader.read_log_stream(ti=self.ti, try_number=1, metadata={})
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
        first_return = ([[('', "try_number=1.")]], [{"end_of_log": True}])
        second_return = ([[('', "try_number=2.")]], [{"end_of_log": True}])
        third_return = ([[('', "try_number=3.")]], [{"end_of_log": True}])
        fourth_return = ([[('', "should never be read")]], [{"end_of_log": True}])
        mock_read.side_effect = [first_return, second_return, third_return, fourth_return]

        task_log_reader = TaskLogReader()
        log_stream = task_log_reader.read_log_stream(ti=self.ti, try_number=None, metadata={})
        assert ['\ntry_number=1.\n', '\ntry_number=2.\n', '\ntry_number=3.\n'] == list(log_stream)

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
