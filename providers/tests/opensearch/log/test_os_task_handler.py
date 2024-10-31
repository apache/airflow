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

import json
import logging
import os
import re
import shutil
from io import StringIO
from pathlib import Path
from unittest import mock
from unittest.mock import Mock, patch

import pendulum
import pytest

opensearchpy = pytest.importorskip("opensearchpy")
from opensearchpy.exceptions import NotFoundError

from airflow.configuration import conf
from airflow.providers.opensearch.log.os_response import OpensearchResponse
from airflow.providers.opensearch.log.os_task_handler import (
    OpensearchTaskHandler,
    get_os_kwargs_from_config,
    getattr_nested,
)
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import datetime

from providers.tests.opensearch.conftest import MockClient
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test

AIRFLOW_SOURCES_ROOT_DIR = Path(__file__).parents[4].resolve()
ES_PROVIDER_YAML_FILE = AIRFLOW_SOURCES_ROOT_DIR / "airflow" / "providers" / "opensearch" / "provider.yaml"


def get_ti(dag_id, task_id, execution_date, create_task_instance):
    ti = create_task_instance(
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date,
        dagrun_state=DagRunState.RUNNING,
        state=TaskInstanceState.RUNNING,
    )
    ti.try_number = 1
    ti.raw = False
    return ti


class TestOpensearchTaskHandler:
    DAG_ID = "dag_for_testing_os_task_handler"
    TASK_ID = "task_for_testing_os_log_handler"
    EXECUTION_DATE = datetime(2016, 1, 1)
    LOG_ID = f"{DAG_ID}-{TASK_ID}-2016-01-01T00:00:00+00:00-1"
    JSON_LOG_ID = f"{DAG_ID}-{TASK_ID}-{OpensearchTaskHandler._clean_date(EXECUTION_DATE)}-1"
    FILENAME_TEMPLATE = "{try_number}.log"

    @pytest.fixture
    def ti(self, create_task_instance, create_log_template):
        create_log_template(self.FILENAME_TEMPLATE, "{dag_id}-{task_id}-{execution_date}-{try_number}")
        yield get_ti(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            execution_date=self.EXECUTION_DATE,
            create_task_instance=create_task_instance,
        )
        clear_db_runs()
        clear_db_dags()

    def setup_method(self):
        self.local_log_location = "local/log/location"
        self.end_of_log_mark = "end_of_log\n"
        self.write_stdout = False
        self.json_format = False
        self.json_fields = "asctime,filename,lineno,levelname,message,exc_text"
        self.host_field = "host"
        self.offset_field = "offset"
        self.username = "admin"
        self.password = "admin"
        self.host = "localhost"
        self.port = 9200
        self.os_task_handler = OpensearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
        )

        self.os_task_handler.client = MockClient()

    def teardown_method(self):
        shutil.rmtree(self.local_log_location.split(os.path.sep)[0], ignore_errors=True)

    def test_os_response(self):
        sample_response = self.os_task_handler.client.sample_log_response()
        response = OpensearchResponse(self.os_task_handler, sample_response)
        logs_by_host = self.os_task_handler._group_logs_by_host(response)

        def concat_logs(lines):
            log_range = -1 if lines[-1].message == self.os_task_handler.end_of_log_mark else None
            return "\n".join(self.os_task_handler._format_msg(line) for line in lines[:log_range])

        for hosted_log in logs_by_host.values():
            message = concat_logs(hosted_log)

        assert (
            message == "Dependencies all met for dep_context=non-requeueable"
            " deps ti=<TaskInstance: example_bash_operator.run_after_loop owen_run_run [queued]>\n"
            "Starting attempt 1 of 1\nExecuting <Task(BashOperator): run_after_loop> "
            "on 2023-07-09 07:47:32+00:00"
        )

    def test_client(self):
        assert self.os_task_handler.index_patterns == "_all"

    def test_client_with_config(self):
        config = dict(conf.getsection("opensearch_configs"))
        expected_dict = {
            "http_compress": False,
            "use_ssl": False,
            "verify_certs": False,
            "ssl_assert_hostname": False,
            "ssl_show_warn": False,
            "ca_certs": "",
        }
        assert config == expected_dict
        # ensure creating with configs does not fail
        OpensearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
            os_kwargs=config,
        )

    def test_client_with_patterns(self):
        # ensure creating with index patterns does not fail
        patterns = "test_*,other_*"
        handler = OpensearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
            index_patterns=patterns,
        )
        assert handler.index_patterns == patterns

    def test_read(self, ti):
        ts = pendulum.now()
        logs, metadatas = self.os_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )

        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert len(logs[0]) == 1
        assert (
            logs[0][0][-1] == "Dependencies all met for dep_context=non-requeueable"
            " deps ti=<TaskInstance: example_bash_operator.run_after_loop owen_run_run [queued]>\n"
            "Starting attempt 1 of 1\nExecuting <Task(BashOperator): run_after_loop> "
            "on 2023-07-09 07:47:32+00:00"
        )
        assert not metadatas[0]["end_of_log"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) > ts

    def test_read_with_patterns(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.os_task_handler, "index_patterns", new="test_*,other_*"):
            logs, metadatas = self.os_task_handler.read(
                ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
            )

        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert len(logs[0]) == 1
        assert (
            logs[0][0][-1] == "Dependencies all met for dep_context=non-requeueable"
            " deps ti=<TaskInstance: example_bash_operator.run_after_loop owen_run_run [queued]>\n"
            "Starting attempt 1 of 1\nExecuting <Task(BashOperator): run_after_loop> "
            "on 2023-07-09 07:47:32+00:00"
        )
        assert not metadatas[0]["end_of_log"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) > ts

    def test_read_with_patterns_no_match(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.os_task_handler, "index_patterns", new="test_other_*,test_another_*"):
            with mock.patch.object(
                self.os_task_handler.client,
                "search",
                return_value={
                    "_shards": {"failed": 0, "skipped": 0, "successful": 7, "total": 7},
                    "hits": {"hits": []},
                    "timed_out": False,
                    "took": 7,
                },
            ):
                logs, metadatas = self.os_task_handler.read(
                    ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
                )

        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert not metadatas[0]["end_of_log"]
        assert "0" == metadatas[0]["offset"]
        # last_log_timestamp won't change if no log lines read.
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) == ts

    def test_read_with_missing_index(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.os_task_handler, "index_patterns", new="nonexistent,test_*"):
            with mock.patch.object(
                self.os_task_handler.client, "count", side_effect=NotFoundError(404, "IndexNotFoundError")
            ):
                with pytest.raises(NotFoundError, match=r"IndexNotFoundError"):
                    self.os_task_handler.read(
                        ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
                    )

    @pytest.mark.parametrize("seconds", [3, 6])
    def test_read_missing_logs(self, seconds, create_task_instance):
        """
        When the log actually isn't there to be found, we only want to wait for 5 seconds.
        In this case we expect to receive a message of the form 'Log {log_id} not found in Opensearch ...'
        """
        ti = get_ti(
            self.DAG_ID,
            self.TASK_ID,
            pendulum.instance(self.EXECUTION_DATE).add(days=1),  # so logs are not found
            create_task_instance=create_task_instance,
        )
        ts = pendulum.now().add(seconds=-seconds)
        with mock.patch.object(
            self.os_task_handler.client,
            "search",
            return_value={
                "_shards": {"failed": 0, "skipped": 0, "successful": 7, "total": 7},
                "hits": {"hits": []},
                "timed_out": False,
                "took": 7,
            },
        ):
            logs, metadatas = self.os_task_handler.read(ti, 1, {"offset": 0, "last_log_timestamp": str(ts)})

        assert 1 == len(logs)
        if seconds > 5:
            # we expect a log not found message when checking began more than 5 seconds ago
            assert len(logs[0]) == 1
            actual_message = logs[0][0][1]
            expected_pattern = r"^\*\*\* Log .* not found in Opensearch.*"
            assert re.match(expected_pattern, actual_message) is not None
            assert metadatas[0]["end_of_log"] is True
        else:
            # we've "waited" less than 5 seconds so it should not be "end of log" and should be no log message
            assert len(logs[0]) == 0
            assert logs == [[]]
            assert metadatas[0]["end_of_log"] is False
        assert len(logs) == len(metadatas)
        assert "0" == metadatas[0]["offset"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) == ts

    def test_read_with_none_metadata(self, ti):
        logs, metadatas = self.os_task_handler.read(ti, 1)
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert (
            logs[0][0][-1] == "Dependencies all met for dep_context=non-requeueable"
            " deps ti=<TaskInstance: example_bash_operator.run_after_loop owen_run_run [queued]>\n"
            "Starting attempt 1 of 1\nExecuting <Task(BashOperator): run_after_loop> "
            "on 2023-07-09 07:47:32+00:00"
        )
        assert not metadatas[0]["end_of_log"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) < pendulum.now()

    def test_set_context(self, ti):
        self.os_task_handler.set_context(ti)
        assert self.os_task_handler.mark_end_on_close

    def test_set_context_w_json_format_and_write_stdout(self, ti):
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.os_task_handler.formatter = formatter
        self.os_task_handler.write_stdout = True
        self.os_task_handler.json_format = True
        self.os_task_handler.set_context(ti)

    def test_close(self, ti):
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.os_task_handler.formatter = formatter

        self.os_task_handler.set_context(ti)
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            # end_of_log_mark may contain characters like '\n' which is needed to
            # have the log uploaded but will not be stored in elasticsearch.
            # so apply the strip() to log_file.read()
            log_line = log_file.read().strip()
            assert log_line.endswith(self.end_of_log_mark.strip())
        assert self.os_task_handler.closed

    def test_close_no_mark_end(self, ti):
        ti.raw = True
        self.os_task_handler.set_context(ti)
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark not in log_file.read()
        assert self.os_task_handler.closed

    def test_close_closed(self, ti):
        self.os_task_handler.closed = True
        self.os_task_handler.set_context(ti)
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert 0 == len(log_file.read())

    def test_close_with_no_handler(self, ti):
        self.os_task_handler.set_context(ti)
        self.os_task_handler.handler = None
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert 0 == len(log_file.read())
        assert self.os_task_handler.closed

    def test_close_with_no_stream(self, ti):
        self.os_task_handler.set_context(ti)
        self.os_task_handler.handler.stream = None
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark in log_file.read()
        assert self.os_task_handler.closed

        self.os_task_handler.set_context(ti)
        self.os_task_handler.handler.stream.close()
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark in log_file.read()
        assert self.os_task_handler.closed

    def test_render_log_id(self, ti):
        assert self.LOG_ID == self.os_task_handler._render_log_id(ti, 1)

        self.os_task_handler.json_format = True
        assert self.JSON_LOG_ID == self.os_task_handler._render_log_id(ti, 1)

    #
    def test_clean_date(self):
        clean_execution_date = self.os_task_handler._clean_date(datetime(2016, 7, 8, 9, 10, 11, 12))
        assert "2016_07_08T09_10_11_000012" == clean_execution_date

    @mock.patch("sys.__stdout__", new_callable=StringIO)
    def test_dynamic_offset(self, stdout_mock, ti, time_machine):
        # arrange
        handler = OpensearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=True,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            json_format=True,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
        )
        handler.formatter = logging.Formatter()

        logger = logging.getLogger(__name__)
        logger.handlers = [handler]
        logger.propagate = False

        ti._log = logger
        handler.set_context(ti)

        t1 = pendulum.local(year=2017, month=1, day=1, hour=1, minute=1, second=15)
        t2, t3 = t1 + pendulum.duration(seconds=5), t1 + pendulum.duration(seconds=10)

        # act
        time_machine.move_to(t1, tick=False)
        ti.log.info("Test")
        time_machine.move_to(t2, tick=False)
        ti.log.info("Test2")
        time_machine.move_to(t3, tick=False)
        ti.log.info("Test3")

        # assert
        first_log, second_log, third_log = map(json.loads, stdout_mock.getvalue().strip().splitlines())
        assert first_log["offset"] < second_log["offset"] < third_log["offset"]
        assert first_log["asctime"] == t1.format("YYYY-MM-DDTHH:mm:ss.SSSZZ")
        assert second_log["asctime"] == t2.format("YYYY-MM-DDTHH:mm:ss.SSSZZ")
        assert third_log["asctime"] == t3.format("YYYY-MM-DDTHH:mm:ss.SSSZZ")

    def test_get_index_patterns_with_callable(self):
        with patch("airflow.providers.opensearch.log.os_task_handler.import_string") as mock_import_string:
            mock_callable = Mock(return_value="callable_index_pattern")
            mock_import_string.return_value = mock_callable

            self.os_task_handler.index_patterns_callable = "path.to.index_pattern_callable"
            result = self.os_task_handler._get_index_patterns({})

            mock_import_string.assert_called_once_with("path.to.index_pattern_callable")
            mock_callable.assert_called_once_with({})
            assert result == "callable_index_pattern"


def test_safe_attrgetter():
    class A: ...

    a = A()
    a.b = "b"
    a.c = None
    a.x = a
    a.x.d = "blah"
    assert getattr_nested(a, "b", None) == "b"  # regular getattr
    assert getattr_nested(a, "x.d", None) == "blah"  # nested val
    assert getattr_nested(a, "aa", "heya") == "heya"  # respects non-none default
    assert getattr_nested(a, "c", "heya") is None  # respects none value
    assert getattr_nested(a, "aa", None) is None  # respects none default


def test_retrieve_config_keys():
    """
    Tests that the OpensearchTaskHandler retrieves the correct configuration keys from the config file.
    * old_parameters are removed
    * parameters from config are automatically added
    * constructor parameters missing from config are also added
    :return:
    """
    with conf_vars(
        {
            ("opensearch_configs", "http_compress"): "False",
            ("opensearch_configs", "timeout"): "10",
        }
    ):
        args_from_config = get_os_kwargs_from_config().keys()
        # verify_certs comes from default config value
        assert "verify_certs" in args_from_config
        # timeout comes from config provided value
        assert "timeout" in args_from_config
        # http_compress comes from config value
        assert "http_compress" in args_from_config
        assert "self" not in args_from_config
