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
from unittest import mock
from unittest.mock import Mock, patch

import pendulum
import pytest
from opensearchpy.exceptions import NotFoundError

from airflow.providers.common.compat.sdk import conf
from airflow.providers.opensearch.log.os_response import OpensearchResponse
from airflow.providers.opensearch.log.os_task_handler import (
    OpensearchTaskHandler,
    get_os_kwargs_from_config,
    getattr_nested,
)
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS
from unit.opensearch.conftest import MockClient

opensearchpy = pytest.importorskip("opensearchpy")


def get_ti(dag_id, task_id, logical_date, create_task_instance):
    ti = create_task_instance(
        dag_id=dag_id,
        task_id=task_id,
        logical_date=logical_date,
        dagrun_state=DagRunState.RUNNING,
        state=TaskInstanceState.RUNNING,
    )
    ti.try_number = 1
    ti.raw = False
    return ti


class TestOpensearchTaskHandler:
    DAG_ID = "dag_for_testing_os_task_handler"
    TASK_ID = "task_for_testing_os_log_handler"
    LOGICAL_DATE = datetime(2016, 1, 1)
    LOG_ID = f"{DAG_ID}-{TASK_ID}-2016-01-01T00:00:00+00:00-1"
    JSON_LOG_ID = f"{DAG_ID}-{TASK_ID}-{OpensearchTaskHandler._clean_date(LOGICAL_DATE)}-1"
    FILENAME_TEMPLATE = "{try_number}.log"

    # TODO: Remove when we stop testing for 2.11 compatibility
    @pytest.fixture(autouse=True)
    def _use_historical_filename_templates(self):
        with conf_vars({("core", "use_historical_filename_templates"): "True"}):
            yield

    @pytest.fixture
    def ti(self, create_task_instance, create_log_template):
        if AIRFLOW_V_3_0_PLUS:
            create_log_template(self.FILENAME_TEMPLATE, "{dag_id}-{task_id}-{logical_date}-{try_number}")
        else:
            create_log_template(
                self.FILENAME_TEMPLATE,
                "{dag_id}-{task_id}-{execution_date}-{try_number}",
            )
        yield get_ti(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            logical_date=self.LOGICAL_DATE,
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

        assert message == "Some Message 1\nAnother Some Message 2"

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

    @pytest.mark.db_test
    def test_read(self, ti):
        ts = pendulum.now()
        logs, metadatas = self.os_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )

        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            expected_msg = "Some Message 1"
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == expected_msg
            metadata = metadatas
        else:
            expected_msg = "Some Message 1\nAnother Some Message 2"
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert logs[0][0][-1] == expected_msg

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert timezone.parse(metadata["last_log_timestamp"]) > ts

    @pytest.mark.db_test
    def test_read_with_patterns(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.os_task_handler, "index_patterns", new="test_*,other_*"):
            logs, metadatas = self.os_task_handler.read(
                ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
            )

        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            expected_msg = "Some Message 1"
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == expected_msg
            metadata = metadatas
        else:
            expected_msg = "Some Message 1\nAnother Some Message 2"
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert logs[0][0][-1] == expected_msg

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert timezone.parse(metadata["last_log_timestamp"]) > ts

    @pytest.mark.db_test
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
                    ti,
                    1,
                    {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False},
                )
        if AIRFLOW_V_3_0_PLUS:
            assert logs == []

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert logs == [[]]

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert metadata["offset"] == "0"
        # last_log_timestamp won't change if no log lines read.
        assert timezone.parse(metadata["last_log_timestamp"]) == ts

    @pytest.mark.db_test
    def test_read_with_missing_index(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.os_task_handler, "index_patterns", new="nonexistent,test_*"):
            with mock.patch.object(
                self.os_task_handler.client,
                "count",
                side_effect=NotFoundError(404, "IndexNotFoundError"),
            ):
                with pytest.raises(NotFoundError, match=r"IndexNotFoundError"):
                    self.os_task_handler.read(
                        ti,
                        1,
                        {
                            "offset": 0,
                            "last_log_timestamp": str(ts),
                            "end_of_log": False,
                        },
                    )

    @pytest.mark.parametrize("seconds", [3, 6])
    @pytest.mark.db_test
    def test_read_missing_logs(self, seconds, create_task_instance):
        """
        When the log actually isn't there to be found, we only want to wait for 5 seconds.
        In this case we expect to receive a message of the form 'Log {log_id} not found in Opensearch ...'
        """
        ti = get_ti(
            self.DAG_ID,
            self.TASK_ID,
            pendulum.instance(self.LOGICAL_DATE).add(days=1),  # so logs are not found
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
        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            if seconds > 5:
                # we expect a log not found message when checking began more than 5 seconds ago
                assert len(logs) == 1
                actual_message = logs[0].event
                expected_pattern = r"^\*\*\* Log .* not found in Opensearch.*"
                assert re.match(expected_pattern, actual_message) is not None
                assert metadatas["end_of_log"] is True
            else:
                # we've "waited" less than 5 seconds so it should not be "end of log" and should be no log message
                assert logs == []
                assert metadatas["end_of_log"] is False
            assert metadatas["offset"] == "0"
            assert timezone.parse(metadatas["last_log_timestamp"]) == ts
        else:
            assert len(logs) == 1
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
            assert metadatas[0]["offset"] == "0"
            assert timezone.parse(metadatas[0]["last_log_timestamp"]) == ts

    @pytest.mark.db_test
    def test_read_with_none_metadata(self, ti):
        logs, metadatas = self.os_task_handler.read(ti, 1)

        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            expected_message = "Some Message 1"
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == expected_message

            metadata = metadatas
        else:
            expected_message = "Some Message 1\nAnother Some Message 2"
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert logs[0][0][-1] == expected_message

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert timezone.parse(metadata["last_log_timestamp"]) < pendulum.now()

    @pytest.mark.db_test
    def test_set_context(self, ti):
        self.os_task_handler.set_context(ti)
        assert self.os_task_handler.mark_end_on_close

    @pytest.mark.db_test
    def test_set_context_w_json_format_and_write_stdout(self, ti):
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.os_task_handler.formatter = formatter
        self.os_task_handler.write_stdout = True
        self.os_task_handler.json_format = True
        self.os_task_handler.set_context(ti)

    @pytest.mark.db_test
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

    @pytest.mark.db_test
    def test_close_no_mark_end(self, ti):
        ti.raw = True
        self.os_task_handler.set_context(ti)
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark not in log_file.read()
        assert self.os_task_handler.closed

    @pytest.mark.db_test
    def test_close_closed(self, ti):
        self.os_task_handler.closed = True
        self.os_task_handler.set_context(ti)
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert len(log_file.read()) == 0

    @pytest.mark.db_test
    def test_close_with_no_handler(self, ti):
        self.os_task_handler.set_context(ti)
        self.os_task_handler.handler = None
        self.os_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert len(log_file.read()) == 0
        assert self.os_task_handler.closed

    @pytest.mark.db_test
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

    @pytest.mark.db_test
    def test_render_log_id(self, ti):
        assert self.os_task_handler._render_log_id(ti, 1) == self.LOG_ID

        self.os_task_handler.json_format = True
        assert self.os_task_handler._render_log_id(ti, 1) == self.JSON_LOG_ID

    def test_clean_date(self):
        clean_execution_date = self.os_task_handler._clean_date(datetime(2016, 7, 8, 9, 10, 11, 12))
        assert clean_execution_date == "2016_07_08T09_10_11_000012"

    @mock.patch("sys.__stdout__", new_callable=StringIO)
    @pytest.mark.db_test
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


# ---------------------------------------------------------------------------
# Tests for the error_detail helpers (issue #63736)
# ---------------------------------------------------------------------------


class TestFormatErrorDetail:
    """Unit tests for _format_error_detail."""

    def test_returns_none_for_empty(self):
        from airflow.providers.opensearch.log.os_task_handler import _format_error_detail

        assert _format_error_detail(None) is None
        assert _format_error_detail([]) is None

    def test_returns_string_for_non_list(self):
        from airflow.providers.opensearch.log.os_task_handler import _format_error_detail

        assert _format_error_detail("raw string") == "raw string"

    def test_formats_single_exception(self):
        from airflow.providers.opensearch.log.os_task_handler import _format_error_detail

        error_detail = [
            {
                "is_cause": False,
                "frames": [
                    {"filename": "/app/task.py", "lineno": 13, "name": "log_and_raise"},
                ],
                "exc_type": "RuntimeError",
                "exc_value": "Something went wrong.",
                "exceptions": [],
                "is_group": False,
            }
        ]
        result = _format_error_detail(error_detail)
        assert result is not None
        assert "Traceback (most recent call last):" in result
        assert 'File "/app/task.py", line 13, in log_and_raise' in result
        assert "RuntimeError: Something went wrong." in result

    def test_formats_chained_exceptions(self):
        from airflow.providers.opensearch.log.os_task_handler import _format_error_detail

        error_detail = [
            {
                "is_cause": True,
                "frames": [{"filename": "/a.py", "lineno": 1, "name": "foo"}],
                "exc_type": "ValueError",
                "exc_value": "original",
                "exceptions": [],
            },
            {
                "is_cause": False,
                "frames": [{"filename": "/b.py", "lineno": 2, "name": "bar"}],
                "exc_type": "RuntimeError",
                "exc_value": "wrapped",
                "exceptions": [],
            },
        ]
        result = _format_error_detail(error_detail)
        assert result is not None
        assert "direct cause" in result
        assert "ValueError: original" in result
        assert "RuntimeError: wrapped" in result

    def test_exc_type_without_value(self):
        from airflow.providers.opensearch.log.os_task_handler import _format_error_detail

        error_detail = [
            {
                "is_cause": False,
                "frames": [],
                "exc_type": "StopIteration",
                "exc_value": "",
            }
        ]
        result = _format_error_detail(error_detail)
        assert result is not None
        assert result.endswith("StopIteration")

    def test_non_dict_items_are_stringified(self):
        from airflow.providers.opensearch.log.os_task_handler import _format_error_detail

        result = _format_error_detail(["unexpected string item"])
        assert result is not None
        assert "unexpected string item" in result


class TestBuildLogFields:
    """Unit tests for _build_log_fields."""

    def test_filters_to_allowed_fields(self):
        from airflow.providers.opensearch.log.os_task_handler import _build_log_fields

        hit = {"event": "hello", "level": "info", "unknown_field": "should be dropped"}
        result = _build_log_fields(hit)
        assert "event" in result
        assert "level" in result
        assert "unknown_field" not in result

    def test_message_mapped_to_event(self):
        from airflow.providers.opensearch.log.os_task_handler import _build_log_fields

        hit = {"message": "plain message", "timestamp": "2024-01-01T00:00:00Z"}
        fields = _build_log_fields(hit)
        assert fields["event"] == "plain message"
        assert "message" not in fields  # Ensure it is popped if used as event

    def test_message_preserved_if_event_exists(self):
        from airflow.providers.opensearch.log.os_task_handler import _build_log_fields

        hit = {"event": "structured event", "message": "plain message"}
        fields = _build_log_fields(hit)
        assert fields["event"] == "structured event"
        # message is preserved if it's in TASK_LOG_FIELDS and doesn't collide with event
        assert fields["message"] == "plain message"

    def test_levelname_mapped_to_level(self):
        from airflow.providers.opensearch.log.os_task_handler import _build_log_fields

        hit = {"event": "msg", "levelname": "ERROR"}
        result = _build_log_fields(hit)
        assert result["level"] == "ERROR"
        assert "levelname" not in result

    def test_at_timestamp_mapped_to_timestamp(self):
        from airflow.providers.opensearch.log.os_task_handler import _build_log_fields

        hit = {"event": "msg", "@timestamp": "2024-01-01T00:00:00Z"}
        result = _build_log_fields(hit)
        assert result["timestamp"] == "2024-01-01T00:00:00Z"
        assert "@timestamp" not in result

    def test_error_detail_is_kept_as_list(self):
        from airflow.providers.opensearch.log.os_task_handler import _build_log_fields

        error_detail = [
            {
                "is_cause": False,
                "frames": [{"filename": "/dag.py", "lineno": 10, "name": "run"}],
                "exc_type": "RuntimeError",
                "exc_value": "Woopsie.",
            }
        ]
        hit = {
            "event": "Task failed with exception",
            "error_detail": error_detail,
        }
        result = _build_log_fields(hit)
        assert result["error_detail"] == error_detail

    def test_error_detail_dropped_when_empty(self):
        from airflow.providers.opensearch.log.os_task_handler import _build_log_fields

        hit = {"event": "msg", "error_detail": []}
        result = _build_log_fields(hit)
        assert "error_detail" not in result

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="StructuredLogMessage only exists in Airflow 3+")
    def test_read_includes_error_detail_in_structured_message(self):
        """End-to-end: a hit with error_detail should surface it in the returned StructuredLogMessage."""
        from airflow.providers.opensearch.log.os_task_handler import OpensearchTaskHandler

        local_log_location = "local/log/location"
        handler = OpensearchTaskHandler(
            base_log_folder=local_log_location,
            end_of_log_mark="end_of_log\n",
            write_stdout=False,
            json_format=False,
            json_fields="asctime,filename,lineno,levelname,message,exc_text",
            host="localhost",
            port=9200,
            username="admin",
            password="password",
        )

        log_id = "test_dag-test_task-test_run--1-1"
        body = {
            "event": "Task failed with exception",
            "log_id": log_id,
            "offset": 1,
            "error_detail": [
                {
                    "is_cause": False,
                    "frames": [
                        {"filename": "/opt/airflow/dags/fail.py", "lineno": 13, "name": "log_and_raise"}
                    ],
                    "exc_type": "RuntimeError",
                    "exc_value": "Woopsie. Something went wrong.",
                }
            ],
        }

        # Instead of firing up an OpenSearch client, we patch the IO and response class
        mock_hit_dict = body.copy()
        from airflow.providers.opensearch.log.os_response import Hit, OpensearchResponse

        mock_hit = Hit({"_source": mock_hit_dict})
        mock_response = mock.MagicMock(spec=OpensearchResponse)
        mock_response.hits = [mock_hit]
        mock_response.__iter__ = mock.Mock(return_value=iter([mock_hit]))
        mock_response.__bool__ = mock.Mock(return_value=True)
        mock_response.__getitem__ = mock.Mock(return_value=mock_hit)

        with mock.patch.object(handler, "_os_read", return_value=mock_response):
            with mock.patch.object(handler, "_group_logs_by_host", return_value={"localhost": [mock_hit]}):
                from airflow.providers.opensearch.log.os_task_handler import _build_log_fields
                from airflow.utils.log.file_task_handler import StructuredLogMessage

                fields = _build_log_fields(mock_hit.to_dict())
                msg = StructuredLogMessage(**fields)

                assert msg.event == "Task failed with exception"
                assert hasattr(msg, "error_detail")
                assert msg.error_detail == body["error_detail"]
