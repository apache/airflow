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

import dataclasses
import json
import logging
import re
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch

import pendulum
import pytest
from opensearchpy.exceptions import NotFoundError

from airflow.providers.common.compat.sdk import conf
from airflow.providers.opensearch.log.os_json_formatter import OpensearchJSONFormatter
from airflow.providers.opensearch.log.os_response import OpensearchResponse
from airflow.providers.opensearch.log.os_task_handler import (
    OpensearchRemoteLogIO,
    OpensearchTaskHandler,
    _build_log_fields,
    _format_error_detail,
    _render_log_id,
    _strip_userinfo,
    get_os_kwargs_from_config,
    getattr_nested,
)
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

opensearchpy = pytest.importorskip("opensearchpy")


@dataclasses.dataclass
class _MockTI:
    dag_id: str = "dag_for_testing_os_log_handler"
    task_id: str = "task_for_testing_os_log_handler"
    run_id: str = "run_for_testing_os_log_handler"
    try_number: int = 1
    map_index: int = -1


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


def _build_os_search_response(*sources: dict, index: str = "test_index", doc_type: str = "_doc") -> dict:
    hits = [
        {
            "_id": str(i),
            "_index": index,
            "_score": 1.0,
            "_source": source,
            "_type": doc_type,
        }
        for i, source in enumerate(sources, start=1)
    ]
    return {
        "_shards": {"failed": 0, "skipped": 0, "successful": 1, "total": 1},
        "hits": {
            "hits": hits,
            "max_score": 1.0,
            "total": {"relation": "eq", "value": len(hits)},
        },
        "timed_out": False,
        "took": 1,
    }


def _make_os_response(search, *sources: dict) -> OpensearchResponse:
    return OpensearchResponse(search, _build_os_search_response(*sources))


def _metadata_from_result(metadatas):
    return metadatas if AIRFLOW_V_3_0_PLUS else metadatas[0]


def _assert_log_events(logs, metadatas, *, expected_events: list[str], expected_sources: list[str]):
    metadata = _metadata_from_result(metadatas)
    if AIRFLOW_V_3_0_PLUS:
        logs = list(logs)
        assert logs[0].event == "::group::Log message source details"
        assert logs[0].sources == expected_sources
        assert logs[1].event == "::endgroup::"
        assert [log.event for log in logs[2:]] == expected_events
    else:
        assert len(logs) == 1
        assert len(logs[0]) == 1
        assert logs[0][0][0] == expected_sources[0]
        assert logs[0][0][1] == "\n".join(expected_events)
    return metadata


def _assert_no_logs(logs, metadatas):
    metadata = _metadata_from_result(metadatas)
    if AIRFLOW_V_3_0_PLUS:
        assert logs == []
    else:
        assert logs == [[]]
    return metadata


def _assert_missing_log_message(logs):
    expected_pattern = r"^\*\*\* Log .* not found in Opensearch.*"
    if AIRFLOW_V_3_0_PLUS:
        logs = list(logs)
        assert len(logs) == 1
        assert logs[0].event is not None
        assert re.match(expected_pattern, logs[0].event) is not None
    else:
        assert len(logs) == 1
        assert len(logs[0]) == 1
        assert re.match(expected_pattern, logs[0][0][1]) is not None


class TestOpensearchTaskHandler:
    DAG_ID = "dag_for_testing_os_task_handler"
    TASK_ID = "task_for_testing_os_log_handler"
    LOGICAL_DATE = datetime(2016, 1, 1)
    LOG_ID = f"{DAG_ID}-{TASK_ID}-2016-01-01T00:00:00+00:00-1"
    JSON_LOG_ID = f"{DAG_ID}-{TASK_ID}-{OpensearchTaskHandler._clean_date(LOGICAL_DATE)}-1"
    FILENAME_TEMPLATE = "{try_number}.log"

    @pytest.fixture(autouse=True)
    def _use_historical_filename_templates(self):
        with conf_vars({("core", "use_historical_filename_templates"): "True"}):
            yield

    @pytest.fixture(autouse=True)
    def _setup_handler(self, tmp_path):
        self.local_log_location = str(tmp_path / "logs")
        self.end_of_log_mark = "end_of_log\n"
        self.write_stdout = False
        self.json_format = False
        self.json_fields = "asctime,filename,lineno,levelname,message,exc_text"
        self.host_field = "host"
        self.offset_field = "offset"
        self.test_message = "Some Message 1"
        self.base_log_source = {
            "message": self.test_message,
            "event": self.test_message,
            "log_id": self.LOG_ID,
            "offset": 1,
        }
        self.os_task_handler = OpensearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            host="localhost",
            port=9200,
            username="admin",
            password="admin",
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
        )

    @pytest.fixture
    def ti(self, create_task_instance, create_log_template):
        create_log_template(
            self.FILENAME_TEMPLATE,
            (
                "{dag_id}-{task_id}-{logical_date}-{try_number}"
                if AIRFLOW_V_3_0_PLUS
                else "{dag_id}-{task_id}-{execution_date}-{try_number}"
            ),
        )
        yield get_ti(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            logical_date=self.LOGICAL_DATE,
            create_task_instance=create_task_instance,
        )
        clear_db_runs()
        clear_db_dags()

    @pytest.mark.parametrize(
        ("host", "expected"),
        [
            ("http://localhost", "http://localhost"),
            ("https://localhost", "https://localhost"),
            ("localhost", "http://localhost"),
            ("someurl", "http://someurl"),
            ("https://", "ValueError"),
        ],
    )
    def test_format_url(self, host, expected):
        if expected == "ValueError":
            with pytest.raises(ValueError, match="'https://' is not a valid URL."):
                OpensearchTaskHandler.format_url(host)
        else:
            assert OpensearchTaskHandler.format_url(host) == expected

    @pytest.mark.parametrize(
        ("host", "expected"),
        [
            ("https://user:pass@opensearch.example.com:9200", "https://opensearch.example.com:9200"),
            ("http://USER:PASS@opensearch.example.com", "http://opensearch.example.com"),
            ("https://opensearch.example.com:9200", "https://opensearch.example.com:9200"),
            ("http://localhost:9200", "http://localhost:9200"),
            ("https://user@opensearch.example.com", "https://opensearch.example.com"),
            ("not-a-url", "not-a-url"),
            ("", ""),
        ],
    )
    def test_strip_userinfo(self, host, expected):
        assert _strip_userinfo(host) == expected

    def test_client(self):
        assert isinstance(self.os_task_handler.client, opensearchpy.OpenSearch)
        assert self.os_task_handler.index_patterns == "_all"

    def test_client_with_config(self):
        os_conf = dict(conf.getsection("opensearch_configs"))
        expected_dict = {
            "http_compress": False,
            "use_ssl": False,
            "verify_certs": False,
            "ssl_assert_hostname": False,
            "ssl_show_warn": False,
            "ca_certs": "",
        }
        assert os_conf == expected_dict
        OpensearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            host="localhost",
            port=9200,
            username="admin",
            password="admin",
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
            os_kwargs=os_conf,
        )

    def test_client_with_patterns(self):
        patterns = "test_*,other_*"
        handler = OpensearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            host="localhost",
            port=9200,
            username="admin",
            password="admin",
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
            index_patterns=patterns,
        )
        assert handler.index_patterns == patterns

    @pytest.mark.db_test
    @pytest.mark.parametrize("metadata_mode", ["provided", "none", "empty"])
    def test_read(self, ti, metadata_mode):
        start_time = pendulum.now()
        response = _make_os_response(self.os_task_handler.io, self.base_log_source)

        with patch.object(self.os_task_handler.io, "_os_read", return_value=response):
            if metadata_mode == "provided":
                logs, metadatas = self.os_task_handler.read(
                    ti,
                    1,
                    {"offset": 0, "last_log_timestamp": str(start_time), "end_of_log": False},
                )
            elif metadata_mode == "empty":
                logs, metadatas = self.os_task_handler.read(ti, 1, {})
            else:
                logs, metadatas = self.os_task_handler.read(ti, 1)

        metadata = _assert_log_events(
            logs,
            metadatas,
            expected_events=[self.test_message],
            expected_sources=["http://localhost"],
        )

        assert not metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) >= start_time

    @pytest.mark.db_test
    def test_read_defaults_offset_when_missing_from_metadata(self, ti):
        start_time = pendulum.now()
        with patch.object(self.os_task_handler.io, "_os_read", return_value=None):
            logs, metadatas = self.os_task_handler.read(ti, 1, {"end_of_log": False})

        metadata = _assert_no_logs(logs, metadatas)
        assert metadata["end_of_log"]
        assert metadata["offset"] == "0"
        assert timezone.parse(metadata["last_log_timestamp"]) >= start_time

    @pytest.mark.db_test
    @pytest.mark.parametrize("seconds", [3, 6])
    def test_read_missing_logs(self, ti, seconds):
        start_time = pendulum.now().add(seconds=-seconds)
        with patch.object(self.os_task_handler.io, "_os_read", return_value=None):
            logs, metadatas = self.os_task_handler.read(
                ti,
                1,
                {"offset": 0, "last_log_timestamp": str(start_time), "end_of_log": False},
            )

        metadata = _metadata_from_result(metadatas)
        if seconds > 5:
            _assert_missing_log_message(logs)
        else:
            _assert_no_logs(logs, metadatas)

        assert metadata["end_of_log"]
        assert metadata["offset"] == "0"
        assert timezone.parse(metadata["last_log_timestamp"]) == start_time

    @pytest.mark.db_test
    def test_read_timeout(self, ti):
        start_time = pendulum.now().subtract(minutes=5)
        with patch.object(self.os_task_handler.io, "_os_read", return_value=None):
            logs, metadatas = self.os_task_handler.read(
                ti,
                1,
                {"offset": 1, "last_log_timestamp": str(start_time), "end_of_log": False},
            )

        metadata = _assert_no_logs(logs, metadatas)
        assert metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) == start_time

    @pytest.mark.db_test
    def test_read_with_custom_offset_and_host_fields(self, ti):
        self.os_task_handler.host_field = "host.name"
        self.os_task_handler.offset_field = "log.offset"
        self.os_task_handler.io.host_field = "host.name"
        self.os_task_handler.io.offset_field = "log.offset"
        response = _make_os_response(
            self.os_task_handler.io,
            {
                "message": self.test_message,
                "event": self.test_message,
                "log_id": self.LOG_ID,
                "log": {"offset": 1},
                "host": {"name": "somehostname"},
            },
        )

        with patch.object(self.os_task_handler.io, "_os_read", return_value=response):
            logs, metadatas = self.os_task_handler.read(
                ti,
                1,
                {"offset": 0, "last_log_timestamp": str(pendulum.now()), "end_of_log": False},
            )

        metadata = _assert_log_events(
            logs,
            metadatas,
            expected_events=[self.test_message],
            expected_sources=["somehostname"],
        )
        assert metadata["offset"] == "1"
        assert not metadata["end_of_log"]

    @pytest.mark.db_test
    def test_set_context(self, ti):
        self.os_task_handler.set_context(ti)
        assert self.os_task_handler.mark_end_on_close

    @pytest.mark.db_test
    def test_set_context_w_json_format_and_write_stdout(self, ti):
        self.os_task_handler.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.os_task_handler.write_stdout = True
        self.os_task_handler.json_format = True

        self.os_task_handler.set_context(ti)

        assert isinstance(self.os_task_handler.formatter, OpensearchJSONFormatter)
        assert isinstance(self.os_task_handler.handler, logging.StreamHandler)
        assert self.os_task_handler.context_set

    @pytest.mark.db_test
    def test_close(self, ti):
        self.os_task_handler.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        self.os_task_handler.set_context(ti)
        self.os_task_handler.close()

        log_file = Path(self.local_log_location) / self.FILENAME_TEMPLATE.format(try_number=1)
        assert log_file.read_text().strip().endswith(self.end_of_log_mark.strip())
        assert self.os_task_handler.closed

    @pytest.mark.db_test
    def test_close_no_mark_end(self, ti):
        ti.raw = True
        self.os_task_handler.set_context(ti)
        self.os_task_handler.close()

        log_file = Path(self.local_log_location) / self.FILENAME_TEMPLATE.format(try_number=1)
        assert self.end_of_log_mark not in log_file.read_text()
        assert self.os_task_handler.closed

    @pytest.mark.db_test
    def test_close_with_no_handler(self, ti):
        self.os_task_handler.set_context(ti)
        self.os_task_handler.handler = None
        self.os_task_handler.close()

        log_file = Path(self.local_log_location) / self.FILENAME_TEMPLATE.format(try_number=1)
        assert log_file.read_text() == ""
        assert self.os_task_handler.closed

    @pytest.mark.db_test
    @pytest.mark.parametrize("stream_state", ["none", "closed"])
    def test_close_reopens_stream(self, ti, stream_state):
        self.os_task_handler.set_context(ti)
        if stream_state == "none":
            self.os_task_handler.handler.stream = None
        else:
            self.os_task_handler.handler.stream.close()

        self.os_task_handler.close()

        log_file = Path(self.local_log_location) / self.FILENAME_TEMPLATE.format(try_number=1)
        assert self.end_of_log_mark in log_file.read_text()
        assert self.os_task_handler.closed

    @pytest.mark.db_test
    def test_render_log_id(self, ti):
        assert self.os_task_handler._render_log_id(ti, 1) == self.LOG_ID

        self.os_task_handler.json_format = True
        assert self.os_task_handler._render_log_id(ti, 1) == self.JSON_LOG_ID

    def test_clean_date(self):
        clean_logical_date = OpensearchTaskHandler._clean_date(datetime(2016, 7, 8, 9, 10, 11, 12))
        assert clean_logical_date == "2016_07_08T09_10_11_000012"

    @pytest.mark.db_test
    @patch("sys.__stdout__", new_callable=StringIO)
    def test_dynamic_offset(self, stdout_mock, ti, time_machine):
        handler = OpensearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=True,
            host="localhost",
            port=9200,
            username="admin",
            password="admin",
            json_format=True,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
        )
        handler.formatter = logging.Formatter()

        logger = logging.getLogger("tests.opensearch.dynamic_offset")
        logger.handlers = [handler]
        logger.propagate = False

        ti._log = logger
        handler.set_context(ti)

        t1 = pendulum.local(year=2017, month=1, day=1, hour=1, minute=1, second=15)
        t2 = t1 + pendulum.duration(seconds=5)
        t3 = t1 + pendulum.duration(seconds=10)

        try:
            time_machine.move_to(t1, tick=False)
            ti.log.info("Test")
            time_machine.move_to(t2, tick=False)
            ti.log.info("Test2")
            time_machine.move_to(t3, tick=False)
            ti.log.info("Test3")
        finally:
            logger.handlers = []

        first_log, second_log, third_log = map(json.loads, stdout_mock.getvalue().strip().splitlines())
        assert first_log["offset"] < second_log["offset"] < third_log["offset"]
        assert first_log["asctime"] == t1.format("YYYY-MM-DDTHH:mm:ss.SSSZZ")
        assert second_log["asctime"] == t2.format("YYYY-MM-DDTHH:mm:ss.SSSZZ")
        assert third_log["asctime"] == t3.format("YYYY-MM-DDTHH:mm:ss.SSSZZ")

    def test_get_index_patterns_with_callable(self):
        with patch("airflow.providers.opensearch.log.os_task_handler.import_string") as mock_import_string:
            mock_callable = Mock(return_value="callable_index_pattern")
            mock_import_string.return_value = mock_callable

            self.os_task_handler.io.index_patterns_callable = "path.to.index_pattern_callable"
            result = self.os_task_handler.io._get_index_patterns({})

        mock_import_string.assert_called_once_with("path.to.index_pattern_callable")
        mock_callable.assert_called_once_with({})
        assert result == "callable_index_pattern"


class TestTaskHandlerHelpers:
    def test_safe_attrgetter(self):
        class A: ...

        a = A()
        a.b = "b"
        a.c = None
        a.x = a
        a.x.d = "blah"
        assert getattr_nested(a, "b", None) == "b"
        assert getattr_nested(a, "x.d", None) == "blah"
        assert getattr_nested(a, "aa", "heya") == "heya"
        assert getattr_nested(a, "c", "heya") is None
        assert getattr_nested(a, "aa", None) is None

    def test_retrieve_config_keys(self):
        with conf_vars(
            {
                ("opensearch_configs", "http_compress"): "False",
                ("opensearch_configs", "timeout"): "10",
            }
        ):
            args_from_config = get_os_kwargs_from_config().keys()
            assert "verify_certs" in args_from_config
            assert "timeout" in args_from_config
            assert "http_compress" in args_from_config
            assert "self" not in args_from_config


class TestOpensearchRemoteLogIO:
    @pytest.fixture(autouse=True)
    def _setup_tests(self, tmp_path):
        self.opensearch_io = OpensearchRemoteLogIO(
            write_to_opensearch=True,
            write_stdout=True,
            delete_local_copy=True,
            host="localhost",
            port=9200,
            username="admin",
            password="admin",
            base_log_folder=tmp_path,
            log_id_template="{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}",
        )

    @pytest.fixture
    def ti(self):
        return _MockTI()

    @pytest.fixture
    def tmp_json_file(self, tmp_path):
        file_path = tmp_path / "1.log"
        sample_logs = [
            {"message": "start"},
            {"message": "processing"},
            {"message": "end"},
        ]
        file_path.write_text("\n".join(json.dumps(log) for log in sample_logs) + "\n")
        return file_path

    def test_write_to_stdout(self, tmp_json_file, ti, capsys):
        self.opensearch_io.write_to_opensearch = False
        self.opensearch_io.upload(tmp_json_file, ti)

        captured = capsys.readouterr()
        stdout_lines = captured.out.strip().splitlines()
        log_entries = [json.loads(line) for line in stdout_lines]
        assert [entry["message"] for entry in log_entries] == ["start", "processing", "end"]

    def test_invalid_task_log_file_path(self, ti):
        with (
            patch.object(self.opensearch_io, "_parse_raw_log") as mock_parse,
            patch.object(self.opensearch_io, "_write_to_opensearch") as mock_write,
        ):
            self.opensearch_io.upload(Path("/invalid/path"), ti)

        mock_parse.assert_not_called()
        mock_write.assert_not_called()

    def test_write_to_opensearch(self, tmp_json_file, ti):
        self.opensearch_io.write_stdout = False
        log_id = _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number)
        expected_log_lines = self.opensearch_io._parse_raw_log(tmp_json_file.read_text(), log_id)

        with patch.object(self.opensearch_io, "_write_to_opensearch", return_value=True) as mock_write:
            self.opensearch_io.upload(tmp_json_file, ti)

        mock_write.assert_called_once_with(expected_log_lines)

    def test_raw_log_contains_log_id_and_offset(self, tmp_json_file, ti):
        raw_log = tmp_json_file.read_text()
        log_id = _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number)
        json_log_lines = self.opensearch_io._parse_raw_log(raw_log, log_id)

        assert len(json_log_lines) == 3
        assert [line["offset"] for line in json_log_lines] == [1, 2, 3]
        assert all(line["log_id"] == log_id for line in json_log_lines)

    def test_os_read_builds_expected_query(self, ti):
        self.opensearch_io.client = Mock()
        self.opensearch_io.client.count.return_value = {"count": 1}
        self.opensearch_io.client.search.return_value = _build_os_search_response(
            {
                "event": "hello",
                "log_id": _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number),
                "offset": 3,
            }
        )
        self.opensearch_io.index_patterns = "airflow-logs-*"
        log_id = _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number)
        query = {
            "query": {
                "bool": {
                    "filter": [{"range": {self.opensearch_io.offset_field: {"gt": 2}}}],
                    "must": [{"match_phrase": {"log_id": log_id}}],
                }
            }
        }

        response = self.opensearch_io._os_read(log_id, 2, ti)

        self.opensearch_io.client.count.assert_called_once_with(index="airflow-logs-*", body=query)
        self.opensearch_io.client.search.assert_called_once_with(
            index="airflow-logs-*",
            body=query,
            sort=[self.opensearch_io.offset_field],
            size=self.opensearch_io.MAX_LINE_PER_PAGE,
            from_=0,
        )
        assert response is not None
        assert response.hits[0].event == "hello"

    def test_os_read_returns_none_when_count_is_zero(self, ti):
        self.opensearch_io.client = Mock()
        self.opensearch_io.client.count.return_value = {"count": 0}

        log_id = _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number)
        response = self.opensearch_io._os_read(log_id, 0, ti)

        assert response is None
        self.opensearch_io.client.search.assert_not_called()

    def test_os_read_propagates_missing_index(self, ti):
        self.opensearch_io.client = Mock()
        self.opensearch_io.client.count.side_effect = NotFoundError(
            404,
            "IndexMissingException[[missing] missing]",
        )

        log_id = _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number)
        with pytest.raises(NotFoundError):
            self.opensearch_io._os_read(log_id, 0, ti)

    def test_os_read_logs_and_returns_none_on_search_error(self, ti):
        self.opensearch_io.client = Mock()
        self.opensearch_io.client.count.return_value = {"count": 1}
        self.opensearch_io.client.search.side_effect = RuntimeError("boom")

        log_id = _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number)
        with patch.object(self.opensearch_io.log, "exception") as mock_exception:
            response = self.opensearch_io._os_read(log_id, 0, ti)

        assert response is None
        mock_exception.assert_called_once()

    def test_read_returns_missing_log_message_when_os_read_returns_none(self, ti):
        with patch.object(self.opensearch_io, "_os_read", return_value=None):
            log_source_info, log_messages = self.opensearch_io.read("", ti)

        log_id = _render_log_id(self.opensearch_io.log_id_template, ti, ti.try_number)
        assert log_source_info == []
        assert f"*** Log {log_id} not found in Opensearch" in log_messages[0]

    def test_get_index_patterns_with_callable(self):
        with patch("airflow.providers.opensearch.log.os_task_handler.import_string") as mock_import_string:
            mock_callable = Mock(return_value="callable_index_pattern")
            mock_import_string.return_value = mock_callable

            self.opensearch_io.index_patterns_callable = "path.to.index_pattern_callable"
            result = self.opensearch_io._get_index_patterns({})

        mock_import_string.assert_called_once_with("path.to.index_pattern_callable")
        mock_callable.assert_called_once_with({})
        assert result == "callable_index_pattern"


class TestFormatErrorDetail:
    def test_returns_none_for_empty(self):
        assert _format_error_detail(None) is None
        assert _format_error_detail([]) is None

    def test_returns_string_for_non_list(self):
        assert _format_error_detail("raw string") == "raw string"

    def test_formats_single_exception(self):
        error_detail = [
            {
                "is_cause": False,
                "frames": [{"filename": "/app/task.py", "lineno": 13, "name": "log_and_raise"}],
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
        result = _format_error_detail(["unexpected string item"])
        assert result is not None
        assert "unexpected string item" in result


class TestBuildStructuredLogFields:
    def test_filters_to_allowed_fields(self):
        hit = {"event": "hello", "level": "info", "unknown_field": "should be dropped"}
        result = _build_log_fields(hit)
        assert "event" in result
        assert "level" in result
        assert "unknown_field" not in result

    def test_message_mapped_to_event(self):
        hit = {"message": "plain message", "timestamp": "2024-01-01T00:00:00Z"}
        fields = _build_log_fields(hit)
        assert fields["event"] == "plain message"
        assert "message" not in fields

    def test_message_preserved_if_event_exists(self):
        hit = {"event": "structured event", "message": "plain message"}
        fields = _build_log_fields(hit)
        assert fields["event"] == "structured event"
        assert fields["message"] == "plain message"

    def test_levelname_mapped_to_level(self):
        hit = {"event": "msg", "levelname": "ERROR"}
        result = _build_log_fields(hit)
        assert result["level"] == "ERROR"
        assert "levelname" not in result

    def test_at_timestamp_mapped_to_timestamp(self):
        hit = {"event": "msg", "@timestamp": "2024-01-01T00:00:00Z"}
        result = _build_log_fields(hit)
        assert result["timestamp"] == "2024-01-01T00:00:00Z"
        assert "@timestamp" not in result

    def test_error_detail_is_kept_as_list(self):
        error_detail = [
            {
                "is_cause": False,
                "frames": [{"filename": "/dag.py", "lineno": 10, "name": "run"}],
                "exc_type": "RuntimeError",
                "exc_value": "Woopsie.",
            }
        ]
        hit = {"event": "Task failed with exception", "error_detail": error_detail}
        result = _build_log_fields(hit)
        assert result["error_detail"] == error_detail

    def test_error_detail_dropped_when_empty(self):
        hit = {"event": "msg", "error_detail": []}
        result = _build_log_fields(hit)
        assert "error_detail" not in result
