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
from unittest import mock
from unittest.mock import Mock, patch
from urllib.parse import quote

import elasticsearch
import pendulum
import pytest

from airflow.providers.common.compat.sdk import conf
from airflow.providers.elasticsearch.log.es_json_formatter import ElasticsearchJSONFormatter
from airflow.providers.elasticsearch.log.es_response import ElasticSearchResponse
from airflow.providers.elasticsearch.log.es_task_handler import (
    VALID_ES_CONFIG_KEYS,
    ElasticsearchRemoteLogIO,
    ElasticsearchTaskHandler,
    _build_log_fields,
    _clean_date,
    _format_error_detail,
    _render_log_id,
    get_es_kwargs_from_config,
    getattr_nested,
)
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


@dataclasses.dataclass
class _MockTI:
    dag_id: str = "dag_for_testing_es_log_handler"
    task_id: str = "task_for_testing_es_log_handler"
    run_id: str = "run_for_testing_es_log_handler"
    try_number: int = 1
    map_index: int = -1


def get_ti(dag_id, task_id, run_id, logical_date, create_task_instance):
    ti = create_task_instance(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        logical_date=logical_date,
        dagrun_state=DagRunState.RUNNING,
        state=TaskInstanceState.RUNNING,
    )
    ti.try_number = 1
    ti.raw = False
    return ti


def _build_es_search_response(*sources: dict, index: str = "test_index", doc_type: str = "_doc") -> dict:
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


def _make_es_response(search, *sources: dict) -> ElasticSearchResponse:
    return ElasticSearchResponse(search, _build_es_search_response(*sources))


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
    expected_pattern = r"^\*\*\* Log .* not found in Elasticsearch.*"
    if AIRFLOW_V_3_0_PLUS:
        logs = list(logs)
        assert len(logs) == 1
        assert logs[0].event is not None
        assert logs[0].event.startswith("*** Log ")
        assert logs[0].event.endswith("may have been removed.")
        assert logs[0].event
        assert re.match(expected_pattern, logs[0].event) is not None
    else:
        assert len(logs) == 1
        assert len(logs[0]) == 1
        assert re.match(expected_pattern, logs[0][0][1]) is not None


class TestElasticsearchTaskHandler:
    DAG_ID = "dag_for_testing_es_task_handler"
    TASK_ID = "task_for_testing_es_log_handler"
    RUN_ID = "run_for_testing_es_log_handler"
    MAP_INDEX = -1
    TRY_NUM = 1
    LOGICAL_DATE = datetime(2016, 1, 1)
    LOG_ID = f"{DAG_ID}-{TASK_ID}-{RUN_ID}-{MAP_INDEX}-{TRY_NUM}"
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
        self.test_message = "some random stuff"
        self.base_log_source = {
            "message": self.test_message,
            "event": self.test_message,
            "log_id": self.LOG_ID,
            "offset": 1,
        }
        self.es_task_handler = ElasticsearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
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
            run_id=self.RUN_ID,
            logical_date=self.LOGICAL_DATE,
            create_task_instance=create_task_instance,
        )
        clear_db_runs()
        clear_db_dags()

    @pytest.mark.parametrize(
        ("host", "expected"),
        [
            ("http://localhost:9200", "http://localhost:9200"),
            ("https://localhost:9200", "https://localhost:9200"),
            ("localhost:9200", "http://localhost:9200"),
            ("someurl", "http://someurl"),
            ("https://", "ValueError"),
        ],
    )
    def test_format_url(self, host, expected):
        if expected == "ValueError":
            with pytest.raises(ValueError, match="'https://' is not a valid URL."):
                ElasticsearchTaskHandler.format_url(host)
        else:
            assert ElasticsearchTaskHandler.format_url(host) == expected

    def test_client(self):
        assert isinstance(self.es_task_handler.client, elasticsearch.Elasticsearch)
        assert self.es_task_handler.index_patterns == "_all"

    def test_client_with_config(self):
        es_conf = dict(conf.getsection("elasticsearch_configs"))
        expected_dict = {
            "http_compress": False,
            "verify_certs": True,
        }
        assert es_conf == expected_dict
        ElasticsearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
            es_kwargs=es_conf,
        )

    @pytest.mark.db_test
    @pytest.mark.parametrize("metadata_mode", ["provided", "none", "empty"])
    def test_read(self, ti, metadata_mode):
        start_time = pendulum.now()
        response = _make_es_response(self.es_task_handler.io, self.base_log_source)

        with patch.object(self.es_task_handler.io, "_es_read", return_value=response):
            if metadata_mode == "provided":
                logs, metadatas = self.es_task_handler.read(
                    ti,
                    1,
                    {"offset": 0, "last_log_timestamp": str(start_time), "end_of_log": False},
                )
            elif metadata_mode == "empty":
                logs, metadatas = self.es_task_handler.read(ti, 1, {})
            else:
                logs, metadatas = self.es_task_handler.read(ti, 1)

        metadata = _assert_log_events(
            logs,
            metadatas,
            expected_events=[self.test_message],
            expected_sources=["http://localhost:9200"],
        )

        assert not metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) >= start_time

    @pytest.mark.db_test
    def test_read_defaults_offset_when_missing_from_metadata(self, ti):
        start_time = pendulum.now()
        with patch.object(self.es_task_handler.io, "_es_read", return_value=None):
            logs, metadatas = self.es_task_handler.read(ti, 1, {"end_of_log": False})

        metadata = _assert_no_logs(logs, metadatas)
        assert metadata["end_of_log"]
        assert metadata["offset"] == "0"
        assert timezone.parse(metadata["last_log_timestamp"]) >= start_time

    @pytest.mark.db_test
    @pytest.mark.parametrize("seconds", [3, 6])
    def test_read_missing_logs(self, ti, seconds):
        start_time = pendulum.now().add(seconds=-seconds)
        with patch.object(self.es_task_handler.io, "_es_read", return_value=None):
            logs, metadatas = self.es_task_handler.read(
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
        with patch.object(self.es_task_handler.io, "_es_read", return_value=None):
            logs, metadatas = self.es_task_handler.read(
                task_instance=ti,
                try_number=1,
                metadata={
                    "offset": 1,
                    "last_log_timestamp": str(start_time),
                    "end_of_log": False,
                },
            )

        metadata = _assert_no_logs(logs, metadatas)
        assert metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) == start_time

    @pytest.mark.db_test
    def test_read_with_custom_offset_and_host_fields(self, ti):
        self.es_task_handler.host_field = "host.name"
        self.es_task_handler.offset_field = "log.offset"
        self.es_task_handler.io.host_field = "host.name"
        self.es_task_handler.io.offset_field = "log.offset"
        response = _make_es_response(
            self.es_task_handler.io,
            {
                "message": self.test_message,
                "event": self.test_message,
                "log_id": self.LOG_ID,
                "log": {"offset": 1},
                "host": {"name": "somehostname"},
            },
        )

        with patch.object(self.es_task_handler.io, "_es_read", return_value=response):
            logs, metadatas = self.es_task_handler.read(
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
        self.es_task_handler.set_context(ti)
        assert self.es_task_handler.mark_end_on_close

    @pytest.mark.db_test
    def test_set_context_w_json_format_and_write_stdout(self, ti):
        self.es_task_handler.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.es_task_handler.write_stdout = True
        self.es_task_handler.json_format = True

        self.es_task_handler.set_context(ti)

        assert isinstance(self.es_task_handler.formatter, ElasticsearchJSONFormatter)
        assert isinstance(self.es_task_handler.handler, logging.StreamHandler)
        assert self.es_task_handler.context_set

    @pytest.mark.db_test
    def test_close(self, ti):
        self.es_task_handler.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        self.es_task_handler.set_context(ti)
        self.es_task_handler.close()

        log_file = Path(self.local_log_location) / self.FILENAME_TEMPLATE.format(try_number=1)
        assert log_file.read_text().strip().endswith(self.end_of_log_mark.strip())
        assert self.es_task_handler.closed

    @pytest.mark.db_test
    def test_close_no_mark_end(self, ti):
        ti.raw = True
        self.es_task_handler.set_context(ti)
        self.es_task_handler.close()

        log_file = Path(self.local_log_location) / self.FILENAME_TEMPLATE.format(try_number=1)
        assert self.end_of_log_mark not in log_file.read_text()
        assert self.es_task_handler.closed

    @pytest.mark.db_test
    def test_close_with_no_handler(self, ti):
        self.es_task_handler.set_context(ti)
        self.es_task_handler.handler = None
        self.es_task_handler.close()

        log_file = Path(self.local_log_location) / self.FILENAME_TEMPLATE.format(try_number=1)
        assert log_file.read_text() == ""
        assert self.es_task_handler.closed

    @pytest.mark.db_test
    @pytest.mark.parametrize("stream_state", ["none", "closed"])
    def test_close_reopens_stream(self, ti, stream_state):
        self.es_task_handler.set_context(ti)
        if stream_state == "none":
            self.es_task_handler.handler.stream = None
        else:
            self.es_task_handler.handler.stream.close()

        self.es_task_handler.close()

        log_file = Path(self.local_log_location) / self.FILENAME_TEMPLATE.format(try_number=1)
        assert self.end_of_log_mark in log_file.read_text()
        assert self.es_task_handler.closed

    @pytest.mark.db_test
    def test_render_log_id(self, ti):
        assert _render_log_id(self.es_task_handler.log_id_template, ti, 1) == self.LOG_ID

    def test_clean_date(self):
        clean_logical_date = _clean_date(datetime(2016, 7, 8, 9, 10, 11, 12))
        assert clean_logical_date == "2016_07_08T09_10_11_000012"

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("json_format", "es_frontend", "expected_url"),
        [
            (True, "localhost:5601/{log_id}", "https://localhost:5601/{log_id}"),
            (False, "localhost:5601/{log_id}", "https://localhost:5601/{log_id}"),
            (False, "localhost:5601", "https://localhost:5601"),
            (False, "https://localhost:5601/path/{log_id}", "https://localhost:5601/path/{log_id}"),
            (False, "http://localhost:5601/path/{log_id}", "http://localhost:5601/path/{log_id}"),
            (False, "other://localhost:5601/path/{log_id}", "other://localhost:5601/path/{log_id}"),
        ],
    )
    def test_get_external_log_url(self, ti, json_format, es_frontend, expected_url):
        es_task_handler = ElasticsearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            json_format=json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
            frontend=es_frontend,
        )
        assert es_task_handler.get_external_log_url(ti, ti.try_number) == expected_url.format(
            log_id=quote(self.LOG_ID)
        )

    @pytest.mark.parametrize(
        ("frontend", "expected"),
        [
            ("localhost:5601/{log_id}", True),
            (None, False),
        ],
    )
    def test_supports_external_link(self, frontend, expected):
        self.es_task_handler.frontend = frontend
        assert self.es_task_handler.supports_external_link == expected

    @pytest.mark.db_test
    @mock.patch("sys.__stdout__", new_callable=StringIO)
    def test_dynamic_offset(self, stdout_mock, ti, time_machine):
        handler = ElasticsearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=True,
            json_format=True,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
        )
        handler.formatter = logging.Formatter()

        logger = logging.getLogger("tests.elasticsearch.dynamic_offset")
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
        with patch("airflow.providers.elasticsearch.log.es_task_handler.import_string") as mock_import_string:
            mock_callable = Mock(return_value="callable_index_pattern")
            mock_import_string.return_value = mock_callable

            self.es_task_handler.io.index_patterns_callable = "path.to.index_pattern_callable"
            result = self.es_task_handler.io._get_index_patterns({})

        mock_import_string.assert_called_once_with("path.to.index_pattern_callable")
        mock_callable.assert_called_once_with({})
        assert result == "callable_index_pattern"

    def test_filename_template_for_backward_compatibility(self):
        ElasticsearchTaskHandler(
            base_log_folder="local/log/location",
            end_of_log_mark="end_of_log\n",
            write_stdout=False,
            json_format=False,
            json_fields="asctime,filename,lineno,levelname,message,exc_text",
            filename_template=None,
        )


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
                ("elasticsearch_configs", "http_compress"): "False",
                ("elasticsearch_configs", "request_timeout"): "10",
            }
        ):
            args_from_config = get_es_kwargs_from_config().keys()
            assert "verify_certs" in args_from_config
            assert "request_timeout" in args_from_config
            assert "http_compress" in args_from_config
            assert "self" not in args_from_config

    def test_retrieve_retry_on_timeout(self):
        with conf_vars(
            {
                ("elasticsearch_configs", "retry_on_timeout"): "True",
            }
        ):
            args_from_config = get_es_kwargs_from_config().keys()
            assert "retry_on_timeout" in args_from_config

    def test_self_not_valid_arg(self):
        assert "self" not in VALID_ES_CONFIG_KEYS


class TestElasticsearchRemoteLogIO:
    @pytest.fixture(autouse=True)
    def _setup_tests(self, tmp_path):
        self.elasticsearch_io = ElasticsearchRemoteLogIO(
            write_to_es=True,
            write_stdout=True,
            delete_local_copy=True,
            host="http://localhost:9200",
            base_log_folder=tmp_path,
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
        self.elasticsearch_io.write_to_es = False
        self.elasticsearch_io.upload(tmp_json_file, ti)

        captured = capsys.readouterr()
        stdout_lines = captured.out.strip().splitlines()
        log_entries = [json.loads(line) for line in stdout_lines]
        assert [entry["message"] for entry in log_entries] == ["start", "processing", "end"]

    def test_invalid_task_log_file_path(self, ti):
        with (
            patch.object(self.elasticsearch_io, "_parse_raw_log") as mock_parse,
            patch.object(self.elasticsearch_io, "_write_to_es") as mock_write,
        ):
            self.elasticsearch_io.upload(Path("/invalid/path"), ti)

        mock_parse.assert_not_called()
        mock_write.assert_not_called()

    def test_raw_log_contains_log_id_and_offset(self, tmp_json_file, ti):
        raw_log = tmp_json_file.read_text()
        log_id = _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number)
        json_log_lines = self.elasticsearch_io._parse_raw_log(raw_log, log_id)

        assert len(json_log_lines) == 3
        assert [line["offset"] for line in json_log_lines] == [1, 2, 3]
        assert all(line["log_id"] == log_id for line in json_log_lines)

    def test_es_read_builds_expected_query(self, ti):
        self.elasticsearch_io.client = Mock()
        self.elasticsearch_io.client.count.return_value = {"count": 1}
        self.elasticsearch_io.client.search.return_value = _build_es_search_response(
            {
                "event": "hello",
                "log_id": _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number),
                "offset": 3,
            }
        )
        self.elasticsearch_io.index_patterns = "airflow-logs-*"
        log_id = _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number)
        query = {
            "bool": {
                "filter": [{"range": {self.elasticsearch_io.offset_field: {"gt": 2}}}],
                "must": [{"match_phrase": {"log_id": log_id}}],
            }
        }

        response = self.elasticsearch_io._es_read(log_id, 2, ti)

        self.elasticsearch_io.client.count.assert_called_once_with(index="airflow-logs-*", query=query)
        self.elasticsearch_io.client.search.assert_called_once_with(
            index="airflow-logs-*",
            query=query,
            sort=[self.elasticsearch_io.offset_field],
            size=self.elasticsearch_io.MAX_LINE_PER_PAGE,
            from_=0,
        )
        assert response is not None
        assert response.hits[0].event == "hello"

    def test_es_read_returns_none_when_count_is_zero(self, ti):
        self.elasticsearch_io.client = Mock()
        self.elasticsearch_io.client.count.return_value = {"count": 0}

        log_id = _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number)
        response = self.elasticsearch_io._es_read(log_id, 0, ti)

        assert response is None
        self.elasticsearch_io.client.search.assert_not_called()

    def test_es_read_propagates_missing_index(self, ti):
        self.elasticsearch_io.client = Mock()
        self.elasticsearch_io.client.count.side_effect = elasticsearch.exceptions.NotFoundError(
            404,
            "IndexMissingException[[missing] missing]",
            {},
        )

        log_id = _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number)
        with pytest.raises(elasticsearch.exceptions.NotFoundError):
            self.elasticsearch_io._es_read(log_id, 0, ti)

    def test_es_read_logs_and_returns_none_on_search_error(self, ti):
        self.elasticsearch_io.client = Mock()
        self.elasticsearch_io.client.count.return_value = {"count": 1}
        self.elasticsearch_io.client.search.side_effect = RuntimeError("boom")

        log_id = _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number)
        with patch.object(self.elasticsearch_io.log, "exception") as mock_exception:
            response = self.elasticsearch_io._es_read(log_id, 0, ti)

        assert response is None
        mock_exception.assert_called_once()

    def test_read_returns_missing_log_message_when_es_read_returns_none(self, ti):
        with patch.object(self.elasticsearch_io, "_es_read", return_value=None):
            log_source_info, log_messages = self.elasticsearch_io.read("", ti)

        log_id = _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number)
        assert log_source_info == []
        assert f"*** Log {log_id} not found in Elasticsearch" in log_messages[0]


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
