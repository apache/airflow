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
from urllib.parse import quote

import elasticsearch
import pendulum
import pytest
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

from airflow.configuration import conf
from airflow.providers.elasticsearch.log.es_response import ElasticSearchResponse
from airflow.providers.elasticsearch.log.es_task_handler import (
    VALID_ES_CONFIG_KEYS,
    ElasticsearchTaskHandler,
    get_es_kwargs_from_config,
    getattr_nested,
)
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import datetime

from providers.tests.elasticsearch.log.elasticmock import elasticmock
from providers.tests.elasticsearch.log.elasticmock.utilities import SearchFailedException

pytestmark = pytest.mark.db_test

AIRFLOW_SOURCES_ROOT_DIR = Path(__file__).parents[4].resolve()
ES_PROVIDER_YAML_FILE = AIRFLOW_SOURCES_ROOT_DIR / "airflow" / "providers" / "elasticsearch" / "provider.yaml"


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


class TestElasticsearchTaskHandler:
    DAG_ID = "dag_for_testing_es_task_handler"
    TASK_ID = "task_for_testing_es_log_handler"
    EXECUTION_DATE = datetime(2016, 1, 1)
    LOG_ID = f"{DAG_ID}-{TASK_ID}-2016-01-01T00:00:00+00:00-1"
    JSON_LOG_ID = f"{DAG_ID}-{TASK_ID}-{ElasticsearchTaskHandler._clean_date(EXECUTION_DATE)}-1"
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

    @elasticmock
    def setup_method(self, method):
        self.local_log_location = "local/log/location"
        self.end_of_log_mark = "end_of_log\n"
        self.write_stdout = False
        self.json_format = False
        self.json_fields = "asctime,filename,lineno,levelname,message,exc_text"
        self.host_field = "host"
        self.offset_field = "offset"
        self.es_task_handler = ElasticsearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
        )

        self.es = elasticsearch.Elasticsearch("http://localhost:9200")
        self.index_name = "test_index"
        self.doc_type = "log"
        self.test_message = "some random stuff"
        self.body = {"message": self.test_message, "log_id": self.LOG_ID, "offset": 1}
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body, id=1)

    def teardown_method(self):
        shutil.rmtree(self.local_log_location.split(os.path.sep)[0], ignore_errors=True)

    def test_es_response(self):
        sample_response = self.es.sample_log_response()
        es_response = ElasticSearchResponse(self.es_task_handler, sample_response)
        logs_by_host = self.es_task_handler._group_logs_by_host(es_response)

        def concat_logs(lines):
            log_range = -1 if lines[-1].message == self.es_task_handler.end_of_log_mark else None
            return "\n".join(self.es_task_handler._format_msg(line) for line in lines[:log_range])

        for hosted_log in logs_by_host.values():
            message = concat_logs(hosted_log)

        assert (
            message == "Dependencies all met for dep_context=non-requeueable"
            " deps ti=<TaskInstance: example_bash_operator.run_after_loop owen_run_run [queued]>\n"
            "Starting attempt 1 of 1\nExecuting <Task(BashOperator): run_after_loop> "
            "on 2023-07-09 07:47:32+00:00"
        )

    @pytest.mark.parametrize(
        "host, expected",
        [
            ("http://localhost:9200", "http://localhost:9200"),
            ("https://localhost:9200", "https://localhost:9200"),
            ("localhost:9200", "http://localhost:9200"),
            ("someurl", "http://someurl"),
            ("https://", "ValueError"),
        ],
    )
    def test_format_url(self, host, expected):
        """
        Test the format_url method of the ElasticsearchTaskHandler class.
        """
        if expected == "ValueError":
            with pytest.raises(ValueError):
                assert ElasticsearchTaskHandler.format_url(host) == expected
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
        # ensure creating with configs does not fail
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

    def test_client_with_patterns(self):
        # ensure creating with index patterns does not fail
        patterns = "test_*,other_*"
        handler = ElasticsearchTaskHandler(
            base_log_folder=self.local_log_location,
            end_of_log_mark=self.end_of_log_mark,
            write_stdout=self.write_stdout,
            json_format=self.json_format,
            json_fields=self.json_fields,
            host_field=self.host_field,
            offset_field=self.offset_field,
            index_patterns=patterns,
        )
        assert handler.index_patterns == patterns

    def test_read(self, ti):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )

        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert len(logs[0]) == 1
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]["end_of_log"]
        assert "1" == metadatas[0]["offset"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) > ts

    def test_read_with_patterns(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.es_task_handler, "index_patterns", new="test_*,other_*"):
            logs, metadatas = self.es_task_handler.read(
                ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
            )

        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert len(logs[0]) == 1
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]["end_of_log"]
        assert "1" == metadatas[0]["offset"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) > ts

    def test_read_with_patterns_no_match(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.es_task_handler, "index_patterns", new="test_other_*,test_another_*"):
            logs, metadatas = self.es_task_handler.read(
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
        with mock.patch.object(self.es_task_handler, "index_patterns", new="nonexistent,test_*"):
            with pytest.raises(elasticsearch.exceptions.NotFoundError, match=r"IndexMissingException.*"):
                self.es_task_handler.read(
                    ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
                )

    @pytest.mark.parametrize("seconds", [3, 6])
    def test_read_missing_logs(self, seconds, create_task_instance):
        """
        When the log actually isn't there to be found, we only want to wait for 5 seconds.
        In this case we expect to receive a message of the form 'Log {log_id} not found in elasticsearch ...'
        """
        ti = get_ti(
            self.DAG_ID,
            self.TASK_ID,
            pendulum.instance(self.EXECUTION_DATE).add(days=1),  # so logs are not found
            create_task_instance=create_task_instance,
        )
        ts = pendulum.now().add(seconds=-seconds)
        logs, metadatas = self.es_task_handler.read(ti, 1, {"offset": 0, "last_log_timestamp": str(ts)})

        assert 1 == len(logs)
        if seconds > 5:
            # we expect a log not found message when checking began more than 5 seconds ago
            assert len(logs[0]) == 1
            actual_message = logs[0][0][1]
            expected_pattern = r"^\*\*\* Log .* not found in Elasticsearch.*"
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

    def test_read_with_match_phrase_query(self, ti):
        similar_log_id = (
            f"{TestElasticsearchTaskHandler.TASK_ID}-"
            f"{TestElasticsearchTaskHandler.DAG_ID}-2016-01-01T00:00:00+00:00-1"
        )
        another_test_message = "another message"

        another_body = {"message": another_test_message, "log_id": similar_log_id, "offset": 1}
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=another_body, id=1)

        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            ti, 1, {"offset": "0", "last_log_timestamp": str(ts), "end_of_log": False, "max_offset": 2}
        )
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert self.test_message == logs[0][0][-1]
        assert another_test_message != logs[0]

        assert not metadatas[0]["end_of_log"]
        assert "1" == metadatas[0]["offset"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) > ts

    def test_read_with_none_metadata(self, ti):
        logs, metadatas = self.es_task_handler.read(ti, 1)
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]["end_of_log"]
        assert "1" == metadatas[0]["offset"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) < pendulum.now()

    def test_read_nonexistent_log(self, ti):
        ts = pendulum.now()
        # In ElasticMock, search is going to return all documents with matching index
        # and doc_type regardless of match filters, so we delete the log entry instead
        # of making a new TaskInstance to query.
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert not metadatas[0]["end_of_log"]
        assert "0" == metadatas[0]["offset"]
        # last_log_timestamp won't change if no log lines read.
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) == ts

    def test_read_with_empty_metadata(self, ti):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(ti, 1, {})
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]["end_of_log"]
        # offset should be initialized to 0 if not provided.
        assert "1" == metadatas[0]["offset"]
        # last_log_timestamp will be initialized using log reading time
        # if not last_log_timestamp is provided.
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) > ts

        # case where offset is missing but metadata not empty.
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(ti, 1, {"end_of_log": False})
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert not metadatas[0]["end_of_log"]
        # offset should be initialized to 0 if not provided.
        assert "0" == metadatas[0]["offset"]
        # last_log_timestamp will be initialized using log reading time
        # if not last_log_timestamp is provided.
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) > ts

    def test_read_timeout(self, ti):
        ts = pendulum.now().subtract(minutes=5)

        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        # in the below call, offset=1 implies that we have already retrieved something
        # if we had never retrieved any logs at all (offset=0), then we would have gotten
        # a "logs not found" message after 5 seconds of trying
        offset = 1
        logs, metadatas = self.es_task_handler.read(
            task_instance=ti,
            try_number=1,
            metadata={
                "offset": offset,
                "last_log_timestamp": str(ts),
                "end_of_log": False,
            },
        )
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert metadatas[0]["end_of_log"]
        assert str(offset) == metadatas[0]["offset"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) == ts

    def test_read_as_download_logs(self, ti):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            ti,
            1,
            {"offset": 0, "last_log_timestamp": str(ts), "download_logs": True, "end_of_log": False},
        )
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert len(logs[0]) == 1
        assert self.test_message == logs[0][0][-1]
        assert not metadatas[0]["end_of_log"]
        assert metadatas[0]["download_logs"]
        assert "1" == metadatas[0]["offset"]
        assert timezone.parse(metadatas[0]["last_log_timestamp"]) > ts

    def test_read_raises(self, ti):
        with mock.patch.object(self.es_task_handler.log, "exception") as mock_exception:
            with mock.patch.object(self.es_task_handler.client, "search") as mock_execute:
                mock_execute.side_effect = SearchFailedException("Failed to read")
                logs, metadatas = self.es_task_handler.read(ti, 1)
            assert mock_exception.call_count == 1
            args, kwargs = mock_exception.call_args
            assert "Could not read log with log_id:" in args[0]
        assert 1 == len(logs)
        assert len(logs) == len(metadatas)
        assert [[]] == logs
        assert not metadatas[0]["end_of_log"]
        assert "0" == metadatas[0]["offset"]

    def test_set_context(self, ti):
        self.es_task_handler.set_context(ti)
        assert self.es_task_handler.mark_end_on_close

    def test_set_context_w_json_format_and_write_stdout(self, ti):
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.es_task_handler.formatter = formatter
        self.es_task_handler.write_stdout = True
        self.es_task_handler.json_format = True
        self.es_task_handler.set_context(ti)

    def test_read_with_json_format(self, ti):
        ts = pendulum.now()
        formatter = logging.Formatter(
            "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s - %(exc_text)s"
        )
        self.es_task_handler.formatter = formatter
        self.es_task_handler.json_format = True

        self.body = {
            "message": self.test_message,
            "log_id": f"{self.DAG_ID}-{self.TASK_ID}-2016_01_01T00_00_00_000000-1",
            "offset": 1,
            "asctime": "2020-12-24 19:25:00,962",
            "filename": "taskinstance.py",
            "lineno": 851,
            "levelname": "INFO",
        }
        self.es_task_handler.set_context(ti)
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body, id=id)

        logs, _ = self.es_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )
        assert "[2020-12-24 19:25:00,962] {taskinstance.py:851} INFO - some random stuff - " == logs[0][0][1]

    def test_read_with_json_format_with_custom_offset_and_host_fields(self, ti):
        ts = pendulum.now()
        formatter = logging.Formatter(
            "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s - %(exc_text)s"
        )
        self.es_task_handler.formatter = formatter
        self.es_task_handler.json_format = True
        self.es_task_handler.host_field = "host.name"
        self.es_task_handler.offset_field = "log.offset"

        self.body = {
            "message": self.test_message,
            "log_id": f"{self.DAG_ID}-{self.TASK_ID}-2016_01_01T00_00_00_000000-1",
            "log": {"offset": 1},
            "host": {"name": "somehostname"},
            "asctime": "2020-12-24 19:25:00,962",
            "filename": "taskinstance.py",
            "lineno": 851,
            "levelname": "INFO",
        }
        self.es_task_handler.set_context(ti)
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body, id=id)

        logs, _ = self.es_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )
        assert "[2020-12-24 19:25:00,962] {taskinstance.py:851} INFO - some random stuff - " == logs[0][0][1]

    def test_read_with_custom_offset_and_host_fields(self, ti):
        ts = pendulum.now()
        # Delete the existing log entry as it doesn't have the new offset and host fields
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)

        self.es_task_handler.host_field = "host.name"
        self.es_task_handler.offset_field = "log.offset"

        self.body = {
            "message": self.test_message,
            "log_id": self.LOG_ID,
            "log": {"offset": 1},
            "host": {"name": "somehostname"},
        }
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body, id=id)

        logs, _ = self.es_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )
        assert self.test_message == logs[0][0][1]

    def test_close(self, ti):
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.es_task_handler.formatter = formatter

        self.es_task_handler.set_context(ti)
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            # end_of_log_mark may contain characters like '\n' which is needed to
            # have the log uploaded but will not be stored in elasticsearch.
            # so apply the strip() to log_file.read()
            log_line = log_file.read().strip()
            assert log_line.endswith(self.end_of_log_mark.strip())
        assert self.es_task_handler.closed

    def test_close_no_mark_end(self, ti):
        ti.raw = True
        self.es_task_handler.set_context(ti)
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark not in log_file.read()
        assert self.es_task_handler.closed

    def test_close_closed(self, ti):
        self.es_task_handler.closed = True
        self.es_task_handler.set_context(ti)
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert 0 == len(log_file.read())

    def test_close_with_no_handler(self, ti):
        self.es_task_handler.set_context(ti)
        self.es_task_handler.handler = None
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert 0 == len(log_file.read())
        assert self.es_task_handler.closed

    def test_close_with_no_stream(self, ti):
        self.es_task_handler.set_context(ti)
        self.es_task_handler.handler.stream = None
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark in log_file.read()
        assert self.es_task_handler.closed

        self.es_task_handler.set_context(ti)
        self.es_task_handler.handler.stream.close()
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark in log_file.read()
        assert self.es_task_handler.closed

    def test_render_log_id(self, ti):
        assert self.LOG_ID == self.es_task_handler._render_log_id(ti, 1)

        self.es_task_handler.json_format = True
        assert self.JSON_LOG_ID == self.es_task_handler._render_log_id(ti, 1)

    def test_clean_date(self):
        clean_execution_date = self.es_task_handler._clean_date(datetime(2016, 7, 8, 9, 10, 11, 12))
        assert "2016_07_08T09_10_11_000012" == clean_execution_date

    @pytest.mark.parametrize(
        "json_format, es_frontend, expected_url",
        [
            # Common cases
            (True, "localhost:5601/{log_id}", "https://localhost:5601/" + quote(JSON_LOG_ID)),
            (False, "localhost:5601/{log_id}", "https://localhost:5601/" + quote(LOG_ID)),
            # Ignore template if "{log_id}"" is missing in the URL
            (False, "localhost:5601", "https://localhost:5601"),
            # scheme handling
            (False, "https://localhost:5601/path/{log_id}", "https://localhost:5601/path/" + quote(LOG_ID)),
            (False, "http://localhost:5601/path/{log_id}", "http://localhost:5601/path/" + quote(LOG_ID)),
            (False, "other://localhost:5601/path/{log_id}", "other://localhost:5601/path/" + quote(LOG_ID)),
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
        url = es_task_handler.get_external_log_url(ti, ti.try_number)
        assert expected_url == url

    @pytest.mark.parametrize(
        "frontend, expected",
        [
            ("localhost:5601/{log_id}", True),
            (None, False),
        ],
    )
    def test_supports_external_link(self, frontend, expected):
        self.es_task_handler.frontend = frontend
        assert self.es_task_handler.supports_external_link == expected

    @mock.patch("sys.__stdout__", new_callable=StringIO)
    def test_dynamic_offset(self, stdout_mock, ti, time_machine):
        # arrange
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
        with patch("airflow.providers.elasticsearch.log.es_task_handler.import_string") as mock_import_string:
            mock_callable = Mock(return_value="callable_index_pattern")
            mock_import_string.return_value = mock_callable

            self.es_task_handler.index_patterns_callable = "path.to.index_pattern_callable"
            result = self.es_task_handler._get_index_patterns({})

            mock_import_string.assert_called_once_with("path.to.index_pattern_callable")
            mock_callable.assert_called_once_with({})
            assert result == "callable_index_pattern"

    def test_filename_template_for_backward_compatibility(self):
        # filename_template arg support for running the latest provider on airflow 2
        ElasticsearchTaskHandler(
            base_log_folder="local/log/location",
            end_of_log_mark="end_of_log\n",
            write_stdout=False,
            json_format=False,
            json_fields="asctime,filename,lineno,levelname,message,exc_text",
            filename_template=None,
        )


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
    Tests that the ElasticsearchTaskHandler retrieves the correct configuration keys from the config file.
    * old_parameters are removed
    * parameters from config are automatically added
    * constructor parameters missing from config are also added
    :return:
    """
    with conf_vars(
        {
            ("elasticsearch_configs", "http_compress"): "False",
            ("elasticsearch_configs", "timeout"): "10",
        }
    ):
        args_from_config = get_es_kwargs_from_config().keys()
        # verify_certs comes from default config value
        assert "verify_certs" in args_from_config
        # timeout comes from config provided value
        assert "timeout" in args_from_config
        # http_compress comes from config value
        assert "http_compress" in args_from_config
        assert "self" not in args_from_config


def test_retrieve_retry_on_timeout():
    """
    Test if retrieve timeout is converted to retry_on_timeout.
    """
    with conf_vars(
        {
            ("elasticsearch_configs", "retry_on_timeout"): "True",
        }
    ):
        args_from_config = get_es_kwargs_from_config().keys()
        # verify_certs comes from default config value
        assert "retry_on_timeout" in args_from_config


def test_self_not_valid_arg():
    """
    Test if self is not a valid argument.
    """
    assert "self" not in VALID_ES_CONFIG_KEYS
