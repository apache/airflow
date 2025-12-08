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
import tempfile
import uuid
from io import StringIO
from pathlib import Path
from unittest import mock
from unittest.mock import Mock, patch
from urllib.parse import quote

import elasticsearch
import pendulum
import pytest

from airflow.configuration import conf
from airflow.providers.elasticsearch.log.es_response import ElasticSearchResponse
from airflow.providers.elasticsearch.log.es_task_handler import (
    VALID_ES_CONFIG_KEYS,
    ElasticsearchRemoteLogIO,
    ElasticsearchTaskHandler,
    _clean_date,
    _render_log_id,
    get_es_kwargs_from_config,
    getattr_nested,
)
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.paths import AIRFLOW_PROVIDERS_ROOT_PATH
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS
from unit.elasticsearch.log.elasticmock import elasticmock
from unit.elasticsearch.log.elasticmock.utilities import SearchFailedException

ES_PROVIDER_YAML_FILE = AIRFLOW_PROVIDERS_ROOT_PATH / "elasticsearch" / "provider.yaml"


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


class TestElasticsearchTaskHandler:
    DAG_ID = "dag_for_testing_es_task_handler"
    TASK_ID = "task_for_testing_es_log_handler"
    RUN_ID = "run_for_testing_es_log_handler"
    MAP_INDEX = -1
    TRY_NUM = 1
    LOGICAL_DATE = datetime(2016, 1, 1)
    LOG_ID = f"{DAG_ID}-{TASK_ID}-{RUN_ID}-{MAP_INDEX}-{TRY_NUM}"
    JSON_LOG_ID = f"{DAG_ID}-{TASK_ID}-{_clean_date(LOGICAL_DATE)}-1"
    FILENAME_TEMPLATE = "{try_number}.log"

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
        self.body = {
            "message": self.test_message,
            "log_id": self.LOG_ID,
            "offset": 1,
            "event": self.test_message,
        }
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body, id=1)

    def teardown_method(self):
        shutil.rmtree(self.local_log_location.split(os.path.sep)[0], ignore_errors=True)

    @pytest.mark.parametrize(
        "sample_response",
        [
            pytest.param(lambda self: self.es.sample_airflow_2_log_response(), id="airflow_2"),
            pytest.param(lambda self: self.es.sample_airflow_3_log_response(), id="airflow_3"),
        ],
    )
    def test_es_response(self, sample_response):
        response = sample_response(self)
        es_response = ElasticSearchResponse(self.es_task_handler, response)
        logs_by_host = self.es_task_handler.io._group_logs_by_host(es_response)

        for hosted_log in logs_by_host.values():
            message = self.es_task_handler.concat_logs(hosted_log)

        assert (
            message == "Dependencies all met for dep_context=non-requeueable"
            " deps ti=<TaskInstance: example_bash_operator.run_after_loop \n"
            "Starting attempt 1 of 1\nExecuting <Task(BashOperator): run_after_loop> "
            "on 2023-07-09 07:47:32+00:00"
        )

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
        """
        Test the format_url method of the ElasticsearchTaskHandler class.
        """
        if expected == "ValueError":
            with pytest.raises(ValueError, match="'https://' is not a valid URL."):
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

    @pytest.mark.db_test
    def test_read(self, ti):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )

        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost:9200"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == "some random stuff"

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert self.test_message == logs[0][0][-1]

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) > ts

    @pytest.mark.db_test
    def test_read_with_patterns(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.es_task_handler, "index_patterns", new="test_*,other_*"):
            logs, metadatas = self.es_task_handler.read(
                ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
            )

        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost:9200"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == "some random stuff"

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert self.test_message == logs[0][0][-1]

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) > ts

    @pytest.mark.db_test
    def test_read_with_patterns_no_match(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.es_task_handler.io, "index_patterns", new="test_other_*,test_another_*"):
            logs, metadatas = self.es_task_handler.read(
                ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
            )

        if AIRFLOW_V_3_0_PLUS:
            assert logs == []

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert logs == [[]]

            metadata = metadatas[0]

        assert metadata["offset"] == "0"
        assert metadata["end_of_log"]
        # last_log_timestamp won't change if no log lines read.
        assert timezone.parse(metadata["last_log_timestamp"]) == ts

    @pytest.mark.db_test
    def test_read_with_missing_index(self, ti):
        ts = pendulum.now()
        with mock.patch.object(self.es_task_handler.io, "index_patterns", new="nonexistent,test_*"):
            with pytest.raises(elasticsearch.exceptions.NotFoundError, match=r"IndexMissingException.*"):
                self.es_task_handler.read(
                    ti,
                    1,
                    {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False},
                )

    @pytest.mark.db_test
    @pytest.mark.parametrize("seconds", [3, 6])
    def test_read_missing_logs(self, seconds, create_task_instance):
        """
        When the log actually isn't there to be found, we only want to wait for 5 seconds.
        In this case we expect to receive a message of the form 'Log {log_id} not found in elasticsearch ...'
        """
        run_id = "wrong_run_id"
        ti = get_ti(
            self.DAG_ID,
            self.TASK_ID,
            run_id,
            pendulum.instance(self.LOGICAL_DATE).add(days=1),  # so logs are not found
            create_task_instance=create_task_instance,
        )
        ts = pendulum.now().add(seconds=-seconds)
        logs, metadatas = self.es_task_handler.read(ti, 1, {"offset": 0, "last_log_timestamp": str(ts)})
        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            if seconds > 5:
                # we expect a log not found message when checking began more than 5 seconds ago
                expected_pattern = r"^\*\*\* Log .* not found in Elasticsearch.*"
                assert re.match(expected_pattern, logs[0].event) is not None
                assert metadatas["end_of_log"] is True
            else:
                # we've "waited" less than 5 seconds so it should not be "end of log" and should be no log message
                assert logs == []
                assert metadatas["end_of_log"] is True
            assert metadatas["offset"] == "0"
            assert timezone.parse(metadatas["last_log_timestamp"]) == ts
        else:
            assert len(logs) == 1
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
                assert metadatas[0]["end_of_log"] is True
            assert len(logs) == len(metadatas)
            assert metadatas[0]["offset"] == "0"
            assert timezone.parse(metadatas[0]["last_log_timestamp"]) == ts

    @pytest.mark.db_test
    def test_read_with_match_phrase_query(self, ti):
        similar_log_id = (
            f"{TestElasticsearchTaskHandler.TASK_ID}-"
            f"{TestElasticsearchTaskHandler.DAG_ID}-2016-01-01T00:00:00+00:00-1"
        )
        another_test_message = "another message"

        another_body = {
            "message": another_test_message,
            "log_id": similar_log_id,
            "offset": 1,
        }
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=another_body, id=1)

        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            ti,
            1,
            {
                "offset": "0",
                "last_log_timestamp": str(ts),
                "end_of_log": False,
                "max_offset": 2,
            },
        )
        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost:9200"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == "some random stuff"

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert self.test_message == logs[0][0][-1]

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) > ts

    @pytest.mark.db_test
    def test_read_with_none_metadata(self, ti):
        logs, metadatas = self.es_task_handler.read(ti, 1)
        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost:9200"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == "some random stuff"

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert self.test_message == logs[0][0][-1]

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) < pendulum.now()

    @pytest.mark.db_test
    def test_read_nonexistent_log(self, ti):
        ts = pendulum.now()
        # In ElasticMock, search is going to return all documents with matching index
        # and doc_type regardless of match filters, so we delete the log entry instead
        # of making a new TaskInstance to query.
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )
        if AIRFLOW_V_3_0_PLUS:
            assert logs == []

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert logs == [[]]

            metadata = metadatas[0]

        assert metadata["offset"] == "0"
        assert metadata["end_of_log"]
        # last_log_timestamp won't change if no log lines read.
        assert timezone.parse(metadata["last_log_timestamp"]) == ts

    @pytest.mark.db_test
    def test_read_with_empty_metadata(self, ti):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(ti, 1, {})
        print(f"metadatas: {metadatas}")
        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost:9200"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == "some random stuff"

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert self.test_message == logs[0][0][-1]

            metadata = metadatas[0]
        print(f"metadatas: {metadatas}")
        assert not metadata["end_of_log"]
        # offset should be initialized to 0 if not provided.
        assert metadata["offset"] == "1"
        # last_log_timestamp will be initialized using log reading time
        # if not last_log_timestamp is provided.
        assert timezone.parse(metadata["last_log_timestamp"]) > ts

        # case where offset is missing but metadata not empty.
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(ti, 1, {"end_of_log": False})
        if AIRFLOW_V_3_0_PLUS:
            assert logs == []

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert logs == [[]]

            metadata = metadatas[0]

        assert metadata["end_of_log"]
        # offset should be initialized to 0 if not provided.
        assert metadata["offset"] == "0"
        # last_log_timestamp will be initialized using log reading time
        # if not last_log_timestamp is provided.
        assert timezone.parse(metadata["last_log_timestamp"]) > ts

    @pytest.mark.db_test
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
        if AIRFLOW_V_3_0_PLUS:
            assert logs == []

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert logs == [[]]

            metadata = metadatas[0]

        assert metadata["end_of_log"]
        assert str(offset) == metadata["offset"]
        assert timezone.parse(metadata["last_log_timestamp"]) == ts

    @pytest.mark.db_test
    def test_read_as_download_logs(self, ti):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(
            ti,
            1,
            {
                "offset": 0,
                "last_log_timestamp": str(ts),
                "download_logs": True,
                "end_of_log": False,
            },
        )
        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[0].event == "::group::Log message source details"
            assert logs[0].sources == ["http://localhost:9200"]
            assert logs[1].event == "::endgroup::"
            assert logs[2].event == "some random stuff"

            metadata = metadatas
        else:
            assert len(logs) == 1
            assert len(logs) == len(metadatas)
            assert len(logs[0]) == 1
            assert self.test_message == logs[0][0][-1]

            metadata = metadatas[0]

        assert not metadata["end_of_log"]
        assert metadata["offset"] == "1"
        assert timezone.parse(metadata["last_log_timestamp"]) > ts

    @pytest.mark.db_test
    def test_read_raises(self, ti):
        with mock.patch.object(self.es_task_handler.io.log, "exception") as mock_exception:
            with mock.patch.object(self.es_task_handler.io.client, "search") as mock_execute:
                mock_execute.side_effect = SearchFailedException("Failed to read")
                log_sources, log_msgs = self.es_task_handler.io.read("", ti)
            assert mock_exception.call_count == 1
            args, kwargs = mock_exception.call_args
            assert "Could not read log with log_id:" in args[0]

        if AIRFLOW_V_3_0_PLUS:
            assert log_sources == []
        else:
            assert len(log_sources) == 0
            assert len(log_msgs) == 1
            assert log_sources == []

        assert "not found in Elasticsearch" in log_msgs[0]

    @pytest.mark.db_test
    def test_set_context(self, ti):
        self.es_task_handler.set_context(ti)
        assert self.es_task_handler.mark_end_on_close

    @pytest.mark.db_test
    def test_set_context_w_json_format_and_write_stdout(self, ti):
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.es_task_handler.formatter = formatter
        self.es_task_handler.write_stdout = True
        self.es_task_handler.json_format = True
        self.es_task_handler.set_context(ti)

    @pytest.mark.db_test
    def test_read_with_json_format(self, ti):
        ts = pendulum.now()
        formatter = logging.Formatter(
            "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s - %(exc_text)s"
        )
        self.es_task_handler.formatter = formatter
        self.es_task_handler.json_format = True

        self.body = {
            "message": self.test_message,
            "event": self.test_message,
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
        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[2].event == self.test_message
        else:
            assert logs[0][0][1] == self.test_message

    @pytest.mark.db_test
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
            "event": self.test_message,
            "log_id": self.LOG_ID,
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
        if AIRFLOW_V_3_0_PLUS:
            logs = list(logs)
            assert logs[2].event == self.test_message
        else:
            assert logs[0][0][1] == self.test_message

    @pytest.mark.db_test
    def test_read_with_custom_offset_and_host_fields(self, ti):
        ts = pendulum.now()
        # Delete the existing log entry as it doesn't have the new offset and host fields
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)

        self.es_task_handler.host_field = "host.name"
        self.es_task_handler.offset_field = "log.offset"

        self.body = {
            "message": self.test_message,
            "event": self.test_message,
            "log_id": self.LOG_ID,
            "log": {"offset": 1},
            "host": {"name": "somehostname"},
        }
        self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body, id=id)

        logs, _ = self.es_task_handler.read(
            ti, 1, {"offset": 0, "last_log_timestamp": str(ts), "end_of_log": False}
        )
        if AIRFLOW_V_3_0_PLUS:
            pass
        else:
            assert logs[0][0][1] == "some random stuff"

    @pytest.mark.db_test
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

    @pytest.mark.db_test
    def test_close_no_mark_end(self, ti):
        ti.raw = True
        self.es_task_handler.set_context(ti)
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert self.end_of_log_mark not in log_file.read()
        assert self.es_task_handler.closed

    @pytest.mark.db_test
    def test_close_closed(self, ti):
        self.es_task_handler.closed = True
        self.es_task_handler.set_context(ti)
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert len(log_file.read()) == 0

    @pytest.mark.db_test
    def test_close_with_no_handler(self, ti):
        self.es_task_handler.set_context(ti)
        self.es_task_handler.handler = None
        self.es_task_handler.close()
        with open(
            os.path.join(self.local_log_location, self.FILENAME_TEMPLATE.format(try_number=1))
        ) as log_file:
            assert len(log_file.read()) == 0
        assert self.es_task_handler.closed

    @pytest.mark.db_test
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

    @pytest.mark.db_test
    def test_render_log_id(self, ti):
        assert _render_log_id(self.es_task_handler.log_id_template, ti, 1) == self.LOG_ID

        self.es_task_handler.json_format = True
        assert _render_log_id(self.es_task_handler.log_id_template, ti, 1) == self.LOG_ID

    def test_clean_date(self):
        clean_logical_date = _clean_date(datetime(2016, 7, 8, 9, 10, 11, 12))
        assert clean_logical_date == "2016_07_08T09_10_11_000012"

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("json_format", "es_frontend", "expected_url"),
        [
            # Common cases
            (
                True,
                "localhost:5601/{log_id}",
                "https://localhost:5601/" + quote(LOG_ID),
            ),
            (
                False,
                "localhost:5601/{log_id}",
                "https://localhost:5601/" + quote(LOG_ID),
            ),
            # Ignore template if "{log_id}"" is missing in the URL
            (False, "localhost:5601", "https://localhost:5601"),
            # scheme handling
            (
                False,
                "https://localhost:5601/path/{log_id}",
                "https://localhost:5601/path/" + quote(LOG_ID),
            ),
            (
                False,
                "http://localhost:5601/path/{log_id}",
                "http://localhost:5601/path/" + quote(LOG_ID),
            ),
            (
                False,
                "other://localhost:5601/path/{log_id}",
                "other://localhost:5601/path/" + quote(LOG_ID),
            ),
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

            self.es_task_handler.io.index_patterns_callable = "path.to.index_pattern_callable"
            result = self.es_task_handler.io._get_index_patterns({})

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


@pytest.mark.db_test
class TestElasticsearchRemoteLogIO:
    DAG_ID = "dag_for_testing_es_log_handler"
    TASK_ID = "task_for_testing_es_log_handler"
    RUN_ID = "run_for_testing_es_log_handler"
    LOGICAL_DATE = datetime(2016, 1, 1)
    FILENAME_TEMPLATE = "{try_number}.log"

    @pytest.fixture(autouse=True)
    def setup_tests(self, ti, es_8_container_url):
        self.elasticsearch_8_url = es_8_container_url
        self.elasticsearch_io = ElasticsearchRemoteLogIO(
            write_to_es=True,
            write_stdout=True,
            delete_local_copy=True,
            host=es_8_container_url,
            base_log_folder=Path(""),
        )

    @pytest.fixture
    def tmp_json_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            file_path = os.path.join(tmpdir, "1.log")
            self.tmp_file = file_path

            sample_logs = [
                {"message": "start"},
                {"message": "processing"},
                {"message": "end"},
            ]
            with open(file_path, "w") as f:
                for log in sample_logs:
                    f.write(json.dumps(log) + "\n")

            yield file_path

            del self.tmp_file

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

    @pytest.fixture
    def unique_index(self):
        """Generate a unique index name for each test."""
        return f"airflow-logs-{uuid.uuid4()}"

    @pytest.mark.setup_timeout(300)
    @pytest.mark.execution_timeout(300)
    @patch(
        "airflow.providers.elasticsearch.log.es_task_handler.TASK_LOG_FIELDS",
        ["message"],
    )
    def test_read_write_to_es(self, tmp_json_file, ti):
        self.elasticsearch_io.client = self.elasticsearch_io.client.options(
            request_timeout=120, retry_on_timeout=True, max_retries=5
        )
        self.elasticsearch_io.write_stdout = False
        self.elasticsearch_io.upload(tmp_json_file, ti)
        self.elasticsearch_io.client.indices.refresh(
            index=self.elasticsearch_io.target_index, request_timeout=120
        )
        log_source_info, log_messages = self.elasticsearch_io.read("", ti)
        assert log_source_info[0] == self.elasticsearch_8_url
        assert len(log_messages) == 3

        expected_msg = ["start", "processing", "end"]
        for msg, log_message in zip(expected_msg, log_messages):
            print(f"msg: {msg}, log_message: {log_message}")
            json_log = json.loads(log_message)
            assert "message" in json_log
            assert json_log["message"] == msg

    def test_write_to_stdout(self, tmp_json_file, ti, capsys):
        self.elasticsearch_io.write_to_es = False
        self.elasticsearch_io.upload(tmp_json_file, ti)

        captured = capsys.readouterr()
        stdout_lines = captured.out.strip().splitlines()
        log_entries = [json.loads(line) for line in stdout_lines]
        assert log_entries[0]["message"] == "start"
        assert log_entries[1]["message"] == "processing"
        assert log_entries[2]["message"] == "end"

    def test_invalid_task_log_file_path(self, ti):
        with (
            patch.object(self.elasticsearch_io, "_parse_raw_log") as mock_parse,
            patch.object(self.elasticsearch_io, "_write_to_es") as mock_write,
        ):
            self.elasticsearch_io.upload(Path("/invalid/path"), ti)

            mock_parse.assert_not_called()
            mock_write.assert_not_called()

    def test_raw_log_should_contain_log_id_and_offset(self, tmp_json_file, ti):
        with open(self.tmp_file) as f:
            raw_log = f.read()
        json_log_lines = self.elasticsearch_io._parse_raw_log(raw_log, ti)
        assert len(json_log_lines) == 3
        for json_log_line in json_log_lines:
            assert "log_id" in json_log_line
            assert "offset" in json_log_line

    @patch("elasticsearch.Elasticsearch.count", return_value={"count": 0})
    def test_read_with_missing_log(self, mocked_count, ti):
        log_source_info, log_messages = self.elasticsearch_io.read("", ti)
        log_id = _render_log_id(self.elasticsearch_io.log_id_template, ti, ti.try_number)
        assert log_source_info == []
        assert f"*** Log {log_id} not found in Elasticsearch" in log_messages[0]
        mocked_count.assert_called_once()
