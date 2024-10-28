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

import pendulum
import pytest

from airflow.providers.elasticsearch.log.es_task_handler import (
    ElasticsearchJSONFormatter,
)


class TestElasticsearchJSONFormatter:
    JSON_FIELDS = ["asctime", "filename", "lineno", "levelname", "message", "exc_text"]
    EXTRA_FIELDS = {
        "dag_id": "dag1",
        "task_id": "task1",
        "execution_date": "2023-11-17",
        "try_number": "1",
        "log_id": "Some_log_id",
    }

    @pytest.fixture
    def es_json_formatter(self):
        return ElasticsearchJSONFormatter()

    @pytest.fixture
    def log_record(self):
        return logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test_file.txt",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

    def test_format_log_record(self, es_json_formatter, log_record):
        """Test the log record formatting."""
        es_json_formatter.json_fields = self.JSON_FIELDS
        formatted = es_json_formatter.format(log_record)
        data = json.loads(formatted)
        assert all(key in self.JSON_FIELDS for key in data.keys())
        assert data["filename"] == "test_file.txt"
        assert data["lineno"] == 1
        assert data["levelname"] == "INFO"
        assert data["message"] == "Test message"

    def test_formattime_in_iso8601_format(self, es_json_formatter, log_record):
        es_json_formatter.json_fields = ["asctime"]
        iso8601_format = es_json_formatter.formatTime(log_record)
        try:
            pendulum.parse(iso8601_format, strict=True)
        except ValueError:
            raise Exception("Time is not in ISO8601 format")

    def test_extra_fields(self, es_json_formatter, log_record):
        es_json_formatter.json_fields = self.JSON_FIELDS
        es_json_formatter.extras = self.EXTRA_FIELDS
        formatted = es_json_formatter.format(log_record)
        data = json.loads(formatted)
        assert all(
            (key in self.JSON_FIELDS or key in self.EXTRA_FIELDS) for key in data.keys()
        )
        assert data["filename"] == "test_file.txt"
        assert data["lineno"] == 1
        assert data["levelname"] == "INFO"
        assert data["dag_id"] == "dag1"
        assert data["task_id"] == "task1"
        assert data["execution_date"] == "2023-11-17"
        assert data["try_number"] == "1"
        assert data["log_id"] == "Some_log_id"
