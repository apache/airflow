from __future__ import annotations

import logging
import json
import uuid
import datetime
from logging import LogRecord
from unittest.mock import MagicMock

import pytest
import requests_mock

from airflow.providers.grafana.loki.log.loki_task_handler import LokiRemoteLogIO, LokiTaskHandler

@pytest.fixture
def mock_runtime_ti():
    ti = MagicMock()
    ti.dag_id = "test_dag"
    ti.task_id = "test_task"
    ti.run_id = "test_run"
    ti.try_number = 1
    ti.map_index = -1
    return ti

@pytest.fixture
def tmp_log_folder(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    return log_dir

def test_loki_upload_deletes_local_copy_on_success(tmp_log_folder, mock_runtime_ti):
    # Mocking a JSON log output exactly like the Task SDK
    log_file = tmp_log_folder / "test.log"
    log_file.write_text(json.dumps({"event": "hello world"}))
    
    io = LokiRemoteLogIO(host="http://localhost:3100", base_log_folder=tmp_log_folder, delete_local_copy=True)
    
    with requests_mock.Mocker() as m:
        m.post("http://localhost:3100/loki/api/v1/push", status_code=204)
        io.upload(log_file, mock_runtime_ti)
        
        # Verify the file was cleaned up correctly after successful push
        assert not log_file.exists()

def test_loki_bloom_filter_labels_and_payload(tmp_log_folder, mock_runtime_ti):
    # Validates that streams are not polluted with high cardinality labels
    log_file = tmp_log_folder / "test2.log"
    log_file.write_text(json.dumps({"event": "another line"}))
    
    io = LokiRemoteLogIO(host="http://localhost:3100", base_log_folder=tmp_log_folder)
    
    with requests_mock.Mocker() as m:
        m.post("http://localhost:3100/loki/api/v1/push", status_code=204)
        io.upload(log_file, mock_runtime_ti)
        
        assert m.called
        request_body = json.loads(m.last_request.text)
        stream_labels = request_body["streams"][0]["stream"]
        
        # Crucial Assertion: Stream labels strictly avoid `task_id` and `try_number`
        assert stream_labels == {"job": "airflow_tasks", "dag_id": "test_dag"}
        
        # Crucial Assertion: TSDB JSON log values contain the task_id explicitly embedded
        log_json = json.loads(request_body["streams"][0]["values"][0][1])
        assert log_json["task_id"] == "test_task"
        assert log_json["try_number"] == "1"

def test_read_generates_optimized_logql(tmp_log_folder, mock_runtime_ti):
    io = LokiRemoteLogIO(host="http://localhost:3100", base_log_folder=tmp_log_folder)
    
    with requests_mock.Mocker() as m:
        # Mock Loki response
        m.get("http://localhost:3100/loki/api/v1/query_range", json={"data": {"result": []}})
        io.read("", mock_runtime_ti)
        
        assert m.called
        # Crucial Assertion: The generated LogQL strictly utilizes JSON Bloom Filters mapping to the embedded keys
        query_param = m.last_request.qs["query"][0]
        assert query_param == '{job="airflow_tasks",dag_id="test_dag"} | json | task_id="test_task" | run_id="test_run" | try_number="1" | map_index="-1"'
