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
from unittest.mock import MagicMock, patch
import pytest
from airflow.exceptions import AirflowException
from airflow.providers.yeedu.hooks.yeedu import YeeduHook

base_url = "https://localhost:8080/"
token = "dgefefefeifeif"
hostname = "localhost:8080"
workspace_id = "8"
hook = YeeduHook(token, hostname, workspace_id)
job_conf_id = "83"
job_id = "653"

@pytest.fixture
def mock_api_request():
    with patch("providers.yeedu.hooks.yeedu.YeeduHook._api_request") as mock:
        yield mock

def test_submit_job(mock_api_request):
    mock_response = {
        "job_id": "653",
        "job_conf_id": "83",
        "cluster_id": "7",
        "tenant_id": "d5a2a0ca-8317-49f0-af76-589e7438b563",
        "created_by_id": "52",
        "modified_by_id": "52",
        "last_update_date": "2023-11-27T05:16:27.502Z",
        "from_date": "2023-11-27T05:16:27.502Z",
    }
    mock_status_code = 200
    mock_api_request.return_value.json.return_value = mock_response
    mock_api_request.return_value.status_code = mock_status_code
    result = hook.submit_job(job_conf_id)
    assert result == job_id

def test_submit_job_null(mock_api_request):
    job_conf_id = "83"
    mock_response = {"job_id": ""}
    mock_status_code = 200
    mock_api_request.return_value.json.return_value = mock_response
    mock_api_request.return_value.status_code = mock_status_code

    with pytest.raises(AirflowException) as context:
        hook.submit_job(job_conf_id)

    assert str(context.value) == str(mock_response)

def test_job_complete(mock_api_request):
    mock_response = {"job_id": 653, "job_application_id": "local-1701062592104", "job_status": "DONE"}
    mock_status_code = 200
    mock_api_request.return_value.json.return_value = mock_response
    mock_api_request.return_value.status_code = mock_status_code
    result = hook.wait_for_completion(job_id)
    assert result == "DONE"

def test_job_retry(mock_api_request):
    mock_response = {"job_id": 653, "job_application_id": "local-1701062592104", "job_status": "ERROR"}
    mock_status_code = 400
    mock_api_request.return_value.json.return_value = mock_response
    mock_api_request.return_value.status_code = mock_status_code
    with pytest.raises(AirflowException) as context:
        hook.wait_for_completion(job_id)

    assert str(context.value) == "Continuous API failure reached the threshold"

def test_job_retry_reset(mock_api_request):
    responses = [
        MagicMock(status_code=400, json=lambda: {"job_id": 653, "job_status": "ERROR"}),
        MagicMock(status_code=200, json=lambda: {"job_id": 653, "job_status": "RUNNING"}),
        MagicMock(status_code=400, json=lambda: {"job_id": 653, "job_status": "DONE"}),
        MagicMock(status_code=400, json=lambda: {"job_id": 653, "job_status": "DONE"}),
        MagicMock(status_code=400, json=lambda: {"job_id": 653, "job_status": "DONE"}),
        MagicMock(status_code=400, json=lambda: {"job_id": 653, "job_status": "DONE"}),
        MagicMock(status_code=400, json=lambda: {"job_id": 653, "job_status": "DONE"}),
    ]
    mock_api_request.side_effect = responses

    with pytest.raises(AirflowException) as context:
        hook.wait_for_completion(job_id)
    assert str(context.value) == "Continuous API failure reached the threshold"
