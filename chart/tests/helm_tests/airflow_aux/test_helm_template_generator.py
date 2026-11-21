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

from email.message import Message
from unittest import mock
from urllib.error import HTTPError, URLError

import pytest
from chart_utils.helm_template_generator import get_schema_k8s


@pytest.fixture(autouse=True)
def clear_schema_cache():
    get_schema_k8s.cache_clear()
    yield
    get_schema_k8s.cache_clear()


@mock.patch("chart_utils.helm_template_generator.time.sleep")
@mock.patch("chart_utils.helm_template_generator.urlopen")
def test_get_schema_k8s_success_first_attempt(mock_urlopen, mock_sleep):
    mock_resp = mock.MagicMock()
    mock_resp.read.return_value = b'{"type": "object", "properties": {}}'
    mock_urlopen.return_value = mock_resp

    result = get_schema_k8s("v1", "Service", "1.31.12")

    assert result == {"type": "object", "properties": {}}
    mock_urlopen.assert_called_once_with(
        "https://airflow.apache.org/k8s-schemas/v1.31.12-standalone-strict/service-v1.json",
        timeout=30,
    )
    mock_sleep.assert_not_called()


@pytest.mark.parametrize(
    "transient_error",
    [
        URLError("The handshake operation timed out"),
        TimeoutError("Connection timed out"),
        OSError("Connection reset by peer"),
        HTTPError("https://airflow.apache.org/", 503, "Service Unavailable", Message(), None),
    ],
)
@mock.patch("chart_utils.helm_template_generator.time.sleep")
@mock.patch("chart_utils.helm_template_generator.urlopen")
def test_get_schema_k8s_retry_on_transient_error(mock_urlopen, mock_sleep, transient_error):
    mock_resp = mock.MagicMock()
    mock_resp.read.return_value = b'{"type": "object"}'
    mock_urlopen.side_effect = [transient_error, mock_resp]

    result = get_schema_k8s("v1", "Pod", "1.31.12")

    assert result == {"type": "object"}
    assert mock_urlopen.call_count == 2
    mock_sleep.assert_called_once_with(1.0)


@pytest.mark.parametrize(
    "persistent_error",
    [
        URLError("The handshake operation timed out"),
        HTTPError("https://airflow.apache.org/", 500, "Internal Server Error", Message(), None),
    ],
)
@mock.patch("chart_utils.helm_template_generator.time.sleep")
@mock.patch("chart_utils.helm_template_generator.urlopen")
def test_get_schema_k8s_exhausts_retries_and_raises(mock_urlopen, mock_sleep, persistent_error):
    mock_urlopen.side_effect = persistent_error

    with pytest.raises(type(persistent_error)):
        get_schema_k8s("v1", "ConfigMap", "1.31.12")

    assert mock_urlopen.call_count == 3
    assert mock_sleep.call_args_list == [mock.call(1.0), mock.call(2.0)]


@mock.patch("chart_utils.helm_template_generator.time.sleep")
@mock.patch("chart_utils.helm_template_generator.urlopen")
def test_get_schema_k8s_http_404_raises_filenotfound_without_retry(mock_urlopen, mock_sleep):
    mock_urlopen.side_effect = HTTPError("https://airflow.apache.org/", 404, "Not Found", Message(), None)

    with pytest.raises(FileNotFoundError, match="K8s JSON schema not found"):
        get_schema_k8s("v1", "NonExistent", "1.31.12")

    mock_urlopen.assert_called_once()
    mock_sleep.assert_not_called()
