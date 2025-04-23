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

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# Import the operator to be tested
from http_to_gcs import HttpToGCSOperator

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context


@pytest.fixture
def dag():
    """
    Fixture to create a DAG for testing.
    """
    return DAG(dag_id="test_http_to_gcs", start_date=datetime(2023, 1, 1))


@pytest.fixture
def context():
    """
    Fixture to create a dummy Airflow context.
    """
    return Context()


@pytest.fixture
def http_to_gcs_operator(dag):
    """
    Fixture to create an HttpToGCSOperator instance with default parameters.
    """
    return HttpToGCSOperator(
        task_id="test_http_to_gcs_operator",
        http_conn_id="http_default",
        bucket_name="test-bucket",
        object_name="test-object",
        endpoint="/test",
        dag=dag,
    )


@patch("airflow.providers.http.hooks.http.HttpHook.run")
@patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
def test_http_to_gcs_operator_execute_success(mock_gcs_upload, mock_http_run, http_to_gcs_operator, context):
    """
    Test that the execute method calls the HTTP hook and GCS hook correctly.
    """
    mock_http_response = MagicMock()
    mock_http_response.content = b"test data"
    mock_http_response.encoding = "utf-8"
    mock_http_run.return_value = mock_http_response

    http_to_gcs_operator.execute(context)

    mock_http_run.assert_called_once_with(endpoint="/test", data={}, headers={}, extra_options=None)
    mock_gcs_upload.assert_called_once()


@patch("airflow.providers.http.hooks.http.HttpHook.run")
@patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
def test_http_to_gcs_operator_execute_with_params(mock_gcs_upload, mock_http_run, dag, context):
    """
    Test that the execute method calls the HTTP hook and GCS hook with parameters.
    """
    operator = HttpToGCSOperator(
        task_id="test_http_to_gcs_operator_with_params",
        http_conn_id="http_default",
        bucket_name="test-bucket",
        object_name="test-object",
        endpoint="/test",
        method="POST",
        data={"key": "value"},
        headers={"X-Custom-Header": "test"},
        extra_options={"timeout": 60},
        dag=dag,
    )

    mock_http_response = MagicMock()
    mock_http_response.content = b"test data"
    mock_http_response.encoding = "utf-8"
    mock_http_run.return_value = mock_http_response

    operator.execute(context)

    mock_http_run.assert_called_once_with(
        endpoint="/test",
        data={"key": "value"},
        headers={"X-Custom-Header": "test"},
        extra_options={"timeout": 60},
    )
    mock_gcs_upload.assert_called_once()


@patch("airflow.providers.http.hooks.http.HttpHook.run")
@patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
def test_http_to_gcs_operator_gcs_upload_params(mock_gcs_upload, mock_http_run, dag, context):
    """
    Test that GCS hook is called with the correct upload parameters.
    """
    operator = HttpToGCSOperator(
        task_id="test_http_to_gcs_operator_gcs_params",
        http_conn_id="http_default",
        bucket_name="test-bucket",
        object_name="test-object",
        endpoint="/test",
        mime_type="application/json",
        gzip=True,
        encoding="latin-1",
        chunk_size=1024,
        timeout=30,
        num_max_attempts=5,
        metadata={"key": "value"},
        cache_control="no-cache",
        user_project="test-project",
        dag=dag,
    )

    mock_http_response = MagicMock()
    mock_http_response.content = b'{"data": "test"}'
    mock_http_response.encoding = "utf-8"
    mock_http_run.return_value = mock_http_response

    operator.execute(context)

    mock_gcs_upload.assert_called_once_with(
        data=b'{"data": "test"}',
        bucket_name="test-bucket",
        object_name="test-object",
        mime_type="application/json",
        gzip=True,
        encoding="latin-1",
        chunk_size=1024,
        timeout=30,
        num_max_attempts=5,
        metadata={"key": "value"},
        cache_control="no-cache",
        user_project="test-project",
    )


@patch("airflow.providers.http.hooks.http.HttpHook.run")
def test_http_to_gcs_operator_http_hook_creation(mock_http_run, dag, context):
    """
    Test that the HttpHook is created with the correct parameters using cached_property.
    """

    operator = HttpToGCSOperator(
        task_id="test_http_hook_cached_property",
        http_conn_id="test_http_conn",
        bucket_name="test-bucket",
        object_name="test-object",
        endpoint="/test",
        method="POST",
        auth_type=None,
        tcp_keep_alive=True,
        tcp_keep_alive_idle=150,
        tcp_keep_alive_count=30,
        tcp_keep_alive_interval=45,
        dag=dag,
    )

    mock_http_response = MagicMock()
    mock_http_response.content = b"test data"
    mock_http_response.encoding = "utf-8"
    mock_http_run.return_value = mock_http_response

    # Access the http_hook property - this should trigger creation
    http_hook_instance_1 = operator.http_hook
    http_hook_instance_2 = operator.http_hook  # Access it again

    assert http_hook_instance_1 is http_hook_instance_2  # Should be the same instance (cached)
    assert isinstance(http_hook_instance_1, HttpHook)
    http_hook_instance_1.run.assert_called_once_with(
        endpoint="/test", data={}, headers={}, extra_options=None
    )


@patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
@patch("airflow.providers.http.hooks.http.HttpHook.run")
def test_http_to_gcs_operator_gcs_hook_creation(mock_http_run, mock_gcs_upload, dag, context):
    """
    Test that the GCSHook is created with the correct parameters using cached_property.
    """

    operator = HttpToGCSOperator(
        task_id="test_gcs_hook_cached_property",
        http_conn_id="test_http_conn",
        gcp_conn_id="test_gcp_conn",
        impersonation_chain="test_impersonation",
        bucket_name="test-bucket",
        object_name="test-object",
        endpoint="/test",
        dag=dag,
    )

    mock_http_response = MagicMock()
    mock_http_response.content = b"test data"
    mock_http_response.encoding = "utf-8"
    mock_http_run.return_value = mock_http_response

    # Access the gcs_hook property - this should trigger creation
    gcs_hook_instance_1 = operator.gcs_hook
    gcs_hook_instance_2 = operator.gcs_hook  # Access it again

    assert gcs_hook_instance_1 is gcs_hook_instance_2  # Should be the same instance (cached)
    assert isinstance(gcs_hook_instance_1, GCSHook)
    mock_gcs_upload.assert_called_once()
