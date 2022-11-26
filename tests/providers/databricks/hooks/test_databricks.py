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

import itertools
import json
import sys
import time

import aiohttp
import pytest
import tenacity
from requests import exceptions as requests_exceptions
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.databricks.hooks.databricks import (
    GET_RUN_ENDPOINT,
    SUBMIT_RUN_ENDPOINT,
    DatabricksHook,
    RunState,
)
from airflow.providers.databricks.hooks.databricks_base import (
    AZURE_DEFAULT_AD_ENDPOINT,
    AZURE_MANAGEMENT_ENDPOINT,
    AZURE_METADATA_SERVICE_INSTANCE_URL,
    AZURE_TOKEN_SERVICE_URL,
    DEFAULT_DATABRICKS_SCOPE,
    TOKEN_REFRESH_LEAD_TIME,
    BearerAuth,
)
from airflow.utils.session import provide_session

if sys.version_info < (3, 8):
    from asynctest import mock
    from asynctest.mock import CoroutineMock as AsyncMock
else:
    from unittest import mock
    from unittest.mock import AsyncMock

TASK_ID = "databricks-operator"
DEFAULT_CONN_ID = "databricks_default"
NOTEBOOK_TASK = {"notebook_path": "/test"}
SPARK_PYTHON_TASK = {"python_file": "test.py", "parameters": ["--param", "123"]}
NEW_CLUSTER = {"spark_version": "2.0.x-scala2.10", "node_type_id": "r3.xlarge", "num_workers": 1}
CLUSTER_ID = "cluster_id"
RUN_ID = 1
JOB_ID = 42
JOB_NAME = "job-name"
DEFAULT_RETRY_NUMBER = 3
DEFAULT_RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(DEFAULT_RETRY_NUMBER),
)
HOST = "xx.cloud.databricks.com"
HOST_WITH_SCHEME = "https://xx.cloud.databricks.com"
LOGIN = "login"
PASSWORD = "password"
TOKEN = "token"
RUN_PAGE_URL = "https://XX.cloud.databricks.com/#jobs/1/runs/1"
LIFE_CYCLE_STATE = "PENDING"
STATE_MESSAGE = "Waiting for cluster"
ERROR_MESSAGE = "error message from databricks API"
GET_RUN_RESPONSE = {
    "job_id": JOB_ID,
    "run_page_url": RUN_PAGE_URL,
    "state": {"life_cycle_state": LIFE_CYCLE_STATE, "state_message": STATE_MESSAGE},
}
GET_RUN_OUTPUT_RESPONSE = {"metadata": {}, "error": ERROR_MESSAGE, "notebook_output": {}}
NOTEBOOK_PARAMS = {"dry-run": "true", "oldest-time-to-consider": "1457570074236"}
JAR_PARAMS = ["param1", "param2"]
RESULT_STATE = ""
LIBRARIES = [
    {"jar": "dbfs:/mnt/libraries/library.jar"},
    {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2", "exclusions": ["slf4j:slf4j"]}},
]
LIST_JOBS_RESPONSE = {
    "jobs": [
        {
            "job_id": JOB_ID,
            "settings": {
                "name": JOB_NAME,
            },
        },
    ],
    "has_more": False,
}
LIST_SPARK_VERSIONS_RESPONSE = {
    "versions": [
        {"key": "8.2.x-scala2.12", "name": "8.2 (includes Apache Spark 3.1.1, Scala 2.12)"},
    ]
}


def run_now_endpoint(host):
    """
    Utility function to generate the run now endpoint given the host.
    """
    return f"https://{host}/api/2.1/jobs/run-now"


def submit_run_endpoint(host):
    """
    Utility function to generate the submit run endpoint given the host.
    """
    return f"https://{host}/api/2.1/jobs/runs/submit"


def get_run_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f"https://{host}/api/2.1/jobs/runs/get"


def get_run_output_endpoint(host):
    """
    Utility function to generate the get run output endpoint given the host.
    """
    return f"https://{host}/api/2.1/jobs/runs/get-output"


def cancel_run_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f"https://{host}/api/2.1/jobs/runs/cancel"


def start_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f"https://{host}/api/2.0/clusters/start"


def restart_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f"https://{host}/api/2.0/clusters/restart"


def terminate_cluster_endpoint(host):
    """
    Utility function to generate the get run endpoint given the host.
    """
    return f"https://{host}/api/2.0/clusters/delete"


def install_endpoint(host):
    """
    Utility function to generate the install endpoint given the host.
    """
    return f"https://{host}/api/2.0/libraries/install"


def uninstall_endpoint(host):
    """
    Utility function to generate the uninstall endpoint given the host.
    """
    return f"https://{host}/api/2.0/libraries/uninstall"


def list_jobs_endpoint(host):
    """
    Utility function to generate the list jobs endpoint given the host
    """
    return f"https://{host}/api/2.1/jobs/list"


def list_spark_versions_endpoint(host):
    """Utility function to generate the list spark versions endpoint given the host"""
    return f"https://{host}/api/2.0/clusters/spark-versions"


def create_valid_response_mock(content):
    response = mock.MagicMock()
    response.json.return_value = content
    response.__aenter__.return_value.json = AsyncMock(return_value=content)
    return response


def create_successful_response_mock(content):
    response = mock.MagicMock()
    response.json.return_value = content
    response.status_code = 200
    return response


def create_post_side_effect(exception, status_code=500):
    if exception != requests_exceptions.HTTPError:
        return exception()
    else:
        response = mock.MagicMock()
        response.status_code = status_code
        response.raise_for_status.side_effect = exception(response=response)
        return response


def setup_mock_requests(mock_requests, exception, status_code=500, error_count=None, response_content=None):
    side_effect = create_post_side_effect(exception, status_code)

    if error_count is None:
        # POST requests will fail indefinitely
        mock_requests.post.side_effect = itertools.repeat(side_effect)
    else:
        # POST requests will fail 'error_count' times, and then they will succeed (once)
        mock_requests.post.side_effect = [side_effect] * error_count + [
            create_valid_response_mock(response_content)
        ]


class TestDatabricksHook:
    """
    Tests for DatabricksHook.
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.login = LOGIN
        conn.password = PASSWORD
        conn.extra = None
        session.commit()

        self.hook = DatabricksHook(retry_delay=0)

    def test_user_agent_string(self):
        op = "DatabricksSql"
        hook = DatabricksHook(retry_delay=0, caller=op)
        ua_string = hook.user_agent_value
        assert ua_string.endswith(f" operator/{op}")

    def test_parse_host_with_proper_host(self):
        host = self.hook._parse_host(HOST)
        assert host == HOST

    def test_parse_host_with_scheme(self):
        host = self.hook._parse_host(HOST_WITH_SCHEME)
        assert host == HOST

    def test_init_bad_retry_limit(self):
        with pytest.raises(ValueError):
            DatabricksHook(retry_limit=0)

    def test_do_api_call_retries_with_retryable_error(self):
        hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch("airflow.providers.databricks.hooks.databricks_base.requests") as mock_requests:
                with mock.patch.object(hook.log, "error") as mock_errors:
                    setup_mock_requests(mock_requests, exception)

                    with pytest.raises(AirflowException):
                        hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    assert mock_errors.call_count == DEFAULT_RETRY_NUMBER

    def test_do_api_call_retries_with_too_many_requests(self):
        hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

        with mock.patch("airflow.providers.databricks.hooks.databricks_base.requests") as mock_requests:
            with mock.patch.object(hook.log, "error") as mock_errors:
                setup_mock_requests(mock_requests, requests_exceptions.HTTPError, status_code=429)

                with pytest.raises(AirflowException):
                    hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                assert mock_errors.call_count == DEFAULT_RETRY_NUMBER

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_do_api_call_does_not_retry_with_non_retryable_error(self, mock_requests):
        hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

        setup_mock_requests(mock_requests, requests_exceptions.HTTPError, status_code=400)

        with mock.patch.object(hook.log, "error") as mock_errors:
            with pytest.raises(AirflowException):
                hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

            mock_errors.assert_not_called()

    def test_do_api_call_succeeds_after_retrying(self):
        hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch("airflow.providers.databricks.hooks.databricks_base.requests") as mock_requests:
                with mock.patch.object(hook.log, "error") as mock_errors:
                    setup_mock_requests(
                        mock_requests, exception, error_count=2, response_content={"run_id": "1"}
                    )

                    response = hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    assert mock_errors.call_count == 2
                    assert response == {"run_id": "1"}

    def test_do_api_call_custom_retry(self):
        hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch("airflow.providers.databricks.hooks.databricks_base.requests") as mock_requests:
                with mock.patch.object(hook.log, "error") as mock_errors:
                    setup_mock_requests(mock_requests, exception)

                    with pytest.raises(AirflowException):
                        hook._do_api_call(SUBMIT_RUN_ENDPOINT, {})

                    assert mock_errors.call_count == DEFAULT_RETRY_NUMBER

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_do_api_call_patch(self, mock_requests):
        mock_requests.patch.return_value.json.return_value = {"cluster_name": "new_name"}
        data = {"cluster_name": "new_name"}
        patched_cluster_name = self.hook._do_api_call(("PATCH", "api/2.1/jobs/runs/submit"), data)

        assert patched_cluster_name["cluster_name"] == "new_name"
        mock_requests.patch.assert_called_once_with(
            submit_run_endpoint(HOST),
            json={"cluster_name": "new_name"},
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_submit_run(self, mock_requests):
        mock_requests.post.return_value.json.return_value = {"run_id": "1"}
        data = {"notebook_task": NOTEBOOK_TASK, "new_cluster": NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        assert run_id == "1"
        mock_requests.post.assert_called_once_with(
            submit_run_endpoint(HOST),
            json={
                "notebook_task": NOTEBOOK_TASK,
                "new_cluster": NEW_CLUSTER,
            },
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_spark_python_submit_run(self, mock_requests):
        mock_requests.post.return_value.json.return_value = {"run_id": "1"}
        data = {"spark_python_task": SPARK_PYTHON_TASK, "new_cluster": NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        assert run_id == "1"
        mock_requests.post.assert_called_once_with(
            submit_run_endpoint(HOST),
            json={
                "spark_python_task": SPARK_PYTHON_TASK,
                "new_cluster": NEW_CLUSTER,
            },
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_run_now(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {"run_id": "1"}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {"notebook_params": NOTEBOOK_PARAMS, "jar_params": JAR_PARAMS, "job_id": JOB_ID}
        run_id = self.hook.run_now(data)

        assert run_id == "1"

        mock_requests.post.assert_called_once_with(
            run_now_endpoint(HOST),
            json={"notebook_params": NOTEBOOK_PARAMS, "jar_params": JAR_PARAMS, "job_id": JOB_ID},
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_run_page_url(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        run_page_url = self.hook.get_run_page_url(RUN_ID)

        assert run_page_url == RUN_PAGE_URL
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json=None,
            params={"run_id": RUN_ID},
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_job_id(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        job_id = self.hook.get_job_id(RUN_ID)

        assert job_id == JOB_ID
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json=None,
            params={"run_id": RUN_ID},
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_run_output(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_OUTPUT_RESPONSE

        run_output_error = self.hook.get_run_output(RUN_ID).get("error")

        assert run_output_error == ERROR_MESSAGE
        mock_requests.get.assert_called_once_with(
            get_run_output_endpoint(HOST),
            json=None,
            params={"run_id": RUN_ID},
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_run_state(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        run_state = self.hook.get_run_state(RUN_ID)

        assert run_state == RunState(LIFE_CYCLE_STATE, RESULT_STATE, STATE_MESSAGE)
        mock_requests.get.assert_called_once_with(
            get_run_endpoint(HOST),
            json=None,
            params={"run_id": RUN_ID},
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_run_state_str(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE
        run_state_str = self.hook.get_run_state_str(RUN_ID)
        assert run_state_str == f"State: {LIFE_CYCLE_STATE}. Result: {RESULT_STATE}. {STATE_MESSAGE}"

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_run_state_lifecycle(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE
        lifecycle_state = self.hook.get_run_state_lifecycle(RUN_ID)
        assert lifecycle_state == LIFE_CYCLE_STATE

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_run_state_result(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE
        result_state = self.hook.get_run_state_result(RUN_ID)
        assert result_state == RESULT_STATE

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_run_state_cycle(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE
        state_message = self.hook.get_run_state_message(RUN_ID)
        assert state_message == STATE_MESSAGE

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_cancel_run(self, mock_requests):
        mock_requests.post.return_value.json.return_value = GET_RUN_RESPONSE

        self.hook.cancel_run(RUN_ID)

        mock_requests.post.assert_called_once_with(
            cancel_run_endpoint(HOST),
            json={"run_id": RUN_ID},
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_start_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.start_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            start_cluster_endpoint(HOST),
            json={"cluster_id": CLUSTER_ID},
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_restart_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.restart_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            restart_cluster_endpoint(HOST),
            json={"cluster_id": CLUSTER_ID},
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_terminate_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        self.hook.terminate_cluster({"cluster_id": CLUSTER_ID})

        mock_requests.post.assert_called_once_with(
            terminate_cluster_endpoint(HOST),
            json={"cluster_id": CLUSTER_ID},
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_install_libs_on_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        data = {"cluster_id": CLUSTER_ID, "libraries": LIBRARIES}
        self.hook.install(data)

        mock_requests.post.assert_called_once_with(
            install_endpoint(HOST),
            json={"cluster_id": CLUSTER_ID, "libraries": LIBRARIES},
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_uninstall_libs_on_cluster(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock

        data = {"cluster_id": CLUSTER_ID, "libraries": LIBRARIES}
        self.hook.uninstall(data)

        mock_requests.post.assert_called_once_with(
            uninstall_endpoint(HOST),
            json={"cluster_id": CLUSTER_ID, "libraries": LIBRARIES},
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    def test_is_aad_token_valid_returns_true(self):
        aad_token = {"token": "my_token", "expires_on": int(time.time()) + TOKEN_REFRESH_LEAD_TIME + 10}
        assert self.hook._is_aad_token_valid(aad_token)

    def test_is_aad_token_valid_returns_false(self):
        aad_token = {"token": "my_token", "expires_on": int(time.time())}
        assert not self.hook._is_aad_token_valid(aad_token)

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_list_jobs_success_single_page(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.return_value.json.return_value = LIST_JOBS_RESPONSE

        jobs = self.hook.list_jobs()

        mock_requests.get.assert_called_once_with(
            list_jobs_endpoint(HOST),
            json=None,
            params={"limit": 25, "offset": 0, "expand_tasks": False},
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

        assert jobs == LIST_JOBS_RESPONSE["jobs"]

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_list_jobs_success_multiple_pages(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.side_effect = [
            create_successful_response_mock({**LIST_JOBS_RESPONSE, "has_more": True}),
            create_successful_response_mock(LIST_JOBS_RESPONSE),
        ]

        jobs = self.hook.list_jobs()

        assert mock_requests.get.call_count == 2

        first_call_args = mock_requests.method_calls[0]
        assert first_call_args[1][0] == list_jobs_endpoint(HOST)
        assert first_call_args[2]["params"] == {"limit": 25, "offset": 0, "expand_tasks": False}

        second_call_args = mock_requests.method_calls[1]
        assert second_call_args[1][0] == list_jobs_endpoint(HOST)
        assert second_call_args[2]["params"] == {"limit": 25, "offset": 1, "expand_tasks": False}

        assert len(jobs) == 2
        assert jobs == LIST_JOBS_RESPONSE["jobs"] * 2

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_job_id_by_name_success(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.return_value.json.return_value = LIST_JOBS_RESPONSE

        job_id = self.hook.find_job_id_by_name(JOB_NAME)

        mock_requests.get.assert_called_once_with(
            list_jobs_endpoint(HOST),
            json=None,
            params={"limit": 25, "offset": 0, "expand_tasks": False, "name": JOB_NAME},
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

        assert job_id == JOB_ID

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_job_id_by_name_not_found(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.return_value.json.return_value = LIST_JOBS_RESPONSE

        job_name = "Non existing job"
        job_id = self.hook.find_job_id_by_name(job_name)

        mock_requests.get.assert_called_once_with(
            list_jobs_endpoint(HOST),
            json=None,
            params={"limit": 25, "offset": 0, "expand_tasks": False, "name": job_name},
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

        assert job_id is None

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_get_job_id_by_name_raise_exception_with_duplicates(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.return_value.json.return_value = {
            **LIST_JOBS_RESPONSE,
            "jobs": LIST_JOBS_RESPONSE["jobs"] * 2,
        }

        exception_message = f"There are more than one job with name {JOB_NAME}."
        with pytest.raises(AirflowException, match=exception_message):
            self.hook.find_job_id_by_name(JOB_NAME)

        mock_requests.get.assert_called_once_with(
            list_jobs_endpoint(HOST),
            json=None,
            params={"limit": 25, "offset": 0, "expand_tasks": False, "name": JOB_NAME},
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_connection_success(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.return_value.json.return_value = LIST_SPARK_VERSIONS_RESPONSE
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.get.return_value).status_code = status_code_mock
        response = self.hook.test_connection()
        assert response == (True, "Connection successfully tested")
        mock_requests.get.assert_called_once_with(
            list_spark_versions_endpoint(HOST),
            json=None,
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_connection_failure(self, mock_requests):
        mock_requests.codes.ok = 404
        mock_requests.get.side_effect = Exception("Connection Failure")
        status_code_mock = mock.PropertyMock(return_value=404)
        type(mock_requests.get.return_value).status_code = status_code_mock
        response = self.hook.test_connection()
        assert response == (False, "Connection Failure")
        mock_requests.get.assert_called_once_with(
            list_spark_versions_endpoint(HOST),
            json=None,
            params=None,
            auth=HTTPBasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )


class TestDatabricksHookToken:
    """
    Tests for DatabricksHook when auth is done with token.
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.extra = json.dumps({"token": TOKEN, "host": HOST})

        session.commit()

        self.hook = DatabricksHook()

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {"run_id": "1"}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {"notebook_task": NOTEBOOK_TASK, "new_cluster": NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        assert run_id == "1"
        args = mock_requests.post.call_args
        kwargs = args[1]
        assert kwargs["auth"].token == TOKEN


class TestDatabricksHookTokenInPassword:
    """
    Tests for DatabricksHook.
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.login = None
        conn.password = TOKEN
        conn.extra = None
        session.commit()

        self.hook = DatabricksHook(retry_delay=0)

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.return_value.json.return_value = {"run_id": "1"}
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {"notebook_task": NOTEBOOK_TASK, "new_cluster": NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        assert run_id == "1"
        args = mock_requests.post.call_args
        kwargs = args[1]
        assert kwargs["auth"].token == TOKEN


class TestDatabricksHookTokenWhenNoHostIsProvidedInExtra(TestDatabricksHookToken):
    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.extra = json.dumps({"token": TOKEN})

        session.commit()

        self.hook = DatabricksHook()


class TestRunState:
    def test_is_terminal_true(self):
        terminal_states = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
        for state in terminal_states:
            run_state = RunState(state, "", "")
            assert run_state.is_terminal

    def test_is_terminal_false(self):
        non_terminal_states = ["PENDING", "RUNNING", "TERMINATING"]
        for state in non_terminal_states:
            run_state = RunState(state, "", "")
            assert not run_state.is_terminal

    def test_is_terminal_with_nonexistent_life_cycle_state(self):
        run_state = RunState("blah", "", "")
        with pytest.raises(AirflowException):
            assert run_state.is_terminal

    def test_is_successful(self):
        run_state = RunState("TERMINATED", "SUCCESS", "")
        assert run_state.is_successful

    def test_to_json(self):
        run_state = RunState("TERMINATED", "SUCCESS", "")
        expected = json.dumps(
            {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "state_message": ""}
        )
        assert expected == run_state.to_json()

    def test_from_json(self):
        state = {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "state_message": ""}
        expected = RunState("TERMINATED", "SUCCESS", "")
        assert expected == RunState.from_json(json.dumps(state))


def create_aad_token_for_resource(resource: str) -> dict:
    return {
        "token_type": "Bearer",
        "expires_in": "599",
        "ext_expires_in": "599",
        "expires_on": "1575500666",
        "not_before": "1575499766",
        "resource": resource,
        "access_token": TOKEN,
    }


class TestDatabricksHookAadToken:
    """
    Tests for DatabricksHook when auth is done with AAD token for SP as user inside workspace.
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.login = "9ff815a6-4404-4ab8-85cb-cd0e6f879c1d"
        conn.password = "secret"
        conn.extra = json.dumps(
            {
                "host": HOST,
                "azure_tenant_id": "3ff810a6-5504-4ab8-85cb-cd0e6f879c1d",
            }
        )
        session.commit()
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.side_effect = [
            create_successful_response_mock(create_aad_token_for_resource(DEFAULT_DATABRICKS_SCOPE)),
            create_successful_response_mock({"run_id": "1"}),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {"notebook_task": NOTEBOOK_TASK, "new_cluster": NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        assert run_id == "1"
        args = mock_requests.post.call_args
        kwargs = args[1]
        assert kwargs["auth"].token == TOKEN


class TestDatabricksHookAadTokenOtherClouds:
    """
    Tests for DatabricksHook when auth is done with AAD token for SP as user inside workspace and
    using non-global Azure cloud (China, GovCloud, Germany)
    """

    @provide_session
    def setup_method(self, method, session=None):
        self.tenant_id = "3ff810a6-5504-4ab8-85cb-cd0e6f879c1d"
        self.ad_endpoint = "https://login.microsoftonline.de"
        self.client_id = "9ff815a6-4404-4ab8-85cb-cd0e6f879c1d"
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.login = self.client_id
        conn.password = "secret"
        conn.extra = json.dumps(
            {
                "host": HOST,
                "azure_tenant_id": self.tenant_id,
                "azure_ad_endpoint": self.ad_endpoint,
            }
        )
        session.commit()
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.side_effect = [
            create_successful_response_mock(create_aad_token_for_resource(DEFAULT_DATABRICKS_SCOPE)),
            create_successful_response_mock({"run_id": "1"}),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {"notebook_task": NOTEBOOK_TASK, "new_cluster": NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        ad_call_args = mock_requests.method_calls[0]
        assert ad_call_args[1][0] == AZURE_TOKEN_SERVICE_URL.format(self.ad_endpoint, self.tenant_id)
        assert ad_call_args[2]["data"]["client_id"] == self.client_id
        assert ad_call_args[2]["data"]["resource"] == DEFAULT_DATABRICKS_SCOPE

        assert run_id == "1"
        args = mock_requests.post.call_args
        kwargs = args[1]
        assert kwargs["auth"].token == TOKEN


class TestDatabricksHookAadTokenSpOutside:
    """
    Tests for DatabricksHook when auth is done with AAD token for SP outside of workspace.
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        self.tenant_id = "3ff810a6-5504-4ab8-85cb-cd0e6f879c1d"
        self.client_id = "9ff815a6-4404-4ab8-85cb-cd0e6f879c1d"
        conn.login = self.client_id
        conn.password = "secret"
        conn.host = HOST
        conn.extra = json.dumps(
            {
                "azure_resource_id": "/Some/resource",
                "azure_tenant_id": "3ff810a6-5504-4ab8-85cb-cd0e6f879c1d",
            }
        )
        session.commit()
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.post.side_effect = [
            create_successful_response_mock(create_aad_token_for_resource(AZURE_MANAGEMENT_ENDPOINT)),
            create_successful_response_mock(create_aad_token_for_resource(DEFAULT_DATABRICKS_SCOPE)),
            create_successful_response_mock({"run_id": "1"}),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {"notebook_task": NOTEBOOK_TASK, "new_cluster": NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        ad_call_args = mock_requests.method_calls[0]
        assert ad_call_args[1][0] == AZURE_TOKEN_SERVICE_URL.format(AZURE_DEFAULT_AD_ENDPOINT, self.tenant_id)
        assert ad_call_args[2]["data"]["client_id"] == self.client_id
        assert ad_call_args[2]["data"]["resource"] == AZURE_MANAGEMENT_ENDPOINT

        ad_call_args = mock_requests.method_calls[1]
        assert ad_call_args[1][0] == AZURE_TOKEN_SERVICE_URL.format(AZURE_DEFAULT_AD_ENDPOINT, self.tenant_id)
        assert ad_call_args[2]["data"]["client_id"] == self.client_id
        assert ad_call_args[2]["data"]["resource"] == DEFAULT_DATABRICKS_SCOPE

        assert run_id == "1"
        args = mock_requests.post.call_args
        kwargs = args[1]
        assert kwargs["auth"].token == TOKEN
        assert kwargs["headers"]["X-Databricks-Azure-Workspace-Resource-Id"] == "/Some/resource"
        assert kwargs["headers"]["X-Databricks-Azure-SP-Management-Token"] == TOKEN


class TestDatabricksHookAadTokenManagedIdentity:
    """
    Tests for DatabricksHook when auth is done with AAD leveraging Managed Identity authentication
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.extra = json.dumps(
            {
                "use_azure_managed_identity": True,
            }
        )
        session.commit()
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests")
    def test_submit_run(self, mock_requests):
        mock_requests.codes.ok = 200
        mock_requests.get.side_effect = [
            create_successful_response_mock({"compute": {"azEnvironment": "AZUREPUBLICCLOUD"}}),
            create_successful_response_mock(create_aad_token_for_resource(DEFAULT_DATABRICKS_SCOPE)),
        ]
        mock_requests.post.side_effect = [
            create_successful_response_mock({"run_id": "1"}),
        ]
        status_code_mock = mock.PropertyMock(return_value=200)
        type(mock_requests.post.return_value).status_code = status_code_mock
        data = {"notebook_task": NOTEBOOK_TASK, "new_cluster": NEW_CLUSTER}
        run_id = self.hook.submit_run(data)

        ad_call_args = mock_requests.method_calls[0]
        assert ad_call_args[1][0] == AZURE_METADATA_SERVICE_INSTANCE_URL
        assert ad_call_args[2]["params"]["api-version"] > "2018-02-01"
        assert ad_call_args[2]["headers"]["Metadata"] == "true"

        assert run_id == "1"
        args = mock_requests.post.call_args
        kwargs = args[1]
        assert kwargs["auth"].token == TOKEN


class TestDatabricksHookAsyncMethods:
    """
    Tests for async functionality of DatabricksHook.
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.login = LOGIN
        conn.password = PASSWORD
        conn.extra = None
        session.commit()

        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @pytest.mark.asyncio
    async def test_init_async_session(self):
        async with self.hook:
            assert isinstance(self.hook._session, aiohttp.ClientSession)
        assert self.hook._session is None

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    async def test_do_api_call_retries_with_retryable_error(self, mock_get):
        mock_get.side_effect = aiohttp.ClientResponseError(None, None, status=500)
        with mock.patch.object(self.hook.log, "error") as mock_errors:
            async with self.hook:
                with pytest.raises(AirflowException):
                    await self.hook._a_do_api_call(GET_RUN_ENDPOINT, {})
                assert mock_errors.call_count == DEFAULT_RETRY_NUMBER

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    async def test_do_api_call_does_not_retry_with_non_retryable_error(self, mock_get):
        mock_get.side_effect = aiohttp.ClientResponseError(None, None, status=400)
        with mock.patch.object(self.hook.log, "error") as mock_errors:
            async with self.hook:
                with pytest.raises(AirflowException):
                    await self.hook._a_do_api_call(GET_RUN_ENDPOINT, {})
                mock_errors.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    async def test_do_api_call_succeeds_after_retrying(self, mock_get):
        mock_get.side_effect = [
            aiohttp.ClientResponseError(None, None, status=500),
            create_valid_response_mock({"run_id": "1"}),
        ]
        with mock.patch.object(self.hook.log, "error") as mock_errors:
            async with self.hook:
                response = await self.hook._a_do_api_call(GET_RUN_ENDPOINT, {})
                assert mock_errors.call_count == 1
                assert response == {"run_id": "1"}

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    async def test_do_api_call_waits_between_retries(self, mock_get):
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

        mock_get.side_effect = aiohttp.ClientResponseError(None, None, status=500)
        with mock.patch.object(self.hook.log, "error") as mock_errors:
            async with self.hook:
                with pytest.raises(AirflowException):
                    await self.hook._a_do_api_call(GET_RUN_ENDPOINT, {})
                assert mock_errors.call_count == DEFAULT_RETRY_NUMBER

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.patch")
    async def test_do_api_call_patch(self, mock_patch):
        mock_patch.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={"cluster_name": "new_name"}
        )
        data = {"cluster_name": "new_name"}
        async with self.hook:
            patched_cluster_name = await self.hook._a_do_api_call(("PATCH", "api/2.1/jobs/runs/submit"), data)

        assert patched_cluster_name["cluster_name"] == "new_name"
        mock_patch.assert_called_once_with(
            submit_run_endpoint(HOST),
            json={"cluster_name": "new_name"},
            auth=aiohttp.BasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    async def test_get_run_page_url(self, mock_get):
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value=GET_RUN_RESPONSE)
        async with self.hook:
            run_page_url = await self.hook.a_get_run_page_url(RUN_ID)

        assert run_page_url == RUN_PAGE_URL
        mock_get.assert_called_once_with(
            get_run_endpoint(HOST),
            json={"run_id": RUN_ID},
            auth=aiohttp.BasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    async def test_get_run_state(self, mock_get):
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value=GET_RUN_RESPONSE)

        async with self.hook:
            run_state = await self.hook.a_get_run_state(RUN_ID)

        assert run_state == RunState(LIFE_CYCLE_STATE, RESULT_STATE, STATE_MESSAGE)
        mock_get.assert_called_once_with(
            get_run_endpoint(HOST),
            json={"run_id": RUN_ID},
            auth=aiohttp.BasicAuth(LOGIN, PASSWORD),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )


class TestDatabricksHookAsyncAadToken:
    """
    Tests for DatabricksHook using async methods when
    auth is done with AAD token for SP as user inside workspace.
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.login = "9ff815a6-4404-4ab8-85cb-cd0e6f879c1d"
        conn.password = "secret"
        conn.extra = json.dumps(
            {
                "host": HOST,
                "azure_tenant_id": "3ff810a6-5504-4ab8-85cb-cd0e6f879c1d",
            }
        )
        session.commit()
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.post")
    async def test_get_run_state(self, mock_post, mock_get):
        mock_post.return_value.__aenter__.return_value.json = AsyncMock(
            return_value=create_aad_token_for_resource(DEFAULT_DATABRICKS_SCOPE)
        )
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value=GET_RUN_RESPONSE)

        async with self.hook:
            run_state = await self.hook.a_get_run_state(RUN_ID)

        assert run_state == RunState(LIFE_CYCLE_STATE, RESULT_STATE, STATE_MESSAGE)
        mock_get.assert_called_once_with(
            get_run_endpoint(HOST),
            json={"run_id": RUN_ID},
            auth=BearerAuth(TOKEN),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )


class TestDatabricksHookAsyncAadTokenOtherClouds:
    """
    Tests for DatabricksHook using async methodswhen auth is done with AAD token
    for SP as user inside workspace and using non-global Azure cloud (China, GovCloud, Germany)
    """

    @provide_session
    def setup_method(self, method, session=None):
        self.tenant_id = "3ff810a6-5504-4ab8-85cb-cd0e6f879c1d"
        self.ad_endpoint = "https://login.microsoftonline.de"
        self.client_id = "9ff815a6-4404-4ab8-85cb-cd0e6f879c1d"
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.login = self.client_id
        conn.password = "secret"
        conn.extra = json.dumps(
            {
                "host": HOST,
                "azure_tenant_id": self.tenant_id,
                "azure_ad_endpoint": self.ad_endpoint,
            }
        )
        session.commit()
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.post")
    async def test_get_run_state(self, mock_post, mock_get):
        mock_post.return_value.__aenter__.return_value.json = AsyncMock(
            return_value=create_aad_token_for_resource(DEFAULT_DATABRICKS_SCOPE)
        )
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value=GET_RUN_RESPONSE)

        async with self.hook:
            run_state = await self.hook.a_get_run_state(RUN_ID)

        assert run_state == RunState(LIFE_CYCLE_STATE, RESULT_STATE, STATE_MESSAGE)

        ad_call_args = mock_post.call_args_list[0]
        assert ad_call_args[1]["url"] == AZURE_TOKEN_SERVICE_URL.format(self.ad_endpoint, self.tenant_id)
        assert ad_call_args[1]["data"]["client_id"] == self.client_id
        assert ad_call_args[1]["data"]["resource"] == DEFAULT_DATABRICKS_SCOPE

        mock_get.assert_called_once_with(
            get_run_endpoint(HOST),
            json={"run_id": RUN_ID},
            auth=BearerAuth(TOKEN),
            headers=self.hook.user_agent_header,
            timeout=self.hook.timeout_seconds,
        )


class TestDatabricksHookAsyncAadTokenSpOutside:
    """
    Tests for DatabricksHook using async methods when auth is done with AAD token for SP outside of workspace.
    """

    @provide_session
    def setup_method(self, method, session=None):
        self.tenant_id = "3ff810a6-5504-4ab8-85cb-cd0e6f879c1d"
        self.client_id = "9ff815a6-4404-4ab8-85cb-cd0e6f879c1d"
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.login = self.client_id
        conn.password = "secret"
        conn.host = HOST
        conn.extra = json.dumps(
            {
                "azure_resource_id": "/Some/resource",
                "azure_tenant_id": self.tenant_id,
            }
        )
        session.commit()
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.post")
    async def test_get_run_state(self, mock_post, mock_get):
        mock_post.return_value.__aenter__.return_value.json.side_effect = AsyncMock(
            side_effect=[
                create_aad_token_for_resource(AZURE_MANAGEMENT_ENDPOINT),
                create_aad_token_for_resource(DEFAULT_DATABRICKS_SCOPE),
            ]
        )
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value=GET_RUN_RESPONSE)

        async with self.hook:
            run_state = await self.hook.a_get_run_state(RUN_ID)

        assert run_state == RunState(LIFE_CYCLE_STATE, RESULT_STATE, STATE_MESSAGE)

        ad_call_args = mock_post.call_args_list[0]
        assert ad_call_args[1]["url"] == AZURE_TOKEN_SERVICE_URL.format(
            AZURE_DEFAULT_AD_ENDPOINT, self.tenant_id
        )
        assert ad_call_args[1]["data"]["client_id"] == self.client_id
        assert ad_call_args[1]["data"]["resource"] == AZURE_MANAGEMENT_ENDPOINT

        ad_call_args = mock_post.call_args_list[1]
        assert ad_call_args[1]["url"] == AZURE_TOKEN_SERVICE_URL.format(
            AZURE_DEFAULT_AD_ENDPOINT, self.tenant_id
        )
        assert ad_call_args[1]["data"]["client_id"] == self.client_id
        assert ad_call_args[1]["data"]["resource"] == DEFAULT_DATABRICKS_SCOPE

        mock_get.assert_called_once_with(
            get_run_endpoint(HOST),
            json={"run_id": RUN_ID},
            auth=BearerAuth(TOKEN),
            headers={
                **self.hook.user_agent_header,
                "X-Databricks-Azure-Workspace-Resource-Id": "/Some/resource",
                "X-Databricks-Azure-SP-Management-Token": TOKEN,
            },
            timeout=self.hook.timeout_seconds,
        )


class TestDatabricksHookAsyncAadTokenManagedIdentity:
    """
    Tests for DatabricksHook using async methods when
    auth is done with AAD leveraging Managed Identity authentication
    """

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.extra = json.dumps(
            {
                "use_azure_managed_identity": True,
            }
        )
        session.commit()
        session.commit()
        self.hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession.get")
    async def test_get_run_state(self, mock_get):
        mock_get.return_value.__aenter__.return_value.json.side_effect = AsyncMock(
            side_effect=[
                {"compute": {"azEnvironment": "AZUREPUBLICCLOUD"}},
                create_aad_token_for_resource(DEFAULT_DATABRICKS_SCOPE),
                GET_RUN_RESPONSE,
            ]
        )

        async with self.hook:
            run_state = await self.hook.a_get_run_state(RUN_ID)

        assert run_state == RunState(LIFE_CYCLE_STATE, RESULT_STATE, STATE_MESSAGE)

        ad_call_args = mock_get.call_args_list[0]
        assert ad_call_args[1]["url"] == AZURE_METADATA_SERVICE_INSTANCE_URL
        assert ad_call_args[1]["params"]["api-version"] > "2018-02-01"
        assert ad_call_args[1]["headers"]["Metadata"] == "true"
