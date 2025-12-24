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

from unittest import mock

import pytest
from airbyte_api.api import CancelJobRequest, GetJobRequest
from airbyte_api.models import JobResponse, JobStatusEnum, JobTypeEnum

from airflow.models import Connection
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.providers.common.compat.sdk import AirflowException


class TestAirbyteHook:
    """
    Test all functions from Airbyte Hook
    """

    conn_type = "airbyte"
    airbyte_conn_id = "airbyte_conn_id_test"
    airbyte_conn_id_with_proxy = "airbyte_conn_id_test_with_proxy"
    connection_id = "conn_test_sync"
    job_id = 1
    host = "http://test-airbyte:8000/public/v1/api/"
    port = 8001
    sync_connection_endpoint = "http://test-airbyte:8001/api/v1/connections/sync"
    get_job_endpoint = "http://test-airbyte:8001/api/v1/jobs/get"
    cancel_job_endpoint = "http://test-airbyte:8001/api/v1/jobs/cancel"

    health_endpoint = "http://test-airbyte:8001/api/v1/health"
    _mock_proxy = {"proxies": {"http": "http://proxy:8080", "https": "https://proxy:8080"}}
    _mock_sync_conn_success_response_body = {"job": {"id": 1}}
    _mock_job_status_success_response_body = {"job": {"status": "succeeded"}}
    _mock_job_cancel_status = "cancelled"

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=self.airbyte_conn_id,
                conn_type=self.conn_type,
                host=self.host,
                port=self.port,
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=self.airbyte_conn_id_with_proxy,
                conn_type=self.conn_type,
                host=self.host,
                port=self.port,
                extra=self._mock_proxy,
            )
        )
        self.hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id)
        self.hook_with_proxy = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id_with_proxy)

    def return_value_get_job(self, status):
        response = mock.Mock()
        response.job_response = JobResponse(
            connection_id="connection-mock",
            job_id=self.job_id,
            start_time="today",
            job_type=JobTypeEnum.SYNC,
            status=status,
        )
        return response

    @mock.patch("airbyte_api.jobs.Jobs.create_job")
    def test_submit_sync_connection(self, create_job_mock):
        mock_response = mock.Mock()
        mock_response.job_response = self._mock_sync_conn_success_response_body
        create_job_mock.return_value = mock_response

        resp = self.hook.submit_sync_connection(connection_id=self.connection_id)
        assert resp == self._mock_sync_conn_success_response_body

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_get_job_status(self, get_job_mock):
        mock_response = mock.AsyncMock()
        mock_response.job_response = JobResponse(
            connection_id="connection-mock",
            job_id=1,
            start_time="today",
            job_type=JobTypeEnum.SYNC,
            status=JobStatusEnum.RUNNING,
        )
        get_job_mock.return_value = mock_response
        resp = self.hook.get_job_status(job_id=self.job_id)
        assert resp == JobStatusEnum.RUNNING

    @mock.patch("airbyte_api.jobs.Jobs.cancel_job")
    def test_cancel_job(self, cancel_job_mock):
        mock_response = mock.Mock()
        mock_response.job_response = JobResponse(
            connection_id="connection-mock",
            job_id=1,
            start_time="today",
            job_type=JobTypeEnum.SYNC,
            status=JobStatusEnum.CANCELLED,
        )
        cancel_job_mock.return_value = mock_response

        resp = self.hook.cancel_job(job_id=self.job_id)
        assert resp.status == JobStatusEnum.CANCELLED

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_wait_for_job_succeeded(self, mock_get_job):
        mock_get_job.side_effect = [self.return_value_get_job(JobStatusEnum.SUCCEEDED)]
        self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)
        mock_get_job.assert_called_once_with(request=GetJobRequest(self.job_id))

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_wait_for_job_error(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.RUNNING),
            self.return_value_get_job(JobStatusEnum.FAILED),
        ]
        with pytest.raises(AirflowException, match="Job failed"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(request=GetJobRequest(self.job_id)), mock.call(request=GetJobRequest(self.job_id))]
        mock_get_job.assert_has_calls(calls)

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_wait_for_job_incomplete_succeeded(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.INCOMPLETE),
            self.return_value_get_job(JobStatusEnum.SUCCEEDED),
        ]
        self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(request=GetJobRequest(self.job_id)), mock.call(request=GetJobRequest(self.job_id))]
        mock_get_job.assert_has_calls(calls)

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    @mock.patch("airbyte_api.jobs.Jobs.cancel_job")
    def test_wait_for_job_timeout(self, mock_cancel_job, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.PENDING),
            self.return_value_get_job(JobStatusEnum.RUNNING),
        ]
        with pytest.raises(AirflowException, match="Timeout"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=2, timeout=1)

        get_calls = [
            mock.call(request=GetJobRequest(self.job_id)),
        ]
        cancel_calls = [mock.call(request=CancelJobRequest(self.job_id))]
        mock_get_job.assert_has_calls(get_calls)
        mock_cancel_job.assert_has_calls(cancel_calls)
        assert mock_get_job.mock_calls == get_calls
        assert mock_cancel_job.mock_calls == cancel_calls

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_wait_for_job_state_unrecognized(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.RUNNING),
            self.return_value_get_job("UNRECOGNIZED"),
        ]
        with pytest.raises(AirflowException, match="unexpected state"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(request=GetJobRequest(self.job_id)), mock.call(request=GetJobRequest(self.job_id))]
        mock_get_job.assert_has_calls(calls)

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_wait_for_job_cancelled(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.RUNNING),
            self.return_value_get_job(JobStatusEnum.CANCELLED),
        ]
        with pytest.raises(AirflowException, match="Job was cancelled"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(request=GetJobRequest(self.job_id)), mock.call(request=GetJobRequest(self.job_id))]
        mock_get_job.assert_has_calls(calls)

    @mock.patch("airbyte_api.health.Health.get_health_check")
    def test_connection_success(self, mock_get_health_check):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_get_health_check.return_value = mock_response

        status, msg = self.hook.test_connection()
        assert status is True
        assert msg == "Connection successfully tested"

    @mock.patch("airbyte_api.health.Health.get_health_check")
    def test_connection_failure(self, mock_get_health_check):
        mock_response = mock.Mock()
        mock_response.status_code = 502
        mock_response.raw_response = '{"message": "internal server error"}'
        mock_get_health_check.return_value = mock_response

        status, msg = self.hook.test_connection()
        assert status is False
        assert msg == '{"message": "internal server error"}'

    def test_create_api_session_with_proxy(self):
        """
        Test the creation of the API session with proxy settings.
        """
        # Create a new AirbyteHook instance
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id_with_proxy)

        # Check if the session is created correctly
        assert hook.airbyte_api is not None
        assert hook.airbyte_api.sdk_configuration.client.proxies == self._mock_proxy["proxies"]

    def test_get_ui_field_behaviour(self):
        """
        Test the UI field behavior configuration for Airbyte connections.
        """
        assert AirbyteHook.get_ui_field_behaviour() == {
            "hidden_fields": ["extra", "port"],
            "relabeling": {
                "host": "Server URL",
                "login": "Client ID",
                "password": "Client Secret",
                "schema": "Token URL",
            },
            "placeholders": {},
        }
