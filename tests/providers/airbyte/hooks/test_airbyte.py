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
from airbyte_api.models import JobStatusEnum

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.utils import db

# those tests will not work with database isolation because they mock requests
pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestAirbyteHook:
    """
    Test all functions from Airbyte Hook
    """

    airbyte_conn_id = "airbyte_conn_id_test"
    connection_id = "conn_test_sync"
    job_id = 1
    sync_connection_endpoint = "http://test-airbyte:8001/api/v1/connections/sync"
    get_job_endpoint = "http://test-airbyte:8001/api/v1/jobs/get"
    cancel_job_endpoint = "http://test-airbyte:8001/api/v1/jobs/cancel"

    health_endpoint = "http://test-airbyte:8001/api/v1/health"
    _mock_sync_conn_success_response_body = {"job": {"id": 1}}
    _mock_job_status_success_response_body = {"job": {"status": "succeeded"}}
    _mock_job_cancel_status = "cancelled"

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="airbyte_conn_id_test", conn_type="airbyte", host="http://test-airbyte", port=8001
            )
        )
        self.hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id)

    def return_value_get_job(self, status):
        response = mock.Mock()
        response.json.return_value = {"job": {"status": status}}
        return response

    @mock.patch("airbyte_api.jobs.Jobs.create_job")
    def test_submit_sync_connection(self, create_job_mock):
        mock_response = mock.Mock()
        mock_response.job_response = self._mock_sync_conn_success_response_body
        create_job_mock.return_value = mock_response

        resp = self.hook.submit_sync_connection(connection_id=self.connection_id)
        assert resp == self._mock_sync_conn_success_response_body

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "host, port, schema, expected_base_url, description",
        [
            ("test-airbyte", 8001, "http", "http://test-airbyte:8001", "uri_with_port_and_schema"),
            ("test-airbyte", None, "https", "https://test-airbyte", "uri_with_schema"),
            ("test-airbyte", None, None, "http://test-airbyte", "uri_without_port_and_schema"),
        ],
    )
    async def test_get_base_url(self, host, port, schema, expected_base_url, description):
        conn_id = f"test_conn_{description}"
        conn = Connection(conn_id=conn_id, conn_type="airbyte", host=host, port=port, schema=schema)
        hook = AirbyteHook(airbyte_conn_id=conn_id)
        db.merge_conn(conn)
        _, base_url = await hook.get_headers_tenants_from_connection()
        assert base_url == expected_base_url

    def test_get_job_status(self, requests_mock):
        requests_mock.post(
            self.get_job_endpoint, status_code=200, json=self._mock_job_status_success_response_body
        )
        resp = self.hook.get_job_status(job_id=self.job_id)
        assert resp.status_code == 200
        assert resp.json() == self._mock_job_status_success_response_body

    def test_cancel_job(self, requests_mock):
        requests_mock.post(
            self.cancel_job_endpoint, status_code=200, json=self._mock_job_status_success_response_body
        )
        resp = self.hook.cancel_job(job_id=self.job_id)
        assert resp.status_code == 200

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_details")
    def test_wait_for_job_succeeded(self, mock_get_job):
        mock_get_job.side_effect = [self.return_value_get_job(JobStatusEnum.SUCCEEDED)]
        self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)
        mock_get_job.assert_called_once_with(job_id=self.job_id)

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_details")
    def test_wait_for_job_error(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.RUNNING),
            self.return_value_get_job(JobStatusEnum.FAILED),
        ]
        with pytest.raises(AirflowException, match="Job failed"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(job_id=self.job_id), mock.call(job_id=self.job_id)]
        mock_get_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_details")
    def test_wait_for_job_incomplete_succeeded(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.INCOMPLETE),
            self.return_value_get_job(JobStatusEnum.SUCCEEDED),
        ]
        self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(job_id=self.job_id), mock.call(job_id=self.job_id)]
        mock_get_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_details")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.cancel_job")
    def test_wait_for_job_timeout(self, mock_cancel_job, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.PENDING),
            self.return_value_get_job(JobStatusEnum.RUNNING),
        ]
        with pytest.raises(AirflowException, match="Timeout"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=2, timeout=1)

        calls = [mock.call(job_id=self.job_id)]
        mock_get_job.assert_has_calls(calls)
        mock_cancel_job.assert_has_calls(calls)
        assert mock_get_job.mock_calls == calls
        assert mock_cancel_job.mock_calls == calls

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_details")
    def test_wait_for_job_state_unrecognized(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.RUNNING),
            self.return_value_get_job("UNRECOGNIZED"),
        ]
        with pytest.raises(AirflowException, match="unexpected state"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(job_id=self.job_id), mock.call(job_id=self.job_id)]
        mock_get_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_details")
    def test_wait_for_job_cancelled(self, mock_get_job):
        mock_get_job.side_effect = [
            self.return_value_get_job(JobStatusEnum.RUNNING),
            self.return_value_get_job(JobStatusEnum.CANCELLED),
        ]
        with pytest.raises(AirflowException, match="Job was cancelled"):
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=0)

        calls = [mock.call(job_id=self.job_id), mock.call(job_id=self.job_id)]
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
