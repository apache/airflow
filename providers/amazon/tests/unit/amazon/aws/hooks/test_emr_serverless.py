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

from unittest.mock import MagicMock, PropertyMock, patch

from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook

task_id = "test_emr_serverless_create_application_operator"
application_id = "test_application_id"
release_label = "test"
job_type = "test"
client_request_token = "eac427d0-1c6d4df=-96aa-32423412"
config = {"name": "test_application_emr_serverless"}


class TestEmrServerlessHook:
    def test_conn_attribute(self):
        hook = EmrServerlessHook(aws_conn_id="aws_default")
        assert hasattr(hook, "conn")
        # Testing conn is a cached property
        conn = hook.conn
        conn2 = hook.conn
        assert conn is conn2

    @patch.object(EmrServerlessHook, "conn", new_callable=PropertyMock)
    def test_cancel_jobs(self, conn_mock: MagicMock):
        conn_mock().get_paginator().paginate.return_value = [{"jobRuns": [{"id": "job1"}, {"id": "job2"}]}]
        hook = EmrServerlessHook(aws_conn_id="aws_default")
        waiter_mock = MagicMock()
        hook.get_waiter = waiter_mock  # type:ignore[method-assign]

        hook.cancel_running_jobs("app")

        assert conn_mock().cancel_job_run.call_count == 2
        conn_mock().cancel_job_run.assert_any_call(applicationId="app", jobRunId="job1")
        conn_mock().cancel_job_run.assert_any_call(applicationId="app", jobRunId="job2")
        waiter_mock.assert_called_with("no_job_running")

    @patch.object(EmrServerlessHook, "conn", new_callable=PropertyMock)
    def test_cancel_jobs_several_calls(self, conn_mock: MagicMock):
        conn_mock().get_paginator().paginate.return_value = [
            {"jobRuns": [{"id": "job1"}, {"id": "job2"}]},
            {"jobRuns": [{"id": "job3"}, {"id": "job4"}]},
        ]
        hook = EmrServerlessHook(aws_conn_id="aws_default")
        waiter_mock = MagicMock()
        hook.get_waiter = waiter_mock  # type:ignore[method-assign]

        hook.cancel_running_jobs("app")

        assert conn_mock().cancel_job_run.call_count == 4
        waiter_mock.assert_called_once()  # we should wait once for all jobs, not once per page

    @patch.object(EmrServerlessHook, "conn", new_callable=PropertyMock)
    def test_cancel_jobs_but_no_jobs(self, conn_mock: MagicMock):
        conn_mock.return_value.list_job_runs.return_value = {"jobRuns": []}
        hook = EmrServerlessHook(aws_conn_id="aws_default")

        hook.cancel_running_jobs("app")

        # nothing very interesting should happen
        conn_mock.assert_called_once()


class TestEmrServerlessHookSession:
    @patch.object(EmrServerlessHook, "conn", new_callable=PropertyMock)
    def test_start_session_minimal(self, conn_mock: MagicMock):
        conn_mock().start_session.return_value = {"sessionId": "sess-1"}
        hook = EmrServerlessHook(aws_conn_id="aws_default")

        session_id = hook.start_session(application_id="app", execution_role_arn="role")

        assert session_id == "sess-1"
        conn_mock().start_session.assert_called_once_with(applicationId="app", executionRoleArn="role")

    @patch.object(EmrServerlessHook, "conn", new_callable=PropertyMock)
    def test_start_session_with_optional_params(self, conn_mock: MagicMock):
        conn_mock().start_session.return_value = {"sessionId": "sess-2"}
        hook = EmrServerlessHook(aws_conn_id="aws_default")

        session_id = hook.start_session(
            application_id="app",
            execution_role_arn="role",
            name="my-session",
            idle_timeout_minutes=15,
            configuration_overrides={"applicationConfiguration": []},
        )

        assert session_id == "sess-2"
        conn_mock().start_session.assert_called_once_with(
            applicationId="app",
            executionRoleArn="role",
            name="my-session",
            idleTimeoutMinutes=15,
            configurationOverrides={"applicationConfiguration": []},
        )

    @patch.object(EmrServerlessHook, "conn", new_callable=PropertyMock)
    def test_get_session_state(self, conn_mock: MagicMock):
        conn_mock().get_session.return_value = {"session": {"state": "STARTED"}}
        hook = EmrServerlessHook(aws_conn_id="aws_default")

        assert hook.get_session_state("app", "sess-1") == "STARTED"
        conn_mock().get_session.assert_called_once_with(applicationId="app", sessionId="sess-1")

    @patch.object(EmrServerlessHook, "conn", new_callable=PropertyMock)
    def test_get_session_endpoint_returns_raw_response(self, conn_mock: MagicMock):
        raw = {
            "applicationId": "app",
            "sessionId": "sess-1",
            "endpoint": "https://sess-1.example.amazonaws.com",
            "authToken": "secret-token",
            "authTokenExpiresAt": "2026-01-01T00:00:00Z",
        }
        conn_mock().get_session_endpoint.return_value = raw
        hook = EmrServerlessHook(aws_conn_id="aws_default")

        result = hook.get_session_endpoint("app", "sess-1")

        assert result is raw
        conn_mock().get_session_endpoint.assert_called_once_with(applicationId="app", sessionId="sess-1")

    @patch.object(EmrServerlessHook, "conn", new_callable=PropertyMock)
    def test_terminate_session(self, conn_mock: MagicMock):
        hook = EmrServerlessHook(aws_conn_id="aws_default")

        hook.terminate_session("app", "sess-1")

        conn_mock().terminate_session.assert_called_once_with(applicationId="app", sessionId="sess-1")
