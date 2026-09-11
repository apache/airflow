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

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessGetSessionEndpointOperator,
    EmrServerlessStartSessionOperator,
    EmrServerlessStopSessionOperator,
)
from airflow.providers.amazon.aws.triggers.emr import (
    EmrServerlessSessionTrigger,
    EmrServerlessStopSessionTrigger,
)

APP_ID = "app-123"
SESSION_ID = "sess-abc"
ROLE = "arn:aws:iam::111122223333:role/emr-exec"
WAIT = "airflow.providers.amazon.aws.operators.emr.wait"


class TestEmrServerlessStartSessionOperator:
    @mock.patch(WAIT)
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "start_session")
    def test_start_and_wait(self, start_session, get_waiter, wait_mock):
        start_session.return_value = SESSION_ID
        op = EmrServerlessStartSessionOperator(
            task_id="start",
            application_id=APP_ID,
            execution_role_arn=ROLE,
            idle_timeout_minutes=15,
        )
        result = op.execute({})

        start_session.assert_called_once_with(
            application_id=APP_ID,
            execution_role_arn=ROLE,
            name=None,
            idle_timeout_minutes=15,
            configuration_overrides=None,
        )
        wait_mock.assert_called_once()
        get_waiter.assert_called_once_with("serverless_session_ready")
        assert result == {"application_id": APP_ID, "session_id": SESSION_ID}

    @mock.patch(WAIT)
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "start_session")
    def test_no_wait(self, start_session, get_waiter, wait_mock):
        start_session.return_value = SESSION_ID
        op = EmrServerlessStartSessionOperator(
            task_id="start",
            application_id=APP_ID,
            execution_role_arn=ROLE,
            wait_for_completion=False,
        )
        op.execute({})
        wait_mock.assert_not_called()

    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "start_session")
    def test_deferrable_defers(self, start_session, get_waiter):
        start_session.return_value = SESSION_ID
        op = EmrServerlessStartSessionOperator(
            task_id="start",
            application_id=APP_ID,
            execution_role_arn=ROLE,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as deferred:
            op.execute({})

        assert isinstance(deferred.value.trigger, EmrServerlessSessionTrigger)
        get_waiter.assert_not_called()

    def test_execute_complete_success(self):
        op = EmrServerlessStartSessionOperator(
            task_id="start", application_id=APP_ID, execution_role_arn=ROLE
        )
        result = op.execute_complete({}, {"status": "success", "session_id": SESSION_ID})
        assert result == {"application_id": APP_ID, "session_id": SESSION_ID}

    def test_execute_complete_failure_raises(self):
        op = EmrServerlessStartSessionOperator(
            task_id="start", application_id=APP_ID, execution_role_arn=ROLE
        )
        with pytest.raises(RuntimeError):
            op.execute_complete({}, {"status": "failure", "session_id": SESSION_ID})


class TestEmrServerlessGetSessionEndpointOperator:
    RAW_RESPONSE = {
        "applicationId": APP_ID,
        "sessionId": SESSION_ID,
        "endpoint": "https://sess-abc.s.emr-serverless-services.us-east-1.amazonaws.com",
        "authToken": "tok",
        "authTokenExpiresAt": None,
    }

    @mock.patch.object(EmrServerlessHook, "get_session_endpoint")
    def test_returns_transformed_endpoint(self, get_session_endpoint):
        get_session_endpoint.return_value = self.RAW_RESPONSE
        op = EmrServerlessGetSessionEndpointOperator(
            task_id="ep", application_id=APP_ID, session_id=SESSION_ID
        )
        out = op.execute({})
        get_session_endpoint.assert_called_once_with(APP_ID, SESSION_ID)
        assert out == {
            "endpoint": "https://sess-abc.s.emr-serverless-services.us-east-1.amazonaws.com",
            "auth_token": "tok",
            "auth_token_expires_at": None,
        }

    @mock.patch("airflow.providers.amazon.aws.operators.emr.mask_secret")
    @mock.patch.object(EmrServerlessHook, "get_session_endpoint")
    def test_auth_token_is_masked(self, get_session_endpoint, mask_secret_mock):
        get_session_endpoint.return_value = self.RAW_RESPONSE
        op = EmrServerlessGetSessionEndpointOperator(
            task_id="ep", application_id=APP_ID, session_id=SESSION_ID
        )
        op.execute({})
        mask_secret_mock.assert_called_once_with("tok")


class TestEmrServerlessStopSessionOperator:
    @mock.patch(WAIT)
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "terminate_session")
    def test_terminate_and_wait(self, terminate_session, get_waiter, wait_mock):
        op = EmrServerlessStopSessionOperator(task_id="stop", application_id=APP_ID, session_id=SESSION_ID)
        op.execute({})

        terminate_session.assert_called_once_with(APP_ID, SESSION_ID)
        wait_mock.assert_called_once()
        get_waiter.assert_called_once_with("serverless_session_terminated")

    @mock.patch(WAIT)
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "terminate_session")
    def test_terminate_no_wait(self, terminate_session, get_waiter, wait_mock):
        op = EmrServerlessStopSessionOperator(
            task_id="stop",
            application_id=APP_ID,
            session_id=SESSION_ID,
            wait_for_completion=False,
        )
        op.execute({})
        wait_mock.assert_not_called()

    @mock.patch(WAIT)
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "terminate_session")
    def test_deferrable_defers(self, terminate_session, get_waiter, wait_mock):
        op = EmrServerlessStopSessionOperator(
            task_id="stop",
            application_id=APP_ID,
            session_id=SESSION_ID,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as deferred:
            op.execute({})

        terminate_session.assert_called_once_with(APP_ID, SESSION_ID)
        assert isinstance(deferred.value.trigger, EmrServerlessStopSessionTrigger)
        wait_mock.assert_not_called()

    def test_execute_complete_success(self):
        op = EmrServerlessStopSessionOperator(task_id="stop", application_id=APP_ID, session_id=SESSION_ID)
        assert op.execute_complete({}, {"status": "success", "session_id": SESSION_ID}) is None

    def test_execute_complete_failure_raises(self):
        op = EmrServerlessStopSessionOperator(task_id="stop", application_id=APP_ID, session_id=SESSION_ID)
        with pytest.raises(RuntimeError):
            op.execute_complete({}, {"status": "failure", "session_id": SESSION_ID})
