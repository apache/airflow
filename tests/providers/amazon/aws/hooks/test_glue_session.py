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
from typing import TYPE_CHECKING
from unittest import mock

import boto3
import pytest
from moto import mock_aws

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.glue_session import GlueSessionHook

if TYPE_CHECKING:
    from unittest.mock import MagicMock


class TestGlueSessionHook:
    def setup_method(self):
        self.some_aws_region = "us-west-2"

    @mock_aws
    @pytest.mark.parametrize("role_path", ["/", "/custom-path/"])
    def test_get_iam_execution_role(self, role_path):
        expected_role = "my_test_role"
        boto3.client("iam").create_role(
            Path=role_path,
            RoleName=expected_role,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    },
                }
            ),
        )

        hook = GlueSessionHook(
            aws_conn_id=None,
            session_id="aws_test_glue_session",
            iam_role_name=expected_role,
        )
        iam_role = hook.get_iam_execution_role()
        assert iam_role is not None
        assert "Role" in iam_role
        assert "Arn" in iam_role["Role"]
        assert iam_role["Role"]["Arn"] == f"arn:aws:iam::123456789012:role{role_path}{expected_role}"

    @mock.patch.object(GlueSessionHook, "get_iam_execution_role")
    @mock.patch.object(GlueSessionHook, "conn")
    def test_init_iam_role_value_error(self, mock_conn, mock_get_iam_execution_role):
        mock_get_iam_execution_role.return_value = mock.MagicMock(
            Role={"RoleName": "my_test_role_name", "RoleArn": "my_test_role"}
        )

        with pytest.raises(ValueError, match="Cannot set iam_role_arn and iam_role_name simultaneously"):
            GlueSessionHook(
                session_id="aws_test_glue_session",
                desc="This is test case job from Airflow",
                iam_role_name="my_test_role_name",
                iam_role_arn="my_test_role",
            )

    @mock.patch.object(AwsBaseHook, "conn")
    def test_has_session_exists(self, mock_conn):
        session_id = "aws_test_glue_session"
        mock_conn.get_session.return_value = {"Session": {"Id": session_id}}

        hook = GlueSessionHook(aws_conn_id=None, session_id=session_id)
        result = hook.has_session(session_id)
        assert result is True
        mock_conn.get_session.assert_called_once_with(Id=hook.session_id)

    @mock.patch.object(AwsBaseHook, "conn")
    def test_has_session_doesnt_exists(self, mock_conn):
        class SessionNotFoundException(Exception):
            pass

        mock_conn.exceptions.EntityNotFoundException = SessionNotFoundException
        mock_conn.get_session.side_effect = SessionNotFoundException()

        session_id = "aws_test_glue_session"
        hook = GlueSessionHook(aws_conn_id=None, session_id=session_id)
        result = hook.has_session(session_id)
        assert result is False
        mock_conn.get_session.assert_called_once_with(Id=session_id)

    @mock.patch.object(GlueSessionHook, "get_iam_execution_role")
    @mock.patch.object(GlueSessionHook, "conn")
    def test_init_worker_type_value_error(self, mock_conn, mock_get_iam_execution_role):
        mock_get_iam_execution_role.return_value = mock.MagicMock(Role={"RoleName": "my_test_role"})

        with pytest.raises(ValueError, match="Cannot specify num_of_dpus with custom WorkerType"):
            GlueSessionHook(
                session_id="aws_test_glue_session",
                desc="This is test case job from Airflow",
                iam_role_name="my_test_role",
                region_name=self.some_aws_region,
                num_of_dpus=20,
                create_session_kwargs={"WorkerType": "G.2X", "NumberOfWorkers": 60},
            )

    @mock.patch.object(GlueSessionHook, "get_iam_execution_role")
    @mock.patch.object(GlueSessionHook, "conn")
    def test_initialize_session_does_not_exist(self, mock_conn, mock_get_iam_execution_role):
        """
        Calls 'initialize_session' with no existing session.
        Should create a new session.
        """

        class SessionNotFoundException(Exception):
            pass

        expected_session_id = "aws_test_glue_session"
        session_description = "This is test case session from Airflow"
        role_name = "my_test_role"
        role_name_arn = "test_role"

        mock_conn.exceptions.EntityNotFoundException = SessionNotFoundException
        mock_conn.get_session.side_effect = SessionNotFoundException()
        mock_get_iam_execution_role.return_value = {"Role": {"RoleName": role_name, "Arn": role_name_arn}}

        hook = GlueSessionHook(
            session_id=expected_session_id,
            desc=session_description,
            num_of_dpus=5,
            iam_role_name=role_name,
            create_session_kwargs={"Command": {}},
            region_name=self.some_aws_region,
        )

        result = hook.initialize_session()

        mock_conn.get_session.assert_called_once_with(Id=expected_session_id)
        mock_conn.create_session.assert_called_once_with(
            Command={},
            Description=session_description,
            Id=expected_session_id,
            MaxCapacity=5,
            Role=role_name_arn,
        )
        mock_conn.update_job.assert_not_called()
        assert result == expected_session_id

    @mock.patch.object(GlueSessionHook, "get_iam_execution_role")
    @mock.patch.object(GlueSessionHook, "conn")
    def test_initialize_session_exists(self, mock_conn, mock_get_iam_execution_role):
        """
        Calls 'initialize' with existing session.
        Should not create a new session.
        """

        session_id = "aws_test_glue_session"

        mock_conn.get_session.return_value = {"Session": {"Id": session_id}}

        hook = GlueSessionHook(
            session_id=session_id,
            iam_role_name="test_role",
        )

        result = hook.initialize_session()

        mock_conn.get_session.assert_called_once_with(Id=session_id)
        mock_conn.create_session.assert_not_called()
        assert result == session_id

    @mock.patch.object(GlueSessionHook, "get_session_state")
    def test_session_readiness_success(self, get_state_mock: MagicMock):
        hook = GlueSessionHook(session_poll_interval=0)
        get_state_mock.side_effect = [
            "PROVISIONING",
            "PROVISIONING",
            "READY",
        ]

        hook.session_readiness("session_id")

        assert get_state_mock.call_count == 3
        get_state_mock.assert_called_with("session_id")

    @mock.patch.object(GlueSessionHook, "get_session_state")
    def test_job_completion_failure(self, get_state_mock: MagicMock):
        hook = GlueSessionHook(session_poll_interval=0)
        get_state_mock.side_effect = [
            "PROVISIONING",
            "PROVISIONING",
            "FAILED",
        ]

        with pytest.raises(AirflowException):
            hook.session_readiness("session_id")

        assert get_state_mock.call_count == 3

    @mock.patch.object(GlueSessionHook, "async_get_session_state")
    async def test_async_session_readiness_success(self, get_state_mock: MagicMock):
        hook = GlueSessionHook(session_poll_interval=0)
        get_state_mock.side_effect = [
            "PROVISIONING",
            "PROVISIONING",
            "READY",
        ]

        await hook.session_readiness("session_id")

        assert get_state_mock.call_count == 3
        get_state_mock.assert_called_with("session_id")

    @mock.patch.object(GlueSessionHook, "async_get_session_state")
    async def test_async_job_completion_failure(self, get_state_mock: MagicMock):
        hook = GlueSessionHook(session_poll_interval=0)
        get_state_mock.side_effect = [
            "PROVISIONING",
            "PROVISIONING",
            "FAILED",
        ]

        with pytest.raises(AirflowException):
            await hook.async_session_readiness("session_id")

        assert get_state_mock.call_count == 3
