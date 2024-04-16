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
from unittest import mock

import boto3
import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.glue_session import GlueSessionHook

DEFAULT_CONN_ID: str = "aws_default"


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

        hook = GlueSessionHook()
        iam_role = hook.expand_role(expected_role)
        assert iam_role == f"arn:aws:iam::123456789012:role{role_path}{expected_role}"

    @mock.patch.object(AwsBaseHook, "conn")
    def test_has_session_exists(self, mock_conn):
        session_id = "aws_test_glue_session"
        mock_conn.get_session.return_value = {"Session": {"Id": session_id}}

        hook = GlueSessionHook()
        result = hook.has_session(session_id)
        assert result is True
        mock_conn.get_session.assert_called_once_with(Id=session_id)

    @mock.patch.object(AwsBaseHook, "conn")
    def test_has_session_doesnt_exists(self, mock_conn):
        class SessionNotFoundException(Exception):
            pass

        mock_conn.exceptions.EntityNotFoundException = SessionNotFoundException
        mock_conn.get_session.side_effect = SessionNotFoundException()

        session_id = "aws_test_glue_session"
        hook = GlueSessionHook()
        result = hook.has_session(session_id)
        assert result is False
        mock_conn.get_session.assert_called_once_with(Id=session_id)
