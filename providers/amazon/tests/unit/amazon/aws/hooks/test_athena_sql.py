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

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.athena_sql import AthenaSQLHook
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper

from tests_common.test_utils.version_compat import SQLALCHEMY_V_1_4

REGION_NAME = "us-east-1"
WORK_GROUP = "test-work-group"
SCHEMA_NAME = "athena_sql_schema"
AWS_ACCESS_KEY_ID = "aws_access_key_id"
AWS_SECRET_ACCESS_KEY = "aws_secret_access_key"
AWS_SESSION_TOKEN = "aws_session_token"

AWS_CONN_ID = "aws_not_default"
AWS_ATHENA_CONN_ID = "aws_athena_not_default"


class TestAthenaSQLHookConn:
    def setup_method(self):
        conn = Connection(
            conn_type="athena",
            schema=SCHEMA_NAME,
            extra={"work_group": WORK_GROUP, "region_name": REGION_NAME},
        )
        self.conn_athena = AwsConnectionWrapper(conn)

        self.db_hook = AthenaSQLHook()

        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = conn

    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_credentials")
    def test_get_uri(self, mock_get_credentials):
        mock_get_credentials.return_value = mock.Mock(
            access_key=AWS_ACCESS_KEY_ID, secret_key=AWS_SECRET_ACCESS_KEY, token=AWS_SESSION_TOKEN
        )

        expected_athena_uri = "awsathena+rest://aws_access_key_id:aws_secret_access_key@athena.us-east-1.amazonaws.com:443/athena_sql_schema?aws_session_token=aws_session_token&region_name=us-east-1&work_group=test-work-group"

        athena_uri = self.db_hook.get_uri()

        mock_get_credentials.assert_called_once_with(region_name=REGION_NAME)

        if SQLALCHEMY_V_1_4:
            assert str(athena_uri) == expected_athena_uri
        else:
            assert athena_uri.render_as_string(hide_password=False) == expected_athena_uri

    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook._get_conn_params")
    def test_get_uri_change_driver(self, mock_get_conn_params):
        mock_get_conn_params.return_value = dict(
            driver="arrow", schema_name=SCHEMA_NAME, region_name=REGION_NAME, aws_domain="amazonaws.com"
        )

        athena_uri = self.db_hook.get_uri()

        assert str(athena_uri).startswith("awsathena+arrow://")

    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.pyathena.connect")
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_session")
    def test_get_conn(self, mock_get_session, mock_connect):
        self.db_hook.get_conn()

        mock_get_session.assert_called_once_with(region_name=REGION_NAME)

        mock_connect.assert_called_once_with(
            schema_name=SCHEMA_NAME,
            region_name=REGION_NAME,
            session=mock_get_session.return_value,
            work_group=WORK_GROUP,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.pyathena.connect")
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_session")
    def test_get_conn_with_aws_conn(self, mock_get_session, mock_connect):
        self.db_hook.get_conn()

        mock_get_session.assert_called_once_with(region_name=REGION_NAME)

        mock_connect.assert_called_once_with(
            schema_name=SCHEMA_NAME,
            region_name=REGION_NAME,
            session=mock_get_session.return_value,
            work_group=WORK_GROUP,
        )

    @pytest.mark.parametrize(
        "conn_params, conn_extra, expected_call_args",
        [
            (
                {"schema": "athena_sql_schema1"},
                {"region_name": "us-east-2"},
                {"region_name": "us-east-2", "schema_name": "athena_sql_schema1", "session": mock.ANY},
            ),
            (
                {"schema": "athena_sql_schema2"},
                {"work_group": "test-work-group", "region_name": "us-east-2"},
                {
                    "region_name": "us-east-2",
                    "schema_name": "athena_sql_schema2",
                    "work_group": "test-work-group",
                    "session": mock.ANY,
                },
            ),
            (
                {"schema": "athena_sql_schema3"},
                {"s3_staging_dir": "s3://test-bucket/", "region_name": "us-east-3"},
                {
                    "region_name": "us-east-3",
                    "schema_name": "athena_sql_schema3",
                    "s3_staging_dir": "s3://test-bucket/",
                    "session": mock.ANY,
                },
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.pyathena.connect")
    def test_get_conn_passing_args(self, mock_connect, conn_params, conn_extra, expected_call_args):
        with mock.patch(
            "airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.conn",
            AwsConnectionWrapper(Connection(conn_type="athena", extra=conn_extra, **conn_params)),
        ):
            self.db_hook.get_conn()
            mock_connect.assert_called_once_with(**expected_call_args)

    def test_conn_id_default_setter(self):
        assert self.db_hook.athena_conn_id == "athena_default"
        assert self.db_hook.aws_conn_id == "aws_default"

    def test_conn_id_override_setter(self):
        hook = AthenaSQLHook(athena_conn_id=AWS_ATHENA_CONN_ID, aws_conn_id=AWS_CONN_ID)
        assert hook.athena_conn_id == AWS_ATHENA_CONN_ID
        assert hook.aws_conn_id == AWS_CONN_ID

    def test_hook_params_handling(self):
        """Test that hook_params are properly handled and don't cause TypeError."""
        # Test that hook_params with Athena-specific parameters don't cause errors
        hook = AthenaSQLHook(
            athena_conn_id="test_conn",
            s3_staging_dir="s3://test-bucket/staging/",
            work_group="test-workgroup",
            driver="rest",
            aws_domain="amazonaws.com",
            session_kwargs={"profile_name": "test"},
            config_kwargs={"retries": {"max_attempts": 5}},
            role_arn="arn:aws:iam::123456789012:role/test-role",
            assume_role_method="assume_role",
            assume_role_kwargs={"RoleSessionName": "airflow-test"},
            aws_session_token="test-token",
            endpoint_url="https://athena.us-east-1.amazonaws.com",
        )
        
        # Verify that the parameters were extracted correctly
        assert hook.s3_staging_dir == "s3://test-bucket/staging/"
        assert hook.work_group == "test-workgroup"
        assert hook.driver == "rest"
        assert hook.aws_domain == "amazonaws.com"
        assert hook.session_kwargs == {"profile_name": "test"}
        assert hook.config_kwargs == {"retries": {"max_attempts": 5}}
        assert hook.role_arn == "arn:aws:iam::123456789012:role/test-role"
        assert hook.assume_role_method == "assume_role"
        assert hook.assume_role_kwargs == {"RoleSessionName": "airflow-test"}
        assert hook.aws_session_token == "test-token"
        assert hook.endpoint_url == "https://athena.us-east-1.amazonaws.com"

    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.pyathena.connect")
    @mock.patch("airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_session")
    def test_get_conn_with_hook_params(self, mock_get_session, mock_connect):
        """Test that get_conn uses hook_params when provided."""
        # Create hook with hook_params
        hook = AthenaSQLHook(
            athena_conn_id="test_conn",
            s3_staging_dir="s3://test-bucket/staging/",
            work_group="test-workgroup",
        )
        
        # Mock the connection
        conn = Connection(
            conn_type="athena",
            schema="test_schema",
            extra={"region_name": "us-east-1"},
        )
        hook.get_connection = mock.Mock(return_value=conn)
        
        # Call get_conn
        hook.get_conn()
        
        # Verify that pyathena.connect was called with hook_params
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[1]  # Get keyword arguments
        assert call_args["s3_staging_dir"] == "s3://test-bucket/staging/"
        assert call_args["work_group"] == "test-workgroup"

    def test_sql_value_check_operator_compatibility(self):
        """Test that AthenaSQLHook works with SQLValueCheckOperator (reproduces issue #55678)."""
        from airflow.providers.common.sql.operators.sql import SQLValueCheckOperator
        from unittest.mock import patch
        
        # Mock Athena connection with s3_staging_dir in extra (as described in the issue)
        athena_conn = Connection(
            conn_id="athena_conn",
            conn_type="athena",
            description="Connection to a Athena API",
            schema="athena_sql_schema1",
            extra={"s3_staging_dir": "s3://mybucket/athena/", "region_name": "eu-west-1"},
        )

        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=athena_conn):
            # This should NOT raise TypeError: AwsGenericHook.__init__() got an unexpected keyword argument 's3_staging_dir'
            operator = SQLValueCheckOperator(
                task_id="value_check", 
                sql="SELECT TRUE", 
                pass_value=True, 
                conn_id="athena_conn"
            )
            
            # The operator should be created successfully
            assert operator.task_id == "value_check"
            assert operator.sql == "SELECT TRUE"
            assert operator.pass_value == "True"
            assert operator.conn_id == "athena_conn"
