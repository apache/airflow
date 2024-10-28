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

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.get_credentials"
    )
    def test_get_uri(self, mock_get_credentials):
        mock_get_credentials.return_value = mock.Mock(
            access_key=AWS_ACCESS_KEY_ID,
            secret_key=AWS_SECRET_ACCESS_KEY,
            token=AWS_SESSION_TOKEN,
        )

        expected_athena_uri = "awsathena+rest://aws_access_key_id:aws_secret_access_key@athena.us-east-1.amazonaws.com:443/athena_sql_schema?aws_session_token=aws_session_token&region_name=us-east-1&work_group=test-work-group"

        athena_uri = self.db_hook.get_uri()

        mock_get_credentials.assert_called_once_with(region_name=REGION_NAME)

        assert str(athena_uri) == expected_athena_uri

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook._get_conn_params"
    )
    def test_get_uri_change_driver(self, mock_get_conn_params):
        mock_get_conn_params.return_value = dict(
            driver="arrow",
            schema_name=SCHEMA_NAME,
            region_name=REGION_NAME,
            aws_domain="amazonaws.com",
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
                {
                    "region_name": "us-east-2",
                    "schema_name": "athena_sql_schema1",
                    "session": mock.ANY,
                },
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
    def test_get_conn_passing_args(
        self, mock_connect, conn_params, conn_extra, expected_call_args
    ):
        with mock.patch(
            "airflow.providers.amazon.aws.hooks.athena_sql.AthenaSQLHook.conn",
            AwsConnectionWrapper(
                Connection(conn_type="athena", extra=conn_extra, **conn_params)
            ),
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
