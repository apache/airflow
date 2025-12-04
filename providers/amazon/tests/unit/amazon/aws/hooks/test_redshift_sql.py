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

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.version_compat import NOTSET

LOGIN_USER = "login"
LOGIN_PASSWORD = "password"
LOGIN_HOST = "host"
LOGIN_PORT = 5439
LOGIN_SCHEMA = "dev"
MOCK_REGION_NAME = "eu-north-1"


class TestRedshiftSQLHookConn:
    def setup_method(self):
        self.connection = Connection(
            conn_type="redshift",
            login=LOGIN_USER,
            password=LOGIN_PASSWORD,
            host=LOGIN_HOST,
            port=LOGIN_PORT,
            schema=LOGIN_SCHEMA,
        )

        self.db_hook = RedshiftSQLHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    def test_get_uri(self):
        db_uri = self.db_hook.get_uri()
        expected = "postgresql://login:password@host:5439/dev"
        assert db_uri == expected

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.redshift_connector.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login", password="password", host="host", port=5439, database="dev"
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.redshift_connector.connect")
    def test_get_conn_extra(self, mock_connect):
        self.connection.extra = json.dumps(
            {
                "iam": False,
                "cluster_identifier": "my-test-cluster",
                "profile": "default",
            }
        )
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user=LOGIN_USER,
            password=LOGIN_PASSWORD,
            host=LOGIN_HOST,
            port=LOGIN_PORT,
            cluster_identifier="my-test-cluster",
            profile="default",
            database=LOGIN_SCHEMA,
            iam=False,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.redshift_connector.connect")
    @pytest.mark.parametrize("aws_conn_id", [NOTSET, None, "mock_aws_conn"])
    def test_get_conn_iam(self, mock_connect, mock_aws_hook_conn, aws_conn_id):
        mock_conn_extra = {"iam": True, "profile": "default", "cluster_identifier": "my-test-cluster"}
        if aws_conn_id is not NOTSET:
            self.db_hook.aws_conn_id = aws_conn_id
        self.connection.extra = json.dumps(mock_conn_extra)

        mock_db_user = f"IAM:{self.connection.login}"
        mock_db_pass = "aws_token"

        # Mock AWS Connection
        mock_aws_hook_conn.get_cluster_credentials.return_value = {
            "DbPassword": mock_db_pass,
            "DbUser": mock_db_user,
        }

        self.db_hook.get_conn()

        # Check boto3 'redshift' client method `get_cluster_credentials` call args
        mock_aws_hook_conn.get_cluster_credentials.assert_called_once_with(
            DbUser=LOGIN_USER,
            DbName=LOGIN_SCHEMA,
            ClusterIdentifier="my-test-cluster",
            AutoCreate=False,
        )

        mock_connect.assert_called_once_with(
            user=mock_db_user,
            password=mock_db_pass,
            host=LOGIN_HOST,
            port=LOGIN_PORT,
            cluster_identifier="my-test-cluster",
            profile="default",
            database=LOGIN_SCHEMA,
            iam=True,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.redshift_connector.connect")
    @pytest.mark.parametrize("aws_conn_id", [NOTSET, None, "mock_aws_conn"])
    def test_get_conn_iam_serverless_redshift(self, mock_connect, mock_aws_hook_conn, aws_conn_id):
        mock_work_group = "my-test-workgroup"
        mock_conn_extra = {
            "iam": True,
            "is_serverless": True,
            "profile": "default",
            "serverless_work_group": mock_work_group,
        }
        if aws_conn_id is not NOTSET:
            self.db_hook.aws_conn_id = aws_conn_id
        self.connection.extra = json.dumps(mock_conn_extra)

        mock_db_user = f"IAM:{self.connection.login}"
        mock_db_pass = "aws_token"

        # Mock AWS Connection
        mock_aws_hook_conn.get_credentials.return_value = {
            "dbPassword": mock_db_pass,
            "dbUser": mock_db_user,
        }

        self.db_hook.get_conn()

        # Check boto3 'redshift' client method `get_cluster_credentials` call args
        mock_aws_hook_conn.get_credentials.assert_called_once_with(
            dbName=LOGIN_SCHEMA,
            workgroupName=mock_work_group,
            durationSeconds=3600,
        )

        mock_connect.assert_called_once_with(
            user=mock_db_user,
            password=mock_db_pass,
            host=LOGIN_HOST,
            port=LOGIN_PORT,
            serverless_work_group=mock_work_group,
            profile="default",
            database=LOGIN_SCHEMA,
            iam=True,
            is_serverless=True,
        )

    @pytest.mark.parametrize(
        ("conn_params", "conn_extra", "expected_call_args"),
        [
            ({}, {}, {}),
            ({"login": "test"}, {}, {"user": "test"}),
            ({}, {"user": "test"}, {"user": "test"}),
            ({"login": "original"}, {"user": "overridden"}, {"user": "overridden"}),
            ({"login": "test1"}, {"password": "test2"}, {"user": "test1", "password": "test2"}),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.redshift_connector.connect")
    def test_get_conn_overrides_correctly(self, mock_connect, conn_params, conn_extra, expected_call_args):
        with mock.patch(
            "airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.conn",
            Connection(conn_type="redshift", extra=conn_extra, **conn_params),
        ):
            self.db_hook.get_conn()
            mock_connect.assert_called_once_with(**expected_call_args)

    @pytest.mark.parametrize(
        ("connection_host", "connection_extra", "expected_cluster_identifier", "expected_exception_msg"),
        [
            # test without a connection host and without a cluster_identifier in connection extra
            (None, {"iam": True}, None, "Please set cluster_identifier or host in redshift connection."),
            # test without a connection host but with a cluster_identifier in connection extra
            (
                None,
                {"iam": True, "cluster_identifier": "cluster_identifier_from_extra"},
                "cluster_identifier_from_extra",
                None,
            ),
            # test with a connection host and without a cluster_identifier in connection extra
            ("cluster_identifier_from_host.x.y", {"iam": True}, "cluster_identifier_from_host", None),
            # test with both connection host and cluster_identifier in connection extra
            (
                "cluster_identifier_from_host.x.y",
                {"iam": True, "cluster_identifier": "cluster_identifier_from_extra"},
                "cluster_identifier_from_extra",
                None,
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.redshift_connector.connect")
    def test_get_iam_token(
        self,
        mock_connect,
        mock_aws_hook_conn,
        connection_host,
        connection_extra,
        expected_cluster_identifier,
        expected_exception_msg,
    ):
        self.connection.host = connection_host
        self.connection.extra = json.dumps(connection_extra)

        mock_db_user = f"IAM:{self.connection.login}"
        mock_db_pass = "aws_token"

        # Mock AWS Connection
        mock_aws_hook_conn.get_cluster_credentials.return_value = {
            "DbPassword": mock_db_pass,
            "DbUser": mock_db_user,
        }
        if expected_exception_msg is not None:
            with pytest.raises(AirflowException, match=expected_exception_msg):
                self.db_hook.get_conn()
        else:
            self.db_hook.get_conn()
            mock_aws_hook_conn.get_cluster_credentials.assert_called_once_with(
                DbUser=LOGIN_USER,
                DbName=LOGIN_SCHEMA,
                ClusterIdentifier=expected_cluster_identifier,
                AutoCreate=False,
            )

    @mock.patch.dict("os.environ", AIRFLOW_CONN_AWS_DEFAULT=f"aws://?region_name={MOCK_REGION_NAME}")
    @pytest.mark.parametrize(
        ("connection_host", "connection_extra", "expected_identity"),
        [
            # test without a connection host but with a cluster_identifier in connection extra
            (
                None,
                {"iam": True, "cluster_identifier": "cluster_identifier_from_extra"},
                f"cluster_identifier_from_extra.{MOCK_REGION_NAME}",
            ),
            # test with a connection host and without a cluster_identifier in connection extra
            (
                "cluster_identifier_from_host.id.my_region.redshift.amazonaws.com",
                {"iam": True},
                "cluster_identifier_from_host.my_region",
            ),
            # test with both connection host and cluster_identifier in connection extra
            (
                "cluster_identifier_from_host.x.y",
                {"iam": True, "cluster_identifier": "cluster_identifier_from_extra"},
                f"cluster_identifier_from_extra.{MOCK_REGION_NAME}",
            ),
            # test when hostname doesn't match pattern
            (
                "1.2.3.4",
                {},
                "1.2.3.4",
            ),
        ],
    )
    def test_get_openlineage_redshift_authority_part(
        self,
        connection_host,
        connection_extra,
        expected_identity,
    ):
        self.connection.host = connection_host
        self.connection.extra = json.dumps(connection_extra)

        assert f"{expected_identity}:{LOGIN_PORT}" == self.db_hook._get_openlineage_redshift_authority_part(
            self.connection
        )
