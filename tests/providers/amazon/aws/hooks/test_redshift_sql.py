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

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class TestRedshiftSQLHookConn:
    def setup_method(self):
        self.connection = Connection(
            conn_type="redshift", login="login", password="password", host="host", port=5439, schema="dev"
        )

        self.db_hook = RedshiftSQLHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    def test_get_uri(self):
        expected = "redshift+redshift_connector://login:password@host:5439/dev"
        x = self.db_hook.get_uri()
        assert x == expected

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
                "iam": True,
                "cluster_identifier": "my-test-cluster",
                "profile": "default",
            }
        )
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login",
            password="password",
            host="host",
            port=5439,
            cluster_identifier="my-test-cluster",
            profile="default",
            database="dev",
            iam=True,
        )

    @pytest.mark.parametrize(
        "conn_params, conn_extra, expected_call_args",
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
