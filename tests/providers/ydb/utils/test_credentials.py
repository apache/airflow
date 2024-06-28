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

from unittest.mock import patch

from airflow.models.connection import Connection
from airflow.providers.ydb.utils.credentials import get_credentials_from_connection

TEST_ENDPOINT = "my_endpoint"
TEST_DATABASE = "/my_db"
MAGIC_CONST = 42


@patch("ydb.StaticCredentials")
def test_static_creds(mock):
    mock.return_value = MAGIC_CONST
    c = Connection(conn_type="ydb", host="localhost", login="my_login", password="my_pwd")
    credentials = get_credentials_from_connection(TEST_ENDPOINT, TEST_DATABASE, c, {})
    assert credentials == MAGIC_CONST

    assert len(mock.call_args.args) == 1
    driver_config = mock.call_args.args[0]
    assert driver_config.endpoint == TEST_ENDPOINT
    assert driver_config.database == TEST_DATABASE
    assert mock.call_args.kwargs == {"user": "my_login", "password": "my_pwd"}


@patch("ydb.AccessTokenCredentials")
def test_token_creds(mock):
    mock.return_value = MAGIC_CONST
    c = Connection(conn_type="ydb", host="localhost")
    credentials = get_credentials_from_connection(TEST_ENDPOINT, TEST_DATABASE, c, {"token": "my_token"})
    assert credentials == MAGIC_CONST

    mock.assert_called_with("my_token")


@patch("ydb.AnonymousCredentials")
def test_anonymous_creds(mock):
    mock.return_value = MAGIC_CONST
    c = Connection(conn_type="ydb", host="localhost")
    credentials = get_credentials_from_connection(TEST_ENDPOINT, TEST_DATABASE, c)
    assert credentials == MAGIC_CONST
    mock.assert_called_once()


@patch("ydb.iam.auth.MetadataUrlCredentials")
def test_vm_metadata_creds(mock):
    mock.return_value = MAGIC_CONST
    c = Connection(conn_type="ydb", host="localhost")
    credentials = get_credentials_from_connection(TEST_ENDPOINT, TEST_DATABASE, c, {"use_vm_metadata": True})
    assert credentials == MAGIC_CONST
    mock.assert_called_once()


@patch("ydb.iam.auth.BaseJWTCredentials.from_content")
def test_service_account_json_creds(mock):
    mock.return_value = MAGIC_CONST
    c = Connection(conn_type="ydb", host="localhost")

    credentials = get_credentials_from_connection(
        TEST_ENDPOINT, TEST_DATABASE, c, {"service_account_json": "my_json"}
    )
    assert credentials == MAGIC_CONST
    mock.assert_called_once()

    assert mock.call_args.args == ("my_json",)


@patch("ydb.iam.auth.BaseJWTCredentials.from_file")
def test_service_account_json_path_creds(mock):
    mock.return_value = MAGIC_CONST
    c = Connection(conn_type="ydb", host="localhost")

    credentials = get_credentials_from_connection(
        TEST_ENDPOINT, TEST_DATABASE, c, {"service_account_json_path": "my_path"}
    )
    assert credentials == MAGIC_CONST
    mock.assert_called_once()

    assert mock.call_args.args == ("my_path",)


def test_creds_priority():
    # 1. static creds
    with patch("ydb.StaticCredentials") as mock:
        c = Connection(conn_type="ydb", host="localhost", login="my_login", password="my_pwd")
        mock.return_value = MAGIC_CONST
        credentials = get_credentials_from_connection(
            TEST_ENDPOINT,
            TEST_DATABASE,
            c,
            {
                "service_account_json": "my_json",
                "service_account_json_path": "my_path",
                "use_vm_metadata": True,
                "token": "my_token",
            },
        )
        assert credentials == MAGIC_CONST
        mock.assert_called_once()

    # 2. token
    with patch("ydb.AccessTokenCredentials") as mock:
        c = Connection(conn_type="ydb", host="localhost", password="my_pwd")
        mock.return_value = MAGIC_CONST
        credentials = get_credentials_from_connection(
            TEST_ENDPOINT,
            TEST_DATABASE,
            c,
            {
                "service_account_json": "my_json",
                "service_account_json_path": "my_path",
                "use_vm_metadata": True,
                "token": "my_token",
            },
        )
        assert credentials == MAGIC_CONST
        mock.assert_called_once()

    # 3. service account json path
    with patch("ydb.iam.auth.BaseJWTCredentials.from_file") as mock:
        c = Connection(conn_type="ydb", host="localhost")
        mock.return_value = MAGIC_CONST
        credentials = get_credentials_from_connection(
            TEST_ENDPOINT,
            TEST_DATABASE,
            c,
            {
                "service_account_json": "my_json",
                "service_account_json_path": "my_path",
                "use_vm_metadata": True,
            },
        )
        assert credentials == MAGIC_CONST
        mock.assert_called_once()

    # 4. service account json
    with patch("ydb.iam.auth.BaseJWTCredentials.from_content") as mock:
        c = Connection(conn_type="ydb", host="localhost")
        mock.return_value = MAGIC_CONST
        credentials = get_credentials_from_connection(
            TEST_ENDPOINT,
            TEST_DATABASE,
            c,
            {
                "service_account_json": "my_json",
                "use_vm_metadata": True,
            },
        )
        assert credentials == MAGIC_CONST
        mock.assert_called_once()

    # 5. vm metadata
    with patch("ydb.iam.auth.MetadataUrlCredentials") as mock:
        c = Connection(conn_type="ydb", host="localhost")
        mock.return_value = MAGIC_CONST
        credentials = get_credentials_from_connection(
            TEST_ENDPOINT,
            TEST_DATABASE,
            c,
            {
                "use_vm_metadata": True,
            },
        )
        assert credentials == MAGIC_CONST
        mock.assert_called_once()

    # 6. anonymous
    with patch("ydb.AnonymousCredentials") as mock:
        c = Connection(conn_type="ydb", host="localhost")
        mock.return_value = MAGIC_CONST
        credentials = get_credentials_from_connection(TEST_ENDPOINT, TEST_DATABASE, c, {})
        assert credentials == MAGIC_CONST
        mock.assert_called_once()
