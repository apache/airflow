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

import sys
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection

pytestmark = [pytest.mark.db_test]

if sys.version_info < (3, 10):
    pytestmark.append(
        pytest.mark.skip(
            f"Skipping {__name__} as the cloudant provider is not supported on Python 3.9, see #41555."
        )
    )
else:
    from airflow.providers.cloudant.hooks.cloudant import CloudantHook


class TestCloudantHook:
    def setup_method(self):
        self.cloudant_hook = CloudantHook()

    @patch(
        "airflow.providers.cloudant.hooks.cloudant.CloudantHook.get_connection",
        return_value=Connection(
            login="the_user", password="the_password", host="the_account"
        ),
    )
    @patch("airflow.providers.cloudant.hooks.cloudant.CouchDbSessionAuthenticator")
    @patch("airflow.providers.cloudant.hooks.cloudant.CloudantV1")
    def test_get_conn_passes_expected_params_and_returns_cloudant_object(
        self, mock_cloudant_v1, mock_session_authenticator, mock_get_connection
    ):
        cloudant_session = self.cloudant_hook.get_conn()

        conn = mock_get_connection.return_value

        mock_session_authenticator.assert_called_once_with(
            username=conn.login, password=conn.password
        )
        mock_cloudant_v1.assert_called_once_with(
            authenticator=mock_session_authenticator.return_value
        )

        cloudant_service = mock_cloudant_v1.return_value
        cloudant_service.set_service_url.assert_called_once_with(
            f"https://{conn.host}.cloudant.com"
        )

        assert cloudant_session == cloudant_service

    @pytest.mark.parametrize(
        "conn",
        [
            Connection(),
            Connection(host="acct"),
            Connection(login="user"),
            Connection(password="pwd"),
            Connection(host="acct", login="user"),
            Connection(host="acct", password="pwd"),
            Connection(login="user", password="pwd"),
        ],
    )
    @patch("airflow.providers.cloudant.hooks.cloudant.CloudantHook.get_connection")
    def test_get_conn_invalid_connection(self, mock_get_connection, conn):
        mock_get_connection.return_value = conn
        with pytest.raises(AirflowException):
            self.cloudant_hook.get_conn()
