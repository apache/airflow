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
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from airflow import configuration, models
from airflow.exceptions import AirflowException
from airflow.providers.tableau.hooks.tableau import TableauHook, TableauJobFinishCode


class TestTableauHook:
    """
    Test class for TableauHook
    """

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        configuration.conf.load_test_config()

        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_password",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"site_id": "my_site"}',
            )
        )
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_ssl_connection_certificates_path",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"verify": "my_cert_path", "cert": "my_client_cert_path"}',
            )
        )
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_ssl_false_connection",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"verify": "False"}',
            )
        )
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_ssl_bool_param_connection",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"verify": false}',
            )
        )
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_jwt_auth",
                conn_type="tableau",
                host="tableau",
                extra='{"auth": "jwt", "jwt_token": "fake_jwt_token", "site_id": ""}',
            )
        )
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_ssl_connection_certificates_path_with_jwt",
                conn_type="tableau",
                host="tableau",
                extra='{"auth": "jwt", "jwt_token": "fake_jwt_token", "site_id": "", "verify": "my_cert_path", "cert": "my_client_cert_path"}',
            )
        )
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_jwt_auth_no_token",
                conn_type="tableau",
                host="tableau",
                extra='{"auth": "jwt", "site_id": ""}',
            )
        )
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_both_auth",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"auth": "jwt", "jwt_token": "fake_jwt_token", "site_id": ""}',
            )
        )

    @patch("airflow.providers.tableau.hooks.tableau.TableauAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_get_conn_auth_via_password_and_site_in_connection(self, mock_server, mock_tableau_auth):
        """
        Test get conn auth via password
        """
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_tableau_auth.assert_called_once_with(
                username=tableau_hook.conn.login,
                password=tableau_hook.conn.password,
                site_id=tableau_hook.conn.extra_dejson["site_id"],
            )
            mock_server.return_value.auth.sign_in.assert_called_once_with(mock_tableau_auth.return_value)
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch("airflow.providers.tableau.hooks.tableau.JWTAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_jwt_auth(self, mock_server, mock_tableau_jwt_auth):
        """
        Test get conn using JWT authentication via a token string
        """
        with TableauHook(tableau_conn_id="tableau_test_jwt_auth") as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_tableau_jwt_auth.assert_called_once_with(
                jwt="fake_jwt_token",
                site_id="",
            )
            mock_server.return_value.auth.sign_in.assert_called_once_with(mock_tableau_jwt_auth.return_value)
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch("airflow.providers.tableau.hooks.tableau.JWTAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_jwt_auth_with_ssl(self, mock_server, mock_tableau_jwt_auth):
        """
        Test get conn using JWT authentication via a token string and ssl
        """
        with TableauHook(
            tableau_conn_id="tableau_test_ssl_connection_certificates_path_with_jwt"
        ) as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_server.return_value.add_http_options.assert_called_once_with(
                options_dict={
                    "verify": tableau_hook.conn.extra_dejson["verify"],
                    "cert": tableau_hook.conn.extra_dejson["cert"],
                }
            )
            mock_tableau_jwt_auth.assert_called_once_with(
                jwt="fake_jwt_token",
                site_id="",
            )
            mock_server.return_value.auth.sign_in.assert_called_once_with(mock_tableau_jwt_auth.return_value)
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    def test_jwt_auth_with_no_token_provided(self):
        """
        Test get conn using JWT authentication without providing a token
        """
        with pytest.raises(
            ValueError,
            match=r"When auth set to 'jwt' then expected exactly one parameter 'jwt_file' or 'jwt_token' in connection extra, but none of them provided.",
        ):
            TableauHook(tableau_conn_id="tableau_test_jwt_auth_no_token").get_conn()

    @patch("airflow.providers.tableau.hooks.tableau.JWTAuth")
    def test_jwt_auth_with_two_tokens_provided(self, mock_tableau_jwt_auth, create_connection_without_db):
        """
        Test get conn using JWT authentication while providing both a string token and a path

        The connection setup is done within this test to handle the creation of a temporary file
        for the JWT token, keeping the shared setup_connections function focused solely on connection logic
        """
        fake_jwt_token = "fake_jwt_token_from_file"
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as jwt_file:
            jwt_file.write(fake_jwt_token)
            jwt_file_path = jwt_file.name
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_jwt_file_auth_two_tokens",
                conn_type="tableau",
                host="tableau",
                extra=json.dumps(
                    {"auth": "jwt", "jwt_file": jwt_file_path, "jwt_token": "fake_jwt_token", "site_id": ""}
                ),
            )
        )
        with pytest.raises(
            ValueError,
            match=r"When auth set to 'jwt' then expected exactly one parameter 'jwt_file' or 'jwt_token' in connection extra, but provided both.",
        ):
            TableauHook(tableau_conn_id="tableau_test_jwt_file_auth_two_tokens").get_conn()
        mock_tableau_jwt_auth.assert_not_called()

    @patch("airflow.providers.tableau.hooks.tableau.JWTAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_jwt_auth_from_file(self, mock_server, mock_tableau_jwt_auth, create_connection_without_db):
        """
        Test get conn using JWT token read from file

        The connection setup is done within this test to handle the creation of a temporary file
        for the JWT token, keeping the shared setup_connections function focused solely on connection logic
        """
        fake_jwt_token = "fake_jwt_token_from_file"
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as jwt_file:
            jwt_file.write(fake_jwt_token)
            jwt_file_path = jwt_file.name
        create_connection_without_db(
            models.Connection(
                conn_id="tableau_test_jwt_file_auth",
                conn_type="tableau",
                host="tableau",
                extra=json.dumps({"auth": "jwt", "jwt_file": jwt_file_path, "site_id": ""}),
            )
        )

        with TableauHook(tableau_conn_id="tableau_test_jwt_file_auth") as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_tableau_jwt_auth.assert_called_once_with(
                jwt=fake_jwt_token,
                site_id="",
            )
            mock_server.return_value.auth.sign_in.assert_called_once_with(mock_tableau_jwt_auth.return_value)
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch("airflow.providers.tableau.hooks.tableau.TableauAuth")
    def test_both_auth(self, mock_tableau_auth):
        """
        Test whether an error is thrown if both auth types are set
        """
        with pytest.raises(
            AirflowException,
            match=r"Username/password authentication and JWT authentication cannot be used simultaneously. Please specify only one authentication method.",
        ):
            TableauHook(tableau_conn_id="tableau_test_both_auth").get_conn()
        mock_tableau_auth.assert_not_called()

    @patch("airflow.providers.tableau.hooks.tableau.TableauAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_get_conn_ssl_cert_path(self, mock_server, mock_tableau_auth):
        """
        Test get conn with SSL parameters, verify as path
        """
        with TableauHook(tableau_conn_id="tableau_test_ssl_connection_certificates_path") as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_server.return_value.add_http_options.assert_called_once_with(
                options_dict={
                    "verify": tableau_hook.conn.extra_dejson["verify"],
                    "cert": tableau_hook.conn.extra_dejson["cert"],
                }
            )
            mock_tableau_auth.assert_called_once_with(
                username=tableau_hook.conn.login,
                password=tableau_hook.conn.password,
                site_id="",
            )
            mock_server.return_value.auth.sign_in.assert_called_once_with(mock_tableau_auth.return_value)
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch("airflow.providers.tableau.hooks.tableau.TableauAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_get_conn_ssl_default(self, mock_server, mock_tableau_auth):
        """
        Test get conn with default SSL parameters
        """
        with (
            TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook,
        ):
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_server.return_value.add_http_options.assert_called_once_with(
                options_dict={"verify": True, "cert": None}
            )
            mock_tableau_auth.assert_called_once_with(
                username=tableau_hook.conn.login,
                password=tableau_hook.conn.password,
                site_id=tableau_hook.conn.extra_dejson["site_id"],
            )
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch("airflow.providers.tableau.hooks.tableau.TableauAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_get_conn_ssl_disabled(self, mock_server, mock_tableau_auth):
        """
        Test get conn with default SSL disabled parameters
        """
        with TableauHook(tableau_conn_id="tableau_test_ssl_false_connection") as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_server.return_value.add_http_options.assert_called_once_with(
                options_dict={"verify": False, "cert": None}
            )
            mock_tableau_auth.assert_called_once_with(
                username=tableau_hook.conn.login,
                password=tableau_hook.conn.password,
                site_id="",
            )
            mock_server.return_value.auth.sign_in.assert_called_once_with(mock_tableau_auth.return_value)
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch("airflow.providers.tableau.hooks.tableau.TableauAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_get_conn_ssl_bool_param(self, mock_server, mock_tableau_auth):
        """
        Test get conn with SSL Verify parameter as bool
        """
        with TableauHook(tableau_conn_id="tableau_test_ssl_bool_param_connection") as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_server.return_value.add_http_options.assert_called_once_with(
                options_dict={"verify": False, "cert": None}
            )
            mock_tableau_auth.assert_called_once_with(
                username=tableau_hook.conn.login,
                password=tableau_hook.conn.password,
                site_id="",
            )
            mock_server.return_value.auth.sign_in.assert_called_once_with(mock_tableau_auth.return_value)
        mock_server.return_value.auth.sign_out.assert_called_once_with()

    @patch("airflow.providers.tableau.hooks.tableau.TableauAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    @patch("airflow.providers.tableau.hooks.tableau.Pager", return_value=[1, 2, 3])
    def test_get_all(self, mock_pager, mock_server, mock_tableau_auth):
        """
        Test get all
        """
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            jobs = tableau_hook.get_all(resource_name="jobs")
            assert jobs == mock_pager.return_value

        mock_pager.assert_called_once_with(mock_server.return_value.jobs.get)

    @pytest.mark.parametrize(
        ("finish_code", "expected_status"),
        [
            pytest.param(0, TableauJobFinishCode.SUCCESS, id="SUCCESS"),
            pytest.param(1, TableauJobFinishCode.ERROR, id="ERROR"),
            pytest.param(2, TableauJobFinishCode.CANCELED, id="CANCELED"),
        ],
    )
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_get_job_status(self, mock_tableau_server, finish_code, expected_status):
        """
        Test get job status
        """
        mock_tableau_server.jobs.get_by_id.return_value.finish_code = finish_code
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            tableau_hook.server = mock_tableau_server
            jobs_status = tableau_hook.get_job_status(job_id="j1")
            assert jobs_status == expected_status

    @patch("time.sleep", return_value=None)
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_wait_for_state(self, mock_tableau_server, sleep_mock):
        """
        Test wait_for_state
        """
        # Test SUCCESS Positive
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            tableau_hook.get_job_status = MagicMock(
                name="get_job_status",
                side_effect=[TableauJobFinishCode.PENDING, TableauJobFinishCode.SUCCESS],
            )
            assert tableau_hook.wait_for_state(
                job_id="j1", target_state=TableauJobFinishCode.SUCCESS, check_interval=1
            )

        # Test SUCCESS Negative
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            tableau_hook.get_job_status = MagicMock(
                name="get_job_status",
                side_effect=[
                    TableauJobFinishCode.PENDING,
                    TableauJobFinishCode.PENDING,
                    TableauJobFinishCode.ERROR,
                ],
            )
            assert not tableau_hook.wait_for_state(
                job_id="j1", target_state=TableauJobFinishCode.SUCCESS, check_interval=1
            )

        # Test ERROR Positive
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            tableau_hook.get_job_status = MagicMock(
                name="get_job_status",
                side_effect=[
                    TableauJobFinishCode.PENDING,
                    TableauJobFinishCode.PENDING,
                    TableauJobFinishCode.ERROR,
                ],
            )
            assert tableau_hook.wait_for_state(
                job_id="j1", target_state=TableauJobFinishCode.ERROR, check_interval=1
            )

        # Test CANCELLED Positive
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            tableau_hook.get_job_status = MagicMock(
                name="get_job_status",
                side_effect=[
                    TableauJobFinishCode.PENDING,
                    TableauJobFinishCode.PENDING,
                    TableauJobFinishCode.CANCELED,
                ],
            )
            assert tableau_hook.wait_for_state(
                job_id="j1", target_state=TableauJobFinishCode.CANCELED, check_interval=1
            )

        # Test PENDING Positive
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            tableau_hook.get_job_status = MagicMock(
                name="get_job_status",
                side_effect=[
                    TableauJobFinishCode.PENDING,
                    TableauJobFinishCode.ERROR,
                ],
            )
            assert tableau_hook.wait_for_state(
                job_id="j1", target_state=TableauJobFinishCode.PENDING, check_interval=1
            )
