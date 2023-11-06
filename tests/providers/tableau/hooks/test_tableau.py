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

from unittest.mock import MagicMock, patch

import pytest

from airflow import configuration, models
from airflow.providers.tableau.hooks.tableau import TableauHook, TableauJobFinishCode
from airflow.utils import db

pytestmark = pytest.mark.db_test


class TestTableauHook:
    """
    Test class for TableauHook
    """

    def setup_method(self):
        configuration.conf.load_test_config()

        db.merge_conn(
            models.Connection(
                conn_id="tableau_test_password",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"site_id": "my_site"}',
            )
        )
        db.merge_conn(
            models.Connection(
                conn_id="tableau_test_token",
                conn_type="tableau",
                host="tableau",
                extra='{"token_name": "my_token", "personal_access_token": "my_personal_access_token"}',
            )
        )
        db.merge_conn(
            models.Connection(
                conn_id="tableau_test_ssl_connection_certificates_path",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"verify": "my_cert_path", "cert": "my_client_cert_path"}',
            )
        )
        db.merge_conn(
            models.Connection(
                conn_id="tableau_test_ssl_false_connection",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"verify": "False"}',
            )
        )
        db.merge_conn(
            models.Connection(
                conn_id="tableau_test_ssl_bool_param_connection",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"verify": false}',
            )
        )
        db.merge_conn(
            models.Connection(
                conn_id="tableau_test_ssl_connection_default",
                conn_type="tableau",
                host="tableau",
                extra='{"token_name": "my_token", "personal_access_token": "my_personal_access_token"}',
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

    @patch("airflow.providers.tableau.hooks.tableau.PersonalAccessTokenAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_get_conn_auth_via_token_and_site_in_init(self, mock_server, mock_tableau_auth):
        """
        Test get conn auth via token
        """
        with TableauHook(site_id="test", tableau_conn_id="tableau_test_token") as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_tableau_auth.assert_called_once_with(
                token_name=tableau_hook.conn.extra_dejson["token_name"],
                personal_access_token=tableau_hook.conn.extra_dejson["personal_access_token"],
                site_id=tableau_hook.site_id,
            )
            mock_server.return_value.auth.sign_in_with_personal_access_token.assert_called_once_with(
                mock_tableau_auth.return_value
            )
        mock_server.return_value.auth.sign_out.assert_called_once_with()

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

    @patch("airflow.providers.tableau.hooks.tableau.PersonalAccessTokenAuth")
    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_get_conn_ssl_default(self, mock_server, mock_tableau_auth):
        """
        Test get conn with default SSL parameters
        """
        with TableauHook(tableau_conn_id="tableau_test_ssl_connection_default") as tableau_hook:
            mock_server.assert_called_once_with(tableau_hook.conn.host)
            mock_server.return_value.add_http_options.assert_called_once_with(
                options_dict={"verify": True, "cert": None}
            )
            mock_tableau_auth.assert_called_once_with(
                token_name=tableau_hook.conn.extra_dejson["token_name"],
                personal_access_token=tableau_hook.conn.extra_dejson["personal_access_token"],
                site_id="",
            )
            mock_server.return_value.auth.sign_in_with_personal_access_token.assert_called_once_with(
                mock_tableau_auth.return_value
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
        "finish_code, expected_status",
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

    @patch("airflow.providers.tableau.hooks.tableau.Server")
    def test_wait_for_state(self, mock_tableau_server):
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
