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

from contextlib import nullcontext as _noop_cm
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.users import UserResponse


@pytest.mark.db_test
class TestUsers:
    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_user_ok(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        dummy_out = UserResponse(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
            roles=None,
            active=True,
            login_count=0,
            fail_login_count=0,
        )
        mock_users.create_user.return_value = dummy_out

        with as_user():
            payload = {
                "username": "alice",
                "email": "alice@example.com",
                "first_name": "Alice",
                "last_name": "Liddell",
                "password": "s3cr3t",
                "roles": [{"name": "Viewer"}],
            }
            resp = test_client.post("/fab/v1/users", json=payload)
            assert resp.status_code == 200
            assert resp.json() == dummy_out.model_dump(by_alias=True)
            mock_users.create_user.assert_called_once()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_user_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.post(
                "/fab/v1/users",
                json={
                    "username": "bob",
                    "email": "bob@example.com",
                    "first_name": "Bob",
                    "last_name": "Builder",
                    "password": "pw",
                },
            )
            assert resp.status_code == 403
            mock_users.create_user.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_user_validation_422_empty_username(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.post(
                "/fab/v1/users",
                json={
                    "username": "",
                    "email": "e@example.com",
                    "first_name": "E",
                    "last_name": "Mpty",
                    "password": "pw",
                },
            )
            assert resp.status_code == 422
            mock_users.create_user.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_user_validation_422_missing_username(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.post(
                "/fab/v1/users",
                json={
                    "email": "nouser@example.com",
                    "first_name": "No",
                    "last_name": "User",
                    "password": "pw",
                },
            )
            assert resp.status_code == 422
            mock_users.create_user.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_user_validation_422_missing_password(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.post(
                "/fab/v1/users",
                json={
                    "username": "no-pass",
                    "email": "np@example.com",
                    "first_name": "No",
                    "last_name": "Pass",
                    # password missing
                },
            )
            assert resp.status_code == 422
            mock_users.create_user.assert_not_called()
