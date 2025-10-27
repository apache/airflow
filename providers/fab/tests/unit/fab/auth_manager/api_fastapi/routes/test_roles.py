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

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleResponse


@pytest.mark.db_test
class TestRoles:
    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_role(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        dummy_out = RoleResponse(name="my_new_role", permissions=[])
        mock_roles.create_role.return_value = dummy_out

        with as_user():
            resp = test_client.post("/fab/v1/roles", json={"name": "my_new_role", "actions": []})
            assert resp.status_code == 200
            assert resp.json() == dummy_out.model_dump(by_alias=True)
            mock_roles.create_role.assert_called_once()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_role_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.post("/fab/v1/roles", json={"name": "r", "actions": []})
            assert resp.status_code == 403
            mock_roles.create_role.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_role_validation_422_empty_name(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.post("/fab/v1/roles", json={"name": "", "actions": []})
            assert resp.status_code == 422
            mock_roles.create_role.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_create_role_validation_422_missing_name(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.post("/fab/v1/roles", json={"actions": []})
            assert resp.status_code == 422
            mock_roles.create_role.assert_not_called()
