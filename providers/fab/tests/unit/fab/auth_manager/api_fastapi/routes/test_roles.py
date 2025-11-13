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
from fastapi import HTTPException, status

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import (
    RoleCollectionResponse,
    RoleResponse,
)


@pytest.mark.db_test
class TestRoles:
    # POST /roles

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

    # GET /roles

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_roles_success_defaults(
        self,
        conf_mock,
        mock_get_application_builder,
        mock_get_auth_manager,
        mock_roles,
        test_client,
        as_user,
    ):
        conf_mock.getint.side_effect = lambda section, option: {
            "maximum_page_limit": 500,
            "fallback_page_limit": 25,
        }[option]

        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        dummy = RoleCollectionResponse(
            roles=[RoleResponse(name="viewer", permissions=[])],
            total_entries=1,
            limit=100,
            offset=0,
        )
        mock_roles.get_roles.return_value = dummy

        with as_user():
            resp = test_client.get("/fab/v1/roles")
            assert resp.status_code == 200
            assert resp.json() == dummy.model_dump(by_alias=True)
            mock_roles.get_roles.assert_called_once_with(order_by="name", limit=100, offset=0)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_roles_passes_params_and_clamps_limit(
        self,
        conf_mock,
        mock_get_application_builder,
        mock_get_auth_manager,
        mock_roles,
        test_client,
        as_user,
    ):
        conf_mock.getint.side_effect = lambda section, option: {
            "maximum_page_limit": 50,
            "fallback_page_limit": 20,
        }[option]

        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        dummy = RoleCollectionResponse(roles=[], total_entries=0, limit=50, offset=7)
        mock_roles.get_roles.return_value = dummy

        with as_user():
            resp = test_client.get("/fab/v1/roles", params={"order_by": "-name", "limit": 1000, "offset": 7})
            assert resp.status_code == 200
            assert resp.json() == dummy.model_dump(by_alias=True)
            mock_roles.get_roles.assert_called_once_with(order_by="-name", limit=50, offset=7)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_roles_uses_fallback_when_limit_zero(
        self,
        conf_mock,
        mock_get_application_builder,
        mock_get_auth_manager,
        mock_roles,
        test_client,
        as_user,
    ):
        conf_mock.getint.side_effect = lambda section, option: {
            "maximum_page_limit": 100,
            "fallback_page_limit": 33,
        }[option]

        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        dummy = RoleCollectionResponse(roles=[], total_entries=0, limit=33, offset=0)
        mock_roles.get_roles.return_value = dummy

        with as_user():
            resp = test_client.get("/fab/v1/roles", params={"limit": 0})
            assert resp.status_code == 200
            assert resp.json() == dummy.model_dump(by_alias=True)
            mock_roles.get_roles.assert_called_once_with(order_by="name", limit=33, offset=0)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_roles_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.get("/fab/v1/roles")
            assert resp.status_code == 403
            mock_roles.get_roles.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_roles_validation_422_negative_offset(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.get("/fab/v1/roles", params={"offset": -1})
            assert resp.status_code == 422
            mock_roles.get_roles.assert_not_called()

    # DELETE /roles/{name}

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_delete_role(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_roles.delete_role.return_value = None
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.delete("/fab/v1/roles/roleA")
            assert resp.status_code == 200
            mock_roles.delete_role.assert_called_once_with(name="roleA")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_delete_role_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.delete("/fab/v1/roles/roleA")
            assert resp.status_code == 403
            mock_roles.delete_role.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_delete_role_validation_404_not_found(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr
        mock_roles.delete_role.side_effect = HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role with name 'non_existent_role' does not exist.",
        )

        with as_user():
            resp = test_client.delete("/fab/v1/roles/non_existent_role")
            assert resp.status_code == 404
            mock_roles.delete_role.assert_called_once_with(name="non_existent_role")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_delete_role_validation_404_empty_name(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.delete("/fab/v1/roles/")
            assert resp.status_code == 404
            mock_roles.delete_role.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_role(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        dummy_out = RoleResponse(name="roleA", permissions=[])
        mock_roles.get_role.return_value = dummy_out

        with as_user():
            resp = test_client.get("/fab/v1/roles/roleA")
            assert resp.status_code == 200
            assert resp.json() == dummy_out.model_dump(by_alias=True)
            mock_roles.get_role.assert_called_once_with(name="roleA")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_role_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.get("/fab/v1/roles/roleA")
            assert resp.status_code == 403
            mock_roles.get_role.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_role_validation_404_not_found(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr
        mock_roles.get_role.side_effect = HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role with name 'non_existent_role' does not exist.",
        )

        with as_user():
            resp = test_client.get("/fab/v1/roles/non_existent_role")
            assert resp.status_code == 404
            mock_roles.get_role.assert_called_once_with(name="non_existent_role")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.roles.FABAuthManagerRoles")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.roles.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_role_validation_404_empty_name(
        self, mock_get_application_builder, mock_get_auth_manager, mock_roles, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.get("/fab/v1/roles/")
            assert resp.status_code == 404
            mock_roles.get_role.assert_not_called()
