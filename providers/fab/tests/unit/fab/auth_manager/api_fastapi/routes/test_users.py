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
from unittest.mock import ANY, MagicMock, patch

import pytest
from fastapi import HTTPException, status

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.users import (
    UserCollectionResponse,
    UserResponse,
)


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

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_users_success_defaults(
        self,
        conf_mock,
        mock_get_application_builder,
        mock_get_auth_manager,
        mock_users,
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

        dummy = UserCollectionResponse(
            users=[
                UserResponse(
                    username="alice",
                    email="alice@example.com",
                    first_name="Alice",
                    last_name="Liddell",
                )
            ],
            total_entries=1,
        )
        mock_users.get_users.return_value = dummy

        with as_user():
            resp = test_client.get("/fab/v1/users")
            assert resp.status_code == 200
            assert resp.json() == dummy.model_dump(by_alias=True)
            mock_users.get_users.assert_called_once_with(order_by="id", limit=100, offset=0)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_users_with_params(
        self,
        conf_mock,
        mock_get_application_builder,
        mock_get_auth_manager,
        mock_users,
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

        dummy = UserCollectionResponse(users=[], total_entries=0)
        mock_users.get_users.return_value = dummy

        with as_user():
            resp = test_client.get(
                "/fab/v1/users", params={"order_by": "-username", "limit": 1000, "offset": 5}
            )
            assert resp.status_code == 200
            mock_users.get_users.assert_called_once_with(order_by="-username", limit=50, offset=5)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_users_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.get("/fab/v1/users")
            assert resp.status_code == 403
            mock_users.get_users.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_users_validation_422_negative_offset(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.get("/fab/v1/users", params={"offset": -1})
            assert resp.status_code == 422
            mock_users.get_users.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_user_success(
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
        )
        mock_users.get_user.return_value = dummy_out

        with as_user():
            resp = test_client.get("/fab/v1/users/alice")
            assert resp.status_code == 200
            assert resp.json() == dummy_out.model_dump(by_alias=True)
            mock_users.get_user.assert_called_once_with(username="alice")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_user_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.get("/fab/v1/users/alice")
            assert resp.status_code == 403
            mock_users.get_user.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_user_not_found(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr
        mock_users.get_user.side_effect = HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The User with username `nonexistent` was not found",
        )

        with as_user():
            resp = test_client.get("/fab/v1/users/nonexistent")
            assert resp.status_code == 404
            mock_users.get_user.assert_called_once_with(username="nonexistent")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_get_user_empty_username_404(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.get("/fab/v1/users/")
            assert resp.status_code == 404
            mock_users.get_user.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_update_user_success(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        dummy_out = UserResponse(
            username="alice",
            email="alice_updated@example.com",
            first_name="Alice",
            last_name="Updated",
        )
        mock_users.update_user.return_value = dummy_out

        with as_user():
            resp = test_client.patch(
                "/fab/v1/users/alice",
                json={"email": "alice_updated@example.com", "last_name": "Updated"},
            )
            assert resp.status_code == 200
            assert resp.json() == dummy_out.model_dump(by_alias=True)
            mock_users.update_user.assert_called_once_with(username="alice", body=ANY, update_mask=None)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_update_user_with_update_mask(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        dummy_out = UserResponse(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Updated",
        )
        mock_users.update_user.return_value = dummy_out

        with as_user():
            resp = test_client.patch(
                "/fab/v1/users/alice",
                json={"last_name": "Updated"},
                params={"update_mask": "last_name"},
            )
            assert resp.status_code == 200
            mock_users.update_user.assert_called_once_with(
                username="alice", body=ANY, update_mask="last_name"
            )

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_update_user_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.patch("/fab/v1/users/alice", json={"last_name": "Updated"})
            assert resp.status_code == 403
            mock_users.update_user.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_update_user_not_found(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr
        mock_users.update_user.side_effect = HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The User with username `nonexistent` was not found",
        )

        with as_user():
            resp = test_client.patch("/fab/v1/users/nonexistent", json={"last_name": "Updated"})
            assert resp.status_code == 404
            mock_users.update_user.assert_called_once()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_update_user_unknown_update_mask(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr
        mock_users.update_user.side_effect = HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unknown update masks: 'invalid_field'",
        )

        with as_user():
            resp = test_client.patch(
                "/fab/v1/users/alice",
                json={"last_name": "Updated"},
                params={"update_mask": "invalid_field"},
            )
            assert resp.status_code == 400
            mock_users.update_user.assert_called_once()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_delete_user_success(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_users.delete_user.return_value = None
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.delete("/fab/v1/users/alice")
            assert resp.status_code == 204
            mock_users.delete_user.assert_called_once_with(username="alice")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_delete_user_forbidden(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.delete("/fab/v1/users/alice")
            assert resp.status_code == 403
            mock_users.delete_user.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_delete_user_not_found(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr
        mock_users.delete_user.side_effect = HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The User with username `nonexistent` was not found",
        )

        with as_user():
            resp = test_client.delete("/fab/v1/users/nonexistent")
            assert resp.status_code == 404
            mock_users.delete_user.assert_called_once_with(username="nonexistent")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users.get_application_builder",
        return_value=_noop_cm(),
    )
    def test_delete_user_empty_username_404(
        self, mock_get_application_builder, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.delete("/fab/v1/users/")
            assert resp.status_code == 404
            mock_users.delete_user.assert_not_called()
