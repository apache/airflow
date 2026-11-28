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

from unittest.mock import ANY, MagicMock, patch

import pytest
from fastapi import HTTPException, status

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.users import (
    UserCollectionResponse,
    UserResponse,
)
from airflow.providers.fab.www.extensions.init_appbuilder import AirflowAppBuilder

from unit.fab.auth_manager.test_utils import create_user


@pytest.mark.db_test
class TestUsers:
    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_create_user_ok(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_create_user_forbidden(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_create_user_validation_422_empty_username(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_create_user_validation_422_missing_username(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_create_user_validation_422_missing_password(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_users_success_defaults(
        self,
        conf_mock,
        mock_get_flask_app,
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
        mock_get_flask_app.return_value.app_context.assert_called_once()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.users.FABAuthManagerUsers")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_users_with_params(
        self,
        conf_mock,
        mock_get_flask_app,
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_get_users_forbidden(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_get_users_validation_422_negative_offset(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_get_user_success(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_get_user_forbidden(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_get_user_not_found(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_get_user_empty_username_404(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_update_user_success(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_update_user_with_update_mask(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_update_user_forbidden(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_update_user_not_found(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_update_user_unknown_update_mask(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_delete_user_success(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_delete_user_forbidden(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_delete_user_not_found(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
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
        "airflow.providers.fab.auth_manager.api_fastapi.routes.users._get_flask_app",
    )
    def test_delete_user_empty_username_404(
        self, mock_get_flask_app, mock_get_auth_manager, mock_users, test_client, as_user
    ):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        mock_get_auth_manager.return_value = mgr

        with as_user():
            resp = test_client.delete("/fab/v1/users/")
            assert resp.status_code == 404
            mock_users.delete_user.assert_not_called()

    def test_requests_reuse_the_auth_manager_flask_app(self, real_app_client):
        auth_manager, client = real_app_client
        flask_app, appbuilder = auth_manager.flask_app, auth_manager.appbuilder

        with patch.object(
            AirflowAppBuilder, "init_app", autospec=True, side_effect=AirflowAppBuilder.init_app
        ) as init_app:
            responses = [client.get("/fab/v1/users") for _ in range(2)]

        init_app.assert_not_called()
        assert auth_manager.flask_app is flask_app
        assert auth_manager.appbuilder is appbuilder
        assert [response.status_code for response in responses] == [200, 200]
        assert "users" in responses[1].json()


@pytest.mark.db_test
class TestPatchUserWithDatabase:
    """
    Run ``PATCH /users`` against the real ``find_user`` and database.

    The tests above mock the service, and the service tests mock ``find_user``, so neither sees
    how ``find_user`` actually matches, e.g. usernames ignoring case.
    """

    ALICE = "patch_db_alice"
    ALICE_EMAIL = "patch_db_alice@example.com"
    CAROL = "Patch_DB_Carol"
    CAROL_EMAIL = "patch_db_carol@example.com"
    USERS = {ALICE: ALICE_EMAIL, CAROL: CAROL_EMAIL}

    @pytest.fixture
    def db_client(self, real_app_client):
        auth_manager, client = real_app_client
        app = auth_manager.flask_app
        # Outside an app context, add_user logs an error and returns None instead of raising.
        with app.app_context():
            sm = app.appbuilder.sm
            self._cleanup(sm)
            for username, email in self.USERS.items():
                assert create_user(app, username, email=email) is not None
            yield client, sm
            sm.session.rollback()
            self._cleanup(sm)

    def _cleanup(self, sm):
        # find_user ignores case, so this also finds a user a test renamed to a different case.
        for username in self.USERS:
            if user := sm.find_user(username=username):
                sm.del_register_user(user)

    @pytest.mark.parametrize(
        ("path_username", "body"),
        [
            pytest.param(ALICE, {"username": "Patch_DB_Alice"}, id="case-only-rename"),
            pytest.param(
                "patch_db_carol",
                {"username": CAROL, "first_name": "Caro"},
                id="path-in-lower-case-keeps-stored-name",
            ),
        ],
    )
    def test_patch_keeping_own_username_is_not_a_conflict(self, db_client, path_username, body):
        client, sm = db_client

        response = client.patch(f"/fab/v1/users/{path_username}", json=body)

        assert response.status_code == 200
        user = sm.find_user(username=path_username)
        assert {field: getattr(user, field) for field in body} == body

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param({"username": "patch_db_carol"}, id="username-of-another-user-in-other-case"),
            pytest.param({"email": CAROL_EMAIL}, id="email-of-another-user"),
        ],
    )
    def test_patch_to_another_users_value_is_a_conflict(self, db_client, body):
        client, sm = db_client

        response = client.patch(f"/fab/v1/users/{self.ALICE}", json=body)

        assert response.status_code == 409
        users = [sm.find_user(username=username) for username in self.USERS]
        assert {user.username: user.email for user in users} == self.USERS

    def test_patch_ignores_conflicts_outside_the_update_mask(self, db_client):
        client, sm = db_client

        response = client.patch(
            f"/fab/v1/users/{self.ALICE}",
            params={"update_mask": "first_name"},
            json={"email": self.CAROL_EMAIL, "first_name": "Alicia"},
        )

        assert response.status_code == 200
        user = sm.find_user(username=self.ALICE)
        assert (user.first_name, user.email) == ("Alicia", self.ALICE_EMAIL)
