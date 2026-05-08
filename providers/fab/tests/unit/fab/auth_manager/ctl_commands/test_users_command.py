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

from argparse import Namespace
from unittest.mock import MagicMock

import pytest

from airflow.providers.fab.auth_manager.ctl_commands import users_command


@pytest.fixture
def fake_client():
    client = MagicMock(name="api_client")

    def _resp(payload):
        r = MagicMock()
        r.json.return_value = payload
        return r

    client._resp = _resp
    return client


def _ns(**kwargs):
    return Namespace(**kwargs)


class TestListUsers:
    def test_calls_api_with_pagination(self, fake_client):
        fake_client.get.return_value = fake_client._resp({"users": [{"username": "u"}], "total_entries": 1})
        users_command.list_users(
            _ns(limit=50, offset=10, order_by="-id", output="json"),
            api_client=fake_client,
        )
        fake_client.get.assert_called_once_with(
            "/fab/v1/users", params={"limit": 50, "offset": 10, "order_by": "-id"}
        )


class TestGetUser:
    def test_calls_api_with_username_in_path(self, fake_client):
        fake_client.get.return_value = fake_client._resp({"username": "alice"})
        users_command.get_user(_ns(username="alice", output="json"), api_client=fake_client)
        fake_client.get.assert_called_once_with("/fab/v1/users/alice")


class TestCreateUser:
    def test_posts_full_body(self, fake_client):
        fake_client.post.return_value = fake_client._resp({"username": "alice"})
        users_command.create_user(
            _ns(
                username="alice",
                email="alice@example.com",
                firstname="Alice",
                lastname="Liddell",
                password="secret",
                role=["Admin"],
                output="json",
            ),
            api_client=fake_client,
        )
        fake_client.post.assert_called_once()
        endpoint, kwargs = fake_client.post.call_args
        assert endpoint == ("/fab/v1/users",)
        assert kwargs["json"] == {
            "username": "alice",
            "email": "alice@example.com",
            "first_name": "Alice",
            "last_name": "Liddell",
            "password": "secret",
            "roles": [{"name": "Admin"}],
        }

    def test_omits_roles_when_no_role_passed(self, fake_client):
        fake_client.post.return_value = fake_client._resp({})
        users_command.create_user(
            _ns(
                username="bob",
                email="bob@x",
                firstname="B",
                lastname="B",
                password="p",
                role=None,
                output="json",
            ),
            api_client=fake_client,
        )
        body = fake_client.post.call_args.kwargs["json"]
        assert "roles" not in body


class TestUpdateUser:
    def test_sends_only_supplied_fields_with_derived_mask(self, fake_client):
        fake_client.patch.return_value = fake_client._resp({"username": "alice"})
        users_command.update_user(
            _ns(
                username="alice",
                email="new@x",
                firstname=None,
                lastname=None,
                password="newpass",
                role=None,
                update_mask=None,
                output="json",
            ),
            api_client=fake_client,
        )
        fake_client.patch.assert_called_once_with(
            "/fab/v1/users/alice",
            json={"email": "new@x", "password": "newpass"},
            params={"update_mask": "email,password"},
        )

    def test_explicit_update_mask_overrides_derived(self, fake_client):
        fake_client.patch.return_value = fake_client._resp({})
        users_command.update_user(
            _ns(
                username="alice",
                email="new@x",
                firstname=None,
                lastname=None,
                password=None,
                role=None,
                update_mask="email",
                output="json",
            ),
            api_client=fake_client,
        )
        fake_client.patch.assert_called_once_with(
            "/fab/v1/users/alice",
            json={"email": "new@x"},
            params={"update_mask": "email"},
        )

    def test_no_fields_does_not_call_api(self, fake_client):
        users_command.update_user(
            _ns(
                username="alice",
                email=None,
                firstname=None,
                lastname=None,
                password=None,
                role=None,
                update_mask=None,
                output="json",
            ),
            api_client=fake_client,
        )
        fake_client.patch.assert_not_called()

    def test_role_update_sends_roles_payload(self, fake_client):
        fake_client.patch.return_value = fake_client._resp({})
        users_command.update_user(
            _ns(
                username="alice",
                email=None,
                firstname=None,
                lastname=None,
                password=None,
                role=["Admin", "User"],
                update_mask=None,
                output="json",
            ),
            api_client=fake_client,
        )
        body = fake_client.patch.call_args.kwargs["json"]
        assert body == {"roles": [{"name": "Admin"}, {"name": "User"}]}
        assert fake_client.patch.call_args.kwargs["params"] == {"update_mask": "roles"}


class TestDeleteUser:
    def test_calls_delete(self, fake_client):
        users_command.delete_user(_ns(username="alice"), api_client=fake_client)
        fake_client.delete.assert_called_once_with("/fab/v1/users/alice")
