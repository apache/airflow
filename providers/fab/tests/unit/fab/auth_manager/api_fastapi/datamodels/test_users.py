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
import types
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import SecretStr, ValidationError

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import Role
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.users import (
    UserBody,
    UserCollectionResponse,
    UserPatchBody,
    UserResponse,
)


class TestUserModels:
    def test_userbody_accepts_role_dicts_and_Roles_and_secretstr(self):
        data = {
            "username": "alice",
            "email": "alice@example.com",
            "first_name": "Alice",
            "last_name": "Liddell",
            "password": "s3cr3t",  # should coerce into SecretStr
            "roles": [{"name": "Admin"}, Role(name="User")],
        }
        body = UserBody.model_validate(data)
        assert body.username == "alice"
        assert body.email == "alice@example.com"
        assert isinstance(body.password, SecretStr)
        assert body.password.get_secret_value() == "s3cr3t"
        assert body.roles is not None
        assert [r.name for r in body.roles] == ["Admin", "User"]

        # SecretStr should be masked on JSON serialization by default
        dumped_json = body.model_dump_json()
        payload = json.loads(dumped_json)
        assert payload["password"] == "**********"

    def test_userbody_roles_default_none_when_omitted(self):
        body = UserBody.model_validate(
            {
                "username": "bob",
                "email": "bob@example.com",
                "first_name": "Bob",
                "last_name": "Builder",
                "password": "pw",
            }
        )
        assert body.roles is None

    @pytest.mark.parametrize(
        "patch",
        [
            {"username": ""},  # min_length=1
            {"email": ""},  # min_length=1
            {"first_name": ""},  # min_length=1
            {"last_name": ""},  # min_length=1
        ],
    )
    def test_userbody_min_length_enforced(self, patch):
        base = {
            "username": "ok",
            "email": "ok@example.com",
            "first_name": "OK",
            "last_name": "User",
            "password": "pw",
        }
        base.update(patch)
        with pytest.raises(ValidationError):
            UserBody.model_validate(base)

    def test_userbody_password_is_required(self):
        with pytest.raises(ValidationError):
            UserBody.model_validate(
                {
                    "username": "no-pass",
                    "email": "np@example.com",
                    "first_name": "No",
                    "last_name": "Pass",
                }
            )

    def test_userresponse_accepts_naive_datetimes(self):
        naive_created = datetime(2025, 1, 2, 3, 4, 5)
        resp = UserResponse.model_validate(
            {
                "username": "alice",
                "email": "alice@example.com",
                "first_name": "Alice",
                "last_name": "Liddell",
                "created_on": naive_created,
            }
        )
        assert resp.created_on is not None
        assert resp.created_on.tzinfo is None
        assert resp.created_on == naive_created

    def test_userresponse_accepts_aware_datetimes(self):
        aware = datetime(2024, 12, 1, 9, 30, tzinfo=timezone(timedelta(hours=9)))
        resp = UserResponse.model_validate(
            {
                "username": "bob",
                "email": "bob@example.com",
                "first_name": "Bob",
                "last_name": "Builder",
                "changed_on": aware,
            }
        )
        assert resp.changed_on == aware

    def test_userresponse_model_validate_from_simple_namespace(self):
        obj = types.SimpleNamespace(
            username="eve",
            email="eve@example.com",
            first_name="Eve",
            last_name="Adams",
            roles=[types.SimpleNamespace(name="Viewer")],
            active=True,
            login_count=10,
        )
        resp = UserResponse.model_validate(obj)
        assert resp.username == "eve"
        assert resp.roles is not None
        assert resp.roles[0].name == "Viewer"
        assert resp.active is True
        assert resp.login_count == 10

    def test_userpatchbody_all_fields_optional(self):
        """UserPatchBody should accept an empty payload (all fields optional)."""
        body = UserPatchBody.model_validate({})
        assert body.username is None
        assert body.email is None
        assert body.first_name is None
        assert body.last_name is None
        assert body.roles is None
        assert body.password is None

    def test_userpatchbody_partial_update(self):
        """UserPatchBody should accept partial updates."""
        body = UserPatchBody.model_validate({"last_name": "Updated"})
        assert body.last_name == "Updated"
        assert body.username is None
        assert body.email is None

    def test_userpatchbody_password_coerces_to_secretstr(self):
        """Password field should coerce to SecretStr when provided."""
        body = UserPatchBody.model_validate({"password": "newpass"})
        assert isinstance(body.password, SecretStr)
        assert body.password.get_secret_value() == "newpass"

    def test_userpatchbody_roles_accepts_dicts_and_role_objects(self):
        """Roles field should accept both dicts and Role objects."""
        body = UserPatchBody.model_validate({"roles": [{"name": "Admin"}, Role(name="User")]})
        assert body.roles is not None
        assert [r.name for r in body.roles] == ["Admin", "User"]

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("username", ""),
            ("email", ""),
            ("first_name", ""),
            ("last_name", ""),
        ],
    )
    def test_userpatchbody_min_length_enforced(self, field, value):
        """Non-null string fields should enforce min_length=1."""
        with pytest.raises(ValidationError):
            UserPatchBody.model_validate({field: value})

    def test_usercollectionresponse_structure(self):
        """UserCollectionResponse should contain users list and total_entries."""
        resp = UserCollectionResponse.model_validate(
            {
                "users": [
                    {
                        "username": "alice",
                        "email": "alice@example.com",
                        "first_name": "Alice",
                        "last_name": "Liddell",
                    }
                ],
                "total_entries": 1,
            }
        )
        assert resp.total_entries == 1
        assert len(resp.users) == 1
        assert resp.users[0].username == "alice"

    def test_usercollectionresponse_empty_users(self):
        """UserCollectionResponse should handle empty users list."""
        resp = UserCollectionResponse.model_validate(
            {
                "users": [],
                "total_entries": 0,
            }
        )
        assert resp.total_entries == 0
        assert resp.users == []

    def test_usercollectionresponse_multiple_users(self):
        """UserCollectionResponse should handle multiple users."""
        resp = UserCollectionResponse.model_validate(
            {
                "users": [
                    {"username": "alice", "email": "a@b.com", "first_name": "A", "last_name": "L"},
                    {"username": "bob", "email": "b@b.com", "first_name": "B", "last_name": "B"},
                ],
                "total_entries": 100,
            }
        )
        assert resp.total_entries == 100
        assert len(resp.users) == 2
