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

    def test_userresponse_coerces_naive_datetimes_to_utc(self):
        utc_created = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        resp = UserResponse.model_validate(
            {
                "username": "alice",
                "email": "alice@example.com",
                "first_name": "Alice",
                "last_name": "Liddell",
                "created_on": utc_created,
            }
        )
        assert resp.created_on is not None
        assert resp.created_on.utcoffset() == timedelta(0)
        assert resp.created_on.replace(tzinfo=None) == utc_created.replace(tzinfo=None)

    def test_userresponse_preserves_aware_datetimes(self):
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
        expected_utc = aware.astimezone(timezone.utc)
        assert resp.changed_on.utcoffset() == timedelta(0)
        assert resp.changed_on.timestamp() == expected_utc.timestamp()

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
