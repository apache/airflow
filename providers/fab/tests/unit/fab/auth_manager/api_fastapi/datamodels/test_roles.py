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

import types

import pytest
from pydantic import ValidationError

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import (
    ActionResourceResponse,
    ActionResponse,
    ResourceResponse,
    RoleBody,
    RoleCollectionResponse,
    RoleResponse,
)


class TestRoleModels:
    def test_rolebody_accepts_actions_alias_and_maps_to_permissions(self):
        data = {
            "name": "viewer",
            "actions": [
                {"action": {"name": "can_read"}, "resource": {"name": "DAG"}},
                {"action": {"name": "can_read"}, "resource": {"name": "Connection"}},
            ],
        }
        body = RoleBody.model_validate(data)
        assert body.name == "viewer"
        # Field(validation_alias="actions") should populate `permissions`
        assert len(body.permissions) == 2
        assert body.permissions[0].action.name == "can_read"
        assert body.permissions[0].resource.name == "DAG"

    def test_rolebody_defaults_permissions_to_empty_when_actions_missing(self):
        body = RoleBody.model_validate({"name": "empty"})
        assert body.name == "empty"
        assert body.permissions == []

    def test_rolebody_name_min_length_enforced(self):
        with pytest.raises(ValidationError):
            RoleBody.model_validate({"name": "", "actions": []})

    def test_roleresponse_serializes_permissions_under_actions_alias(self):
        ar = ActionResourceResponse(
            action=ActionResponse(name="can_read"),
            resource=ResourceResponse(name="DAG"),
        )
        rr = RoleResponse(name="viewer", permissions=[ar])

        dumped = rr.model_dump(by_alias=True)
        # Field(serialization_alias="actions") should rename `permissions` -> `actions`
        assert "actions" in dumped
        assert "permissions" not in dumped
        assert dumped["name"] == "viewer"
        assert dumped["actions"][0]["action"]["name"] == "can_read"
        assert dumped["actions"][0]["resource"]["name"] == "DAG"

    def test_roleresponse_model_validate_from_simple_namespace(self):
        # Service returns plain objects; ensure model_validate handles them
        obj = types.SimpleNamespace(
            name="viewer",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="can_read"),
                    resource=types.SimpleNamespace(name="DAG"),
                )
            ],
        )
        rr = RoleResponse.model_validate(obj)
        assert rr.name == "viewer"
        assert rr.permissions
        first = rr.permissions[0]
        assert first.action.name == "can_read"

    def test_rolecollection_response_dump_and_counts(self):
        ar = ActionResourceResponse(
            action=ActionResponse(name="can_read"),
            resource=ResourceResponse(name="DAG"),
        )
        rc = RoleCollectionResponse(
            roles=[RoleResponse(name="viewer", permissions=[ar])],
            total_entries=1,
        )
        dumped = rc.model_dump(by_alias=True)
        assert dumped["total_entries"] == 1
        assert isinstance(dumped["roles"], list)
        assert dumped["roles"][0]["name"] == "viewer"
        assert "actions" in dumped["roles"][0]
        assert "permissions" not in dumped["roles"][0]

    def test_rolecollection_model_validate_from_objects(self):
        obj = types.SimpleNamespace(
            roles=[
                types.SimpleNamespace(
                    name="admin",
                    permissions=[
                        types.SimpleNamespace(
                            action=types.SimpleNamespace(name="can_read"),
                            resource=types.SimpleNamespace(name="DAG"),
                        )
                    ],
                )
            ],
            total_entries=1,
        )
        rc = RoleCollectionResponse.model_validate(obj)
        assert rc.total_entries == 1
        assert len(rc.roles) == 1
        assert rc.roles[0].name == "admin"
        assert rc.roles[0].permissions[0].action.name == "can_read"

    def test_rolecollection_missing_total_entries_raises(self):
        with pytest.raises(ValidationError):
            RoleCollectionResponse.model_validate({"roles": []})
