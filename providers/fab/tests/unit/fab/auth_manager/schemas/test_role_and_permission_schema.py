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

import pytest

from airflow.providers.fab.auth_manager.schemas.role_and_permission_schema import (
    RoleCollection,
    role_collection_schema,
    role_schema,
)
from airflow.providers.fab.www.security import permissions

from unit.fab.auth_manager.api_endpoints.api_connexion_utils import create_role, delete_role

pytestmark = pytest.mark.db_test


class TestRoleCollectionItemSchema:
    @pytest.fixture(scope="class")
    def role(self, minimal_app_for_auth_api):
        with minimal_app_for_auth_api.app_context():
            yield create_role(
                minimal_app_for_auth_api,
                name="Test",
                permissions=[
                    (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
                ],
            )
            delete_role(minimal_app_for_auth_api, "Test")

    @pytest.fixture(autouse=True)
    def _set_attrs(self, minimal_app_for_auth_api, role):
        self.app = minimal_app_for_auth_api
        self.role = role

    def test_serialize(self):
        deserialized_role = role_schema.dump(self.role)
        assert deserialized_role == {
            "name": "Test",
            "actions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
        }

    def test_deserialize(self):
        role = {
            "name": "Test",
            "actions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
        }
        role_obj = role_schema.load(role)
        assert role_obj == {
            "name": "Test",
            "permissions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
        }


class TestRoleCollectionSchema:
    def test_serialize(self, minimal_app_for_auth_api):
        with minimal_app_for_auth_api.app_context():
            role1 = create_role(
                minimal_app_for_auth_api,
                name="Test1",
                permissions=[
                    (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
                ],
            )
            role2 = create_role(
                minimal_app_for_auth_api,
                name="Test2",
                permissions=[
                    (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
                ],
            )

            instance = RoleCollection([role1, role2], total_entries=2)
            deserialized = role_collection_schema.dump(instance)
            assert deserialized == {
                "roles": [
                    {
                        "name": "Test1",
                        "actions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
                    },
                    {
                        "name": "Test2",
                        "actions": [{"resource": {"name": "DAGs"}, "action": {"name": "can_edit"}}],
                    },
                ],
                "total_entries": 2,
            }

            delete_role(minimal_app_for_auth_api, "Test1")
            delete_role(minimal_app_for_auth_api, "Test2")
