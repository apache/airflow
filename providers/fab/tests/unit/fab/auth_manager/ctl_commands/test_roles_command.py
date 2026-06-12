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
from argparse import Namespace
from unittest.mock import MagicMock

import pytest

from airflow.providers.fab.auth_manager.ctl_commands import roles_command


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


class TestListRoles:
    def test_calls_api_with_pagination(self, fake_client):
        fake_client.get.return_value = fake_client._resp({"roles": [{"name": "Admin"}]})
        roles_command.list_roles(
            _ns(limit=20, offset=0, order_by="name", output="json"),
            api_client=fake_client,
        )
        fake_client.get.assert_called_once_with(
            "/fab/v1/roles", params={"limit": 20, "offset": 0, "order_by": "name"}
        )


class TestGetRole:
    def test_calls_api_with_name(self, fake_client):
        fake_client.get.return_value = fake_client._resp({"name": "Admin"})
        roles_command.get_role(_ns(name="Admin", output="json"), api_client=fake_client)
        fake_client.get.assert_called_once_with("/fab/v1/roles/Admin")


class TestCreateRole:
    def test_posts_with_empty_actions_when_none_given(self, fake_client):
        fake_client.post.return_value = fake_client._resp({"name": "Op"})
        roles_command.create_role(_ns(name="Op", actions=None, output="json"), api_client=fake_client)
        fake_client.post.assert_called_once_with("/fab/v1/roles", json={"name": "Op", "actions": []})

    def test_posts_with_parsed_actions_json(self, fake_client):
        actions = [{"action": {"name": "can_read"}, "resource": {"name": "Dag"}}]
        fake_client.post.return_value = fake_client._resp({})
        roles_command.create_role(
            _ns(name="Op", actions=json.dumps(actions), output="json"),
            api_client=fake_client,
        )
        fake_client.post.assert_called_once_with("/fab/v1/roles", json={"name": "Op", "actions": actions})


class TestUpdateRole:
    def test_renames_role(self, fake_client):
        fake_client.patch.return_value = fake_client._resp({})
        roles_command.update_role(
            _ns(name="Op", new_name="Operator", actions=None, update_mask=None, output="json"),
            api_client=fake_client,
        )
        fake_client.patch.assert_called_once_with(
            "/fab/v1/roles/Op",
            json={"name": "Operator"},
            params={"update_mask": "name"},
        )

    def test_replaces_actions(self, fake_client):
        new_actions = [{"action": {"name": "can_edit"}, "resource": {"name": "Dag"}}]
        fake_client.patch.return_value = fake_client._resp({})
        roles_command.update_role(
            _ns(
                name="Op",
                new_name=None,
                actions=json.dumps(new_actions),
                update_mask=None,
                output="json",
            ),
            api_client=fake_client,
        )
        fake_client.patch.assert_called_once_with(
            "/fab/v1/roles/Op",
            json={"name": "Op", "actions": new_actions},
            params={"update_mask": "actions"},
        )

    def test_no_fields_does_not_call_api(self, fake_client):
        roles_command.update_role(
            _ns(name="Op", new_name=None, actions=None, update_mask=None, output="json"),
            api_client=fake_client,
        )
        fake_client.patch.assert_not_called()


class TestDeleteRole:
    def test_calls_delete(self, fake_client):
        roles_command.delete_role(_ns(name="Op"), api_client=fake_client)
        fake_client.delete.assert_called_once_with("/fab/v1/roles/Op")
