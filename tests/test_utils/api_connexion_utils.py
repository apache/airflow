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

from contextlib import contextmanager
from typing import TYPE_CHECKING

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from tests.test_utils.compat import ignore_provider_compatibility_error

with ignore_provider_compatibility_error("2.9.0+", __file__):
    from airflow.providers.fab.auth_manager.security_manager.override import EXISTING_ROLES

if TYPE_CHECKING:
    from flask import Flask


@contextmanager
def create_test_client(app, user_name, role_name, permissions):
    """
    Helper function to create a client with a temporary user which will be deleted once done
    """
    client = app.test_client()
    with create_user_scope(app, username=user_name, role_name=role_name, permissions=permissions) as _:
        resp = client.post("/login/", data={"username": user_name, "password": user_name})
        assert resp.status_code == 302
        yield client


@contextmanager
def create_user_scope(app, username, **kwargs):
    """
    Helper function designed to be used with pytest fixture mainly.
    It will create a user and provide it for the fixture via YIELD (generator)
    then will tidy up once test is complete
    """
    test_user = create_user(app, username, **kwargs)

    try:
        yield test_user
    finally:
        delete_user(app, username)


def create_user(app: Flask, username: str, role_name: str | None):
    # Removes user and role so each test has isolated test data.
    delete_user(app, username)

    users = app.config.get("SIMPLE_AUTH_MANAGER_USERS", [])
    users.append(
        {
            "username": username,
            "role": role_name,
        }
    )

    app.config["SIMPLE_AUTH_MANAGER_USERS"] = users


def create_role(app, name, permissions=None):
    appbuilder = app.appbuilder
    role = appbuilder.sm.find_role(name)
    if not role:
        role = appbuilder.sm.add_role(name)
    if not permissions:
        permissions = []
    for permission in permissions:
        perm_object = appbuilder.sm.get_permission(*permission)
        appbuilder.sm.add_permission_to_role(role, perm_object)
    return role


def delete_role(app, name):
    if name not in EXISTING_ROLES:
        if app.appbuilder.sm.find_role(name):
            app.appbuilder.sm.delete_role(name)


def delete_roles(app):
    for role in app.appbuilder.sm.get_all_roles():
        delete_role(app, role.name)


def delete_user(app: Flask, username):
    users = app.config.get("SIMPLE_AUTH_MANAGER_USERS", [])
    users = [user for user in users if user["username"] != username]

    app.config["SIMPLE_AUTH_MANAGER_USERS"] = users


def assert_401(response):
    assert response.status_code == 401, f"Current code: {response.status_code}"
    assert response.json == {
        "detail": None,
        "status": 401,
        "title": "Unauthorized",
        "type": EXCEPTIONS_LINK_MAP[401],
    }
