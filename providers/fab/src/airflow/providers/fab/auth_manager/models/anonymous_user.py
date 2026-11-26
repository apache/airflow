#
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

from typing import TYPE_CHECKING

from flask import current_app
from flask_login import AnonymousUserMixin

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.models import Role


class AnonymousUser(AnonymousUserMixin, BaseUser):
    """User object used when no active user is logged in."""

    # ``None`` means not loaded yet; an empty set means loaded with nothing granted, so a
    # missing public role does not fall back to ``current_app``, which FastAPI does not have.
    _roles: set[Role] | None = None
    _perms: set[tuple[str, str]] | None = None

    first_name = "Anonymous"
    last_name = ""

    @property
    def roles(self):
        if self._roles is None:
            public_role = current_app.config.get("AUTH_ROLE_PUBLIC", None)
            role = current_app.appbuilder.sm.find_role(public_role) if public_role else None
            self._roles = {role} if role is not None else set()
        return list(self._roles)

    @roles.setter
    def roles(self, roles):
        self._roles = roles
        self._perms = None

    @property
    def groups(self):
        return []

    @property
    def perms(self):
        if self._perms is None:
            self._perms = {
                (perm.action.name, perm.resource.name) for role in self.roles for perm in role.permissions
            }
        return self._perms

    def get_name(self) -> str:
        return "Anonymous"
