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

import logging

from sqlalchemy import and_, literal

from airflow.auth.managers.fab.models import (
    Action,
    Permission,
    RegisterUser,
    Resource,
    Role,
    User,
    assoc_permission_role,
)
from airflow.www.fab_security.manager import BaseSecurityManager

log = logging.getLogger(__name__)


class SecurityManager(BaseSecurityManager):
    """
    Responsible for authentication, registering security views, role and permission auto management.

    If you want to change anything just inherit and override, then
    pass your own security manager to AppBuilder.
    """

    user_model = User
    """ Override to set your own User Model """
    role_model = Role
    """ Override to set your own Role Model """
    action_model = Action
    resource_model = Resource
    permission_model = Permission
    registeruser_model = RegisterUser

    def __init__(self, appbuilder, **kwargs):
        """
        Class constructor.

        :param appbuilder: F.A.B AppBuilder main object
        """
        super().__init__(appbuilder)

    @property
    def get_session(self):
        return self.appbuilder.get_session

    def permission_exists_in_one_or_more_roles(
        self, resource_name: str, action_name: str, role_ids: list[int]
    ) -> bool:
        """
        Efficiently check if a certain permission exists on a list of role ids; used by `has_access`.

        :param resource_name: The view's name to check if exists on one of the roles
        :param action_name: The permission name to check if exists
        :param role_ids: a list of Role ids
        :return: Boolean
        """
        q = (
            self.appbuilder.get_session.query(self.permission_model)
            .join(
                assoc_permission_role,
                and_(self.permission_model.id == assoc_permission_role.c.permission_view_id),
            )
            .join(self.role_model)
            .join(self.action_model)
            .join(self.resource_model)
            .filter(
                self.resource_model.name == resource_name,
                self.action_model.name == action_name,
                self.role_model.id.in_(role_ids),
            )
            .exists()
        )
        # Special case for MSSQL/Oracle (works on PG and MySQL > 8)
        if self.appbuilder.get_session.bind.dialect.name in ("mssql", "oracle"):
            return self.appbuilder.get_session.query(literal(True)).filter(q).scalar()
        return self.appbuilder.get_session.query(q).scalar()

    def perms_include_action(self, perms, action_name):
        return any(perm.action and perm.action.name == action_name for perm in perms)
