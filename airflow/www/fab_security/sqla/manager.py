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

import logging
from typing import List, Optional, Set, Tuple

from flask_appbuilder import const as c
from flask_appbuilder.models.sqla import Base
from flask_appbuilder.models.sqla.interface import SQLAInterface
from sqlalchemy import and_, func, literal
from sqlalchemy.engine.reflection import Inspector
from werkzeug.security import generate_password_hash

from airflow.security import permissions
from airflow.www.fab_security.auth_manager import AuthorizationManager
from airflow.www.fab_security.manager import BaseSecurityManager
from airflow.www.fab_security.sqla.models import (
    Action,
    Permission,
    RegisterUser,
    Resource,
    Role,
    User,
    assoc_permission_role,
)

log = logging.getLogger(__name__)


class SecurityManager(BaseSecurityManager):
    """
    Responsible for authentication, registering security views,
    role and permission auto management

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

    def __init__(self, appbuilder):
        """
        Class constructor
        param appbuilder:
            F.A.B AppBuilder main object
        """
        super().__init__(appbuilder)
        user_datamodel = SQLAInterface(self.user_model)
        if self.auth_type == c.AUTH_DB:
            self.userdbmodelview.datamodel = user_datamodel
        elif self.auth_type == c.AUTH_LDAP:
            self.userldapmodelview.datamodel = user_datamodel
        elif self.auth_type == c.AUTH_OID:
            self.useroidmodelview.datamodel = user_datamodel
        elif self.auth_type == c.AUTH_OAUTH:
            self.useroauthmodelview.datamodel = user_datamodel
        elif self.auth_type == c.AUTH_REMOTE_USER:
            self.userremoteusermodelview.datamodel = user_datamodel

        if self.userstatschartview:
            self.userstatschartview.datamodel = user_datamodel
        if self.auth_user_registration:
            self.registerusermodelview.datamodel = SQLAInterface(self.registeruser_model)

        self.rolemodelview.datamodel = SQLAInterface(self.role_model)
        self.actionmodelview.datamodel = SQLAInterface(self.action_model)
        self.resourcemodelview.datamodel = SQLAInterface(self.resource_model)
        self.permissionmodelview.datamodel = SQLAInterface(self.permission_model)
        self.create_db()

    @property
    def get_session(self):
        return self.appbuilder.get_session

    def create_db(self):
        try:
            engine = self.get_session.get_bind(mapper=None, clause=None)
            inspector = Inspector.from_engine(engine)
            if "ab_user" not in inspector.get_table_names():
                log.info(c.LOGMSG_INF_SEC_NO_DB)
                Base.metadata.create_all(engine)
                log.info(c.LOGMSG_INF_SEC_ADD_DB)
            super().create_db()
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_CREATE_DB.format(str(e)))
            exit(1)

    def add_register_user(self, username, first_name, last_name, email, password="", hashed_password=""):
        """
        Add a registration request for the user.

        :rtype : RegisterUser
        """
        return self.auth.add_register_user(username, first_name, last_name, email, password, hashed_password)

    def find_register_user(self, registration_hash):
        return self.auth.find_register_user(registration_hash)

    def del_register_user(self, register_user):
        """
        Deletes registration object from database

        :param register_user: RegisterUser object to delete
        """
        return self.auth.del_register_user(register_user)

    def add_user(
        self,
        username,
        first_name,
        last_name,
        email,
        role,
        password="",
        hashed_password="",
    ):
        """Generic function to create user"""
        return self.auth.add_user(username, first_name, last_name, email, role, password, hashed_password)

    def get_user_by_id(self, pk):
        return self.auth.get_user_by_id(pk)

    def find_user(self, username=None, email=None):
        """Finds user by username or email"""
        if username:
            case_sensitive = not self.auth_username_ci
            return self.auth.get_user_by_username(username, case_sensitive=case_sensitive)
        elif email:
            return self.auth.get_user_by_username(email)

    def get_all_users(self):
        return self.auth.get_all_users()

    def update_user(self, user):
        return self.auth.update_user(user)

    def count_users(self):
        return self.auth.count_users()

    def add_role(self, name: str) -> Optional[Role]:
        role = self.auth.add_role(name)
        if role.name != self.auth_role_public:
            website_perm = self.create_permission(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)
            self.add_permission_to_role(role, website_perm)
        return role

    def find_role(self, name):
        return self.auth.get_role(name)

    def update_role(self, role_id, name: str) -> Optional[Role]:
        return self.auth.update_role(role_id, name)

    def delete_role(self, name):
        return self.auth.delete_role(name)

    def get_all_roles(self):
        return self.auth.get_all_roles()

    def create_action(self, name):
        """
        Adds an action to the backend, model action

        :param name:
            name of the action: 'can_add','can_edit' etc...
        """
        return self.auth.create_action(name)

    def get_action(self, name: str) -> Action:
        """
        Gets an existing action record.

        :param name: name
        :type name: str
        :return: Action record, if it exists
        :rtype: Action
        """
        return self.auth.get_action(name)

    def delete_action(self, name: str) -> bool:
        """
        Deletes a permission action.

        :param name: Name of action to delete (e.g. can_read).
        :type name: str
        :return: Whether or not delete was successful.
        :rtype: bool
        """
        return self.auth.delete_action(name)

    def create_resource(self, name) -> Resource:
        """
        Create a resource with the given name.

        :param name: The name of the resource to create created.
        :type name: str
        :return: The FAB resource created.
        :rtype: Resource
        """
        return self.auth.create_resource(name=name)

    def get_resource(self, name: str) -> Resource:
        """
        Returns a resource record by name, if it exists.

        :param name: Name of resource
        :type name: str
        :return: Resource record
        :rtype: Resource
        """
        return self.auth.get_resource(name=name)

    def delete_resource(self, name: str) -> bool:
        """
        Deletes a Resource from the backend

        :param name:
            name of the resource
        """
        return self.auth.delete_resource(name=name)

    def get_all_resources(self) -> List[Resource]:
        """
        Gets all existing resource records.

        :return: List of all resources
        :rtype: List[Resource]
        """
        return self.auth.get_all_resources()

    def create_permission(self, action_name, resource_name):
        """
        Adds a permission on a resource to the backend

        :param action_name:
            name of the action to add: 'can_add','can_edit' etc...
        :param resource_name:
            name of the resource to add
        """
        return self.auth.create_permission(action_name, resource_name)

    def get_permission(self, action_name: str, resource_name: str) -> Permission:
        """
        Gets a permission made with the given action->resource pair, if the permission already exists.

        :param action_name: Name of action
        :type action_name: str
        :param resource_name: Name of resource
        :type resource_name: str
        :return: The existing permission
        :rtype: Permission
        """
        return self.auth.get_permission(action_name, resource_name)

    def get_all_permissions(self) -> Set[Tuple[str, str]]:
        """Returns all permissions as a set of tuples with the action and resource names"""
        return self.auth.get_all_permissions()

    def delete_permission(self, action_name: str, resource_name: str) -> None:
        """
        Deletes the permission linking an action->resource pair. Doesn't delete the
        underlying action or resource.

        :param action_name: Name of existing action
        :type action_name: str
        :param resource_name: Name of existing resource
        :type resource_name: str
        :return: None
        :rtype: None
        """
        return self.auth.delete_permission(action_name, resource_name)

    def add_permission_to_role(self, role: Role, permission: Permission) -> None:
        """
        Add an existing permission pair to a role.

        :param role: The role about to get a new permission.
        :type role: Role
        :param permission: The permission pair to add to a role.
        :type permission: Permission
        :return: None
        :rtype: None
        """
        return self.auth.add_permission_to_role(role, permission)

    def remove_permission_from_role(self, role: Role, permission: Permission) -> None:
        """
        Remove a permission pair from a role.

        :param role: User role containing permissions.
        :type role: Role
        :param permission: Object representing resource-> action pair
        :type permission: Permission
        """
        return self.auth.remove_permission_from_role(role, permission)
