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
import uuid

from flask_appbuilder import const as c
from flask_appbuilder.models.sqla import Base
from flask_appbuilder.models.sqla.interface import SQLAInterface
from sqlalchemy import and_, func, inspect, literal
from sqlalchemy.orm.exc import MultipleResultsFound
from werkzeug.security import generate_password_hash

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

    def register_views(self):
        super().register_views()

    def create_db(self):
        try:
            engine = self.get_session.get_bind(mapper=None, clause=None)
            inspector = inspect(engine)
            if "ab_user" not in inspector.get_table_names():
                log.info(c.LOGMSG_INF_SEC_NO_DB)
                Base.metadata.create_all(engine)
                log.info(c.LOGMSG_INF_SEC_ADD_DB)
            super().create_db()
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_CREATE_DB.format(str(e)))
            exit(1)

    def find_register_user(self, registration_hash):
        return (
            self.get_session.query(self.registeruser_model)
            .filter(self.registeruser_model.registration_hash == registration_hash)
            .scalar()
        )

    def add_register_user(self, username, first_name, last_name, email, password="", hashed_password=""):
        """
        Add a registration request for the user.

        :rtype : RegisterUser
        """
        register_user = self.registeruser_model()
        register_user.username = username
        register_user.email = email
        register_user.first_name = first_name
        register_user.last_name = last_name
        if hashed_password:
            register_user.password = hashed_password
        else:
            register_user.password = generate_password_hash(password)
        register_user.registration_hash = str(uuid.uuid1())
        try:
            self.get_session.add(register_user)
            self.get_session.commit()
            return register_user
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_ADD_REGISTER_USER.format(str(e)))
            self.appbuilder.get_session.rollback()
            return None

    def del_register_user(self, register_user):
        """
        Deletes registration object from database.

        :param register_user: RegisterUser object to delete
        """
        try:
            self.get_session.delete(register_user)
            self.get_session.commit()
            return True
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_REGISTER_USER.format(str(e)))
            self.get_session.rollback()
            return False

    def find_user(self, username=None, email=None):
        """Finds user by username or email."""
        if username:
            try:
                if self.auth_username_ci:
                    return (
                        self.get_session.query(self.user_model)
                        .filter(func.lower(self.user_model.username) == func.lower(username))
                        .one_or_none()
                    )
                else:
                    return (
                        self.get_session.query(self.user_model)
                        .filter(func.lower(self.user_model.username) == func.lower(username))
                        .one_or_none()
                    )
            except MultipleResultsFound:
                log.error("Multiple results found for user %s", username)
                return None
        elif email:
            try:
                return self.get_session.query(self.user_model).filter_by(email=email).one_or_none()
            except MultipleResultsFound:
                log.error("Multiple results found for user with email %s", email)
                return None

    def get_all_users(self):
        return self.get_session.query(self.user_model).all()

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
        """Generic function to create user."""
        try:
            user = self.user_model()
            user.first_name = first_name
            user.last_name = last_name
            user.username = username
            user.email = email
            user.active = True
            user.roles = role if isinstance(role, list) else [role]
            if hashed_password:
                user.password = hashed_password
            else:
                user.password = generate_password_hash(password)
            self.get_session.add(user)
            self.get_session.commit()
            log.info(c.LOGMSG_INF_SEC_ADD_USER.format(username))
            return user
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_ADD_USER.format(str(e)))
            self.get_session.rollback()
            return False

    def count_users(self):
        return self.get_session.query(func.count(self.user_model.id)).scalar()

    def update_user(self, user):
        try:
            self.get_session.merge(user)
            self.get_session.commit()
            log.info(c.LOGMSG_INF_SEC_UPD_USER.format(user))
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_UPD_USER.format(str(e)))
            self.get_session.rollback()
            return False

    def add_role(self, name: str) -> Role:
        role = self.find_role(name)
        if role is None:
            try:
                role = self.role_model()
                role.name = name
                self.get_session.add(role)
                self.get_session.commit()
                log.info(c.LOGMSG_INF_SEC_ADD_ROLE.format(name))
                return role
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_ROLE.format(str(e)))
                self.get_session.rollback()
        return role

    def update_role(self, role_id, name: str) -> Role | None:
        role = self.get_session.get(self.role_model, role_id)
        if not role:
            return None
        try:
            role.name = name
            self.get_session.merge(role)
            self.get_session.commit()
            log.info(c.LOGMSG_INF_SEC_UPD_ROLE.format(role))
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_UPD_ROLE.format(str(e)))
            self.get_session.rollback()
            return None
        return role

    def find_role(self, name):
        return self.get_session.query(self.role_model).filter_by(name=name).one_or_none()

    def get_all_roles(self):
        return self.get_session.query(self.role_model).all()

    def get_public_role(self):
        return self.get_session.query(self.role_model).filter_by(name=self.auth_role_public).one_or_none()

    def get_action(self, name: str) -> Action:
        """
        Gets an existing action record.

        :param name: name
        :return: Action record, if it exists
        """
        return self.get_session.query(self.action_model).filter_by(name=name).one_or_none()

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

    def filter_roles_by_perm_with_action(self, action_name: str, role_ids: list[int]):
        """Find roles with permission."""
        return (
            self.appbuilder.get_session.query(self.permission_model)
            .join(
                assoc_permission_role,
                and_(self.permission_model.id == assoc_permission_role.c.permission_view_id),
            )
            .join(self.role_model)
            .join(self.action_model)
            .join(self.resource_model)
            .filter(
                self.action_model.name == action_name,
                self.role_model.id.in_(role_ids),
            )
        ).all()

    def create_action(self, name):
        """
        Adds an action to the backend, model action.

        :param name:
            name of the action: 'can_add','can_edit' etc...
        """
        action = self.get_action(name)
        if action is None:
            try:
                action = self.action_model()
                action.name = name
                self.get_session.add(action)
                self.get_session.commit()
                return action
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_PERMISSION.format(str(e)))
                self.get_session.rollback()
        return action

    def delete_action(self, name: str) -> bool:
        """
        Deletes a permission action.

        :param name: Name of action to delete (e.g. can_read).
        :return: Whether or not delete was successful.
        """
        action = self.get_action(name)
        if not action:
            log.warning(c.LOGMSG_WAR_SEC_DEL_PERMISSION.format(name))
            return False
        try:
            perms = (
                self.get_session.query(self.permission_model)
                .filter(self.permission_model.action == action)
                .all()
            )
            if perms:
                log.warning(c.LOGMSG_WAR_SEC_DEL_PERM_PVM.format(action, perms))
                return False
            self.get_session.delete(action)
            self.get_session.commit()
            return True
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_PERMISSION.format(str(e)))
            self.get_session.rollback()
            return False

    def get_resource(self, name: str) -> Resource:
        """
        Returns a resource record by name, if it exists.

        :param name: Name of resource
        :return: Resource record
        """
        return self.get_session.query(self.resource_model).filter_by(name=name).one_or_none()

    def get_all_resources(self) -> list[Resource]:
        """
        Gets all existing resource records.

        :return: List of all resources
        """
        return self.get_session.query(self.resource_model).all()

    def create_resource(self, name) -> Resource:
        """
        Create a resource with the given name.

        :param name: The name of the resource to create created.
        :return: The FAB resource created.
        """
        resource = self.get_resource(name)
        if resource is None:
            try:
                resource = self.resource_model()
                resource.name = name
                self.get_session.add(resource)
                self.get_session.commit()
                return resource
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_VIEWMENU.format(str(e)))
                self.get_session.rollback()
        return resource

    def delete_resource(self, name: str) -> bool:
        """
        Deletes a Resource from the backend.

        :param name:
            name of the resource
        """
        resource = self.get_resource(name)
        if not resource:
            log.warning(c.LOGMSG_WAR_SEC_DEL_VIEWMENU.format(name))
            return False
        try:
            perms = (
                self.get_session.query(self.permission_model)
                .filter(self.permission_model.resource == resource)
                .all()
            )
            if perms:
                log.warning(c.LOGMSG_WAR_SEC_DEL_VIEWMENU_PVM.format(resource, perms))
                return False
            self.get_session.delete(resource)
            self.get_session.commit()
            return True
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_PERMISSION.format(str(e)))
            self.get_session.rollback()
            return False

    """
    ----------------------
     PERMISSION VIEW MENU
    ----------------------
    """

    def get_permission(
        self,
        action_name: str,
        resource_name: str,
    ) -> Permission | None:
        """
        Gets a permission made with the given action->resource pair, if the permission already exists.

        :param action_name: Name of action
        :param resource_name: Name of resource
        :return: The existing permission
        """
        action = self.get_action(action_name)
        resource = self.get_resource(resource_name)
        if action and resource:
            return (
                self.get_session.query(self.permission_model)
                .filter_by(action=action, resource=resource)
                .one_or_none()
            )
        return None

    def get_resource_permissions(self, resource: Resource) -> Permission:
        """
        Retrieve permission pairs associated with a specific resource object.

        :param resource: Object representing a single resource.
        :return: Action objects representing resource->action pair
        """
        return self.get_session.query(self.permission_model).filter_by(resource_id=resource.id).all()

    def create_permission(self, action_name, resource_name) -> Permission | None:
        """
        Adds a permission on a resource to the backend.

        :param action_name:
            name of the action to add: 'can_add','can_edit' etc...
        :param resource_name:
            name of the resource to add
        """
        if not (action_name and resource_name):
            return None
        perm = self.get_permission(action_name, resource_name)
        if perm:
            return perm
        resource = self.create_resource(resource_name)
        action = self.create_action(action_name)
        perm = self.permission_model()
        perm.resource_id, perm.action_id = resource.id, action.id
        try:
            self.get_session.add(perm)
            self.get_session.commit()
            log.info(c.LOGMSG_INF_SEC_ADD_PERMVIEW.format(str(perm)))
            return perm
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_ADD_PERMVIEW.format(str(e)))
            self.get_session.rollback()
            return None

    def delete_permission(self, action_name: str, resource_name: str) -> None:
        """
        Deletes the permission linking an action->resource pair.

        Doesn't delete the underlying action or resource.

        :param action_name: Name of existing action
        :param resource_name: Name of existing resource
        :return: None
        """
        if not (action_name and resource_name):
            return
        perm = self.get_permission(action_name, resource_name)
        if not perm:
            return
        roles = (
            self.get_session.query(self.role_model).filter(self.role_model.permissions.contains(perm)).first()
        )
        if roles:
            log.warning(c.LOGMSG_WAR_SEC_DEL_PERMVIEW.format(resource_name, action_name, roles))
            return
        try:
            # delete permission on resource
            self.get_session.delete(perm)
            self.get_session.commit()
            # if no more permission on permission view, delete permission
            if not self.get_session.query(self.permission_model).filter_by(action=perm.action).all():
                self.delete_action(perm.action.name)
            log.info(c.LOGMSG_INF_SEC_DEL_PERMVIEW.format(action_name, resource_name))
        except Exception as e:
            log.error(c.LOGMSG_ERR_SEC_DEL_PERMVIEW.format(str(e)))
            self.get_session.rollback()

    def perms_include_action(self, perms, action_name):
        return any(perm.action and perm.action.name == action_name for perm in perms)

    def add_permission_to_role(self, role: Role, permission: Permission | None) -> None:
        """
        Add an existing permission pair to a role.

        :param role: The role about to get a new permission.
        :param permission: The permission pair to add to a role.
        :return: None
        """
        if permission and permission not in role.permissions:
            try:
                role.permissions.append(permission)
                self.get_session.merge(role)
                self.get_session.commit()
                log.info(c.LOGMSG_INF_SEC_ADD_PERMROLE.format(str(permission), role.name))
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_ADD_PERMROLE.format(str(e)))
                self.get_session.rollback()

    def remove_permission_from_role(self, role: Role, permission: Permission) -> None:
        """
        Remove a permission pair from a role.

        :param role: User role containing permissions.
        :param permission: Object representing resource-> action pair
        """
        if permission in role.permissions:
            try:
                role.permissions.remove(permission)
                self.get_session.merge(role)
                self.get_session.commit()
                log.info(c.LOGMSG_INF_SEC_DEL_PERMROLE.format(str(permission), role.name))
            except Exception as e:
                log.error(c.LOGMSG_ERR_SEC_DEL_PERMROLE.format(str(e)))
                self.get_session.rollback()
