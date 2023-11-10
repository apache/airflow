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

# mypy: disable-error-code=var-annotated
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import re2
from flask import g, session, url_for
from flask_appbuilder.security.registerviews import (
    RegisterUserDBView,
    RegisterUserOAuthView,
    RegisterUserOIDView,
)
from flask_appbuilder.security.views import (
    AuthDBView,
    AuthLDAPView,
    AuthOAuthView,
    AuthOIDView,
    AuthRemoteUserView,
    PermissionModelView,
    RegisterUserModelView,
    ResetMyPasswordView,
    ResetPasswordView,
    RoleModelView,
    UserInfoEditView,
    UserLDAPModelView,
    UserOAuthModelView,
    UserOIDModelView,
    UserRemoteUserModelView,
    UserStatsChartView,
)
from flask_jwt_extended import current_user as current_user_jwt
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from flask import Flask
    from flask_appbuilder import AppBuilder

    from airflow.auth.managers.fab.models import Action, Permission, RegisterUser, Resource, Role, User

# This product contains a modified portion of 'Flask App Builder' developed by Daniel Vaz Gaspar.
# (https://github.com/dpgaspar/Flask-AppBuilder).
# Copyright 2013, Daniel Vaz Gaspar
log = logging.getLogger(__name__)

__lazy_imports = {
    "AUTH_DB": "flask_appbuilder.const",
    "AUTH_LDAP": "flask_appbuilder.const",
    "LOGMSG_WAR_SEC_LOGIN_FAILED": "flask_appbuilder.const",
}


def __getattr__(name: str):
    # PEP-562: Lazy loaded attributes on python modules
    path = __lazy_imports.get(name)
    if not path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from airflow.utils.module_loading import import_string

    val = import_string(f"{path}.{name}")
    # Store for next time
    globals()[name] = val
    return val


def _oauth_tokengetter(token=None):
    """Return the current user oauth token from session cookie."""
    token = session.get("oauth")
    log.debug("Token Get: %s", token)
    return token


class BaseSecurityManager:
    """Base class to define the Security Manager interface."""

    appbuilder: AppBuilder
    """The appbuilder instance for the current security manager."""
    auth_view = None
    """ The obj instance for authentication view """
    user_view = None
    """ The obj instance for user view """
    registeruser_view = None
    """ The obj instance for registering user view """
    lm = None
    """ Flask-Login LoginManager """

    user_model: type[User]
    """ Override to set your own User Model """
    role_model: type[Role]
    """ Override to set your own Role Model """
    action_model: type[Action]
    """ Override to set your own Action Model """
    resource_model: type[Resource]
    """ Override to set your own Resource Model """
    permission_model: type[Permission]
    """ Override to set your own Permission Model """
    registeruser_model: type[RegisterUser]
    """ Override to set your own RegisterUser Model """

    userldapmodelview = UserLDAPModelView
    """ Override if you want your own user ldap view """
    useroidmodelview = UserOIDModelView
    """ Override if you want your own user OID view """
    useroauthmodelview = UserOAuthModelView
    """ Override if you want your own user OAuth view """
    userremoteusermodelview = UserRemoteUserModelView
    """ Override if you want your own user REMOTE_USER view """
    registerusermodelview = RegisterUserModelView

    authdbview = AuthDBView
    """ Override if you want your own Authentication DB view """
    authldapview = AuthLDAPView
    """ Override if you want your own Authentication LDAP view """
    authoidview = AuthOIDView
    """ Override if you want your own Authentication OID view """
    authoauthview = AuthOAuthView
    """ Override if you want your own Authentication OAuth view """
    authremoteuserview = AuthRemoteUserView
    """ Override if you want your own Authentication REMOTE_USER view """

    registeruserdbview = RegisterUserDBView
    """ Override if you want your own register user db view """
    registeruseroidview = RegisterUserOIDView
    """ Override if you want your own register user OpenID view """
    registeruseroauthview = RegisterUserOAuthView
    """ Override if you want your own register user OAuth view """

    resetmypasswordview = ResetMyPasswordView
    """ Override if you want your own reset my password view """
    resetpasswordview = ResetPasswordView
    """ Override if you want your own reset password view """
    userinfoeditview = UserInfoEditView
    """ Override if you want your own User information edit view """

    rolemodelview = RoleModelView
    actionmodelview = PermissionModelView
    userstatschartview = UserStatsChartView
    permissionmodelview = PermissionModelView

    def __init__(self, appbuilder):
        self.appbuilder = appbuilder
        app = self.appbuilder.get_app

        # Setup Flask-Limiter
        self.limiter = self.create_limiter(app)

    def create_limiter(self, app: Flask) -> Limiter:
        limiter = Limiter(key_func=get_remote_address)
        limiter.init_app(app)
        return limiter

    def add_role(self, name: str) -> Role:
        raise NotImplementedError

    @property
    def get_url_for_registeruser(self):
        """Gets the URL for Register User."""
        return url_for(f"{self.registeruser_view.endpoint}.{self.registeruser_view.default_view}")

    @property
    def get_user_datamodel(self):
        """Gets the User data model."""
        return self.user_view.datamodel

    @property
    def get_register_user_datamodel(self):
        """Gets the Register User data model."""
        return self.registerusermodelview.datamodel

    @property
    def builtin_roles(self):
        """Get the builtin roles."""
        return self._builtin_roles

    @property
    def auth_role_admin(self):
        """Gets the admin role."""
        return self.appbuilder.get_app.config["AUTH_ROLE_ADMIN"]

    @property
    def auth_roles_sync_at_login(self) -> bool:
        """Should roles be synced at login."""
        return self.appbuilder.get_app.config["AUTH_ROLES_SYNC_AT_LOGIN"]

    @property
    def current_user(self):
        """Current user object."""
        if get_auth_manager().is_logged_in():
            return g.user
        elif current_user_jwt:
            return current_user_jwt

    def _has_access_builtin_roles(self, role, action_name: str, resource_name: str) -> bool:
        """Checks permission on builtin role."""
        perms = self.builtin_roles.get(role.name, [])
        for _resource_name, _action_name in perms:
            if re2.match(_resource_name, resource_name) and re2.match(_action_name, action_name):
                return True
        return False

    def add_limit_view(self, baseview):
        if not baseview.limits:
            return

        for limit in baseview.limits:
            self.limiter.limit(
                limit_value=limit.limit_value,
                key_func=limit.key_func,
                per_method=limit.per_method,
                methods=limit.methods,
                error_message=limit.error_message,
                exempt_when=limit.exempt_when,
                override_defaults=limit.override_defaults,
                deduct_when=limit.deduct_when,
                on_breach=limit.on_breach,
                cost=limit.cost,
            )(baseview.blueprint)

    def add_permissions_view(self, base_action_names, resource_name):  # Keep name for compatibility with FAB.
        """
        Add an action on a resource to the backend.

        :param base_action_names:
            list of permissions from view (all exposed methods):
             'can_add','can_edit' etc...
        :param resource_name:
            name of the resource to add
        """
        resource = self.create_resource(resource_name)
        perms = self.get_resource_permissions(resource)

        if not perms:
            # No permissions yet on this view
            for action_name in base_action_names:
                action = self.create_permission(action_name, resource_name)
                if self.auth_role_admin not in self.builtin_roles:
                    admin_role = self.find_role(self.auth_role_admin)
                    self.add_permission_to_role(admin_role, action)
        else:
            # Permissions on this view exist but....
            admin_role = self.find_role(self.auth_role_admin)
            for action_name in base_action_names:
                # Check if base view permissions exist
                if not self.perms_include_action(perms, action_name):
                    action = self.create_permission(action_name, resource_name)
                    if self.auth_role_admin not in self.builtin_roles:
                        self.add_permission_to_role(admin_role, action)
            for perm in perms:
                if perm.action is None:
                    # Skip this perm, it has a null permission
                    continue
                if perm.action.name not in base_action_names:
                    # perm to delete
                    roles = self.get_all_roles()
                    # del permission from all roles
                    for role in roles:
                        # TODO: An action can't be removed from a role.
                        # This is a bug in FAB. It has been reported.
                        self.remove_permission_from_role(role, perm)
                    self.delete_permission(perm.action.name, resource_name)
                elif self.auth_role_admin not in self.builtin_roles and perm not in admin_role.permissions:
                    # Role Admin must have all permissions
                    self.add_permission_to_role(admin_role, perm)

    def add_permissions_menu(self, resource_name):
        """
        Add menu_access to resource on permission_resource.

        :param resource_name:
            The resource name
        """
        self.create_resource(resource_name)
        perm = self.get_permission("menu_access", resource_name)
        if not perm:
            perm = self.create_permission("menu_access", resource_name)
        if self.auth_role_admin not in self.builtin_roles:
            role_admin = self.find_role(self.auth_role_admin)
            self.add_permission_to_role(role_admin, perm)

    def get_resource(self, name: str) -> Resource:
        raise NotImplementedError

    def get_action(self, name: str) -> Action:
        raise NotImplementedError

    def security_cleanup(self, baseviews, menus):
        """
        Cleanup all unused permissions from the database.

        :param baseviews: A list of BaseViews class
        :param menus: Menu class
        """
        resources = self.get_all_resources()
        roles = self.get_all_roles()
        for resource in resources:
            found = False
            for baseview in baseviews:
                if resource.name == baseview.class_permission_name:
                    found = True
                    break
            if menus.find(resource.name):
                found = True
            if not found:
                permissions = self.get_resource_permissions(resource)
                for permission in permissions:
                    for role in roles:
                        self.remove_permission_from_role(role, permission)
                    self.delete_permission(permission.action.name, resource.name)
                self.delete_resource(resource.name)

    def find_user(self, username=None, email=None):
        """Find a user by its username or email."""
        raise NotImplementedError

    def get_role_permissions_from_db(self, role_id: int) -> list[Permission]:
        """Get all DB permissions from a role id."""
        raise NotImplementedError

    def add_user(self, username, first_name, last_name, email, role, password=""):
        """Create user."""
        raise NotImplementedError

    def update_user(self, user):
        """
        Update user.

        :param user: User model to update to database
        """
        raise NotImplementedError

    def find_role(self, name):
        raise NotImplementedError

    def get_all_roles(self):
        raise NotImplementedError

    def get_public_role(self):
        """Return all permissions from public role."""
        raise NotImplementedError

    def permission_exists_in_one_or_more_roles(
        self, resource_name: str, action_name: str, role_ids: list[int]
    ) -> bool:
        """Find and returns permission views for a group of roles."""
        raise NotImplementedError

    """
    ----------------------
     PRIMITIVES VIEW MENU
    ----------------------
    """

    def get_all_resources(self) -> list[Resource]:
        """
        Get all existing resource records.

        :return: List of all resources
        """
        raise NotImplementedError

    def create_resource(self, name):
        """
        Create a resource with the given name.

        :param name: The name of the resource to create created.
        """
        raise NotImplementedError

    def delete_resource(self, name):
        """
        Delete a Resource from the backend.

        :param name:
            name of the Resource
        """
        raise NotImplementedError

    """
    ----------------------
     PERMISSION VIEW MENU
    ----------------------
    """

    def get_permission(self, action_name: str, resource_name: str) -> Permission | None:
        """
        Get a permission made with the given action->resource pair, if the permission already exists.

        :param action_name: Name of action
        :param resource_name: Name of resource
        :return: The existing permission
        """
        raise NotImplementedError

    def get_resource_permissions(self, resource) -> Permission:
        """
        Retrieve permission pairs associated with a specific resource object.

        :param resource: Object representing a single resource.
        :return: Action objects representing resource->action pair
        """
        raise NotImplementedError

    def create_permission(self, action_name: str, resource_name: str) -> Permission | None:
        """
        Create a permission linking an action and resource.

        :param action_name: Name of existing action
        :param resource_name: Name of existing resource
        :return: Resource created
        """
        raise NotImplementedError

    def delete_permission(self, action_name: str, resource_name: str) -> None:
        """
        Delete the permission linking an action->resource pair.

        Doesn't delete the underlying action or resource.

        :param action_name: Name of existing action
        :param resource_name: Name of existing resource
        :return: None
        """
        raise NotImplementedError

    def perms_include_action(self, perms, action_name):
        raise NotImplementedError

    def add_permission_to_role(self, role, permission) -> None:
        """
        Add an existing permission pair to a role.

        :param role: The role about to get a new permission.
        :param permission: The permission pair to add to a role.
        :return: None
        """
        raise NotImplementedError

    def remove_permission_from_role(self, role, permission) -> None:
        """
        Remove a permission pair from a role.

        :param role: User role containing permissions.
        :param permission: Object representing resource-> action pair
        """
        raise NotImplementedError

    @staticmethod
    def before_request():
        """Run hook before request."""
        g.user = get_auth_manager().get_user()
