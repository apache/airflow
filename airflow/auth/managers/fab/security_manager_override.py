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

from functools import cached_property

from flask import Flask
from flask_appbuilder import AppBuilder
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER
from flask_babel import lazy_gettext
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address


class FabAirflowSecurityManagerOverride:
    """
    This security manager overrides the default AirflowSecurityManager security manager.

    This security manager is used only if the auth manager FabAuthManager is used.

    :param appbuilder: The appbuilder.
    :param actionmodelview: The obj instance for action model view.
    :param authdbview: The class for auth db view.
    :param authldapview: The class for auth ldap view.
    :param authoauthview: The class for auth oauth view.
    :param authoidview: The class for auth oid view.
    :param authremoteuserview: The class for auth remote user view.
    :param permissionmodelview: The class for permission model view.
    :param registeruser_view: The class for register user view.
    :param registeruserdbview: The class for register user db view.
    :param registeruseroauthview: The class for register user oauth view.
    :param registerusermodelview: The class for register user model view.
    :param registeruseroidview: The class for register user oid view.
    :param resetmypasswordview: The class for reset my password view.
    :param resetpasswordview: The class for reset password view.
    :param rolemodelview: The class for role model view.
    :param userinfoeditview: The class for user info edit view.
    :param userdbmodelview: The class for user db model view.
    :param userldapmodelview: The class for user ldap model view.
    :param useroauthmodelview: The class for user oauth model view.
    :param useroidmodelview: The class for user oid model view.
    :param userremoteusermodelview: The class for user remote user model view.
    :param userstatschartview: The class for user stats chart view.
    """

    """ The obj instance for authentication view """
    auth_view = None
    """ The obj instance for user view """
    user_view = None

    def __init__(
        self,
        appbuilder: AppBuilder,
        actionmodelview,
        authdbview,
        authldapview,
        authoauthview,
        authoidview,
        authremoteuserview,
        permissionmodelview,
        registeruser_view,
        registeruserdbview,
        registeruseroauthview,
        registerusermodelview,
        registeruseroidview,
        resetmypasswordview,
        resetpasswordview,
        rolemodelview,
        userinfoeditview,
        userdbmodelview,
        userldapmodelview,
        useroauthmodelview,
        useroidmodelview,
        userremoteusermodelview,
        userstatschartview,
        **kwargs,
    ):
        self.appbuilder = appbuilder
        self.actionmodelview = actionmodelview
        self.authdbview = authdbview
        self.authldapview = authldapview
        self.authoauthview = authoauthview
        self.authoidview = authoidview
        self.authremoteuserview = authremoteuserview
        self.permissionmodelview = permissionmodelview
        self.registeruser_view = registeruser_view
        self.registeruserdbview = registeruserdbview
        self.registeruseroauthview = registeruseroauthview
        self.registerusermodelview = registerusermodelview
        self.registeruseroidview = registeruseroidview
        self.resetmypasswordview = resetmypasswordview
        self.resetpasswordview = resetpasswordview
        self.rolemodelview = rolemodelview
        self.userinfoeditview = userinfoeditview
        self.userdbmodelview = userdbmodelview
        self.userldapmodelview = userldapmodelview
        self.useroauthmodelview = useroauthmodelview
        self.useroidmodelview = useroidmodelview
        self.userremoteusermodelview = userremoteusermodelview
        self.userstatschartview = userstatschartview

        # Setup Flask-Limiter
        self.limiter = self.create_limiter(appbuilder.get_app())

    def create_limiter(self, app: Flask) -> Limiter:
        """
        Create a Flask limiter.

        :param app: The Flask app.
        """
        limiter = Limiter(key_func=get_remote_address)
        limiter.init_app(app)
        return limiter

    def register_views(self):
        """Register FAB auth manager related views."""
        if not self.appbuilder.app.config.get("FAB_ADD_SECURITY_VIEWS", True):
            return

        if self.auth_user_registration:
            if self.auth_type == AUTH_DB:
                self.registeruser_view = self.registeruserdbview()
            elif self.auth_type == AUTH_OID:
                self.registeruser_view = self.registeruseroidview()
            elif self.auth_type == AUTH_OAUTH:
                self.registeruser_view = self.registeruseroauthview()
            if self.registeruser_view:
                self.appbuilder.add_view_no_menu(self.registeruser_view)

        self.appbuilder.add_view_no_menu(self.resetpasswordview())
        self.appbuilder.add_view_no_menu(self.resetmypasswordview())
        self.appbuilder.add_view_no_menu(self.userinfoeditview())

        if self.auth_type == AUTH_DB:
            self.user_view = self.userdbmodelview
            self.auth_view = self.authdbview()
        elif self.auth_type == AUTH_LDAP:
            self.user_view = self.userldapmodelview
            self.auth_view = self.authldapview()
        elif self.auth_type == AUTH_OAUTH:
            self.user_view = self.useroauthmodelview
            self.auth_view = self.authoauthview()
        elif self.auth_type == AUTH_REMOTE_USER:
            self.user_view = self.userremoteusermodelview
            self.auth_view = self.authremoteuserview()
        else:
            self.user_view = self.useroidmodelview
            self.auth_view = self.authoidview()

        self.appbuilder.add_view_no_menu(self.auth_view)

        # this needs to be done after the view is added, otherwise the blueprint
        # is not initialized
        if self.is_auth_limited:
            self.limiter.limit(self.auth_rate_limit, methods=["POST"])(self.auth_view.blueprint)

        self.user_view = self.appbuilder.add_view(
            self.user_view,
            "List Users",
            icon="fa-user",
            label=lazy_gettext("List Users"),
            category="Security",
            category_icon="fa-cogs",
            category_label=lazy_gettext("Security"),
        )

        role_view = self.appbuilder.add_view(
            self.rolemodelview,
            "List Roles",
            icon="fa-group",
            label=lazy_gettext("List Roles"),
            category="Security",
            category_icon="fa-cogs",
        )
        role_view.related_views = [self.user_view.__class__]

        if self.userstatschartview:
            self.appbuilder.add_view(
                self.userstatschartview,
                "User's Statistics",
                icon="fa-bar-chart-o",
                label=lazy_gettext("User's Statistics"),
                category="Security",
            )
        if self.auth_user_registration:
            self.appbuilder.add_view(
                self.registerusermodelview,
                "User's Statistics",
                icon="fa-user-plus",
                label=lazy_gettext("User Registrations"),
                category="Security",
            )
        self.appbuilder.menu.add_separator("Security")
        if self.appbuilder.app.config.get("FAB_ADD_SECURITY_PERMISSION_VIEW", True):
            self.appbuilder.add_view(
                self.actionmodelview,
                "Actions",
                icon="fa-lock",
                label=lazy_gettext("Actions"),
                category="Security",
            )
        if self.appbuilder.app.config.get("FAB_ADD_SECURITY_VIEW_MENU_VIEW", True):
            self.appbuilder.add_view(
                self.resourcemodelview,
                "Resources",
                icon="fa-list-alt",
                label=lazy_gettext("Resources"),
                category="Security",
            )
        if self.appbuilder.app.config.get("FAB_ADD_SECURITY_PERMISSION_VIEWS_VIEW", True):
            self.appbuilder.add_view(
                self.permissionmodelview,
                "Permission Pairs",
                icon="fa-link",
                label=lazy_gettext("Permissions"),
                category="Security",
            )

    @property
    def auth_user_registration(self):
        """Will user self registration be allowed."""
        return self.appbuilder.get_app.config["AUTH_USER_REGISTRATION"]

    @property
    def auth_type(self):
        """Get the auth type."""
        return self.appbuilder.get_app.config["AUTH_TYPE"]

    @property
    def is_auth_limited(self) -> bool:
        """Is the auth rate limited."""
        return self.appbuilder.get_app.config["AUTH_RATE_LIMITED"]

    @property
    def auth_rate_limit(self) -> str:
        """Get the auth rate limit."""
        return self.appbuilder.get_app.config["AUTH_RATE_LIMIT"]

    @cached_property
    def resourcemodelview(self):
        """Return the resource model view."""
        from airflow.www.views import ResourceModelView

        return ResourceModelView
