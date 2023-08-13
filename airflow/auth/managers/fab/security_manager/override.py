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

from flask import flash, g
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER
from flask_babel import lazy_gettext
from flask_jwt_extended import JWTManager
from flask_login import LoginManager
from itsdangerous import want_bytes
from markupsafe import Markup
from werkzeug.security import generate_password_hash

from airflow.auth.managers.fab.models import User
from airflow.auth.managers.fab.models.anonymous_user import AnonymousUser
from airflow.www.session import AirflowDatabaseSessionInterface

# This is the limit of DB user sessions that we consider as "healthy". If you have more sessions that this
# number then we will refuse to delete sessions that have expired and old user sessions when resetting
# user's password, and raise a warning in the UI instead. Usually when you have that many sessions, it means
# that there is something wrong with your deployment - for example you have an automated API call that
# continuously creates new sessions. Such setup should be fixed by reusing sessions or by periodically
# purging the old sessions by using `airflow db clean` command.
MAX_NUM_DATABASE_USER_SESSIONS = 50000


class FabAirflowSecurityManagerOverride:
    """
    This security manager overrides the default AirflowSecurityManager security manager.

    This security manager is used only if the auth manager FabAuthManager is used. It defines everything in
    the security manager that is needed for the FabAuthManager to work. Any operation specific to
    the AirflowSecurityManager should be defined here instead of AirflowSecurityManager.

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
    :param user_model: The user model.
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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.appbuilder = kwargs["appbuilder"]
        self.actionmodelview = kwargs["actionmodelview"]
        self.authdbview = kwargs["authdbview"]
        self.authldapview = kwargs["authldapview"]
        self.authoauthview = kwargs["authoauthview"]
        self.authoidview = kwargs["authoidview"]
        self.authremoteuserview = kwargs["authremoteuserview"]
        self.permissionmodelview = kwargs["permissionmodelview"]
        self.registeruser_view = kwargs["registeruser_view"]
        self.registeruserdbview = kwargs["registeruserdbview"]
        self.registeruseroauthview = kwargs["registeruseroauthview"]
        self.registerusermodelview = kwargs["registerusermodelview"]
        self.registeruseroidview = kwargs["registeruseroidview"]
        self.resetmypasswordview = kwargs["resetmypasswordview"]
        self.resetpasswordview = kwargs["resetpasswordview"]
        self.rolemodelview = kwargs["rolemodelview"]
        self.user_model = kwargs["user_model"]
        self.userinfoeditview = kwargs["userinfoeditview"]
        self.userdbmodelview = kwargs["userdbmodelview"]
        self.userldapmodelview = kwargs["userldapmodelview"]
        self.useroauthmodelview = kwargs["useroauthmodelview"]
        self.useroidmodelview = kwargs["useroidmodelview"]
        self.userremoteusermodelview = kwargs["userremoteusermodelview"]
        self.userstatschartview = kwargs["userstatschartview"]

        # Setup Flask login
        self.lm = self.create_login_manager()

        # Setup Flask-Jwt-Extended
        self.create_jwt_manager()

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

    def create_login_manager(self) -> LoginManager:
        """Create the login manager."""
        lm = LoginManager(self.appbuilder.app)
        lm.anonymous_user = AnonymousUser
        lm.login_view = "login"
        lm.user_loader(self.load_user)
        return lm

    def create_jwt_manager(self):
        """Create the JWT manager."""
        jwt_manager = JWTManager()
        jwt_manager.init_app(self.appbuilder.app)
        jwt_manager.user_lookup_loader(self.load_user_jwt)

    def reset_password(self, userid, password):
        """
        Change/Reset a user's password for authdb.

        Password will be hashed and saved.
        :param userid: the user id to reset the password
        :param password: the clear text password to reset and save hashed on the db
        """
        user = self.get_user_by_id(userid)
        user.password = generate_password_hash(password)
        self.reset_user_sessions(user)
        self.update_user(user)

    def reset_user_sessions(self, user: User) -> None:
        if isinstance(self.appbuilder.get_app.session_interface, AirflowDatabaseSessionInterface):
            interface = self.appbuilder.get_app.session_interface
            session = interface.db.session
            user_session_model = interface.sql_session_model
            num_sessions = session.query(user_session_model).count()
            if num_sessions > MAX_NUM_DATABASE_USER_SESSIONS:
                flash(
                    Markup(
                        f"The old sessions for user {user.username} have <b>NOT</b> been deleted!<br>"
                        f"You have a lot ({num_sessions}) of user sessions in the 'SESSIONS' table in "
                        f"your database.<br> "
                        "This indicates that this deployment might have an automated API calls that create "
                        "and not reuse sessions.<br>You should consider reusing sessions or cleaning them "
                        "periodically using db clean.<br>"
                        "Make sure to reset password for the user again after cleaning the session table "
                        "to remove old sessions of the user."
                    ),
                    "warning",
                )
            else:
                for s in session.query(user_session_model):
                    session_details = interface.serializer.loads(want_bytes(s.data))
                    if session_details.get("_user_id") == user.id:
                        session.delete(s)
        else:
            flash(
                Markup(
                    "Since you are using `securecookie` session backend mechanism, we cannot prevent "
                    f"some old sessions for user {user.username} to be reused.<br> If you want to make sure "
                    "that the user is logged out from all sessions, you should consider using "
                    "`database` session backend mechanism.<br> You can also change the 'secret_key` "
                    "webserver configuration for all your webserver instances and restart the webserver. "
                    "This however will logout all users from all sessions."
                ),
                "warning",
            )

    def load_user(self, user_id):
        """Load user by ID."""
        return self.get_user_by_id(int(user_id))

    def load_user_jwt(self, _jwt_header, jwt_data):
        identity = jwt_data["sub"]
        user = self.load_user(identity)
        # Set flask g.user to JWT user, we can't do it on before request
        g.user = user
        return user

    def get_user_by_id(self, pk):
        return self.appbuilder.get_session.get(self.user_model, pk)

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
