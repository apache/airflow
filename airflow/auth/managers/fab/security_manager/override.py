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

import base64
import json
import logging
import os
import random
import uuid
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Container, Iterable, Sequence

import re2
from flask import flash, g, session
from flask_appbuilder import const
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER
from flask_appbuilder.models.sqla import Base
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_babel import lazy_gettext
from flask_jwt_extended import JWTManager
from flask_login import LoginManager
from itsdangerous import want_bytes
from markupsafe import Markup
from sqlalchemy import func, inspect, select
from sqlalchemy.exc import MultipleResultsFound
from werkzeug.security import generate_password_hash

from airflow.auth.managers.fab.fab_auth_manager import MAP_METHOD_NAME_TO_FAB_ACTION_NAME
from airflow.auth.managers.fab.models import Action, Permission, RegisterUser, Resource, Role
from airflow.auth.managers.fab.models.anonymous_user import AnonymousUser
from airflow.exceptions import AirflowException, RemovedInAirflow3Warning
from airflow.models import DagModel
from airflow.security import permissions
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.security_manager import AirflowSecurityManagerV2
from airflow.www.session import AirflowDatabaseSessionInterface

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.auth.managers.base_auth_manager import ResourceMethod
    from airflow.auth.managers.fab.models import User

log = logging.getLogger(__name__)

# This is the limit of DB user sessions that we consider as "healthy". If you have more sessions that this
# number then we will refuse to delete sessions that have expired and old user sessions when resetting
# user's password, and raise a warning in the UI instead. Usually when you have that many sessions, it means
# that there is something wrong with your deployment - for example you have an automated API call that
# continuously creates new sessions. Such setup should be fixed by reusing sessions or by periodically
# purging the old sessions by using `airflow db clean` command.
MAX_NUM_DATABASE_USER_SESSIONS = 50000


class FabAirflowSecurityManagerOverride(AirflowSecurityManagerV2):
    """
    This security manager overrides the default AirflowSecurityManager security manager.

    This security manager is used only if the auth manager FabAuthManager is used. It defines everything in
    the security manager that is needed for the FabAuthManager to work. Any operation specific to
    the AirflowSecurityManager should be defined here instead of AirflowSecurityManager.

    :param appbuilder: The appbuilder.
    """

    """ The obj instance for authentication view """
    auth_view = None
    """ The obj instance for user view """
    user_view = None
    """ Models """
    role_model = Role
    action_model = Action
    resource_model = Resource
    permission_model = Permission
    registeruser_model = RegisterUser

    """ Initialized (remote_app) providers dict {'provider_name', OBJ } """
    oauth_allow_list: dict[str, list] = {}

    def __init__(self, appbuilder):
        # done in super, but we need it before we can call super.
        self.appbuilder = appbuilder

        self._init_config()
        self._init_auth()
        self._init_data_model()
        # can only call super once data model init has been done
        # because of the view.datamodel hack that's done in the init there.
        super().__init__(appbuilder=appbuilder)

        self._builtin_roles: dict = self.create_builtin_roles()

        self.create_db()

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

    def load_user_jwt(self, _jwt_header, jwt_data):
        identity = jwt_data["sub"]
        user = self.load_user(identity)
        # Set flask g.user to JWT user, we can't do it on before request
        g.user = user
        return user

    @property
    def auth_user_registration(self):
        """Will user self registration be allowed."""
        return self.appbuilder.app.config["AUTH_USER_REGISTRATION"]

    @property
    def auth_type(self):
        """Get the auth type."""
        return self.appbuilder.app.config["AUTH_TYPE"]

    @property
    def is_auth_limited(self) -> bool:
        """Is the auth rate limited."""
        return self.appbuilder.app.config["AUTH_RATE_LIMITED"]

    @property
    def auth_rate_limit(self) -> str:
        """Get the auth rate limit."""
        return self.appbuilder.app.config["AUTH_RATE_LIMIT"]

    @cached_property
    def resourcemodelview(self):
        """Return the resource model view."""
        from airflow.auth.managers.fab.views.permissions import ResourceModelView

        return ResourceModelView

    @property
    def auth_role_public(self):
        """Gets the public role."""
        return self.appbuilder.app.config["AUTH_ROLE_PUBLIC"]

    @property
    def oauth_providers(self):
        """Oauth providers."""
        return self.appbuilder.app.config["OAUTH_PROVIDERS"]

    @property
    def oauth_whitelists(self):
        warnings.warn(
            "The 'oauth_whitelists' property is deprecated. Please use 'oauth_allow_list' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.oauth_allow_list

    def create_builtin_roles(self):
        """Returns FAB builtin roles."""
        return self.appbuilder.app.config.get("FAB_ROLES", {})

    def create_admin_standalone(self) -> tuple[str | None, str | None]:
        """Create an Admin user with a random password so that users can access airflow."""
        from airflow.configuration import AIRFLOW_HOME, make_group_other_inaccessible

        user_name = "admin"

        # We want a streamlined first-run experience, but we do not want to
        # use a preset password as people will inevitably run this on a public
        # server. Thus, we make a random password and store it in AIRFLOW_HOME,
        # with the reasoning that if you can read that directory, you can see
        # the database credentials anyway.
        password_path = os.path.join(AIRFLOW_HOME, "standalone_admin_password.txt")

        user_exists = self.find_user(user_name) is not None
        we_know_password = os.path.isfile(password_path)

        # If the user does not exist, make a random password and make it
        if not user_exists:
            print(f"FlaskAppBuilder Authentication Manager: Creating {user_name} user")
            role = self.find_role("Admin")
            assert role is not None
            # password does not contain visually similar characters: ijlIJL1oO0
            password = "".join(random.choices("abcdefghkmnpqrstuvwxyzABCDEFGHKMNPQRSTUVWXYZ23456789", k=16))
            with open(password_path, "w") as file:
                file.write(password)
            make_group_other_inaccessible(password_path)
            self.add_user(user_name, "Admin", "User", "admin@example.com", role, password)
            print(f"FlaskAppBuilder Authentication Manager: Created {user_name} user")
        # If the user does exist, and we know its password, read the password
        elif user_exists and we_know_password:
            with open(password_path) as file:
                password = file.read().strip()
        # Otherwise we don't know the password
        else:
            password = None
        return user_name, password

    def _init_config(self):
        """
        Initialize config.

        :meta private:
        """
        app = self.appbuilder.get_app
        # Base Security Config
        app.config.setdefault("AUTH_ROLE_ADMIN", "Admin")
        app.config.setdefault("AUTH_ROLE_PUBLIC", "Public")
        app.config.setdefault("AUTH_TYPE", AUTH_DB)
        # Self Registration
        app.config.setdefault("AUTH_USER_REGISTRATION", False)
        app.config.setdefault("AUTH_USER_REGISTRATION_ROLE", self.auth_role_public)
        app.config.setdefault("AUTH_USER_REGISTRATION_ROLE_JMESPATH", None)
        # Role Mapping
        app.config.setdefault("AUTH_ROLES_MAPPING", {})
        app.config.setdefault("AUTH_ROLES_SYNC_AT_LOGIN", False)
        app.config.setdefault("AUTH_API_LOGIN_ALLOW_MULTIPLE_PROVIDERS", False)

        # LDAP Config
        if self.auth_type == AUTH_LDAP:
            if "AUTH_LDAP_SERVER" not in app.config:
                raise Exception("No AUTH_LDAP_SERVER defined on config with AUTH_LDAP authentication type.")
            app.config.setdefault("AUTH_LDAP_SEARCH", "")
            app.config.setdefault("AUTH_LDAP_SEARCH_FILTER", "")
            app.config.setdefault("AUTH_LDAP_APPEND_DOMAIN", "")
            app.config.setdefault("AUTH_LDAP_USERNAME_FORMAT", "")
            app.config.setdefault("AUTH_LDAP_BIND_USER", "")
            app.config.setdefault("AUTH_LDAP_BIND_PASSWORD", "")
            # TLS options
            app.config.setdefault("AUTH_LDAP_USE_TLS", False)
            app.config.setdefault("AUTH_LDAP_ALLOW_SELF_SIGNED", False)
            app.config.setdefault("AUTH_LDAP_TLS_DEMAND", False)
            app.config.setdefault("AUTH_LDAP_TLS_CACERTDIR", "")
            app.config.setdefault("AUTH_LDAP_TLS_CACERTFILE", "")
            app.config.setdefault("AUTH_LDAP_TLS_CERTFILE", "")
            app.config.setdefault("AUTH_LDAP_TLS_KEYFILE", "")
            # Mapping options
            app.config.setdefault("AUTH_LDAP_UID_FIELD", "uid")
            app.config.setdefault("AUTH_LDAP_GROUP_FIELD", "memberOf")
            app.config.setdefault("AUTH_LDAP_FIRSTNAME_FIELD", "givenName")
            app.config.setdefault("AUTH_LDAP_LASTNAME_FIELD", "sn")
            app.config.setdefault("AUTH_LDAP_EMAIL_FIELD", "mail")

        # Rate limiting
        app.config.setdefault("AUTH_RATE_LIMITED", True)
        app.config.setdefault("AUTH_RATE_LIMIT", "5 per 40 second")

    def _init_auth(self):
        """
        Initialize authentication configuration.

        :meta private:
        """
        app = self.appbuilder.get_app
        if self.auth_type == AUTH_OID:
            from flask_openid import OpenID

            self.oid = OpenID(app)
        if self.auth_type == AUTH_OAUTH:
            from authlib.integrations.flask_client import OAuth

            self.oauth = OAuth(app)
            self.oauth_remotes = {}
            for provider in self.oauth_providers:
                provider_name = provider["name"]
                log.debug("OAuth providers init %s", provider_name)
                obj_provider = self.oauth.register(provider_name, **provider["remote_app"])
                obj_provider._tokengetter = self.oauth_token_getter
                if not self.oauth_user_info:
                    self.oauth_user_info = self.get_oauth_user_info
                # Whitelist only users with matching emails
                if "whitelist" in provider:
                    self.oauth_allow_list[provider_name] = provider["whitelist"]
                self.oauth_remotes[provider_name] = obj_provider

    def _init_data_model(self):
        user_data_model = SQLAInterface(self.user_model)
        if self.auth_type == const.AUTH_DB:
            self.userdbmodelview.datamodel = user_data_model
        elif self.auth_type == const.AUTH_LDAP:
            self.userldapmodelview.datamodel = user_data_model
        elif self.auth_type == const.AUTH_OID:
            self.useroidmodelview.datamodel = user_data_model
        elif self.auth_type == const.AUTH_OAUTH:
            self.useroauthmodelview.datamodel = user_data_model
        elif self.auth_type == const.AUTH_REMOTE_USER:
            self.userremoteusermodelview.datamodel = user_data_model

        if self.userstatschartview:
            self.userstatschartview.datamodel = user_data_model
        if self.auth_user_registration:
            self.registerusermodelview.datamodel = SQLAInterface(self.registeruser_model)

        self.rolemodelview.datamodel = SQLAInterface(self.role_model)
        self.actionmodelview.datamodel = SQLAInterface(self.action_model)
        self.resourcemodelview.datamodel = SQLAInterface(self.resource_model)
        self.permissionmodelview.datamodel = SQLAInterface(self.permission_model)

    def create_db(self):
        """
        Create the database.

        Creates admin and public roles if they don't exist.
        """
        if not self.appbuilder.update_perms:
            log.debug("Skipping db since appbuilder disables update_perms")
            return
        try:
            engine = self.get_session.get_bind(mapper=None, clause=None)
            inspector = inspect(engine)
            if "ab_user" not in inspector.get_table_names():
                log.info(const.LOGMSG_INF_SEC_NO_DB)
                Base.metadata.create_all(engine)
                log.info(const.LOGMSG_INF_SEC_ADD_DB)

            roles_mapping = self.appbuilder.app.config.get("FAB_ROLES_MAPPING", {})
            for pk, name in roles_mapping.items():
                self.update_role(pk, name)
            for role_name in self._builtin_roles:
                self.add_role(role_name)
            if self.auth_role_admin not in self._builtin_roles:
                self.add_role(self.auth_role_admin)
            self.add_role(self.auth_role_public)
            if self.count_users() == 0 and self.auth_role_public != self.auth_role_admin:
                log.warning(const.LOGMSG_WAR_SEC_NO_USER)
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_CREATE_DB, e)
            exit(1)

    def get_readable_dags(self, user) -> Iterable[DagModel]:
        """Gets the DAGs readable by authenticated user."""
        warnings.warn(
            "`get_readable_dags` has been deprecated. Please use `get_readable_dag_ids` instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RemovedInAirflow3Warning)
            return self.get_accessible_dags([permissions.ACTION_CAN_READ], user)

    def get_editable_dags(self, user) -> Iterable[DagModel]:
        """Gets the DAGs editable by authenticated user."""
        warnings.warn(
            "`get_editable_dags` has been deprecated. Please use `get_editable_dag_ids` instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RemovedInAirflow3Warning)
            return self.get_accessible_dags([permissions.ACTION_CAN_EDIT], user)

    @provide_session
    def get_accessible_dags(
        self,
        user_actions: Container[str] | None,
        user,
        session: Session = NEW_SESSION,
    ) -> Iterable[DagModel]:
        warnings.warn(
            "`get_accessible_dags` has been deprecated. Please use `get_accessible_dag_ids` instead.",
            RemovedInAirflow3Warning,
            stacklevel=3,
        )

        dag_ids = self.get_accessible_dag_ids(user, user_actions, session)
        return session.scalars(select(DagModel).where(DagModel.dag_id.in_(dag_ids)))

    @provide_session
    def get_accessible_dag_ids(
        self,
        user,
        user_actions: Container[str] | None = None,
        session: Session = NEW_SESSION,
    ) -> set[str]:
        warnings.warn(
            "`get_accessible_dag_ids` has been deprecated. Please use `get_permitted_dag_ids` instead.",
            RemovedInAirflow3Warning,
            stacklevel=3,
        )
        if not user_actions:
            user_actions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]
        fab_action_name_to_method_name = {v: k for k, v in MAP_METHOD_NAME_TO_FAB_ACTION_NAME.items()}
        user_methods: Container[ResourceMethod] = [
            fab_action_name_to_method_name[action]
            for action in fab_action_name_to_method_name
            if action in user_actions
        ]
        return self.get_permitted_dag_ids(user=user, methods=user_methods, session=session)

    def can_access_some_dags(self, action: str, dag_id: str | None = None) -> bool:
        """Checks if user has read or write access to some dags."""
        if dag_id and dag_id != "~":
            root_dag_id = self._get_root_dag_id(dag_id)
            return self.has_access(action, permissions.resource_name_for_dag(root_dag_id))

        user = g.user
        if action == permissions.ACTION_CAN_READ:
            return any(self.get_readable_dag_ids(user))
        return any(self.get_editable_dag_ids(user))

    """
    -----------
    Role entity
    -----------
    """

    def update_role(self, role_id, name: str) -> Role | None:
        """Update a role in the database."""
        role = self.get_session.get(self.role_model, role_id)
        if not role:
            return None
        try:
            role.name = name
            self.get_session.merge(role)
            self.get_session.commit()
            log.info(const.LOGMSG_INF_SEC_UPD_ROLE, role)
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_UPD_ROLE, e)
            self.get_session.rollback()
            return None
        return role

    def add_role(self, name: str) -> Role:
        """Add a role in the database."""
        role = self.find_role(name)
        if role is None:
            try:
                role = self.role_model()
                role.name = name
                self.get_session.add(role)
                self.get_session.commit()
                log.info(const.LOGMSG_INF_SEC_ADD_ROLE, name)
                return role
            except Exception as e:
                log.error(const.LOGMSG_ERR_SEC_ADD_ROLE, e)
                self.get_session.rollback()
        return role

    def find_role(self, name):
        """
        Find a role in the database.

        :param name: the role name
        """
        return self.get_session.query(self.role_model).filter_by(name=name).one_or_none()

    def get_all_roles(self):
        return self.get_session.query(self.role_model).all()

    def get_public_role(self):
        return self.get_session.query(self.role_model).filter_by(name=self.auth_role_public).one_or_none()

    def delete_role(self, role_name: str) -> None:
        """
        Delete the given Role.

        :param role_name: the name of a role in the ab_role table
        """
        session = self.get_session
        role = session.query(Role).filter(Role.name == role_name).first()
        if role:
            log.info("Deleting role '%s'", role_name)
            session.delete(role)
            session.commit()
        else:
            raise AirflowException(f"Role named '{role_name}' does not exist")

    """
    -----------
    User entity
    -----------
    """

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
            log.info(const.LOGMSG_INF_SEC_ADD_USER, username)
            return user
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_ADD_USER, e)
            self.get_session.rollback()
            return False

    def load_user(self, user_id):
        """Load user by ID."""
        return self.get_user_by_id(int(user_id))

    def get_user_by_id(self, pk):
        return self.get_session.get(self.user_model, pk)

    def count_users(self):
        """Return the number of users in the database."""
        return self.get_session.query(func.count(self.user_model.id)).scalar()

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
            log.error(const.LOGMSG_ERR_SEC_ADD_REGISTER_USER, e)
            self.get_session.rollback()
            return None

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

    def find_register_user(self, registration_hash):
        return self.get_session.scalar(
            select(self.registeruser_mode)
            .where(self.registeruser_model.registration_hash == registration_hash)
            .limit(1)
        )

    def update_user(self, user):
        try:
            self.get_session.merge(user)
            self.get_session.commit()
            log.info(const.LOGMSG_INF_SEC_UPD_USER, user)
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_UPD_USER, e)
            self.get_session.rollback()
            return False

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
            log.error(const.LOGMSG_ERR_SEC_DEL_REGISTER_USER, e)
            self.get_session.rollback()
            return False

    def get_all_users(self):
        return self.get_session.query(self.user_model).all()

    """
    -------------
    Action entity
    -------------
    """

    def get_action(self, name: str) -> Action:
        """
        Gets an existing action record.

        :param name: name
        :return: Action record, if it exists
        """
        return self.get_session.query(self.action_model).filter_by(name=name).one_or_none()

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
                log.error(const.LOGMSG_ERR_SEC_ADD_PERMISSION, e)
                self.get_session.rollback()
        return action

    def delete_action(self, name: str) -> bool:
        """
        Deletes a permission action.

        :param name: Name of action to delete (e.g. can_read).
        """
        action = self.get_action(name)
        if not action:
            log.warning(const.LOGMSG_WAR_SEC_DEL_PERMISSION, name)
            return False
        try:
            perms = (
                self.get_session.query(self.permission_model)
                .filter(self.permission_model.action == action)
                .all()
            )
            if perms:
                log.warning(const.LOGMSG_WAR_SEC_DEL_PERM_PVM, action, perms)
                return False
            self.get_session.delete(action)
            self.get_session.commit()
            return True
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_DEL_PERMISSION, e)
            self.get_session.rollback()
            return False

    """
    ---------------
    Resource entity
    ---------------
    """

    def get_resource(self, name: str) -> Resource:
        """
        Returns a resource record by name, if it exists.

        :param name: Name of resource
        """
        return self.get_session.query(self.resource_model).filter_by(name=name).one_or_none()

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
                log.error(const.LOGMSG_ERR_SEC_ADD_VIEWMENU, e)
                self.get_session.rollback()
        return resource

    def get_all_resources(self) -> list[Resource]:
        """
        Gets all existing resource records.

        :return: List of all resources
        """
        return self.get_session.query(self.resource_model).all()

    def delete_resource(self, name: str) -> bool:
        """
        Deletes a Resource from the backend.

        :param name:
            name of the resource
        """
        resource = self.get_resource(name)
        if not resource:
            log.warning(const.LOGMSG_WAR_SEC_DEL_VIEWMENU, name)
            return False
        try:
            perms = (
                self.get_session.query(self.permission_model)
                .filter(self.permission_model.resource == resource)
                .all()
            )
            if perms:
                log.warning(const.LOGMSG_WAR_SEC_DEL_VIEWMENU_PVM, resource, perms)
                return False
            self.get_session.delete(resource)
            self.get_session.commit()
            return True
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_DEL_PERMISSION, e)
            self.get_session.rollback()
            return False

    """
    ---------------
    Permission entity
    ---------------
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
            log.info(const.LOGMSG_INF_SEC_ADD_PERMVIEW, perm)
            return perm
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_ADD_PERMVIEW, e)
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
            log.warning(const.LOGMSG_WAR_SEC_DEL_PERMVIEW, resource_name, action_name, roles)
            return
        try:
            # delete permission on resource
            self.get_session.delete(perm)
            self.get_session.commit()
            # if no more permission on permission view, delete permission
            if not self.get_session.query(self.permission_model).filter_by(action=perm.action).all():
                self.delete_action(perm.action.name)
            log.info(const.LOGMSG_INF_SEC_DEL_PERMVIEW, action_name, resource_name)
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_DEL_PERMVIEW, e)
            self.get_session.rollback()

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
                log.info(const.LOGMSG_INF_SEC_ADD_PERMROLE, permission, role.name)
            except Exception as e:
                log.error(const.LOGMSG_ERR_SEC_ADD_PERMROLE, e)
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
                log.info(const.LOGMSG_INF_SEC_DEL_PERMROLE, permission, role.name)
            except Exception as e:
                log.error(const.LOGMSG_ERR_SEC_DEL_PERMROLE, e)
                self.get_session.rollback()

    def get_oauth_user_info(self, provider, resp):
        """
        Get the OAuth user information from different OAuth APIs.

        All providers have different ways to retrieve user info.
        """
        # for GITHUB
        if provider == "github" or provider == "githublocal":
            me = self.oauth_remotes[provider].get("user")
            data = me.json()
            log.debug("User info from GitHub: %s", data)
            return {"username": "github_" + data.get("login")}
        # for twitter
        if provider == "twitter":
            me = self.oauth_remotes[provider].get("account/settings.json")
            data = me.json()
            log.debug("User info from Twitter: %s", data)
            return {"username": "twitter_" + data.get("screen_name", "")}
        # for linkedin
        if provider == "linkedin":
            me = self.oauth_remotes[provider].get(
                "people/~:(id,email-address,first-name,last-name)?format=json"
            )
            data = me.json()
            log.debug("User info from LinkedIn: %s", data)
            return {
                "username": "linkedin_" + data.get("id", ""),
                "email": data.get("email-address", ""),
                "first_name": data.get("firstName", ""),
                "last_name": data.get("lastName", ""),
            }
        # for Google
        if provider == "google":
            me = self.oauth_remotes[provider].get("userinfo")
            data = me.json()
            log.debug("User info from Google: %s", data)
            return {
                "username": "google_" + data.get("id", ""),
                "first_name": data.get("given_name", ""),
                "last_name": data.get("family_name", ""),
                "email": data.get("email", ""),
            }
        # for Azure AD Tenant. Azure OAuth response contains
        # JWT token which has user info.
        # JWT token needs to be base64 decoded.
        # https://docs.microsoft.com/en-us/azure/active-directory/develop/
        # active-directory-protocols-oauth-code
        if provider == "azure":
            log.debug("Azure response received : %s", resp)
            id_token = resp["id_token"]
            log.debug(str(id_token))
            me = self._azure_jwt_token_parse(id_token)
            log.debug("Parse JWT token : %s", me)
            return {
                "name": me.get("name", ""),
                "email": me["upn"],
                "first_name": me.get("given_name", ""),
                "last_name": me.get("family_name", ""),
                "id": me["oid"],
                "username": me["oid"],
                "role_keys": me.get("roles", []),
            }
        # for OpenShift
        if provider == "openshift":
            me = self.oauth_remotes[provider].get("apis/user.openshift.io/v1/users/~")
            data = me.json()
            log.debug("User info from OpenShift: %s", data)
            return {"username": "openshift_" + data.get("metadata").get("name")}
        # for Okta
        if provider == "okta":
            me = self.oauth_remotes[provider].get("userinfo")
            data = me.json()
            log.debug("User info from Okta: %s", data)
            return {
                "username": "okta_" + data.get("sub", ""),
                "first_name": data.get("given_name", ""),
                "last_name": data.get("family_name", ""),
                "email": data.get("email", ""),
                "role_keys": data.get("groups", []),
            }
        # for Keycloak
        if provider in ["keycloak", "keycloak_before_17"]:
            me = self.oauth_remotes[provider].get("openid-connect/userinfo")
            me.raise_for_status()
            data = me.json()
            log.debug("User info from Keycloak: %s", data)
            return {
                "username": data.get("preferred_username", ""),
                "first_name": data.get("given_name", ""),
                "last_name": data.get("family_name", ""),
                "email": data.get("email", ""),
            }
        else:
            return {}

    @staticmethod
    def oauth_token_getter():
        """Authentication (OAuth) token getter function."""
        token = session.get("oauth")
        log.debug("Token Get: %s", token)
        return token

    def check_authorization(
        self,
        perms: Sequence[tuple[str, str]] | None = None,
        dag_id: str | None = None,
    ) -> bool:
        """Checks that the logged in user has the specified permissions."""
        if not perms:
            return True

        for perm in perms:
            if perm in (
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG),
            ):
                can_access_all_dags = self.has_access(*perm)
                if not can_access_all_dags:
                    action = perm[0]
                    if not self.can_access_some_dags(action, dag_id):
                        return False
            elif not self.has_access(*perm):
                return False

        return True

    @staticmethod
    def _azure_parse_jwt(token):
        """
        Parse Azure JWT token content.

        :param token: the JWT token

        :meta private:
        """
        jwt_token_parts = r"^([^\.\s]*)\.([^\.\s]+)\.([^\.\s]*)$"
        matches = re2.search(jwt_token_parts, token)
        if not matches or len(matches.groups()) < 3:
            log.error("Unable to parse token.")
            return {}
        return {
            "header": matches.group(1),
            "Payload": matches.group(2),
            "Sig": matches.group(3),
        }

    @staticmethod
    def _azure_jwt_token_parse(token):
        """
        Parse and decode Azure JWT token.

        :param token: the JWT token

        :meta private:
        """
        jwt_split_token = FabAirflowSecurityManagerOverride._azure_parse_jwt(token)
        if not jwt_split_token:
            return

        jwt_payload = jwt_split_token["Payload"]
        # Prepare for base64 decoding
        payload_b64_string = jwt_payload
        payload_b64_string += "=" * (4 - (len(jwt_payload) % 4))
        decoded_payload = base64.urlsafe_b64decode(payload_b64_string.encode("ascii"))

        if not decoded_payload:
            log.error("Payload of id_token could not be base64 url decoded.")
            return

        jwt_decoded_payload = json.loads(decoded_payload.decode("utf-8"))

        return jwt_decoded_payload
