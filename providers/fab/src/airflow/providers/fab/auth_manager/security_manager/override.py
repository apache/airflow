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

import copy
import datetime
import importlib
import itertools
import logging
import uuid
from collections.abc import Collection, Iterable, Mapping
from typing import TYPE_CHECKING, Any

import jwt
from flask import current_app, flash, g, has_app_context, has_request_context, session
from flask_appbuilder import Model, const
from flask_appbuilder.const import (
    AUTH_DB,
    AUTH_LDAP,
    AUTH_OAUTH,
    AUTH_REMOTE_USER,
    LOGMSG_ERR_SEC_ADD_REGISTER_USER,
    LOGMSG_ERR_SEC_AUTH_LDAP,
    LOGMSG_ERR_SEC_AUTH_LDAP_TLS,
    LOGMSG_WAR_SEC_LOGIN_FAILED,
    LOGMSG_WAR_SEC_NOLDAP_OBJ,
    MICROSOFT_KEY_SET_URL,
)
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.security.api import SecurityApi
from flask_appbuilder.security.registerviews import (
    RegisterUserDBView,
    RegisterUserOAuthView,
)
from flask_appbuilder.security.views import (
    AuthDBView,
    AuthLDAPView,
    AuthOAuthView,
    AuthRemoteUserView,
    RegisterUserModelView,
    UserGroupModelView,
)
from flask_babel import lazy_gettext
from flask_jwt_extended import JWTManager
from flask_login import LoginManager
from itsdangerous import want_bytes
from markupsafe import Markup, escape
from packaging.version import Version
from sqlalchemy import delete, func, inspect, or_, select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import joinedload
from werkzeug.security import check_password_hash, generate_password_hash

from airflow.configuration import conf
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.fab.auth_manager.models import (
    Action,
    Group,
    Permission,
    RegisterUser,
    Resource,
    Role,
    User,
)
from airflow.providers.fab.auth_manager.models.anonymous_user import AnonymousUser
from airflow.providers.fab.auth_manager.security_manager.constants import EXISTING_ROLES
from airflow.providers.fab.auth_manager.views.permissions import (
    ActionModelView,
    PermissionPairModelView,
    ResourceModelView,
)
from airflow.providers.fab.auth_manager.views.roles_list import CustomRoleModelView
from airflow.providers.fab.auth_manager.views.user import (
    CustomUserDBModelView,
    CustomUserLDAPModelView,
    CustomUserOAuthModelView,
    CustomUserRemoteUserModelView,
)
from airflow.providers.fab.auth_manager.views.user_edit import (
    CustomResetMyPasswordView,
    CustomResetPasswordView,
    CustomUserInfoEditView,
)
from airflow.providers.fab.auth_manager.views.user_stats import CustomUserStatsChartView
from airflow.providers.fab.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.fab.www.security import permissions
from airflow.providers.fab.www.security_manager import AirflowSecurityManagerV2
from airflow.providers.fab.www.session import AirflowDatabaseSessionInterface

if TYPE_CHECKING:
    from airflow.providers.fab.www.security.permissions import (
        RESOURCE_ASSET,
        RESOURCE_ASSET_ALIAS,
    )
    from airflow.sdk import DAG
    from airflow.serialization.definitions.dag import SerializedDAG
else:
    from airflow.providers.common.compat.security.permissions import (
        RESOURCE_ASSET,
        RESOURCE_ASSET_ALIAS,
    )

if AIRFLOW_V_3_1_PLUS:
    from airflow.models.dagbag import DBDagBag
    from airflow.utils.session import create_session

    def _iter_dags() -> Iterable[DAG | SerializedDAG]:
        with create_session() as session:
            yield from DBDagBag().iter_all_latest_version_dags(session=session)
else:
    try:
        from airflow.models.dagbag import DagBag
    except (ImportError, AttributeError):
        DagBag = None

    def _iter_dags() -> Iterable[DAG | SerializedDAG]:
        if DagBag is None:
            return []
        dagbag = DagBag(read_dags_from_db=True)
        if hasattr(dagbag, "collect_dags_from_db"):
            dagbag.collect_dags_from_db()
        return dagbag.dags.values()


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

    auth_view = None
    """ The obj instance for authentication view """
    registeruser_view = None
    """ The obj instance for registering user view """
    user_view = None
    """ The obj instance for user view """

    """ Models """
    user_model = User
    role_model = Role
    group_model = Group
    action_model = Action
    resource_model = Resource
    permission_model = Permission

    """ Views """
    authdbview = AuthDBView
    """ Override if you want your own Authentication DB view """
    authldapview = AuthLDAPView
    """ Override if you want your own Authentication LDAP view """
    authoauthview = AuthOAuthView
    """ Override if you want your own Authentication OAuth view """
    authremoteuserview = AuthRemoteUserView
    """ Override if you want your own Authentication REMOTE_USER view """
    registeruserdbview = RegisterUserDBView
    """ Override if you want your own register user db view """
    registeruseroauthview = RegisterUserOAuthView
    """ Override if you want your own register user OAuth view """
    actionmodelview = ActionModelView
    permissionmodelview = PermissionPairModelView
    rolemodelview = CustomRoleModelView
    groupmodelview = UserGroupModelView
    registeruser_model = RegisterUser
    registerusermodelview = RegisterUserModelView
    resourcemodelview = ResourceModelView
    userdbmodelview = CustomUserDBModelView
    resetmypasswordview = CustomResetMyPasswordView
    resetpasswordview = CustomResetPasswordView
    userinfoeditview = CustomUserInfoEditView
    userldapmodelview = CustomUserLDAPModelView
    useroauthmodelview = CustomUserOAuthModelView
    userremoteusermodelview = CustomUserRemoteUserModelView
    userstatschartview = CustomUserStatsChartView

    # API
    security_api = SecurityApi
    """ Override if you want your own Security API login endpoint """

    jwt_manager = None
    """ Flask-JWT-Extended """
    oauth = None
    oauth_remotes: dict[str, Any]
    """ Initialized (remote_app) providers dict {'provider_name', OBJ } """

    oauth_user_info = None

    oauth_allow_list: dict[str, list] = {}
    """ OAuth email allow list """

    # global resource for dag-level access
    DAG_RESOURCES = {permissions.RESOURCE_DAG}

    ###########################################################################
    #                               PERMISSIONS
    ###########################################################################

    # [START security_viewer_perms]
    VIEWER_PERMISSIONS = [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_DEPENDENCIES),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_VERSION),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_WARNING),
        (permissions.ACTION_CAN_READ, RESOURCE_ASSET),
        (permissions.ACTION_CAN_READ, RESOURCE_ASSET_ALIAS),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_BACKFILL),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_CLUSTER_ACTIVITY),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_JOB),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PASSWORD),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PASSWORD),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_SLA_MISS),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_HITL_DETAIL),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_BROWSE_MENU),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG_DEPENDENCIES),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_ACCESS_MENU, RESOURCE_ASSET),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_CLUSTER_ACTIVITY),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DOCS),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DOCS_MENU),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_JOB),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_SLA_MISS),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_INSTANCE),
    ]
    # [END security_viewer_perms]

    # [START security_user_perms]
    USER_PERMISSIONS = [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_HITL_DETAIL),
        (permissions.ACTION_CAN_CREATE, RESOURCE_ASSET),
    ]
    # [END security_user_perms]

    # [START security_op_perms]
    OP_PERMISSIONS = [
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_ADMIN_MENU),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_CONFIG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_PLUGIN),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_PROVIDER),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_XCOM),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_HITL_DETAIL),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PROVIDER),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_XCOM),
        (permissions.ACTION_CAN_CREATE, RESOURCE_ASSET),
        (permissions.ACTION_CAN_DELETE, RESOURCE_ASSET),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_BACKFILL),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_BACKFILL),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_BACKFILL),
    ]
    # [END security_op_perms]

    # [START security_admin_perms]
    ADMIN_PERMISSIONS = [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_AUDIT_LOG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_RESCHEDULE),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_RESCHEDULE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TRIGGER),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TRIGGER),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PASSWORD),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_PASSWORD),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE),
    ]
    # [END security_admin_perms]

    ###########################################################################
    #                     DEFAULT ROLE CONFIGURATIONS
    ###########################################################################

    ROLE_CONFIGS: list[dict[str, Any]] = [
        {"role": "Public", "perms": []},
        {"role": "Viewer", "perms": VIEWER_PERMISSIONS},
        {
            "role": "User",
            "perms": VIEWER_PERMISSIONS + USER_PERMISSIONS,
        },
        {
            "role": "Op",
            "perms": VIEWER_PERMISSIONS + USER_PERMISSIONS + OP_PERMISSIONS,
        },
        {
            "role": "Admin",
            "perms": VIEWER_PERMISSIONS + USER_PERMISSIONS + OP_PERMISSIONS + ADMIN_PERMISSIONS,
        },
    ]

    # global resource for dag-level access
    RESOURCE_DETAILS_MAP = getattr(
        permissions,
        "RESOURCE_DETAILS_MAP",
        {
            permissions.RESOURCE_DAG: {
                "actions": permissions.DAG_ACTIONS,
            }
        },
    )
    DAG_ACTIONS = RESOURCE_DETAILS_MAP[permissions.RESOURCE_DAG]["actions"]

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

    def _get_authentik_jwks(self, jwks_url) -> dict:
        import requests

        resp = requests.get(jwks_url)
        if resp.status_code == 200:
            return resp.json()
        return {}

    def _validate_jwt(self, id_token, jwks):
        from authlib.jose import JsonWebKey, jwt as authlib_jwt

        keyset = JsonWebKey.import_key_set(jwks)
        claims = authlib_jwt.decode(id_token, keyset)
        claims.validate()
        log.info("JWT token is validated")
        return claims

    def _get_authentik_token_info(self, id_token):
        me = jwt.decode(id_token, options={"verify_signature": False})

        verify_signature = self.oauth_remotes["authentik"].client_kwargs.get("verify_signature", True)
        if verify_signature:
            # Validate the token using authentik certificate
            jwks_uri = self.oauth_remotes["authentik"].server_metadata.get("jwks_uri")
            if jwks_uri:
                jwks = self._get_authentik_jwks(jwks_uri)
                if jwks:
                    return self._validate_jwt(id_token, jwks)
            else:
                log.error("jwks_uri not specified in OAuth Providers, could not verify token signature")
        else:
            # Return the token info without validating
            log.warning("JWT token is not validated!")
            return me

        raise AirflowException("OAuth signature verify failed")

    def register_views(self):
        """Register FAB auth manager related views."""
        if not current_app.config.get("FAB_ADD_SECURITY_VIEWS", True):
            return

        # Security APIs
        self.appbuilder.add_api(self.security_api)

        if self.auth_user_registration:
            if self.auth_type == AUTH_DB:
                self.registeruser_view = self.registeruserdbview()
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
            icon="fa-user-gear",
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
        if current_app.config.get("FAB_ADD_SECURITY_PERMISSION_VIEW", True):
            self.appbuilder.add_view(
                self.actionmodelview,
                "Actions",
                icon="fa-lock",
                label=lazy_gettext("Actions"),
                category="Security",
            )
        if current_app.config.get("FAB_ADD_SECURITY_VIEW_MENU_VIEW", True):
            self.appbuilder.add_view(
                self.resourcemodelview,
                "Resources",
                icon="fa-list-alt",
                label=lazy_gettext("Resources"),
                category="Security",
            )
        if current_app.config.get("FAB_ADD_SECURITY_PERMISSION_VIEWS_VIEW", True):
            self.appbuilder.add_view(
                self.permissionmodelview,
                "Permission Pairs",
                icon="fa-link",
                label=lazy_gettext("Permissions"),
                category="Security",
            )

    @property
    def session(self):
        return self.appbuilder.session

    def create_login_manager(self) -> LoginManager:
        """Create the login manager."""
        lm = LoginManager(current_app)
        lm.anonymous_user = AnonymousUser
        lm.login_view = "login"
        lm.user_loader(self.load_user)
        return lm

    def create_jwt_manager(self):
        """Create the JWT manager."""
        jwt_manager = JWTManager()
        jwt_manager.init_app(current_app)
        jwt_manager.user_lookup_loader(self.load_user_jwt)

    def reset_password(self, userid: int, password: str) -> bool:
        """
        Change/Reset a user's password for auth db.

        Password will be hashed and saved.

        :param userid: the user id to reset the password
        :param password: the clear text password to reset and save hashed on the db
        """
        user = self.get_user_by_id(userid)
        user.password = generate_password_hash(password)
        self.reset_user_sessions(user)
        return self.update_user(user)

    def reset_user_sessions(self, user: User) -> None:
        if isinstance(current_app.session_interface, AirflowDatabaseSessionInterface):
            interface = current_app.session_interface
            session = interface.client.session
            user_session_model = interface.sql_session_model
            num_sessions = session.scalars(select(func.count()).select_from(user_session_model)).one()
            if num_sessions > MAX_NUM_DATABASE_USER_SESSIONS:
                safe_username = escape(user.username)
                self._cli_safe_flash(
                    f"The old sessions for user {safe_username} have <b>NOT</b> been deleted!<br>"
                    f"You have a lot ({num_sessions}) of user sessions in the 'SESSIONS' table in "
                    f"your database.<br> "
                    "This indicates that this deployment might have an automated API calls that create "
                    "and not reuse sessions.<br>You should consider reusing sessions or cleaning them "
                    "periodically using db clean.<br>"
                    "Make sure to reset password for the user again after cleaning the session table "
                    "to remove old sessions of the user.",
                    "warning",
                )
            else:
                for s in session.scalars(select(user_session_model)).all():
                    session_details = interface.serializer.decode(want_bytes(s.data))
                    if session_details.get("_user_id") == user.id:
                        session.delete(s)
                session.commit()
        else:
            safe_username = escape(user.username)
            self._cli_safe_flash(
                "Since you are using `securecookie` session backend mechanism, we cannot prevent "
                f"some old sessions for user {safe_username} to be reused.<br> If you want to make sure "
                "that the user is logged out from all sessions, you should consider using "
                "`database` session backend mechanism.<br> You can also change the 'secret_key` "
                "webserver configuration for all your webserver instances and restart the webserver. "
                "This however will logout all users from all sessions.",
                "warning",
            )

    def load_user_jwt(self, _jwt_header, jwt_data):
        identity = jwt_data["sub"]
        user = self.load_user(identity)
        if user and user.is_active:
            # Set flask g.user to JWT user, we can't do it on before request
            g.user = user
            return user

    @property
    def auth_type(self):
        """Get the auth type."""
        return current_app.config["AUTH_TYPE"]

    @property
    def is_auth_limited(self) -> bool:
        """Is the auth rate limited."""
        return current_app.config["AUTH_RATE_LIMITED"]

    @property
    def auth_rate_limit(self) -> str:
        """Get the auth rate limit."""
        return current_app.config["AUTH_RATE_LIMIT"]

    @property
    def auth_role_public(self):
        """Get the public role."""
        return current_app.config.get("AUTH_ROLE_PUBLIC", None)

    @property
    def oauth_providers(self):
        """Oauth providers."""
        return current_app.config["OAUTH_PROVIDERS"]

    @property
    def auth_ldap_tls_cacertdir(self):
        """LDAP TLS CA certificate directory."""
        return current_app.config["AUTH_LDAP_TLS_CACERTDIR"]

    @property
    def auth_ldap_tls_cacertfile(self):
        """LDAP TLS CA certificate file."""
        return current_app.config["AUTH_LDAP_TLS_CACERTFILE"]

    @property
    def auth_ldap_tls_certfile(self):
        """LDAP TLS certificate file."""
        return current_app.config["AUTH_LDAP_TLS_CERTFILE"]

    @property
    def auth_ldap_tls_keyfile(self):
        """LDAP TLS key file."""
        return current_app.config["AUTH_LDAP_TLS_KEYFILE"]

    @property
    def auth_ldap_use_nested_groups_for_roles(self):
        return self.appbuilder.get_app.config["AUTH_LDAP_USE_NESTED_GROUPS_FOR_ROLES"]

    @property
    def auth_ldap_allow_self_signed(self):
        """LDAP allow self signed."""
        return current_app.config["AUTH_LDAP_ALLOW_SELF_SIGNED"]

    @property
    def auth_ldap_tls_demand(self):
        """LDAP TLS demand."""
        return current_app.config["AUTH_LDAP_TLS_DEMAND"]

    @property
    def auth_ldap_server(self):
        """Get the LDAP server object."""
        return current_app.config["AUTH_LDAP_SERVER"]

    @property
    def auth_ldap_use_tls(self):
        """Should LDAP use TLS."""
        return current_app.config["AUTH_LDAP_USE_TLS"]

    @property
    def auth_ldap_bind_user(self):
        """LDAP bind user."""
        return current_app.config["AUTH_LDAP_BIND_USER"]

    @property
    def auth_ldap_bind_password(self):
        """LDAP bind password."""
        return current_app.config["AUTH_LDAP_BIND_PASSWORD"]

    @property
    def auth_ldap_search(self):
        """LDAP search object."""
        return current_app.config["AUTH_LDAP_SEARCH"]

    @property
    def auth_ldap_search_filter(self):
        """LDAP search filter."""
        return current_app.config["AUTH_LDAP_SEARCH_FILTER"]

    @property
    def auth_ldap_uid_field(self):
        """LDAP UID field."""
        return current_app.config["AUTH_LDAP_UID_FIELD"]

    @property
    def auth_ldap_firstname_field(self):
        """LDAP first name field."""
        return current_app.config["AUTH_LDAP_FIRSTNAME_FIELD"]

    @property
    def auth_ldap_lastname_field(self):
        """LDAP last name field."""
        return current_app.config["AUTH_LDAP_LASTNAME_FIELD"]

    @property
    def auth_ldap_email_field(self):
        """LDAP email field."""
        return current_app.config["AUTH_LDAP_EMAIL_FIELD"]

    @property
    def auth_ldap_append_domain(self):
        """LDAP append domain."""
        return current_app.config["AUTH_LDAP_APPEND_DOMAIN"]

    @property
    def auth_ldap_username_format(self):
        """LDAP username format."""
        return current_app.config["AUTH_LDAP_USERNAME_FORMAT"]

    @property
    def auth_ldap_group_field(self) -> str:
        """LDAP group field."""
        return current_app.config["AUTH_LDAP_GROUP_FIELD"]

    @property
    def auth_roles_mapping(self) -> dict[str, list[str]]:
        """The mapping of auth roles."""
        return current_app.config["AUTH_ROLES_MAPPING"]

    @property
    def auth_user_registration_role_jmespath(self) -> str:
        """The JMESPATH role to use for user registration."""
        return current_app.config["AUTH_USER_REGISTRATION_ROLE_JMESPATH"]

    @property
    def auth_remote_user_env_var(self) -> str:
        return current_app.config["AUTH_REMOTE_USER_ENV_VAR"]

    @property
    def auth_username_ci(self):
        """Get the auth username for CI."""
        return current_app.config.get("AUTH_USERNAME_CI", True)

    @property
    def auth_user_registration(self):
        """Will user self registration be allowed."""
        return current_app.config["AUTH_USER_REGISTRATION"]

    @property
    def auth_user_registration_role(self):
        """The default user self registration role."""
        return current_app.config["AUTH_USER_REGISTRATION_ROLE"]

    @property
    def auth_roles_sync_at_login(self) -> bool:
        """Should roles be synced at login."""
        return current_app.config["AUTH_ROLES_SYNC_AT_LOGIN"]

    @property
    def auth_role_admin(self):
        """Get the admin role."""
        return current_app.config["AUTH_ROLE_ADMIN"]

    @property
    def oauth_whitelists(self):
        return self.oauth_allow_list

    @staticmethod
    def create_builtin_roles():
        return current_app.config.get("FAB_ROLES", {})

    @property
    def builtin_roles(self):
        """Get the builtin roles."""
        return self._builtin_roles

    @property
    def api_login_allow_multiple_providers(self):
        return current_app.config["AUTH_API_LOGIN_ALLOW_MULTIPLE_PROVIDERS"]

    @property
    def auth_type_provider_name(self):
        provider_to_auth_type = {AUTH_DB: "db", AUTH_LDAP: "ldap"}
        return provider_to_auth_type.get(self.auth_type)

    def _init_config(self):
        """
        Initialize config.

        :meta private:
        """
        # Base Security Config
        current_app.config.setdefault("AUTH_ROLE_ADMIN", "Admin")
        current_app.config.setdefault("AUTH_TYPE", AUTH_DB)
        # Self Registration
        current_app.config.setdefault("AUTH_USER_REGISTRATION", False)
        current_app.config.setdefault("AUTH_USER_REGISTRATION_ROLE", self.auth_role_public)
        current_app.config.setdefault("AUTH_USER_REGISTRATION_ROLE_JMESPATH", None)
        # Role Mapping
        current_app.config.setdefault("AUTH_ROLES_MAPPING", {})
        current_app.config.setdefault("AUTH_ROLES_SYNC_AT_LOGIN", False)
        current_app.config.setdefault("AUTH_API_LOGIN_ALLOW_MULTIPLE_PROVIDERS", False)

        parsed_werkzeug_version = Version(importlib.metadata.version("werkzeug"))
        if parsed_werkzeug_version < Version("3.0.0"):
            current_app.config.setdefault("FAB_PASSWORD_HASH_METHOD", "pbkdf2:sha256")
            current_app.config.setdefault(
                "AUTH_DB_FAKE_PASSWORD_HASH_CHECK",
                "pbkdf2:sha256:150000$Z3t6fmj2$22da622d94a1f8118"
                "c0976a03d2f18f680bfff877c9a965db9eedc51bc0be87c",
            )
        else:
            current_app.config.setdefault("FAB_PASSWORD_HASH_METHOD", "scrypt")
            current_app.config.setdefault(
                "AUTH_DB_FAKE_PASSWORD_HASH_CHECK",
                "scrypt:32768:8:1$wiDa0ruWlIPhp9LM$6e409d093e62ad54df2af895d0e125b05ff6cf6414"
                "8350189ffc4bcc71286edf1b8ad94a442c00f890224bf2b32153d0750c89ee9"
                "401e62f9dcee5399065e4e5",
            )

        # LDAP Config
        if self.auth_type == AUTH_LDAP:
            if "AUTH_LDAP_SERVER" not in current_app.config:
                raise ValueError("No AUTH_LDAP_SERVER defined on config with AUTH_LDAP authentication type.")
            current_app.config.setdefault("AUTH_LDAP_SEARCH", "")
            current_app.config.setdefault("AUTH_LDAP_SEARCH_FILTER", "")
            current_app.config.setdefault("AUTH_LDAP_APPEND_DOMAIN", "")
            current_app.config.setdefault("AUTH_LDAP_USERNAME_FORMAT", "")
            current_app.config.setdefault("AUTH_LDAP_BIND_USER", "")
            current_app.config.setdefault("AUTH_LDAP_BIND_PASSWORD", "")
            # TLS options
            current_app.config.setdefault("AUTH_LDAP_USE_TLS", False)
            current_app.config.setdefault("AUTH_LDAP_ALLOW_SELF_SIGNED", False)
            current_app.config.setdefault("AUTH_LDAP_TLS_DEMAND", False)
            current_app.config.setdefault("AUTH_LDAP_TLS_CACERTDIR", "")
            current_app.config.setdefault("AUTH_LDAP_TLS_CACERTFILE", "")
            current_app.config.setdefault("AUTH_LDAP_TLS_CERTFILE", "")
            current_app.config.setdefault("AUTH_LDAP_TLS_KEYFILE", "")
            # Mapping options
            current_app.config.setdefault("AUTH_LDAP_UID_FIELD", "uid")
            current_app.config.setdefault("AUTH_LDAP_GROUP_FIELD", "memberOf")
            current_app.config.setdefault("AUTH_LDAP_FIRSTNAME_FIELD", "givenName")
            current_app.config.setdefault("AUTH_LDAP_LASTNAME_FIELD", "sn")
            current_app.config.setdefault("AUTH_LDAP_EMAIL_FIELD", "mail")

            # Nested groups options
            current_app.config.setdefault("AUTH_LDAP_USE_NESTED_GROUPS_FOR_ROLES", False)

        if self.auth_type == AUTH_REMOTE_USER:
            current_app.config.setdefault("AUTH_REMOTE_USER_ENV_VAR", "REMOTE_USER")

        # Rate limiting
        current_app.config.setdefault("AUTH_RATE_LIMITED", True)
        current_app.config.setdefault("AUTH_RATE_LIMIT", "5 per 40 second")

    def _init_auth(self):
        """
        Initialize authentication configuration.

        :meta private:
        """
        if self.auth_type == AUTH_OAUTH:
            from authlib.integrations.flask_client import OAuth

            self.oauth = OAuth(current_app)
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
        elif self.auth_type == const.AUTH_OAUTH:
            self.useroauthmodelview.datamodel = user_data_model
        elif self.auth_type == const.AUTH_REMOTE_USER:
            self.userremoteusermodelview.datamodel = user_data_model

        if self.userstatschartview:
            self.userstatschartview.datamodel = user_data_model
        if self.auth_user_registration:
            self.registerusermodelview.datamodel = SQLAInterface(self.registeruser_model)

        self.rolemodelview.datamodel = SQLAInterface(self.role_model)
        self.groupmodelview.datamodel = SQLAInterface(self.group_model)
        self.actionmodelview.datamodel = SQLAInterface(self.action_model)
        self.resourcemodelview.datamodel = SQLAInterface(self.resource_model)
        self.permissionmodelview.datamodel = SQLAInterface(self.permission_model)

    def create_db(self):
        """
        Create the database.

        Creates admin and public roles if they don't exist.
        """
        if not current_app.config.get("FAB_CREATE_DB", True):
            return
        if not has_app_context():
            # Create a new application context
            with current_app.app_context():
                self._create_db()
        else:
            self._create_db()

    def _create_db(self) -> None:
        engine = self.session.get_bind(mapper=None, clause=None)
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        if "ab_user" not in existing_tables or "ab_group" not in existing_tables:
            log.info(const.LOGMSG_INF_SEC_NO_DB)
            Model.metadata.create_all(engine)
            log.info(const.LOGMSG_INF_SEC_ADD_DB)

        roles_mapping = current_app.config.get("FAB_ROLES_MAPPING", {})
        for pk, name in roles_mapping.items():
            self.update_role(pk, name)
        for role_name in self._builtin_roles:
            self.add_role(role_name)
        if self.auth_role_admin not in self._builtin_roles:
            self.add_role(self.auth_role_admin)
        if self.auth_role_public:
            self.add_role(self.auth_role_public)
        if self.count_users() == 0 and self.auth_role_public != self.auth_role_admin:
            log.warning(const.LOGMSG_WAR_SEC_NO_USER)

    def get_all_permissions(self) -> set[tuple[str, str]]:
        """Return all permissions as a set of tuples with the action and resource names."""
        return set(
            self.session.execute(
                select(self.action_model.name, self.resource_model.name)
                .join(self.permission_model.action)
                .join(self.permission_model.resource)
            )
        )

    def create_dag_specific_permissions(self) -> None:
        """
        Add permissions to all DAGs.

        Creates 'can_read', 'can_edit', and 'can_delete' permissions for all
        DAGs, along with any `access_control` permissions provided in them.

        This does iterate through ALL the DAGs, which can be slow. See `sync_perm_for_dag`
        if you only need to sync a single DAG.
        """
        perms = self.get_all_permissions()

        for dag in _iter_dags():
            print(dag)
            for resource_name, resource_values in self.RESOURCE_DETAILS_MAP.items():
                dag_resource_name = permissions.resource_name(dag.dag_id, resource_name)
                for action_name in resource_values["actions"]:
                    if (action_name, dag_resource_name) not in perms:
                        self._merge_perm(action_name, dag_resource_name)

            if dag.access_control is not None:
                self.sync_perm_for_dag(dag.dag_id, dag.access_control)

    def sync_perm_for_dag(
        self,
        dag_id: str,
        access_control: Mapping[str, Mapping[str, Collection[str]] | Collection[str]] | None = None,
    ) -> None:
        """
        Sync permissions for given dag id.

        The dag id surely exists in our dag bag as only / refresh button or DagBag will call this function.

        :param dag_id: the ID of the DAG whose permissions should be updated
        :param access_control: a dict where each key is a role name and each value can be:
             - a set() of DAGs resource action names (e.g. `{'can_read'}`)
             - or a dict where each key is a resource name ('DAGs' or 'DAG Runs') and each value
             is a set() of action names (e.g., `{'DAG Runs': {'can_create'}, 'DAGs': {'can_read'}}`)
        :return:
        """
        for resource_name, resource_values in self.RESOURCE_DETAILS_MAP.items():
            dag_resource_name = permissions.resource_name(dag_id, resource_name)
            for dag_action_name in resource_values["actions"]:
                self.create_permission(dag_action_name, dag_resource_name)

        if access_control is not None:
            self.log.debug("Syncing DAG-level permissions for DAG '%s'", dag_id)
            self._sync_dag_view_permissions(dag_id, copy.copy(access_control))
        else:
            self.log.debug(
                "Not syncing DAG-level permissions for DAG '%s' as access control is unset.",
                dag_id,
            )

    def _sync_dag_view_permissions(
        self,
        dag_id: str,
        access_control: Mapping[str, Mapping[str, Collection[str]] | Collection[str]],
    ) -> None:
        """
        Set the access policy on the given DAG's ViewModel.

        :param dag_id: the ID of the DAG whose permissions should be updated
        :param access_control: a dict where each key is a role name and each value is:
            - a dict where each key is a resource name ('DAGs' or 'DAG Runs') and each value
            is a set() of action names (e.g., `{'DAG Runs': {'can_create'}, 'DAGs': {'can_read'}}`)
        """

        def _get_or_create_dag_permission(action_name: str, dag_resource_name: str) -> Permission | None:
            perm = self.get_permission(action_name, dag_resource_name)
            if not perm:
                self.log.info(
                    "Creating new action '%s' on resource '%s'",
                    action_name,
                    dag_resource_name,
                )
                perm = self.create_permission(action_name, dag_resource_name)
            return perm

        # Revoking stale permissions for all possible DAG level resources
        for resource_name in self.RESOURCE_DETAILS_MAP.keys():
            dag_resource_name = permissions.resource_name(dag_id, resource_name)
            if resource := self.get_resource(dag_resource_name):
                existing_dag_perms = self.get_resource_permissions(resource)
                for perm in existing_dag_perms:
                    non_admin_roles = [role for role in perm.role if role.name != "Admin"]
                    for role in non_admin_roles:
                        access_control_role = access_control.get(role.name)
                        target_perms_for_role = set()
                        if access_control_role:
                            if isinstance(access_control_role, set):
                                target_perms_for_role = access_control_role
                            elif isinstance(access_control_role, dict):
                                target_perms_for_role = access_control_role.get(resource_name, set())
                        if perm.action.name not in target_perms_for_role:
                            self.log.info(
                                "Revoking '%s' on DAG '%s' for role '%s'",
                                perm.action,
                                dag_resource_name,
                                role.name,
                            )
                            self.remove_permission_from_role(role, perm)

        # Adding the access control permissions
        for rolename, resource_actions_raw in access_control.items():
            role = self.find_role(rolename)
            if not role:
                raise AirflowException(
                    f"The access_control mapping for DAG '{dag_id}' includes a role named "
                    f"'{rolename}', but that role does not exist"
                )

            # Support for old-style access_control where only the actions are specified
            resource_actions = (
                resource_actions_raw
                if isinstance(resource_actions_raw, dict)
                else {permissions.RESOURCE_DAG: set(resource_actions_raw)}
            )

            for resource_name, actions in resource_actions.items():
                if resource_name not in self.RESOURCE_DETAILS_MAP:
                    raise AirflowException(
                        f"The access_control map for DAG '{dag_id}' includes the following invalid "
                        f"resource name: '{resource_name}'; "
                        f"The set of valid resource names is: {self.RESOURCE_DETAILS_MAP.keys()}"
                    )

                dag_resource_name = permissions.resource_name(dag_id, resource_name)
                self.log.debug("Syncing DAG-level permissions for DAG '%s'", dag_resource_name)

                invalid_actions = set(actions) - self.RESOURCE_DETAILS_MAP[resource_name]["actions"]

                if invalid_actions:
                    raise AirflowException(
                        f"The access_control map for DAG '{dag_resource_name}' includes "
                        f"the following invalid permissions: {invalid_actions}; "
                        f"The set of valid permissions is: {self.RESOURCE_DETAILS_MAP[resource_name]['actions']}"
                    )

                for action_name in actions:
                    dag_perm = _get_or_create_dag_permission(action_name, dag_resource_name)
                    if dag_perm:
                        self.add_permission_to_role(role, dag_perm)

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
                    if not admin_role:
                        admin_role = self.add_role(self.auth_role_admin)
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

    def sync_roles(self) -> None:
        """
        Initialize default and custom roles with related permissions.

        1. Init the default role(Admin, Viewer, User, Op, public)
           with related permissions.
        2. Init the custom role(dag-user) with related permissions.
        """
        # Create global all-dag permissions
        self.create_perm_vm_for_all_dag()

        # Sync the default roles (Admin, Viewer, User, Op, public) with related permissions
        self.bulk_sync_roles(self.ROLE_CONFIGS)

        self.add_homepage_access_to_custom_roles()
        # init existing roles, the rest role could be created through UI.
        self.update_admin_permission()
        self.clean_perms()

    def create_perm_vm_for_all_dag(self) -> None:
        """Create perm-vm if not exist and insert into FAB security model for all-dags."""
        # create perm for global logical dag
        for resource_name, action_name in itertools.product(self.DAG_RESOURCES, self.DAG_ACTIONS):
            self._merge_perm(action_name, resource_name)

    def add_homepage_access_to_custom_roles(self) -> None:
        """Add Website.can_read access to all custom roles."""
        website_permission = self.create_permission(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)
        custom_roles = [role for role in self.get_all_roles() if role.name not in EXISTING_ROLES]
        for role in custom_roles:
            self.add_permission_to_role(role, website_permission)

        self.session.commit()

    def update_admin_permission(self) -> None:
        """
        Add missing permissions to the table for admin.

        Admin should get all the permissions, except the dag permissions
        because Admin already has Dags permission.
        Add the missing ones to the table for admin.
        """
        prefixes = getattr(permissions, "PREFIX_LIST", [permissions.RESOURCE_DAG_PREFIX])
        dag_resources = self.session.scalars(
            select(Resource).where(or_(*[Resource.name.like(f"{prefix}%") for prefix in prefixes]))
        )
        resource_ids = [resource.id for resource in dag_resources]

        perms = self.session.scalars(select(Permission).where(~Permission.resource_id.in_(resource_ids)))
        perms = [p for p in perms if p.action and p.resource]

        admin = self.find_role("Admin")
        admin.permissions = list(set(admin.permissions) | set(perms))

        self.session.commit()

    def clean_perms(self) -> None:
        """FAB leaves faulty permissions that need to be cleaned up."""
        self.log.debug("Cleaning faulty perms")
        perms = self.session.scalars(
            select(Permission).where(
                or_(
                    Permission.action == None,  # noqa: E711
                    Permission.resource == None,  # noqa: E711
                )
            )
        ).all()
        # Since FAB doesn't define ON DELETE CASCADE on these tables, we need
        # to delete the _object_ so that SQLA knows to delete the many-to-many
        # relationship object too. :(

        deleted_count = 0
        for perm in perms:
            self.session.delete(perm)
            deleted_count += 1
        self.session.commit()
        if deleted_count:
            self.log.info("Deleted %s faulty permissions", deleted_count)

    def perms_include_action(self, perms, action_name):
        return any(perm.action and perm.action.name == action_name for perm in perms)

    def bulk_sync_roles(self, roles: Iterable[dict[str, Any]]) -> None:
        """Sync the provided roles and permissions."""
        existing_roles = self._get_all_roles_with_permissions()
        non_dag_perms = self._get_all_non_dag_permissions()

        for config in roles:
            role_name = config["role"]
            perms = config["perms"]
            role = existing_roles.get(role_name) or self.add_role(role_name)

            for action_name, resource_name in perms:
                perm = non_dag_perms.get((action_name, resource_name)) or self.create_permission(
                    action_name, resource_name
                )

                if perm not in role.permissions:
                    self.add_permission_to_role(role, perm)

    """
    -----------
    Role entity
    -----------
    """

    def update_role(self, role_id, name: str) -> Role | None:
        """Update a role in the database."""
        role = self.session.get(self.role_model, role_id)
        if not role:
            return None
        try:
            role.name = name
            self.session.merge(role)
            self.session.commit()
            log.info(const.LOGMSG_INF_SEC_UPD_ROLE, role)
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_UPD_ROLE, e)
            self.session.rollback()
            return None
        return role

    def add_role(self, name: str) -> Role:
        """Add a role in the database."""
        role = self.find_role(name)
        if role is None:
            try:
                role = self.role_model()
                role.name = name
                self.session.add(role)
                self.session.commit()
                log.info(const.LOGMSG_INF_SEC_ADD_ROLE, name)
                return role
            except Exception as e:
                log.error(const.LOGMSG_ERR_SEC_ADD_ROLE, e)
                self.session.rollback()
        return role

    def find_role(self, name):
        """
        Find a role in the database.

        :param name: the role name
        """
        return self.session.scalars(select(self.role_model).filter_by(name=name)).unique().one_or_none()

    def get_all_roles(self):
        return self.session.scalars(select(self.role_model)).unique().all()

    def delete_role(self, role_name: str) -> None:
        """
        Delete the given Role.

        :param role_name: the name of a role in the ab_role table
        """
        role = self.session.scalars(select(Role).where(Role.name == role_name)).first()
        if role:
            log.info("Deleting role '%s'", role_name)
            self.session.execute(delete(Role).where(Role.name == role_name))
            self.session.commit()
        else:
            raise AirflowException(f"Role named '{role_name}' does not exist")

    def get_roles_from_keys(self, role_keys: list[str]) -> set[Role]:
        """
        Construct a list of FAB role objects, from a list of keys.

        NOTE:
        - keys are things like: "LDAP group DNs" or "OAUTH group names"
        - we use AUTH_ROLES_MAPPING to map from keys, to FAB role names

        :param role_keys: the list of FAB role keys
        """
        _roles = set()
        _role_keys = set(role_keys)
        for role_key, fab_role_names in self.auth_roles_mapping.items():
            if role_key in _role_keys:
                for fab_role_name in fab_role_names:
                    fab_role = self.find_role(fab_role_name)
                    if fab_role:
                        _roles.add(fab_role)
                    else:
                        log.warning(
                            "Can't find role specified in AUTH_ROLES_MAPPING: %s",
                            fab_role_name,
                        )
        return _roles

    def get_public_role(self):
        return (
            self.session.scalars(select(self.role_model).filter_by(name=self.auth_role_public))
            .unique()
            .one_or_none()
        )

    """
    -----------
    User entity
    -----------
    """

    def add_user(
        self,
        username: str,
        first_name: str,
        last_name: str,
        email: str,
        role: list[Role] | Role | None = None,
        password: str = "",
        hashed_password: str = "",
        groups: list[Group] | None = None,
    ):
        """Create a user."""
        roles: list[Role] = []
        if role:
            roles = role if isinstance(role, list) else [role]

        try:
            user = self.user_model()
            user.first_name = first_name
            user.last_name = last_name
            user.username = username
            user.email = email
            user.active = True
            self.session.add(user)
            user.roles = roles
            user.groups = groups or []
            if hashed_password:
                user.password = hashed_password
            else:
                user.password = generate_password_hash(password)
            self.session.commit()
            log.info(const.LOGMSG_INF_SEC_ADD_USER, username)

            return user
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_ADD_USER, e)
            self.session.rollback()
            return False

    def load_user(self, pk: int) -> Any | None:
        user = self.get_user_by_id(int(pk))
        if user and user.is_active:
            return user
        return None

    def get_user_by_id(self, pk):
        return self.session.get(self.user_model, pk)

    def count_users(self):
        """Return the number of users in the database."""
        return self.session.scalar(select(func.count(self.user_model.id)))

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
            self.session.add(register_user)
            self.session.commit()
            return register_user
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_ADD_REGISTER_USER, e)
            self.session.rollback()
            return None

    def find_user(self, username=None, email=None):
        """Find user by username or email."""
        if username:
            try:
                if self.auth_username_ci:
                    return self.session.scalars(
                        select(self.user_model).where(
                            func.lower(self.user_model.username) == func.lower(username)
                        )
                    ).one_or_none()
                return self.session.scalars(
                    select(self.user_model).where(
                        func.lower(self.user_model.username) == func.lower(username)
                    )
                ).one_or_none()
            except MultipleResultsFound:
                log.error("Multiple results found for user %s", username)
                return None
        elif email:
            try:
                return self.session.scalars(select(self.user_model).filter_by(email=email)).one_or_none()
            except MultipleResultsFound:
                log.error("Multiple results found for user with email %s", email)
                return None

    def update_user(self, user: User) -> bool:
        try:
            self.session.merge(user)
            self.session.commit()
            log.info(const.LOGMSG_INF_SEC_UPD_USER, user)
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_UPD_USER, e)
            self.session.rollback()
            return False
        return True

    def del_register_user(self, register_user):
        """
        Delete registration object from database.

        :param register_user: RegisterUser object to delete
        """
        try:
            self.session.delete(register_user)
            self.session.commit()
            return True
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_DEL_REGISTER_USER, e)
            self.session.rollback()
            return False

    def get_all_users(self):
        return self.session.scalars(select(self.user_model)).all()

    def update_user_auth_stat(self, user, success=True):
        """
        Update user authentication stats.

        This is done upon successful/unsuccessful authentication attempts.

        :param user:
            The identified (but possibly not successfully authenticated) user
            model
        :param success:
            Defaults to true, if true increments login_count, updates
            last_login, and resets fail_login_count to 0, if false increments
            fail_login_count on user model.
        """
        if not user.login_count:
            user.login_count = 0
        if not user.fail_login_count:
            user.fail_login_count = 0
        if success:
            user.login_count += 1
            user.last_login = datetime.datetime.now()
            user.fail_login_count = 0
        else:
            user.fail_login_count += 1
        self.update_user(user)

    """
    -------------
    Action entity
    -------------
    """

    def get_action(self, name: str) -> Action:
        """
        Get an existing action record.

        :param name: name
        """
        return self.session.scalars(select(self.action_model).filter_by(name=name)).one_or_none()

    def create_action(self, name):
        """
        Add an action to the backend, model action.

        :param name:
            name of the action: 'can_add','can_edit' etc...
        """
        action = self.get_action(name)
        if action is None:
            try:
                action = self.action_model()
                action.name = name
                self.session.add(action)
                self.session.commit()
                return action
            except Exception as e:
                log.error(const.LOGMSG_ERR_SEC_ADD_PERMISSION, e)
                self.session.rollback()
        return action

    def delete_action(self, name: str) -> bool:
        """
        Delete a permission action.

        :param name: Name of action to delete (e.g. can_read).
        """
        action = self.get_action(name)
        if not action:
            log.warning(const.LOGMSG_WAR_SEC_DEL_PERMISSION, name)
            return False
        try:
            perms = self.session.scalars(
                select(self.permission_model).where(self.permission_model.action_id == action.id)
            ).all()
            if perms:
                log.warning(const.LOGMSG_WAR_SEC_DEL_PERM_PVM, action, perms)
                return False
            self.session.delete(action)
            self.session.commit()
            return True
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_DEL_PERMISSION, e)
            self.session.rollback()
            return False

    """
    ---------------
    Resource entity
    ---------------
    """

    def get_resource(self, name: str) -> Resource | None:
        """
        Return a resource record by name, if it exists.

        :param name: Name of resource
        """
        return self.session.scalars(select(self.resource_model).filter_by(name=name)).one_or_none()

    def create_resource(self, name) -> Resource | None:
        """
        Create a resource with the given name.

        :param name: The name of the resource to create created.
        """
        resource = self.get_resource(name)
        if resource is None:
            try:
                resource = self.resource_model()
                resource.name = name
                self.session.add(resource)
                self.session.commit()
                return resource
            except Exception as e:
                log.error(const.LOGMSG_ERR_SEC_ADD_VIEWMENU, e)
                self.session.rollback()
        return resource

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
        Get a permission made with the given action->resource pair, if the permission already exists.

        :param action_name: Name of action
        :param resource_name: Name of resource
        """
        action = self.get_action(action_name)
        resource = self.get_resource(resource_name)
        if action and resource:
            return (
                self.session.scalars(
                    select(self.permission_model).filter_by(action=action, resource=resource)
                )
                .unique()
                .one_or_none()
            )

        return None

    def get_resource_permissions(self, resource: Resource) -> Permission:
        """
        Retrieve permission pairs associated with a specific resource object.

        :param resource: Object representing a single resource.
        """
        return self.session.scalars(select(self.permission_model).filter_by(resource_id=resource.id)).all()

    def create_permission(self, action_name, resource_name) -> Permission | None:
        """
        Add a permission on a resource to the backend.

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
        if resource is None:
            log.error(const.LOGMSG_ERR_SEC_ADD_PERMVIEW, "Resource creation failed %s", resource_name)
            return None
        action = self.create_action(action_name)
        perm = self.permission_model()
        perm.resource_id, perm.action_id = resource.id, action.id
        try:
            self.session.add(perm)
            self.session.commit()
            log.info(const.LOGMSG_INF_SEC_ADD_PERMVIEW, perm)
            return perm
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_ADD_PERMVIEW, e)
            self.session.rollback()
            return None

    def delete_permission(self, action_name: str, resource_name: str) -> None:
        """
        Delete the permission linking an action->resource pair.

        Doesn't delete the underlying action or resource.

        :param action_name: Name of existing action
        :param resource_name: Name of existing resource
        """
        if not (action_name and resource_name):
            return
        perm = self.get_permission(action_name, resource_name)
        if not perm:
            return
        roles = self.session.scalars(
            select(self.role_model).where(self.role_model.permissions.contains(perm))
        ).first()
        if roles:
            log.warning(const.LOGMSG_WAR_SEC_DEL_PERMVIEW, resource_name, action_name, roles)
            return
        try:
            # delete permission on resource
            self.session.delete(perm)
            self.session.commit()
            # if no more permission on permission view, delete permission
            if not self.session.scalars(select(self.permission_model).filter_by(action=perm.action)).all():
                self.delete_action(perm.action.name)
            log.info(const.LOGMSG_INF_SEC_DEL_PERMVIEW, action_name, resource_name)
        except Exception as e:
            log.error(const.LOGMSG_ERR_SEC_DEL_PERMVIEW, e)
            self.session.rollback()

    def add_permission_to_role(self, role: Role, permission: Permission | None) -> None:
        """
        Add an existing permission pair to a role.

        :param role: The role about to get a new permission.
        :param permission: The permission pair to add to a role.
        """
        if permission and permission not in role.permissions:
            try:
                role.permissions.append(permission)
                self.session.merge(role)
                self.session.commit()
                log.info(const.LOGMSG_INF_SEC_ADD_PERMROLE, permission, role.name)
            except Exception as e:
                log.error(const.LOGMSG_ERR_SEC_ADD_PERMROLE, e)
                self.session.rollback()

    def remove_permission_from_role(self, role: Role, permission: Permission) -> None:
        """
        Remove a permission pair from a role.

        :param role: User role containing permissions.
        :param permission: Object representing resource-> action pair
        """
        if permission in role.permissions:
            try:
                role.permissions.remove(permission)
                self.session.merge(role)
                self.session.commit()
                log.info(const.LOGMSG_INF_SEC_DEL_PERMROLE, permission, role.name)
            except Exception as e:
                log.error(const.LOGMSG_ERR_SEC_DEL_PERMROLE, e)
                self.session.rollback()

    @staticmethod
    def get_user_roles(user=None):
        """
        Get all the roles associated with the user.

        :param user: the ab_user in FAB model.
        :return: a list of roles associated with the user.
        """
        if user is None:
            user = g.user
        return user.roles + [role for group in user.groups for role in group.roles]

    """
    --------------------
    Auth related methods
    --------------------
    """

    def auth_user_ldap(self, username, password, rotate_session_id=True):
        """
        Authenticate user with LDAP.

        NOTE: this depends on python-ldap module.

        :param username: the username
        :param password: the password
        """
        # If no username is provided, go away
        if (username is None) or username == "":
            return None

        # Search the DB for this user
        user = self.find_user(username=username)

        # If user is not active, go away
        if user and (not user.is_active):
            return None

        # If user is not registered, and not self-registration, go away
        if (not user) and (not self.auth_user_registration):
            return None

        # Ensure python-ldap is installed
        try:
            import ldap
        except ImportError:
            log.error("python-ldap library is not installed")
            return None

        try:
            # LDAP certificate settings
            if self.auth_ldap_tls_cacertdir:
                ldap.set_option(ldap.OPT_X_TLS_CACERTDIR, self.auth_ldap_tls_cacertdir)
            if self.auth_ldap_tls_cacertfile:
                ldap.set_option(ldap.OPT_X_TLS_CACERTFILE, self.auth_ldap_tls_cacertfile)
            if self.auth_ldap_tls_certfile:
                ldap.set_option(ldap.OPT_X_TLS_CERTFILE, self.auth_ldap_tls_certfile)
            if self.auth_ldap_tls_keyfile:
                ldap.set_option(ldap.OPT_X_TLS_KEYFILE, self.auth_ldap_tls_keyfile)
            if self.auth_ldap_allow_self_signed:
                ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
                ldap.set_option(ldap.OPT_X_TLS_NEWCTX, 0)
            elif self.auth_ldap_tls_demand:
                ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_DEMAND)
                ldap.set_option(ldap.OPT_X_TLS_NEWCTX, 0)

            # Initialise LDAP connection
            con = ldap.initialize(self.auth_ldap_server)
            con.set_option(ldap.OPT_REFERRALS, 0)
            if self.auth_ldap_use_tls:
                try:
                    con.start_tls_s()
                except Exception:
                    log.error(LOGMSG_ERR_SEC_AUTH_LDAP_TLS, self.auth_ldap_server)
                    return None

            # Define variables, so we can check if they are set in later steps
            user_dn = None
            user_attributes = {}

            # Flow 1 - (Indirect Search Bind):
            #  - in this flow, special bind credentials are used to perform the
            #    LDAP search
            #  - in this flow, AUTH_LDAP_SEARCH must be set
            if self.auth_ldap_bind_user:
                # Bind with AUTH_LDAP_BIND_USER/AUTH_LDAP_BIND_PASSWORD
                # (authorizes for LDAP search)
                self._ldap_bind_indirect(ldap, con)

                # Search for `username`
                #  - returns the `user_dn` needed for binding to validate credentials
                #  - returns the `user_attributes` needed for
                #    AUTH_USER_REGISTRATION/AUTH_ROLES_SYNC_AT_LOGIN
                if self.auth_ldap_search:
                    user_dn, user_attributes = self._search_ldap(ldap, con, username)
                else:
                    log.error("AUTH_LDAP_SEARCH must be set when using AUTH_LDAP_BIND_USER")
                    return None

                # If search failed, go away
                if user_dn is None:
                    log.info(LOGMSG_WAR_SEC_NOLDAP_OBJ, username)
                    return None

                # Bind with user_dn/password (validates credentials)
                if not self._ldap_bind(ldap, con, user_dn, password):
                    if user:
                        self.update_user_auth_stat(user, False)

                    # Invalid credentials, go away
                    log.info(LOGMSG_WAR_SEC_LOGIN_FAILED, username)
                    return None

            # Flow 2 - (Direct Search Bind):
            #  - in this flow, the credentials provided by the end-user are used
            #    to perform the LDAP search
            #  - in this flow, we only search LDAP if AUTH_LDAP_SEARCH is set
            #     - features like AUTH_USER_REGISTRATION & AUTH_ROLES_SYNC_AT_LOGIN
            #       will only work if AUTH_LDAP_SEARCH is set
            else:
                # Copy the provided username (so we can apply formatters)
                bind_username = username

                # update `bind_username` by applying AUTH_LDAP_APPEND_DOMAIN
                #  - for Microsoft AD, which allows binding with userPrincipalName
                if self.auth_ldap_append_domain:
                    bind_username = bind_username + "@" + self.auth_ldap_append_domain

                # Update `bind_username` by applying AUTH_LDAP_USERNAME_FORMAT
                #  - for transforming the username into a DN,
                #    for example: "uid=%s,ou=example,o=test"
                if self.auth_ldap_username_format:
                    bind_username = self.auth_ldap_username_format % bind_username

                # Bind with bind_username/password
                # (validates credentials & authorizes for LDAP search)
                if not self._ldap_bind(ldap, con, bind_username, password):
                    if user:
                        self.update_user_auth_stat(user, False)

                    # Invalid credentials, go away
                    log.info(LOGMSG_WAR_SEC_LOGIN_FAILED, bind_username)
                    return None

                # Search for `username` (if AUTH_LDAP_SEARCH is set)
                #  - returns the `user_attributes`
                #    needed for AUTH_USER_REGISTRATION/AUTH_ROLES_SYNC_AT_LOGIN
                #  - we search on `username` not `bind_username`,
                #    because AUTH_LDAP_APPEND_DOMAIN and AUTH_LDAP_USERNAME_FORMAT
                #    would result in an invalid search filter
                if self.auth_ldap_search:
                    user_dn, user_attributes = self._search_ldap(ldap, con, username)

                    # If search failed, go away
                    if user_dn is None:
                        log.info(LOGMSG_WAR_SEC_NOLDAP_OBJ, username)
                        return None

            # Sync the user's roles
            if user and user_attributes and self.auth_roles_sync_at_login:
                user.roles = self._ldap_calculate_user_roles(user_attributes)
                log.debug("Calculated new roles for user=%r as: %s", user_dn, user.roles)

            # If the user is new, register them
            if (not user) and user_attributes and self.auth_user_registration:
                user = self.add_user(
                    username=username,
                    first_name=self.ldap_extract(user_attributes, self.auth_ldap_firstname_field, ""),
                    last_name=self.ldap_extract(user_attributes, self.auth_ldap_lastname_field, ""),
                    email=self.ldap_extract(
                        user_attributes,
                        self.auth_ldap_email_field,
                        f"{username}@email.notfound",
                    ),
                    role=self._ldap_calculate_user_roles(user_attributes),
                )
                log.debug("New user registered: %s", user)

                # If user registration failed, go away
                if not user:
                    log.info(LOGMSG_ERR_SEC_ADD_REGISTER_USER, username)
                    return None

            # LOGIN SUCCESS (only if user is now registered)
            if user:
                if rotate_session_id:
                    self._rotate_session_id()
                self.update_user_auth_stat(user)
                return user
            return None

        except ldap.LDAPError as e:
            msg = None
            if isinstance(e, dict):
                msg = getattr(e, "message", None)
            if (msg is not None) and ("desc" in msg):
                log.error(LOGMSG_ERR_SEC_AUTH_LDAP, e.message["desc"])
                return None
            log.error(e)
            return None

    def check_password(self, username, password) -> bool:
        """
        Check if the password is correct for the username.

        :param username: the username
        :param password: the password
        """
        user = self.find_user(username=username)
        if user is None:
            user = self.find_user(email=username)
        if user is None:
            return False
        return check_password_hash(user.password, password)

    def auth_user_db(self, username, password, rotate_session_id=True):
        """
        Authenticate user, auth db style.

        :param username:
            The username or registered email address
        :param password:
            The password, will be tested against hashed password on db
        :param rotate_session_id:
            Whether to rotate the session ID
        """
        if username is None or username == "":
            return None
        user = self.find_user(username=username)
        if user is None:
            user = self.find_user(email=username)
        if user is None or (not user.is_active):
            # Balance failure and success
            check_password_hash(
                current_app.config["AUTH_DB_FAKE_PASSWORD_HASH_CHECK"],
                "password",
            )
            log.info(LOGMSG_WAR_SEC_LOGIN_FAILED, username)
            return None
        if check_password_hash(user.password, password):
            if rotate_session_id:
                self._rotate_session_id()
            self.update_user_auth_stat(user, True)
            return user
        self.update_user_auth_stat(user, False)
        log.info(LOGMSG_WAR_SEC_LOGIN_FAILED, username)
        return None

    def set_oauth_session(self, provider, oauth_response):
        """Set the current session with OAuth user secrets."""
        # Get this provider key names for token_key and token_secret
        token_key = self.get_oauth_token_key_name(provider)
        token_secret = self.get_oauth_token_secret_name(provider)
        # Save users token on encrypted session cookie
        session["oauth"] = (
            oauth_response[token_key],
            oauth_response.get(token_secret, ""),
        )
        session["oauth_provider"] = provider

    def get_oauth_token_key_name(self, provider):
        """
        Return the token_key name for the oauth provider.

        If none is configured defaults to oauth_token
        this is configured using OAUTH_PROVIDERS and token_key key.
        """
        for _provider in self.oauth_providers:
            if _provider["name"] == provider:
                return _provider.get("token_key", "oauth_token")

    def get_oauth_token_secret_name(self, provider):
        """
        Get the ``token_secret`` name for the oauth provider.

        If none is configured, defaults to ``oauth_secret``. This is configured
        using ``OAUTH_PROVIDERS`` and ``token_secret``.
        """
        for _provider in self.oauth_providers:
            if _provider["name"] == provider:
                return _provider.get("token_secret", "oauth_token_secret")

    def auth_user_oauth(self, userinfo, rotate_session_id=True):
        """
        Authenticate user with OAuth.

        :userinfo: dict with user information
                   (keys are the same as User model columns)
        """
        # extract the username from `userinfo`
        if "username" in userinfo:
            username = userinfo["username"]
        elif "email" in userinfo:
            username = userinfo["email"]
        else:
            log.error("OAUTH userinfo does not have username or email %s", userinfo)
            return None

        # If username is empty, go away
        if (username is None) or username == "":
            return None

        # Search the DB for this user
        user = self.find_user(username=username)

        # If user is not active, go away
        if user and (not user.is_active):
            return None

        # If user is not registered, and not self-registration, go away
        if (not user) and (not self.auth_user_registration):
            return None

        # Sync the user's roles
        if user and self.auth_roles_sync_at_login:
            user.roles = self._oauth_calculate_user_roles(userinfo)
            log.debug("Calculated new roles for user=%r as: %s", username, user.roles)

        # If the user is new, register them
        if (not user) and self.auth_user_registration:
            user = self.add_user(
                username=username,
                first_name=userinfo.get("first_name", ""),
                last_name=userinfo.get("last_name", ""),
                email=userinfo.get("email", "") or f"{username}@email.notfound",
                role=self._oauth_calculate_user_roles(userinfo),
            )
            log.debug("New user registered: %s", user)

            # If user registration failed, go away
            if not user:
                log.error("Error creating a new OAuth user %s", username)
                return None

        # LOGIN SUCCESS (only if user is now registered)
        if user:
            if rotate_session_id:
                self._rotate_session_id()
            self.update_user_auth_stat(user)
            return user
        return None

    def get_oauth_user_info(self, provider: str, resp: dict[str, Any]) -> dict[str, Any]:
        """
        There are different OAuth APIs with different ways to retrieve user info.

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
        if provider == "azure":
            me = self._decode_and_validate_azure_jwt(resp["id_token"])
            log.debug("User info from Azure: %s", me)
            # https://learn.microsoft.com/en-us/azure/active-directory/develop/id-token-claims-reference#payload-claims
            return {
                "email": me["email"] if "email" in me else me["upn"],
                "first_name": me.get("given_name", ""),
                "last_name": me.get("family_name", ""),
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
            if "error" not in data:
                return {
                    "username": f"{provider}_{data['sub']}",
                    "first_name": data.get("given_name", ""),
                    "last_name": data.get("family_name", ""),
                    "email": data["email"],
                    "role_keys": data.get("groups", []),
                }
            log.error(data.get("error_description"))
            return {}
        # for Auth0
        if provider == "auth0":
            data = self.appbuilder.sm.oauth_remotes[provider].userinfo()
            log.debug("User info from Auth0: %s", data)
            return {
                "username": f"{provider}_{data['sub']}",
                "first_name": data.get("given_name", ""),
                "last_name": data.get("family_name", ""),
                "email": data["email"],
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
                "role_keys": data.get("groups", []),
            }
        # for Authentik
        if provider == "authentik":
            id_token = resp["id_token"]
            me = self._get_authentik_token_info(id_token)
            log.debug("User info from authentik: %s", me)
            return {
                "email": me["preferred_username"],
                "first_name": me.get("given_name", ""),
                "username": me["nickname"],
                "role_keys": me.get("groups", []),
            }
        # for other providers
        data = self.oauth_remotes[provider].userinfo()
        log.debug("User info from %s: %s", provider, data)
        return {
            "username": data.get("preferred_username", ""),
            "first_name": data.get("given_name", ""),
            "last_name": data.get("family_name", ""),
            "email": data.get("email", ""),
            "role_keys": data.get("groups", []),
        }

    @staticmethod
    def oauth_token_getter():
        """Get authentication (OAuth) token."""
        token = session.get("oauth")
        log.debug("OAuth token retrieved from session.")
        return token

    @staticmethod
    def ldap_extract_list(ldap_dict: dict[str, list[bytes]], field_name: str) -> list[str]:
        raw_list = ldap_dict.get(field_name, [])
        # decode - removing empty strings
        return [x.decode("utf-8") for x in raw_list if x.decode("utf-8")]

    @staticmethod
    def ldap_extract(ldap_dict: dict[str, list[bytes]], field_name: str, fallback: str) -> str:
        raw_value = ldap_dict.get(field_name, [b""])
        # decode - if empty string, default to fallback, otherwise take first element
        return raw_value[0].decode("utf-8") or fallback

    def auth_user_remote_user(self, username):
        """
        REMOTE_USER user Authentication.

        :param username: user's username for remote auth
        """
        user = self.find_user(username=username)

        # User does not exist, create one if auto user registration.
        if user is None and self.auth_user_registration:
            user = self.add_user(
                # All we have is REMOTE_USER, so we set
                # the other fields to blank.
                username=username,
                first_name=username,
                last_name="-",
                email=username + "@email.notfound",
                role=self.find_role(self.auth_user_registration_role),
            )

        # If user does not exist on the DB and not auto user registration,
        # or user is inactive, go away.
        elif user is None or (not user.is_active):
            log.info(LOGMSG_WAR_SEC_LOGIN_FAILED, username)
            return None

        self._rotate_session_id()
        self.update_user_auth_stat(user)
        return user

    """
    ---------------
    Private methods
    ---------------
    """

    def _rotate_session_id(self):
        """
        Rotate the session ID.

        We need to do this upon successful authentication when using the
        database session backend.
        """
        if conf.get("fab", "SESSION_BACKEND") == "database":
            session.sid = str(uuid.uuid4())

    def _get_microsoft_jwks(self) -> list[dict[str, Any]]:
        import requests

        return requests.get(MICROSOFT_KEY_SET_URL).json()

    def _decode_and_validate_azure_jwt(self, id_token: str) -> dict[str, str]:
        verify_signature = self.oauth_remotes["azure"].client_kwargs.get("verify_signature", False)
        if verify_signature:
            from authlib.jose import JsonWebKey, jwt as authlib_jwt

            keyset = JsonWebKey.import_key_set(self._get_microsoft_jwks())
            claims = authlib_jwt.decode(id_token, keyset)
            claims.validate()
            return claims

        return jwt.decode(id_token, options={"verify_signature": False})

    def _ldap_bind_indirect(self, ldap, con) -> None:
        """
        Attempt to bind to LDAP using the AUTH_LDAP_BIND_USER.

        :param ldap: The ldap module reference
        :param con: The ldap connection
        """
        if not self.auth_ldap_bind_user:
            # always check AUTH_LDAP_BIND_USER is set before calling this method
            raise ValueError("AUTH_LDAP_BIND_USER must be set")

        try:
            log.debug("LDAP bind indirect TRY with username: %r", self.auth_ldap_bind_user)
            con.simple_bind_s(self.auth_ldap_bind_user, self.auth_ldap_bind_password)
            log.debug("LDAP bind indirect SUCCESS with username: %r", self.auth_ldap_bind_user)
        except ldap.INVALID_CREDENTIALS as ex:
            log.error("AUTH_LDAP_BIND_USER and AUTH_LDAP_BIND_PASSWORD are not valid LDAP bind credentials")
            raise ex

    def _search_ldap(self, ldap, con, username):
        """
        Search LDAP for user.

        :param ldap: The ldap module reference
        :param con: The ldap connection
        :param username: username to match with AUTH_LDAP_UID_FIELD
        :return: ldap object array
        """
        if not self.auth_ldap_search:
            # always check AUTH_LDAP_SEARCH is set before calling this method
            raise ValueError("AUTH_LDAP_SEARCH must be set")

        # build the filter string for the LDAP search
        if self.auth_ldap_search_filter:
            filter_str = f"(&{self.auth_ldap_search_filter}({self.auth_ldap_uid_field}={username}))"
        else:
            filter_str = f"({self.auth_ldap_uid_field}={username})"

        # build what fields to request in the LDAP search
        request_fields = [
            self.auth_ldap_firstname_field,
            self.auth_ldap_lastname_field,
            self.auth_ldap_email_field,
        ]
        if self.auth_roles_mapping:
            request_fields.append(self.auth_ldap_group_field)

        # perform the LDAP search
        log.debug(
            "LDAP search for %r with fields %s in scope %r",
            filter_str,
            request_fields,
            self.auth_ldap_search,
        )
        raw_search_result = con.search_s(
            self.auth_ldap_search, ldap.SCOPE_SUBTREE, filter_str, request_fields
        )
        log.debug("LDAP search returned: %s", raw_search_result)

        # Remove any search referrals from results
        search_result = [
            (dn, attrs) for dn, attrs in raw_search_result if dn is not None and isinstance(attrs, dict)
        ]

        # only continue if 0 or 1 results were returned
        if len(search_result) > 1:
            log.error(
                "LDAP search for %r in scope '%a' returned multiple results",
                self.auth_ldap_search,
                filter_str,
            )
            return None, None

        try:
            # extract the DN
            user_dn = search_result[0][0]
            # extract the other attributes
            user_info = search_result[0][1]
        except (IndexError, NameError):
            return None, None

        # get nested groups for user
        if self.auth_ldap_use_nested_groups_for_roles:
            nested_groups = self._ldap_get_nested_groups(ldap, con, user_dn)

            if self.auth_ldap_group_field in user_info:
                user_info[self.auth_ldap_group_field].extend(nested_groups)
            else:
                user_info[self.auth_ldap_group_field] = nested_groups

        # return
        return user_dn, user_info

    def _ldap_get_nested_groups(self, ldap, con, user_dn) -> list[str]:
        """
        Search nested groups for user.

        Only for MS AD version.

        :param ldap: The ldap module reference
        :param con: The ldap connection
        :param user_dn: user DN to match with CN
        :return: ldap groups array
        """
        log.debug("Nested groups for LDAP enabled.")
        # filter for microsoft active directory only
        nested_groups_filter_str = f"(&(objectCategory=Group)(member:1.2.840.113556.1.4.1941:={user_dn}))"
        nested_groups_request_fields = ["cn"]

        nested_groups_search_result = con.search_s(
            self.auth_ldap_search,
            ldap.SCOPE_SUBTREE,
            nested_groups_filter_str,
            nested_groups_request_fields,
        )
        log.debug(
            "LDAP search for nested groups returned: %s",
            nested_groups_search_result,
        )

        nested_groups = [x[0].encode() for x in nested_groups_search_result if x[0] is not None]
        log.debug("LDAP nested groups for users: %s", nested_groups)
        return nested_groups

    @staticmethod
    def _ldap_bind(ldap, con, dn: str, password: str) -> bool:
        """Validates/binds the provided dn/password with the LDAP sever."""
        try:
            log.debug("LDAP bind TRY with username: %r", dn)
            con.simple_bind_s(dn, password)
            log.debug("LDAP bind SUCCESS with username: %r", dn)
            return True
        except ldap.INVALID_CREDENTIALS:
            return False

    def _ldap_calculate_user_roles(self, user_attributes: dict[str, list[bytes]]) -> list[str]:
        user_role_objects = set()

        # apply AUTH_ROLES_MAPPING
        if self.auth_roles_mapping:
            user_role_keys = self.ldap_extract_list(user_attributes, self.auth_ldap_group_field)
            user_role_objects.update(self.get_roles_from_keys(user_role_keys))

        # apply AUTH_USER_REGISTRATION
        if self.auth_user_registration:
            registration_role_name = self.auth_user_registration_role

            # lookup registration role in flask db
            fab_role = self.find_role(registration_role_name)
            if fab_role:
                user_role_objects.add(fab_role)
            else:
                log.warning("Can't find AUTH_USER_REGISTRATION role: %s", registration_role_name)

        return list(user_role_objects)

    def _merge_perm(self, action_name: str, resource_name: str) -> None:
        """
        Add the new (action, resource) to assoc_permission_role if it doesn't exist.

        It will add the related entry to ab_permission and ab_resource two meta tables as well.

        :param action_name: Name of the action
        :param resource_name: Name of the resource
        """
        action = self.get_action(action_name)
        resource = self.get_resource(resource_name)
        perm = None
        if action and resource:
            perm = self.session.scalar(
                select(self.permission_model).filter_by(action=action, resource=resource).limit(1)
            )
        if not perm and action_name and resource_name:
            self.create_permission(action_name, resource_name)

    def _get_all_roles_with_permissions(self) -> dict[str, Role]:
        """Return a dict with a key of role name and value of role with early loaded permissions."""
        return {
            r.name: r
            for r in self.session.scalars(
                select(self.role_model).options(joinedload(self.role_model.permissions))
            ).unique()
        }

    def _get_all_non_dag_permissions(self) -> dict[tuple[str, str], Permission]:
        """
        Get permissions except those that are for specific DAGs.

        Return a dict with a key of (action_name, resource_name) and value of permission
        with all permissions except those that are for specific DAGs.
        """
        return {
            (action_name, resource_name): viewmodel
            for action_name, resource_name, viewmodel in (
                self.session.execute(
                    select(
                        self.action_model.name,
                        self.resource_model.name,
                        self.permission_model,
                    )
                    .join(self.permission_model.action)
                    .join(self.permission_model.resource)
                    .where(~self.resource_model.name.like(f"{permissions.RESOURCE_DAG_PREFIX}%"))
                )
            )
        }

    @staticmethod
    def _cli_safe_flash(text: str, level: str) -> None:
        """Show a flash in a web context or prints a message if not."""
        if has_request_context():
            flash(Markup(text), level)
        else:
            getattr(log, level)(text.replace("<br>", "\n").replace("<b>", "*").replace("</b>", "*"))

    def _oauth_calculate_user_roles(self, userinfo) -> list[str]:
        user_role_objects = set()

        # apply AUTH_ROLES_MAPPING
        if self.auth_roles_mapping:
            user_role_keys = userinfo.get("role_keys", [])
            user_role_objects.update(self.get_roles_from_keys(user_role_keys))

        # apply AUTH_USER_REGISTRATION_ROLE
        if self.auth_user_registration:
            registration_role_name = self.auth_user_registration_role

            # if AUTH_USER_REGISTRATION_ROLE_JMESPATH is set,
            # use it for the registration role
            if self.auth_user_registration_role_jmespath:
                import jmespath

                registration_role_name = jmespath.search(self.auth_user_registration_role_jmespath, userinfo)

            # lookup registration role in flask db
            fab_role = self.find_role(registration_role_name)
            if fab_role:
                user_role_objects.add(fab_role)
            else:
                log.warning("Can't find AUTH_USER_REGISTRATION role: %s", registration_role_name)

        return list(user_role_objects)
