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

# This product contains a modified portion of 'Flask App Builder' developed by Daniel Vaz Gaspar.
# (https://github.com/dpgaspar/Flask-AppBuilder).
# Copyright 2013, Daniel Vaz Gaspar

import base64
import datetime
import json
import logging
import re
from typing import Dict, List, Set, Tuple

from flask import g, session, url_for
from flask_appbuilder.const import (
    AUTH_DB,
    AUTH_LDAP,
    AUTH_OAUTH,
    AUTH_OID,
    AUTH_REMOTE_USER,
    LOGMSG_ERR_SEC_ADD_REGISTER_USER,
    LOGMSG_ERR_SEC_AUTH_LDAP,
    LOGMSG_ERR_SEC_AUTH_LDAP_TLS,
    LOGMSG_WAR_SEC_LOGIN_FAILED,
    LOGMSG_WAR_SEC_NO_USER,
    LOGMSG_WAR_SEC_NOLDAP_OBJ,
    PERMISSION_PREFIX,
)
from flask_appbuilder.security.api import SecurityApi
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
    PermissionViewModelView,
    RegisterUserModelView,
    ResetMyPasswordView,
    ResetPasswordView,
    RoleModelView,
    UserDBModelView,
    UserInfoEditView,
    UserLDAPModelView,
    UserOAuthModelView,
    UserOIDModelView,
    UserRemoteUserModelView,
    UserStatsChartView,
    ViewMenuModelView,
)
from flask_babel import lazy_gettext as _
from flask_jwt_extended import JWTManager, current_user as current_user_jwt
from flask_login import LoginManager, current_user
from werkzeug.security import check_password_hash, generate_password_hash

log = logging.getLogger(__name__)


def _oauth_tokengetter(token=None):
    """
    Default function to return the current user oauth token
    from session cookie.
    """
    token = session.get("oauth")
    log.debug(f"Token Get: {token}")
    return token


class BaseSecurityManager:
    """Base class to define the Security Manager interface."""

    appbuilder = None
    """The appbuilder instance for the current security manager."""
    auth_view = None
    """ The obj instance for authentication view """
    user_view = None
    """ The obj instance for user view """
    registeruser_view = None
    """ The obj instance for registering user view """
    lm = None
    """ Flask-Login LoginManager """
    jwt_manager = None
    """ Flask-JWT-Extended """
    oid = None
    """ Flask-OpenID OpenID """
    oauth = None
    """ Flask-OAuth """
    oauth_remotes = None
    """ OAuth email whitelists """
    oauth_whitelists = {}
    """ Initialized (remote_app) providers dict {'provider_name', OBJ } """
    oauth_tokengetter = _oauth_tokengetter
    """ OAuth tokengetter function override to implement your own tokengetter method """
    oauth_user_info = None

    user_model = None
    """ Override to set your own User Model """
    role_model = None
    """ Override to set your own Role Model """
    permission_model = None
    """ Override to set your own Permission Model """
    viewmenu_model = None
    """ Override to set your own ViewMenu Model """
    permissionview_model = None
    """ Override to set your own PermissionView Model """
    registeruser_model = None
    """ Override to set your own RegisterUser Model """

    userdbmodelview = UserDBModelView
    """ Override if you want your own user db view """
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

    # API
    security_api = SecurityApi
    """ Override if you want your own Security API login endpoint """

    rolemodelview = RoleModelView
    permissionmodelview = PermissionModelView
    userstatschartview = UserStatsChartView
    viewmenumodelview = ViewMenuModelView
    permissionviewmodelview = PermissionViewModelView

    def __init__(self, appbuilder):
        self.appbuilder = appbuilder
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

        # LDAP Config
        if self.auth_type == AUTH_LDAP:
            if "AUTH_LDAP_SERVER" not in app.config:
                raise Exception(
                    "No AUTH_LDAP_SERVER defined on config" " with AUTH_LDAP authentication type."
                )
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

        if self.auth_type == AUTH_OID:
            from flask_openid import OpenID

            self.oid = OpenID(app)
        if self.auth_type == AUTH_OAUTH:
            from authlib.integrations.flask_client import OAuth

            self.oauth = OAuth(app)
            self.oauth_remotes = {}
            for _provider in self.oauth_providers:
                provider_name = _provider["name"]
                log.debug(f"OAuth providers init {provider_name}")
                obj_provider = self.oauth.register(provider_name, **_provider["remote_app"])
                obj_provider._tokengetter = self.oauth_tokengetter
                if not self.oauth_user_info:
                    self.oauth_user_info = self.get_oauth_user_info
                # Whitelist only users with matching emails
                if "whitelist" in _provider:
                    self.oauth_whitelists[provider_name] = _provider["whitelist"]
                self.oauth_remotes[provider_name] = obj_provider

        self._builtin_roles = self.create_builtin_roles()
        # Setup Flask-Login
        self.lm = self.create_login_manager(app)

        # Setup Flask-Jwt-Extended
        self.jwt_manager = self.create_jwt_manager(app)

    def create_login_manager(self, app) -> LoginManager:
        """
        Override to implement your custom login manager instance

        :param app: Flask app
        """
        lm = LoginManager(app)
        lm.login_view = "login"
        lm.user_loader(self.load_user)
        return lm

    def create_jwt_manager(self, app) -> JWTManager:
        """
        Override to implement your custom JWT manager instance

        :param app: Flask app
        """
        jwt_manager = JWTManager()
        jwt_manager.init_app(app)
        jwt_manager.user_loader_callback_loader(self.load_user_jwt)
        return jwt_manager

    def create_builtin_roles(self):
        """Returns FAB builtin roles."""
        return self.appbuilder.get_app.config.get("FAB_ROLES", {})

    def get_roles_from_keys(self, role_keys: List[str]) -> Set[role_model]:
        """
        Construct a list of FAB role objects, from a list of keys.

        NOTE:
        - keys are things like: "LDAP group DNs" or "OAUTH group names"
        - we use AUTH_ROLES_MAPPING to map from keys, to FAB role names

        :param role_keys: the list of FAB role keys
        :return: a list of RoleModelView
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
                        log.warning(f"Can't find role specified in AUTH_ROLES_MAPPING: {fab_role_name}")
        return _roles

    @property
    def get_url_for_registeruser(self):
        """Gets the URL for Register User"""
        return url_for(f"{self.registeruser_view.endpoint}.{self.registeruser_view.default_view}")

    @property
    def get_register_user_datamodel(self):
        """Gets the Register User data model"""
        return self.registerusermodelview.datamodel

    @property
    def builtin_roles(self):
        """Get the builtin roles"""
        return self._builtin_roles

    @property
    def auth_type(self):
        """Get the auth type"""
        return self.appbuilder.get_app.config["AUTH_TYPE"]

    @property
    def auth_username_ci(self):
        """Gets the auth username for CI"""
        return self.appbuilder.get_app.config.get("AUTH_USERNAME_CI", True)

    @property
    def auth_role_admin(self):
        """Gets the admin role"""
        return self.appbuilder.get_app.config["AUTH_ROLE_ADMIN"]

    @property
    def auth_role_public(self):
        """Gets the public role"""
        return self.appbuilder.get_app.config["AUTH_ROLE_PUBLIC"]

    @property
    def auth_ldap_server(self):
        """Gets the LDAP server object"""
        return self.appbuilder.get_app.config["AUTH_LDAP_SERVER"]

    @property
    def auth_ldap_use_tls(self):
        """Should LDAP use TLS"""
        return self.appbuilder.get_app.config["AUTH_LDAP_USE_TLS"]

    @property
    def auth_user_registration(self):
        """Will user self registration be allowed"""
        return self.appbuilder.get_app.config["AUTH_USER_REGISTRATION"]

    @property
    def auth_user_registration_role(self):
        """The default user self registration role"""
        return self.appbuilder.get_app.config["AUTH_USER_REGISTRATION_ROLE"]

    @property
    def auth_user_registration_role_jmespath(self) -> str:
        """The JMESPATH role to use for user registration."""
        return self.appbuilder.get_app.config["AUTH_USER_REGISTRATION_ROLE_JMESPATH"]

    @property
    def auth_roles_mapping(self) -> Dict[str, List[str]]:
        """The mapping of auth roles."""
        return self.appbuilder.get_app.config["AUTH_ROLES_MAPPING"]

    @property
    def auth_roles_sync_at_login(self) -> bool:
        """Should roles be synced at login."""
        return self.appbuilder.get_app.config["AUTH_ROLES_SYNC_AT_LOGIN"]

    @property
    def auth_ldap_search(self):
        """LDAP search object."""
        return self.appbuilder.get_app.config["AUTH_LDAP_SEARCH"]

    @property
    def auth_ldap_search_filter(self):
        """LDAP search filter."""
        return self.appbuilder.get_app.config["AUTH_LDAP_SEARCH_FILTER"]

    @property
    def auth_ldap_bind_user(self):
        """LDAP bind user."""
        return self.appbuilder.get_app.config["AUTH_LDAP_BIND_USER"]

    @property
    def auth_ldap_bind_password(self):
        """LDAP bind password."""
        return self.appbuilder.get_app.config["AUTH_LDAP_BIND_PASSWORD"]

    @property
    def auth_ldap_append_domain(self):
        """LDAP append domain."""
        return self.appbuilder.get_app.config["AUTH_LDAP_APPEND_DOMAIN"]

    @property
    def auth_ldap_username_format(self):
        """LDAP username format."""
        return self.appbuilder.get_app.config["AUTH_LDAP_USERNAME_FORMAT"]

    @property
    def auth_ldap_uid_field(self):
        """LDAP UID field."""
        return self.appbuilder.get_app.config["AUTH_LDAP_UID_FIELD"]

    @property
    def auth_ldap_group_field(self) -> str:
        """LDAP group field."""
        return self.appbuilder.get_app.config["AUTH_LDAP_GROUP_FIELD"]

    @property
    def auth_ldap_firstname_field(self):
        """LDAP first name field."""
        return self.appbuilder.get_app.config["AUTH_LDAP_FIRSTNAME_FIELD"]

    @property
    def auth_ldap_lastname_field(self):
        """LDAP last name field."""
        return self.appbuilder.get_app.config["AUTH_LDAP_LASTNAME_FIELD"]

    @property
    def auth_ldap_email_field(self):
        """LDAP email field."""
        return self.appbuilder.get_app.config["AUTH_LDAP_EMAIL_FIELD"]

    @property
    def auth_ldap_bind_first(self):
        """LDAP bind first."""
        return self.appbuilder.get_app.config["AUTH_LDAP_BIND_FIRST"]

    @property
    def auth_ldap_allow_self_signed(self):
        """LDAP allow self signed."""
        return self.appbuilder.get_app.config["AUTH_LDAP_ALLOW_SELF_SIGNED"]

    @property
    def auth_ldap_tls_demand(self):
        """LDAP TLS demand."""
        return self.appbuilder.get_app.config["AUTH_LDAP_TLS_DEMAND"]

    @property
    def auth_ldap_tls_cacertdir(self):
        """LDAP TLS CA certificate directory."""
        return self.appbuilder.get_app.config["AUTH_LDAP_TLS_CACERTDIR"]

    @property
    def auth_ldap_tls_cacertfile(self):
        """LDAP TLS CA certificate file."""
        return self.appbuilder.get_app.config["AUTH_LDAP_TLS_CACERTFILE"]

    @property
    def auth_ldap_tls_certfile(self):
        """LDAP TLS certificate file."""
        return self.appbuilder.get_app.config["AUTH_LDAP_TLS_CERTFILE"]

    @property
    def auth_ldap_tls_keyfile(self):
        """LDAP TLS key file."""
        return self.appbuilder.get_app.config["AUTH_LDAP_TLS_KEYFILE"]

    @property
    def openid_providers(self):
        """Openid providers"""
        return self.appbuilder.get_app.config["OPENID_PROVIDERS"]

    @property
    def oauth_providers(self):
        """Oauth providers"""
        return self.appbuilder.get_app.config["OAUTH_PROVIDERS"]

    @property
    def current_user(self):
        """Current user object"""
        if current_user.is_authenticated:
            return g.user
        elif current_user_jwt:
            return current_user_jwt

    def oauth_user_info_getter(self, f):
        """
        Decorator function to be the OAuth user info getter
        for all the providers, receives provider and response
        return a dict with the information returned from the provider.
        The returned user info dict should have it's keys with the same
        name as the User Model.

        Use it like this an example for GitHub ::

            @appbuilder.sm.oauth_user_info_getter
            def my_oauth_user_info(sm, provider, response=None):
                if provider == 'github':
                    me = sm.oauth_remotes[provider].get('user')
                    return {'username': me.data.get('login')}
                else:
                    return {}
        """

        def wraps(provider, response=None):
            ret = f(self, provider, response=response)
            # Checks if decorator is well behaved and returns a dict as supposed.
            if not type(ret) == dict:
                log.error(f"OAuth user info decorated function did not returned a dict, but: {type(ret)}")
                return {}
            return ret

        self.oauth_user_info = wraps
        return wraps

    def get_oauth_token_key_name(self, provider):
        """
        Returns the token_key name for the oauth provider
        if none is configured defaults to oauth_token
        this is configured using OAUTH_PROVIDERS and token_key key.
        """
        for _provider in self.oauth_providers:
            if _provider["name"] == provider:
                return _provider.get("token_key", "oauth_token")

    def get_oauth_token_secret_name(self, provider):
        """
        Returns the token_secret name for the oauth provider
        if none is configured defaults to oauth_secret
        this is configured using OAUTH_PROVIDERS and token_secret
        """
        for _provider in self.oauth_providers:
            if _provider["name"] == provider:
                return _provider.get("token_secret", "oauth_token_secret")

    def set_oauth_session(self, provider, oauth_response):
        """Set the current session with OAuth user secrets"""
        # Get this provider key names for token_key and token_secret
        token_key = self.appbuilder.sm.get_oauth_token_key_name(provider)
        token_secret = self.appbuilder.sm.get_oauth_token_secret_name(provider)
        # Save users token on encrypted session cookie
        session["oauth"] = (
            oauth_response[token_key],
            oauth_response.get(token_secret, ""),
        )
        session["oauth_provider"] = provider

    def get_oauth_user_info(self, provider, resp):
        """
        Since there are different OAuth API's with different ways to
        retrieve user info
        """
        # for GITHUB
        if provider == "github" or provider == "githublocal":
            me = self.appbuilder.sm.oauth_remotes[provider].get("user")
            data = me.json()
            log.debug(f"User info from Github: {data}")
            return {"username": "github_" + data.get("login")}
        # for twitter
        if provider == "twitter":
            me = self.appbuilder.sm.oauth_remotes[provider].get("account/settings.json")
            data = me.json()
            log.debug(f"User info from Twitter: {data}")
            return {"username": "twitter_" + data.get("screen_name", "")}
        # for linkedin
        if provider == "linkedin":
            me = self.appbuilder.sm.oauth_remotes[provider].get(
                "people/~:(id,email-address,first-name,last-name)?format=json"
            )
            data = me.json()
            log.debug(f"User info from Linkedin: {data}")
            return {
                "username": "linkedin_" + data.get("id", ""),
                "email": data.get("email-address", ""),
                "first_name": data.get("firstName", ""),
                "last_name": data.get("lastName", ""),
            }
        # for Google
        if provider == "google":
            me = self.appbuilder.sm.oauth_remotes[provider].get("userinfo")
            data = me.json()
            log.debug(f"User info from Google: {data}")
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
            log.debug(f"Azure response received : {resp}")
            id_token = resp["id_token"]
            log.debug(str(id_token))
            me = self._azure_jwt_token_parse(id_token)
            log.debug(f"Parse JWT token : {me}")
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
            me = self.appbuilder.sm.oauth_remotes[provider].get("apis/user.openshift.io/v1/users/~")
            data = me.json()
            log.debug(f"User info from OpenShift: {data}")
            return {"username": "openshift_" + data.get("metadata").get("name")}
        # for Okta
        if provider == "okta":
            me = self.appbuilder.sm.oauth_remotes[provider].get("userinfo")
            data = me.json()
            log.debug("User info from Okta: %s", data)
            return {
                "username": "okta_" + data.get("sub", ""),
                "first_name": data.get("given_name", ""),
                "last_name": data.get("family_name", ""),
                "email": data.get("email", ""),
                "role_keys": data.get("groups", []),
            }
        else:
            return {}

    def _azure_parse_jwt(self, id_token):
        jwt_token_parts = r"^([^\.\s]*)\.([^\.\s]+)\.([^\.\s]*)$"
        matches = re.search(jwt_token_parts, id_token)
        if not matches or len(matches.groups()) < 3:
            log.error("Unable to parse token.")
            return {}
        return {
            "header": matches.group(1),
            "Payload": matches.group(2),
            "Sig": matches.group(3),
        }

    def _azure_jwt_token_parse(self, id_token):
        jwt_split_token = self._azure_parse_jwt(id_token)
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

    def register_views(self):
        if not self.appbuilder.app.config.get("FAB_ADD_SECURITY_VIEWS", True):
            return
        # Security APIs
        self.appbuilder.add_api(self.security_api)

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
            if self.auth_user_registration:
                pass
                # self.registeruser_view = self.registeruseroidview()
                # self.appbuilder.add_view_no_menu(self.registeruser_view)

        self.appbuilder.add_view_no_menu(self.auth_view)

        self.user_view = self.appbuilder.add_view(
            self.user_view,
            "List Users",
            icon="fa-user",
            label=_("List Users"),
            category="Security",
            category_icon="fa-cogs",
            category_label=_("Security"),
        )

        role_view = self.appbuilder.add_view(
            self.rolemodelview,
            "List Roles",
            icon="fa-group",
            label=_("List Roles"),
            category="Security",
            category_icon="fa-cogs",
        )
        role_view.related_views = [self.user_view.__class__]

        if self.userstatschartview:
            self.appbuilder.add_view(
                self.userstatschartview,
                "User's Statistics",
                icon="fa-bar-chart-o",
                label=_("User's Statistics"),
                category="Security",
            )
        if self.auth_user_registration:
            self.appbuilder.add_view(
                self.registerusermodelview,
                "User's Statistics",
                icon="fa-user-plus",
                label=_("User Registrations"),
                category="Security",
            )
        self.appbuilder.menu.add_separator("Security")
        if self.appbuilder.app.config.get("FAB_ADD_SECURITY_PERMISSION_VIEW", True):
            self.appbuilder.add_view(
                self.permissionmodelview,
                "Base Permissions",
                icon="fa-lock",
                label=_("Base Permissions"),
                category="Security",
            )
        if self.appbuilder.app.config.get("FAB_ADD_SECURITY_VIEW_MENU_VIEW", True):
            self.appbuilder.add_view(
                self.viewmenumodelview,
                "Views/Menus",
                icon="fa-list-alt",
                label=_("Views/Menus"),
                category="Security",
            )
        if self.appbuilder.app.config.get("FAB_ADD_SECURITY_PERMISSION_VIEWS_VIEW", True):
            self.appbuilder.add_view(
                self.permissionviewmodelview,
                "Permission on Views/Menus",
                icon="fa-link",
                label=_("Permission on Views/Menus"),
                category="Security",
            )

    def create_db(self):
        """Setups the DB, creates admin and public roles if they don't exist."""
        roles_mapping = self.appbuilder.get_app.config.get("FAB_ROLES_MAPPING", {})
        for pk, name in roles_mapping.items():
            self.update_role(pk, name)
        for role_name in self.builtin_roles:
            self.add_role(role_name)
        if self.auth_role_admin not in self.builtin_roles:
            self.add_role(self.auth_role_admin)
        self.add_role(self.auth_role_public)
        if self.count_users() == 0:
            log.warning(LOGMSG_WAR_SEC_NO_USER)

    def reset_password(self, userid, password):
        """
        Change/Reset a user's password for authdb.
        Password will be hashed and saved.

        :param userid:
            the user.id to reset the password
        :param password:
            The clear text password to reset and save hashed on the db
        """
        user = self.get_user_by_id(userid)
        user.password = generate_password_hash(password)
        self.update_user(user)

    def update_user_auth_stat(self, user, success=True):
        """
        Update authentication successful to user.

        :param user:
            The authenticated user model
        :param success:
            Default to true, if false increments fail_login_count on user model
        """
        if not user.login_count:
            user.login_count = 0
        if not user.fail_login_count:
            user.fail_login_count = 0
        if success:
            user.login_count += 1
            user.fail_login_count = 0
        else:
            user.fail_login_count += 1
        user.last_login = datetime.datetime.now()
        self.update_user(user)

    def auth_user_db(self, username, password):
        """
        Method for authenticating user, auth db style

        :param username:
            The username or registered email address
        :param password:
            The password, will be tested against hashed password on db
        """
        if username is None or username == "":
            return None
        user = self.find_user(username=username)
        if user is None:
            user = self.find_user(email=username)
        if user is None or (not user.is_active):
            # Balance failure and success
            check_password_hash(
                "pbkdf2:sha256:150000$Z3t6fmj2$22da622d94a1f8118"
                "c0976a03d2f18f680bfff877c9a965db9eedc51bc0be87c",
                "password",
            )
            log.info(LOGMSG_WAR_SEC_LOGIN_FAILED.format(username))
            return None
        elif check_password_hash(user.password, password):
            self.update_user_auth_stat(user, True)
            return user
        else:
            self.update_user_auth_stat(user, False)
            log.info(LOGMSG_WAR_SEC_LOGIN_FAILED.format(username))
            return None

    def _search_ldap(self, ldap, con, username):
        """
        Searches LDAP for user.

        :param ldap: The ldap module reference
        :param con: The ldap connection
        :param username: username to match with AUTH_LDAP_UID_FIELD
        :return: ldap object array
        """
        # always check AUTH_LDAP_SEARCH is set before calling this method
        assert self.auth_ldap_search, "AUTH_LDAP_SEARCH must be set"

        # build the filter string for the LDAP search
        if self.auth_ldap_search_filter:
            filter_str = "(&{}({}={}))".format(
                self.auth_ldap_search_filter, self.auth_ldap_uid_field, username
            )
        else:
            filter_str = f"({self.auth_ldap_uid_field}={username})"

        # build what fields to request in the LDAP search
        request_fields = [
            self.auth_ldap_firstname_field,
            self.auth_ldap_lastname_field,
            self.auth_ldap_email_field,
        ]
        if len(self.auth_roles_mapping) > 0:
            request_fields.append(self.auth_ldap_group_field)

        # preform the LDAP search
        log.debug(
            "LDAP search for '%s' with fields %s in scope '%s'"
            % (filter_str, request_fields, self.auth_ldap_search)
        )
        raw_search_result = con.search_s(
            self.auth_ldap_search, ldap.SCOPE_SUBTREE, filter_str, request_fields
        )
        log.debug(f"LDAP search returned: {raw_search_result}")

        # Remove any search referrals from results
        search_result = [
            (dn, attrs) for dn, attrs in raw_search_result if dn is not None and isinstance(attrs, dict)
        ]

        # only continue if 0 or 1 results were returned
        if len(search_result) > 1:
            log.error(
                "LDAP search for '%s' in scope '%a' returned multiple results"
                % (filter_str, self.auth_ldap_search)
            )
            return None, None

        try:
            # extract the DN
            user_dn = search_result[0][0]
            # extract the other attributes
            user_info = search_result[0][1]
            # return
            return user_dn, user_info
        except (IndexError, NameError):
            return None, None

    def _ldap_calculate_user_roles(self, user_attributes: Dict[str, bytes]) -> List[str]:
        user_role_objects = set()

        # apply AUTH_ROLES_MAPPING
        if len(self.auth_roles_mapping) > 0:
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
                log.warning(f"Can't find AUTH_USER_REGISTRATION role: {registration_role_name}")

        return list(user_role_objects)

    def _ldap_bind_indirect(self, ldap, con) -> None:
        """
        Attempt to bind to LDAP using the AUTH_LDAP_BIND_USER.

        :param ldap: The ldap module reference
        :param con: The ldap connection
        """
        # always check AUTH_LDAP_BIND_USER is set before calling this method
        assert self.auth_ldap_bind_user, "AUTH_LDAP_BIND_USER must be set"

        try:
            log.debug(f"LDAP bind indirect TRY with username: '{self.auth_ldap_bind_user}'")
            con.simple_bind_s(self.auth_ldap_bind_user, self.auth_ldap_bind_password)
            log.debug(f"LDAP bind indirect SUCCESS with username: '{self.auth_ldap_bind_user}'")
        except ldap.INVALID_CREDENTIALS as ex:
            log.error(
                "AUTH_LDAP_BIND_USER and AUTH_LDAP_BIND_PASSWORD are" " not valid LDAP bind credentials"
            )
            raise ex

    @staticmethod
    def _ldap_bind(ldap, con, dn: str, password: str) -> bool:
        """Validates/binds the provided dn/password with the LDAP sever."""
        try:
            log.debug(f"LDAP bind TRY with username: '{dn}'")
            con.simple_bind_s(dn, password)
            log.debug(f"LDAP bind SUCCESS with username: '{dn}'")
            return True
        except ldap.INVALID_CREDENTIALS:
            return False

    @staticmethod
    def ldap_extract(ldap_dict: Dict[str, bytes], field_name: str, fallback: str) -> str:
        raw_value = ldap_dict.get(field_name, [bytes()])
        # decode - if empty string, default to fallback, otherwise take first element
        return raw_value[0].decode("utf-8") or fallback

    @staticmethod
    def ldap_extract_list(ldap_dict: Dict[str, bytes], field_name: str) -> List[str]:
        raw_list = ldap_dict.get(field_name, [])
        # decode - removing empty strings
        return [x.decode("utf-8") for x in raw_list if x.decode("utf-8")]

    def auth_user_ldap(self, username, password):
        """
        Method for authenticating user with LDAP.

        NOTE: this depends on python-ldap module

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
            if self.auth_ldap_allow_self_signed:
                ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_ALLOW)
                ldap.set_option(ldap.OPT_X_TLS_NEWCTX, 0)
            elif self.auth_ldap_tls_demand:
                ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_DEMAND)
                ldap.set_option(ldap.OPT_X_TLS_NEWCTX, 0)
            if self.auth_ldap_tls_cacertdir:
                ldap.set_option(ldap.OPT_X_TLS_CACERTDIR, self.auth_ldap_tls_cacertdir)
            if self.auth_ldap_tls_cacertfile:
                ldap.set_option(ldap.OPT_X_TLS_CACERTFILE, self.auth_ldap_tls_cacertfile)
            if self.auth_ldap_tls_certfile:
                ldap.set_option(ldap.OPT_X_TLS_CERTFILE, self.auth_ldap_tls_certfile)
            if self.auth_ldap_tls_keyfile:
                ldap.set_option(ldap.OPT_X_TLS_KEYFILE, self.auth_ldap_tls_keyfile)

            # Initialise LDAP connection
            con = ldap.initialize(self.auth_ldap_server)
            con.set_option(ldap.OPT_REFERRALS, 0)
            if self.auth_ldap_use_tls:
                try:
                    con.start_tls_s()
                except Exception:
                    log.error(LOGMSG_ERR_SEC_AUTH_LDAP_TLS.format(self.auth_ldap_server))
                    return None

            # Define variables, so we can check if they are set in later steps
            user_dn = None
            user_attributes = {}

            # Flow 1 - (Indirect Search Bind):
            #  - in this flow, special bind credentials are used to preform the
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
                    log.info(LOGMSG_WAR_SEC_NOLDAP_OBJ.format(username))
                    return None

                # Bind with user_dn/password (validates credentials)
                if not self._ldap_bind(ldap, con, user_dn, password):
                    if user:
                        self.update_user_auth_stat(user, False)

                    # Invalid credentials, go away
                    log.info(LOGMSG_WAR_SEC_LOGIN_FAILED.format(username))
                    return None

            # Flow 2 - (Direct Search Bind):
            #  - in this flow, the credentials provided by the end-user are used
            #    to preform the LDAP search
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
                    log.info(LOGMSG_WAR_SEC_LOGIN_FAILED.format(bind_username))
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
                        log.info(LOGMSG_WAR_SEC_NOLDAP_OBJ.format(username))
                        return None

            # Sync the user's roles
            if user and user_attributes and self.auth_roles_sync_at_login:
                user.roles = self._ldap_calculate_user_roles(user_attributes)
                log.debug(f"Calculated new roles for user='{user_dn}' as: {user.roles}")

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
                log.debug(f"New user registered: {user}")

                # If user registration failed, go away
                if not user:
                    log.info(LOGMSG_ERR_SEC_ADD_REGISTER_USER.format(username))
                    return None

            # LOGIN SUCCESS (only if user is now registered)
            if user:
                self.update_user_auth_stat(user)
                return user
            else:
                return None

        except ldap.LDAPError as e:
            msg = None
            if isinstance(e, dict):
                msg = getattr(e, "message", None)
            if (msg is not None) and ("desc" in msg):
                log.error(LOGMSG_ERR_SEC_AUTH_LDAP.format(e.message["desc"]))
                return None
            else:
                log.error(e)
                return None

    def auth_user_oid(self, email):
        """
        Openid user Authentication

        :param email: user's email to authenticate
        :type self: User model
        """
        user = self.find_user(email=email)
        if user is None or (not user.is_active):
            log.info(LOGMSG_WAR_SEC_LOGIN_FAILED.format(email))
            return None
        else:
            self.update_user_auth_stat(user)
            return user

    def auth_user_remote_user(self, username):
        """
        REMOTE_USER user Authentication

        :param username: user's username for remote auth
        :type self: User model
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
            log.info(LOGMSG_WAR_SEC_LOGIN_FAILED.format(username))
            return None

        self.update_user_auth_stat(user)
        return user

    def _oauth_calculate_user_roles(self, userinfo) -> List[str]:
        user_role_objects = set()

        # apply AUTH_ROLES_MAPPING
        if len(self.auth_roles_mapping) > 0:
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
                log.warning(f"Can't find AUTH_USER_REGISTRATION role: {registration_role_name}")

        return list(user_role_objects)

    def auth_user_oauth(self, userinfo):
        """
        Method for authenticating user with OAuth.

        :userinfo: dict with user information
                   (keys are the same as User model columns)
        """
        # extract the username from `userinfo`
        if "username" in userinfo:
            username = userinfo["username"]
        elif "email" in userinfo:
            username = userinfo["email"]
        else:
            log.error(f"OAUTH userinfo does not have username or email {userinfo}")
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
            log.debug(f"Calculated new roles for user='{username}' as: {user.roles}")

        # If the user is new, register them
        if (not user) and self.auth_user_registration:
            user = self.add_user(
                username=username,
                first_name=userinfo.get("first_name", ""),
                last_name=userinfo.get("last_name", ""),
                email=userinfo.get("email", "") or f"{username}@email.notfound",
                role=self._oauth_calculate_user_roles(userinfo),
            )
            log.debug(f"New user registered: {user}")

            # If user registration failed, go away
            if not user:
                log.error(f"Error creating a new OAuth user {username}")
                return None

        # LOGIN SUCCESS (only if user is now registered)
        if user:
            self.update_user_auth_stat(user)
            return user
        else:
            return None

    def is_item_public(self, permission_name, view_name):
        """
        Check if view has public permissions

        :param permission_name:
            the permission: can_show, can_edit...
        :param view_name:
            the name of the class view (child of BaseView)
        """
        permissions = self.get_public_permissions()
        if permissions:
            for i in permissions:
                if (view_name == i.view_menu.name) and (permission_name == i.permission.name):
                    return True
            return False
        else:
            return False

    def _has_access_builtin_roles(self, role, permission_name: str, view_name: str) -> bool:
        """Checks permission on builtin role"""
        builtin_pvms = self.builtin_roles.get(role.name, [])
        for pvm in builtin_pvms:
            _view_name = pvm[0]
            _permission_name = pvm[1]
            if re.match(_view_name, view_name) and re.match(_permission_name, permission_name):
                return True
        return False

    def _has_view_access(self, user: object, permission_name: str, view_name: str) -> bool:
        roles = user.roles
        db_role_ids = []
        # First check against builtin (statically configured) roles
        # because no database query is needed
        for role in roles:
            if role.name in self.builtin_roles:
                if self._has_access_builtin_roles(role, permission_name, view_name):
                    return True
            else:
                db_role_ids.append(role.id)

        # If it's not a builtin role check against database store roles
        return self.exist_permission_on_roles(view_name, permission_name, db_role_ids)

    def get_user_roles(self, user) -> List[object]:
        """Get current user roles, if user is not authenticated returns the public role"""
        if not user.is_authenticated:
            return [self.get_public_role()]
        return user.roles

    def get_role_permissions(self, role) -> Set[Tuple[str, str]]:
        """Get all permissions for a certain role"""
        result = set()
        if role.name in self.builtin_roles:
            for permission in self.builtin_roles[role.name]:
                result.add((permission[1], permission[0]))
        else:
            for permission in self.get_db_role_permissions(role.id):
                result.add((permission.permission.name, permission.view_menu.name))
        return result

    def get_user_permissions(self, user) -> Set[Tuple[str, str]]:
        """Get all permissions from the current user"""
        roles = self.get_user_roles(user)
        result = set()
        for role in roles:
            result.update(self.get_role_permissions(role))
        return result

    def _get_user_permission_view_menus(
        self, user: object, permission_name: str, view_menus_name: List[str]
    ) -> Set[str]:
        """
        Return a set of view menu names with a certain permission name
        that a user has access to. Mainly used to fetch all menu permissions
        on a single db call, will also check public permissions and builtin roles
        """
        db_role_ids = []
        if user is None:
            # include public role
            roles = [self.get_public_role()]
        else:
            roles = user.roles
        # First check against builtin (statically configured) roles
        # because no database query is needed
        result = set()
        for role in roles:
            if role.name in self.builtin_roles:
                for view_menu_name in view_menus_name:
                    if self._has_access_builtin_roles(role, permission_name, view_menu_name):
                        result.add(view_menu_name)
            else:
                db_role_ids.append(role.id)
        # Then check against database-stored roles
        pvms_names = [
            pvm.view_menu.name for pvm in self.find_roles_permission_view_menus(permission_name, db_role_ids)
        ]
        result.update(pvms_names)
        return result

    def has_access(self, permission_name, view_name):
        """Check if current user or public has access to view or menu"""
        if current_user.is_authenticated:
            return self._has_view_access(g.user, permission_name, view_name)
        elif current_user_jwt:
            return self._has_view_access(current_user_jwt, permission_name, view_name)
        else:
            return self.is_item_public(permission_name, view_name)

    def get_user_menu_access(self, menu_names: List[str] = None) -> Set[str]:
        if current_user.is_authenticated:
            return self._get_user_permission_view_menus(g.user, "menu_access", view_menus_name=menu_names)
        elif current_user_jwt:
            return self._get_user_permission_view_menus(
                current_user_jwt, "menu_access", view_menus_name=menu_names
            )
        else:
            return self._get_user_permission_view_menus(None, "menu_access", view_menus_name=menu_names)

    def add_permissions_view(self, base_permissions, view_menu):
        """
        Adds a permission on a view menu to the backend

        :param base_permissions:
            list of permissions from view (all exposed methods):
             'can_add','can_edit' etc...
        :param view_menu:
            name of the view or menu to add
        """
        view_menu_db = self.add_view_menu(view_menu)
        perm_views = self.find_permissions_view_menu(view_menu_db)

        if not perm_views:
            # No permissions yet on this view
            for permission in base_permissions:
                pv = self.add_permission_view_menu(permission, view_menu)
                if self.auth_role_admin not in self.builtin_roles:
                    role_admin = self.find_role(self.auth_role_admin)
                    self.add_permission_role(role_admin, pv)
        else:
            # Permissions on this view exist but....
            role_admin = self.find_role(self.auth_role_admin)
            for permission in base_permissions:
                # Check if base view permissions exist
                if not self.exist_permission_on_views(perm_views, permission):
                    pv = self.add_permission_view_menu(permission, view_menu)
                    if self.auth_role_admin not in self.builtin_roles:
                        self.add_permission_role(role_admin, pv)
            for perm_view in perm_views:
                if perm_view.permission is None:
                    # Skip this perm_view, it has a null permission
                    continue
                if perm_view.permission.name not in base_permissions:
                    # perm to delete
                    roles = self.get_all_roles()
                    perm = self.find_permission(perm_view.permission.name)
                    # del permission from all roles
                    for role in roles:
                        self.del_permission_role(role, perm)
                    self.del_permission_view_menu(perm_view.permission.name, view_menu)
                elif (
                    self.auth_role_admin not in self.builtin_roles and perm_view not in role_admin.permissions
                ):
                    # Role Admin must have all permissions
                    self.add_permission_role(role_admin, perm_view)

    def add_permissions_menu(self, view_menu_name):
        """
        Adds menu_access to menu on permission_view_menu

        :param view_menu_name:
            The menu name
        """
        self.add_view_menu(view_menu_name)
        pv = self.find_permission_view_menu("menu_access", view_menu_name)
        if not pv:
            pv = self.add_permission_view_menu("menu_access", view_menu_name)
        if self.auth_role_admin not in self.builtin_roles:
            role_admin = self.find_role(self.auth_role_admin)
            self.add_permission_role(role_admin, pv)

    def security_cleanup(self, baseviews, menus):
        """
        Will cleanup all unused permissions from the database

        :param baseviews: A list of BaseViews class
        :param menus: Menu class
        """
        viewsmenus = self.get_all_view_menu()
        roles = self.get_all_roles()
        for viewmenu in viewsmenus:
            found = False
            for baseview in baseviews:
                if viewmenu.name == baseview.class_permission_name:
                    found = True
                    break
            if menus.find(viewmenu.name):
                found = True
            if not found:
                permissions = self.find_permissions_view_menu(viewmenu)
                for permission in permissions:
                    for role in roles:
                        self.del_permission_role(role, permission)
                    self.del_permission_view_menu(permission.permission.name, viewmenu.name)
                self.del_view_menu(viewmenu.name)
        self.security_converge(baseviews, menus)

    @staticmethod
    def _get_new_old_permissions(baseview) -> Dict:
        ret = {}
        for method_name, permission_name in baseview.method_permission_name.items():
            old_permission_name = baseview.previous_method_permission_name.get(method_name)
            # Actions do not get prefix when normally defined
            if hasattr(baseview, "actions") and baseview.actions.get(old_permission_name):
                permission_prefix = ""
            else:
                permission_prefix = PERMISSION_PREFIX
            if old_permission_name:
                if PERMISSION_PREFIX + permission_name not in ret:
                    ret[PERMISSION_PREFIX + permission_name] = {permission_prefix + old_permission_name}
                else:
                    ret[PERMISSION_PREFIX + permission_name].add(permission_prefix + old_permission_name)
        return ret

    @staticmethod
    def _add_state_transition(
        state_transition: Dict,
        old_view_name: str,
        old_perm_name: str,
        view_name: str,
        perm_name: str,
    ) -> None:
        old_pvm = state_transition["add"].get((old_view_name, old_perm_name))
        if old_pvm:
            state_transition["add"][(old_view_name, old_perm_name)].add((view_name, perm_name))
        else:
            state_transition["add"][(old_view_name, old_perm_name)] = {(view_name, perm_name)}
        state_transition["del_role_pvm"].add((old_view_name, old_perm_name))
        state_transition["del_views"].add(old_view_name)
        state_transition["del_perms"].add(old_perm_name)

    @staticmethod
    def _update_del_transitions(state_transitions: Dict, baseviews: List) -> None:
        """
        Mutates state_transitions, loop baseviews and prunes all
        views and permissions that are not to delete because references
        exist.

        :param baseview:
        :param state_transitions:
        :return:
        """
        for baseview in baseviews:
            state_transitions["del_views"].discard(baseview.class_permission_name)
            for permission in baseview.base_permissions:
                state_transitions["del_role_pvm"].discard((baseview.class_permission_name, permission))
                state_transitions["del_perms"].discard(permission)

    def create_state_transitions(self, baseviews: List, menus: List) -> Dict:
        """
        Creates a Dict with all the necessary vm/permission transitions

        Dict: {
                "add": {(<VM>, <PERM>): ((<VM>, PERM), ... )}
                "del_role_pvm": ((<VM>, <PERM>), ...)
                "del_views": (<VM>, ... )
                "del_perms": (<PERM>, ... )
              }

        :param baseviews: List with all the registered BaseView, BaseApi
        :param menus: List with all the menu entries
        :return: Dict with state transitions
        """
        state_transitions = {
            "add": {},
            "del_role_pvm": set(),
            "del_views": set(),
            "del_perms": set(),
        }
        for baseview in baseviews:
            add_all_flag = False
            new_view_name = baseview.class_permission_name
            permission_mapping = self._get_new_old_permissions(baseview)
            if baseview.previous_class_permission_name:
                old_view_name = baseview.previous_class_permission_name
                add_all_flag = True
            else:
                new_view_name = baseview.class_permission_name
                old_view_name = new_view_name
            for new_perm_name in baseview.base_permissions:
                if add_all_flag:
                    old_perm_names = permission_mapping.get(new_perm_name)
                    old_perm_names = old_perm_names or (new_perm_name,)
                    for old_perm_name in old_perm_names:
                        self._add_state_transition(
                            state_transitions,
                            old_view_name,
                            old_perm_name,
                            new_view_name,
                            new_perm_name,
                        )
                else:
                    old_perm_names = permission_mapping.get(new_perm_name) or set()
                    for old_perm_name in old_perm_names:
                        self._add_state_transition(
                            state_transitions,
                            old_view_name,
                            old_perm_name,
                            new_view_name,
                            new_perm_name,
                        )
        self._update_del_transitions(state_transitions, baseviews)
        return state_transitions

    def security_converge(self, baseviews: List, menus: List, dry=False) -> Dict:
        """
        Converges overridden permissions on all registered views/api
        will compute all necessary operations from `class_permissions_name`,
        `previous_class_permission_name`, method_permission_name`,
        `previous_method_permission_name` class attributes.

        :param baseviews: List of registered views/apis
        :param menus: List of menu items
        :param dry: If True will not change DB
        :return: Dict with the necessary operations (state_transitions)
        """
        state_transitions = self.create_state_transitions(baseviews, menus)
        if dry:
            return state_transitions
        if not state_transitions:
            log.info("No state transitions found")
            return {}
        log.debug(f"State transitions: {state_transitions}")
        roles = self.get_all_roles()
        for role in roles:
            permissions = list(role.permissions)
            for pvm in permissions:
                new_pvm_states = state_transitions["add"].get((pvm.view_menu.name, pvm.permission.name))
                if not new_pvm_states:
                    continue
                for new_pvm_state in new_pvm_states:
                    new_pvm = self.add_permission_view_menu(new_pvm_state[1], new_pvm_state[0])
                    self.add_permission_role(role, new_pvm)
                if (pvm.view_menu.name, pvm.permission.name) in state_transitions["del_role_pvm"]:
                    self.del_permission_role(role, pvm)
        for pvm in state_transitions["del_role_pvm"]:
            self.del_permission_view_menu(pvm[1], pvm[0], cascade=False)
        for view_name in state_transitions["del_views"]:
            self.del_view_menu(view_name)
        for permission_name in state_transitions["del_perms"]:
            self.del_permission(permission_name)
        return state_transitions

    def find_register_user(self, registration_hash):
        """Generic function to return user registration"""
        raise NotImplementedError

    def add_register_user(self, username, first_name, last_name, email, password="", hashed_password=""):
        """Generic function to add user registration"""
        raise NotImplementedError

    def del_register_user(self, register_user):
        """Generic function to delete user registration"""
        raise NotImplementedError

    def get_user_by_id(self, pk):
        """Generic function to return user by it's id (pk)"""
        raise NotImplementedError

    def find_user(self, username=None, email=None):
        """Generic function find a user by it's username or email"""
        raise NotImplementedError

    def get_all_users(self):
        """Generic function that returns all existing users"""
        raise NotImplementedError

    def get_db_role_permissions(self, role_id: int) -> List[object]:
        """Get all DB permissions from a role id"""
        raise NotImplementedError

    def add_user(self, username, first_name, last_name, email, role, password=""):
        """Generic function to create user"""
        raise NotImplementedError

    def update_user(self, user):
        """
        Generic function to update user

        :param user: User model to update to database
        """
        raise NotImplementedError

    def count_users(self):
        """Generic function to count the existing users"""
        raise NotImplementedError

    def find_role(self, name):
        raise NotImplementedError

    def add_role(self, name):
        raise NotImplementedError

    def update_role(self, role_id, name):
        raise NotImplementedError

    def get_all_roles(self):
        raise NotImplementedError

    def get_public_role(self):
        """Returns all permissions from public role"""
        raise NotImplementedError

    def get_public_permissions(self):
        """Returns all permissions from public role"""
        raise NotImplementedError

    def find_permission(self, name):
        """Finds and returns a Permission by name"""
        raise NotImplementedError

    def find_roles_permission_view_menus(self, permission_name: str, role_ids: List[int]):
        raise NotImplementedError

    def exist_permission_on_roles(self, view_name: str, permission_name: str, role_ids: List[int]) -> bool:
        """Finds and returns permission views for a group of roles"""
        raise NotImplementedError

    def add_permission(self, name):
        """
        Adds a permission to the backend, model permission

        :param name:
            name of the permission: 'can_add','can_edit' etc...
        """
        raise NotImplementedError

    def del_permission(self, name):
        """
        Deletes a permission from the backend, model permission

        :param name:
            name of the permission: 'can_add','can_edit' etc...
        """
        raise NotImplementedError

    """
    ----------------------
     PRIMITIVES VIEW MENU
    ----------------------
    """

    def find_view_menu(self, name):
        """Finds and returns a ViewMenu by name"""
        raise NotImplementedError

    def get_all_view_menu(self):
        raise NotImplementedError

    def add_view_menu(self, name):
        """
        Adds a view or menu to the backend, model view_menu
        :param name:
            name of the view menu to add
        """
        raise NotImplementedError

    def del_view_menu(self, name):
        """
        Deletes a ViewMenu from the backend

        :param name:
            name of the ViewMenu
        """
        raise NotImplementedError

    """
    ----------------------
     PERMISSION VIEW MENU
    ----------------------
    """

    def find_permission_view_menu(self, permission_name, view_menu_name):
        """Finds and returns a PermissionView by names"""
        raise NotImplementedError

    def find_permissions_view_menu(self, view_menu):
        """
        Finds all permissions from ViewMenu, returns list of PermissionView

        :param view_menu: ViewMenu object
        :return: list of PermissionView objects
        """
        raise NotImplementedError

    def add_permission_view_menu(self, permission_name, view_menu_name):
        """
        Adds a permission on a view or menu to the backend

        :param permission_name:
            name of the permission to add: 'can_add','can_edit' etc...
        :param view_menu_name:
            name of the view menu to add
        """
        raise NotImplementedError

    def del_permission_view_menu(self, permission_name, view_menu_name, cascade=True):
        raise NotImplementedError

    def exist_permission_on_views(self, lst, item):
        raise NotImplementedError

    def exist_permission_on_view(self, lst, permission, view_menu):
        raise NotImplementedError

    def add_permission_role(self, role, perm_view):
        """
        Add permission-ViewMenu object to Role

        :param role:
            The role object
        :param perm_view:
            The PermissionViewMenu object
        """
        raise NotImplementedError

    def del_permission_role(self, role, perm_view):
        """
        Remove permission-ViewMenu object to Role

        :param role:
            The role object
        :param perm_view:
            The PermissionViewMenu object
        """
        raise NotImplementedError

    def load_user(self, user_id):
        """Load user by ID"""
        return self.get_user_by_id(int(user_id))

    def load_user_jwt(self, user_id):
        """Load user JWT"""
        user = self.load_user(user_id)
        # Set flask g.user to JWT user, we can't do it on before request
        g.user = user
        return user

    @staticmethod
    def before_request():
        """Hook runs before request"""
        g.user = current_user
