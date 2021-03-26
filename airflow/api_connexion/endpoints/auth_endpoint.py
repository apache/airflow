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

import jwt
from flask import current_app, g, request, session
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER
from flask_login import login_user
from jsonschema import ValidationError

from airflow.api_connexion.exceptions import BadRequest, NotFound, Unauthenticated
from airflow.api_connexion.schemas.auth_schema import info_schema, login_form_schema

log = logging.getLogger(__name__)


def refresh():
    """Refresh token"""


def get_auth_info():
    """Get site authentication info"""
    security_manager = current_app.appbuilder.sm
    oauth_providers = None
    openid_providers = None
    auth_type = security_manager.auth_type
    type_mapping = {
        AUTH_DB: "auth_db",
        AUTH_LDAP: "auth_ldap",
        AUTH_OID: "auth_oid",
        AUTH_OAUTH: "auth_oauth",
        AUTH_REMOTE_USER: "auth_remote_user",
    }

    if auth_type == AUTH_OAUTH:
        oauth_providers = security_manager.oauth_providers
    if auth_type == AUTH_OID:
        openid_providers = security_manager.openid_providers
    return info_schema.dump(
        {
            "auth_type": type_mapping[auth_type],
            "oauth_providers": oauth_providers,
            "openid_providers": openid_providers,
        }
    )


def auth_dblogin():
    """Handle DB login"""
    security_manager = current_app.appbuilder.sm
    auth_type = security_manager.auth_type
    if g.user is not None and g.user.is_authenticated:
        raise Unauthenticated(detail="Client already authenticated")  # For security
    if auth_type not in (AUTH_DB, AUTH_LDAP):
        raise BadRequest(detail="Authentication type do not match")
    body = request.json
    try:
        data = login_form_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err))
    user = security_manager.auth_user_db(data['username'], data['password'])
    if not user:
        user = security_manager.auth_user_ldap(data['username'], data['password'])
    if not user:
        raise NotFound(detail="Invalid login")
    login_user(user, remember=False)
    return security_manager.create_access_token_and_dump_user()


def auth_oauthlogin(provider, register=None, redirect_uri=None):
    """Handle Oauth login"""
    appbuilder = current_app.appbuilder
    if g.user is not None and g.user.is_authenticated:
        pass  # raise Unauthenticated(detail="Client already authenticated")
    if appbuilder.sm.auth_type != AUTH_OAUTH:
        raise BadRequest(detail="Authentication type do not match")
    state = jwt.encode(
        request.args.to_dict(flat=False),
        appbuilder.app.config["SECRET_KEY"],
        algorithm="HS256",
    )
    try:
        auth_provider = appbuilder.sm.oauth_remotes[provider]
        if register:
            session["register"] = True
        if provider == "twitter":
            redirect_uri = redirect_uri + f"&state={state}"
            auth_data = auth_provider.create_authorization_url(redirect_uri=redirect_uri)
            auth_provider.save_authorize_data(request, redirect_uri=redirect_uri, **auth_data)
            return dict(auth_url=auth_data['url'])
        else:
            state = state.decode("ascii") if isinstance(state, bytes) else state
            auth_data = auth_provider.create_authorization_url(
                redirect_uri=redirect_uri,
                state=state,
            )
            auth_provider.save_authorize_data(request, redirect_uri=redirect_uri, **auth_data)
            return dict(auth_url=auth_data['url'])
    except Exception as err:  # pylint: disable=broad-except
        raise NotFound(detail=str(err))


def authorize_oauth(provider, state):
    """Callback to authorize Oauth."""
    appbuilder = current_app.appbuilder
    resp = appbuilder.sm.oauth_remotes[provider].authorize_access_token()
    if resp is None:
        raise BadRequest(detail="You denied the request to sign in")
    log.debug("OAUTH Authorized resp: %s", resp)
    # Verify state
    try:
        jwt.decode(
            state,
            appbuilder.app.config["SECRET_KEY"],
            algorithms=["HS256"],
        )
    except jwt.InvalidTokenError:
        raise BadRequest(detail="State signature is not valid!")
    # Retrieves specific user info from the provider
    try:
        appbuilder.sm.set_oauth_session(provider, resp)
        userinfo = appbuilder.sm.oauth_user_info(provider, resp)
    except Exception as e:  # pylint: disable=broad-except
        log.error("Error returning OAuth user info: %s", e)
        user = None
    else:
        log.debug("User info retrieved from %s: %s", provider, userinfo)
        user = appbuilder.sm.auth_user_oauth(userinfo)

    if user is None:
        raise NotFound(detail="Invalid login")
    login_user(user)
    return appbuilder.sm.create_access_token_and_dump_user()


def auth_remoteuser():
    """Handle remote user auth"""
    appbuilder = current_app.appbuilder
    username = request.environ.get("REMOTE_USER")
    if g.user is not None and g.user.is_authenticated:
        pass  # raise Unauthenticated(detail="Client already authenticated")
    if appbuilder.sm.auth_type != AUTH_REMOTE_USER:
        raise BadRequest(detail="Authentication type do not match")
    if username:
        user = appbuilder.sm.auth_user_remote_user(username)
        if user is None:
            raise NotFound(detail="Invalid login")
        else:
            login_user(user)
    else:
        raise NotFound(detail="Invalid login")
    return appbuilder.sm.create_access_token_and_dump_user()
