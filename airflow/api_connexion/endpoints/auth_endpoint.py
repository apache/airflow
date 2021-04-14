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
from flask import current_app, g, jsonify, request, session as c_session
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER
from flask_jwt_extended import (
    create_access_token,
    decode_token,
    get_jwt_identity,
    get_raw_jwt,
    jwt_refresh_token_required,
    jwt_required,
)
from flask_login import login_user
from marshmallow import ValidationError

from airflow.api_connexion.exceptions import BadRequest, Unauthenticated
from airflow.api_connexion.schemas.auth_schema import info_schema, login_form_schema, token_schema
from airflow.models.auth import TokenBlockList

log = logging.getLogger(__name__)


def get_auth_info():
    """Get site authentication info"""
    security_manager = current_app.appbuilder.sm
    config = current_app.config
    auth_type = security_manager.auth_type
    type_mapping = {
        AUTH_DB: "auth_db",
        AUTH_LDAP: "auth_ldap",
        AUTH_OID: "auth_oid",
        AUTH_OAUTH: "auth_oauth",
        AUTH_REMOTE_USER: "auth_remote_user",
    }
    oauth_providers = config.get("OAUTH_PROVIDERS", None)
    openid_providers = config.get("OPENID_PROVIDERS", None)
    return info_schema.dump(
        {
            "auth_type": type_mapping[auth_type],
            "oauth_providers": oauth_providers,
            "openid_providers": openid_providers,
        }
    )


def auth_login():
    """Handle DB login"""
    body = request.json
    try:
        data = login_form_schema.load(body)
    except ValidationError as err:
        raise Unauthenticated(detail=str(err.messages))
    security_manager = current_app.appbuilder.sm
    user = security_manager.api_login_with_username_and_password(data['username'], data['password'])
    if not user:
        raise Unauthenticated(detail="Invalid login")
    login_user(user, remember=False)
    return security_manager.create_tokens_and_dump(user)


def auth_oauthlogin(provider, register=None, redirect_url=None):
    """Handle Oauth login"""
    appbuilder = current_app.appbuilder
    if g.user is not None and g.user.is_authenticated:
        raise Unauthenticated(detail="Client already authenticated")
    if appbuilder.sm.auth_type != AUTH_OAUTH:
        raise Unauthenticated(detail="Authentication type do not match")
    state = jwt.encode(
        request.args.to_dict(flat=False),
        appbuilder.app.config["SECRET_KEY"],
        algorithm="HS256",
    )
    try:
        auth_provider = appbuilder.sm.oauth_remotes[provider]
        if register:
            c_session["register"] = True
        if provider == "twitter":
            redirect_uri = redirect_url + f"&state={state}"
            auth_data = auth_provider.create_authorization_url(redirect_uri=redirect_uri)
            auth_provider.save_authorize_data(request, redirect_uri=redirect_uri, **auth_data)
            return dict(auth_url=auth_data['url'])
        else:
            state = state.decode("ascii") if isinstance(state, bytes) else state
            auth_data = auth_provider.create_authorization_url(
                redirect_uri=redirect_url,
                state=state,
            )
            auth_provider.save_authorize_data(request, redirect_uri=redirect_url, **auth_data)
            return dict(auth_url=auth_data['url'])
    except Exception as err:  # pylint: disable=broad-except
        raise Unauthenticated(detail=str(err))


def authorize_oauth(provider, state):
    """Callback to authorize Oauth."""
    appbuilder = current_app.appbuilder
    resp = appbuilder.sm.oauth_remotes[provider].authorize_access_token()
    if resp is None:
        raise Unauthenticated(detail="You denied the request to sign in")
    log.debug("OAUTH Authorized resp: %s", resp)
    # Verify state
    try:
        jwt.decode(
            state,
            appbuilder.app.config["SECRET_KEY"],
            algorithms=["HS256"],
        )
    except jwt.InvalidTokenError:
        raise Unauthenticated(detail="State signature is not valid!")
    # Retrieves specific user info from the provider
    try:
        userinfo = appbuilder.sm.oauth_user_info(provider, resp)
    except Exception as e:  # pylint: disable=broad-except
        log.error("Error returning OAuth user info: %s", e)
        user = None
    else:
        log.debug("User info retrieved from %s: %s", provider, userinfo)
        user = appbuilder.sm.auth_user_oauth(userinfo)

    if user is None:
        raise Unauthenticated(detail="Invalid login")
    login_user(user)
    return appbuilder.sm.create_tokens_and_dump(user)


def auth_remoteuser():
    """Handle remote user auth"""
    appbuilder = current_app.appbuilder
    username = request.environ.get("REMOTE_USER")
    if g.user is not None and g.user.is_authenticated:
        raise Unauthenticated(detail="Client already authenticated")
    if appbuilder.sm.auth_type != AUTH_REMOTE_USER:
        raise Unauthenticated(detail="Authentication type do not match")
    if username:
        user = appbuilder.sm.auth_user_remote_user(username)
        if user is None:
            raise Unauthenticated(detail="Invalid login")
        else:
            login_user(user)
    else:
        raise Unauthenticated(detail="Invalid login")
    return appbuilder.sm.create_tokens_and_dump(user)


@jwt_refresh_token_required
def refresh_token():
    """Refresh token"""
    user = get_jwt_identity()
    access_token = create_access_token(identity=user)
    ret = {'access_token': access_token}
    return jsonify(ret), 200


@jwt_required
def revoke_token():
    """
    An endpoint for revoking both access and refresh token.

    This is intended for a case where a logged in user want to revoke
    another user's tokens
    """
    resp = jsonify({"revoked": True})
    body = request.json
    try:
        data = token_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    token = decode_token(data['token'])
    tkn = TokenBlockList.get_token(token['jti'])

    if not tkn:
        TokenBlockList.add_token(jti=token['jti'], expiry_delta=token['exp'])
    return resp


@jwt_required
def revoke_current_user_token():
    """
    An endpoint for revoking current user access token
    This should be called during current user logout
    """
    resp = jsonify({"revoked": True})
    raw_jwt = get_raw_jwt()
    token = TokenBlockList.get_token(raw_jwt['jti'])
    if not token:
        TokenBlockList.add_token(jti=raw_jwt['jti'], expiry_delta=raw_jwt['exp'])
    return resp


@jwt_refresh_token_required
def revoke_current_user_refresh_token():
    """
    An endpoint for revoking current user refresh token
    This should be called during current user logout
    """
    resp = jsonify({"revoked": True})
    raw_jwt = get_raw_jwt()
    token = TokenBlockList.get_token(raw_jwt['jti'])
    if not token:
        TokenBlockList.add_token(jti=raw_jwt['jti'], expiry_delta=raw_jwt['exp'])
    return resp
