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
from datetime import datetime

import jwt
from flask import current_app, g, jsonify, request, session as c_session
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER
from flask_jwt_extended import create_access_token, decode_token, get_jti, get_jwt_identity
from flask_login import current_user, login_user
from marshmallow import ValidationError

from airflow.api_connexion.exceptions import BadRequest, NotFound, Unauthenticated
from airflow.api_connexion.schemas.auth_schema import (
    info_schema,
    login_form_schema,
    logout_schema,
    token_schema,
)
from airflow.api_connexion.security import jwt_refresh_token_required_
from airflow.models.auth import Token
from airflow.utils.session import provide_session

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
    security_manager = current_app.appbuilder.sm
    auth_type = security_manager.auth_type
    if g.user is not None and g.user.is_authenticated:
        raise Unauthenticated(detail="Client already authenticated")  # For security
    if auth_type not in (AUTH_DB, AUTH_LDAP):
        raise Unauthenticated(detail="Authentication type do not match")
    body = request.json
    try:
        data = login_form_schema.load(body)
    except ValidationError as err:
        raise Unauthenticated(detail=str(err.messages))
    if auth_type == AUTH_DB:
        user = security_manager.auth_user_db(data['username'], data['password'])
    else:
        user = security_manager.auth_user_ldap(data['username'], data['password'])
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
        appbuilder.sm.set_oauth_session(provider, resp)
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


@jwt_refresh_token_required_
@provide_session
def refresh_token(session=None):
    """Refresh token"""
    user = get_jwt_identity()
    access_token = create_access_token(identity=user)
    decoded = decode_token(access_token)
    token = Token(jti=decoded['jti'], expiry_delta=decoded['exp'], created_delta=decoded['iat'])
    session.add(token)
    session.commit()
    ret = {'access_token': access_token}
    return jsonify(ret), 200


def logout():
    """Sign out"""
    body = request.json
    try:
        data = logout_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    token_jti = get_jti(data['token'])
    exist = Token.get_token(token_jti)
    if exist:
        Token.delete_token(token_jti)
    refresh_jti = get_jti(data['refresh_token'])
    exist = Token.get_token(refresh_jti)
    if exist:
        Token.delete_token(refresh_jti)
    resp = {"logged_out": True}
    return jsonify(resp), 200


@provide_session
def revoke_token(session=None):
    """Revoke a token"""
    body = request.json
    try:
        data = token_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    jti = get_jti(data['token'])
    token = Token.get_token(jti)
    if token:
        token.is_revoked = True
        token.revoked_by = current_user.username
        token.revoke_reason = data['reason']
        token.date_revoked = datetime.now()
        session.merge(token)
        session.commit()
        resp = jsonify({"revoked": True})
        return resp
    raise NotFound(detail="Token not found")
