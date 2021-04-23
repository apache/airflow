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
import re

import jwt
from flask import current_app, jsonify, request
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
from sqlalchemy.exc import IntegrityError

from airflow.api_connexion.exceptions import BadRequest, Unauthenticated
from airflow.api_connexion.schemas.auth_schema import (
    auth_schema,
    info_schema,
    login_form_schema,
    token_schema,
)
from airflow.configuration import conf
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
    appbuilder = current_app.appbuilder
    body = request.json
    try:
        data = login_form_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    security_manager = appbuilder.sm
    if security_manager.auth_type not in [AUTH_DB, AUTH_LDAP]:
        raise Unauthenticated(detail="Authentication type does not match")
    user = security_manager.login_with_user_pass(data['username'], data['password'])
    if not user:
        raise Unauthenticated(detail="Invalid login")
    login_user(user, remember=False)
    return security_manager.create_tokens_and_dump(user, auth_schema)


def auth_oauthlogin(provider, register=None):
    """Authenticates user and redirect to callback endpoint"""
    appbuilder = current_app.appbuilder
    security_manager = appbuilder.sm
    if security_manager.auth_type != AUTH_OAUTH:
        raise Unauthenticated(detail="Authentication type does not match")
    api_base_path = "/api/v1/"
    base_url = conf.get("webserver", "base_url") + api_base_path
    redirect_uri = base_url + "oauth-authorized?provider=" + provider

    arg = {'register': [str(register)]} if register else {}
    state = jwt.encode(
        arg,
        key=appbuilder.app.config['SECRET_KEY'],
        algorithm='HS256',
    )
    auth_provider = security_manager.oauth_remotes[provider]
    try:
        if provider == "twitter":
            redirect_uri = redirect_uri + f"&state={state}"
            return auth_provider.authorize_redirect(redirect_uri=redirect_uri)
        else:
            state = state.decode("ascii") if isinstance(state, bytes) else state
            return auth_provider.authorize_redirect(
                redirect_uri=redirect_uri,
                state=state,
            )
    except Exception as err:  # pylint: disable=broad-except
        log.error("Error on OAuth authorize: %s", err)
        raise Unauthenticated(detail=str(err))


def authorize_oauth():
    """Callback to authorize Oauth."""
    args = request.args
    provider = args.get('provider', None)
    state = args.get('state', None)
    if not (provider and state):
        raise BadRequest(detail="Missing required parameters: provider and state")
    appbuilder = current_app.appbuilder
    security_manager = appbuilder.sm
    log.debug("Authorized init")
    resp = security_manager.oauth_remotes[provider].authorize_access_token()
    if resp is None:
        raise Unauthenticated(detail="You denied the request to sign in")
    # Verify state
    try:
        jwt.decode(
            state,
            key=appbuilder.app.config['SECRET_KEY'],
            algorithms='HS256',
        )
    except jwt.InvalidTokenError:
        raise BadRequest(detail="State signature is not valid!")
    log.debug("OAUTH Authorized resp: %s", resp)
    # Retrieves specific user info from the provider
    try:  # pylint: disable=too-many-nested-blocks
        userinfo = security_manager.oauth_user_info(provider, resp)
    except Exception as err:  # pylint: disable=broad-except
        log.debug("Error retrieving user info. Error =>%s", err)
        user = None
    else:
        log.debug("User info retrieved from %s: %s", provider, userinfo)
        # User email is not whitelisted
        if provider in security_manager.oauth_whitelists:
            whitelist = security_manager.oauth_whitelists[provider]
            allow = False
            for e in whitelist:
                if re.search(e, userinfo["email"]):
                    allow = True
                    break
            if not allow:
                raise Unauthenticated(detail="You are not authorized.")
        else:
            log.debug("No allow list for OAuth provider")
        user = security_manager.auth_user_oauth(userinfo)
        log.debug("User is %s", user)
    if user is None:
        raise Unauthenticated(detail="Invalid login")
    login_user(user, remember=False)
    return security_manager.create_tokens_and_dump(user, auth_schema)


def auth_remoteuser():
    """Handle remote user auth"""
    appbuilder = current_app.appbuilder
    security_manager = appbuilder.sm
    if security_manager.auth_type != AUTH_REMOTE_USER:
        raise Unauthenticated(detail="Authentication type does not match")
    username = request.environ.get("REMOTE_USER")
    if not username:
        raise Unauthenticated(detail="Invalid login")
    user = security_manager.auth_user_remote_user(username)
    if user is None:
        raise Unauthenticated(detail="Invalid login")
    login_user(user, remember=False)
    return security_manager.create_tokens_and_dump(user, auth_schema)


@jwt_refresh_token_required
def refresh_token():
    """Refresh token"""
    user = get_jwt_identity()
    access_token = create_access_token(identity=user)
    return {'access_token': access_token}


@jwt_required
def revoke_token():
    """
    An endpoint for revoking both access and refresh token.

    This is intended for a case where a logged in user want to revoke
    another user's tokens
    """
    body = request.json
    try:
        data = token_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    token = decode_token(data['token'])
    try:
        TokenBlockList.add_token(jti=token['jti'], expiry_delta=token['exp'])
    except IntegrityError:
        pass
    return jsonify({"revoked": True})


@jwt_required
def revoke_current_user_token():
    """
    An endpoint for revoking current user access token
    This should be called during current user logout
    """
    raw_jwt = get_raw_jwt()
    try:
        TokenBlockList.add_token(jti=raw_jwt['jti'], expiry_delta=raw_jwt['exp'])
    except IntegrityError:
        pass
    return jsonify({"revoked": True})


@jwt_refresh_token_required
def revoke_current_user_refresh_token():
    """
    An endpoint for revoking current user refresh token
    This should be called during current user logout
    """
    raw_jwt = get_raw_jwt()
    try:
        TokenBlockList.add_token(jti=raw_jwt['jti'], expiry_delta=raw_jwt['exp'])
    except IntegrityError:
        pass
    return jsonify({"revoked": True})
