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

from flask import current_app, jsonify, request, session as c_session
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
    user = security_manager.login_with_user_pass(data['username'], data['password'])
    if not user:
        raise Unauthenticated(detail="Invalid login")
    login_user(user, remember=False)
    return security_manager.create_tokens_and_dump(user, auth_schema)


def auth_oauthlogin(provider, register=None, redirect_url=None):
    """Returns OAUTH authorization url"""
    appbuilder = current_app.appbuilder
    if register:
        c_session["register"] = True
    return appbuilder.sm.oauth_authorization_url(appbuilder.app, provider, redirect_url)


def authorize_oauth(provider, state):
    """Callback to authorize Oauth."""
    appbuilder = current_app.appbuilder
    user = appbuilder.sm.oauth_login_user(appbuilder.app, provider, state)
    return appbuilder.sm.create_tokens_and_dump(user, auth_schema)


def auth_remoteuser():
    """Handle remote user auth"""
    appbuilder = current_app.appbuilder
    username = request.environ.get("REMOTE_USER")
    if username:
        user = appbuilder.sm.login_remote_user(username)
    else:
        raise Unauthenticated(detail="Invalid login")
    return appbuilder.sm.create_tokens_and_dump(user, auth_schema)


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
