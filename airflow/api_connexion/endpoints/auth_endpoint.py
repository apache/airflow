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

import jwt
from flask import current_app, g, request, session
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER
from flask_jwt_extended import create_access_token, create_refresh_token
from flask_login import login_user
from jsonschema import ValidationError

from airflow.api_connexion.exceptions import BadRequest, NotFound, Unauthenticated
from airflow.api_connexion.schemas.auth_schema import info_schema, jwt_token_schema, login_form_schema
from airflow.configuration import conf


def get_info():
    """Get information about site including auth methods"""
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
    if auth_type != AUTH_DB or auth_type != AUTH_LDAP:
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
    token = create_access_token(identity=user.id, fresh=True)
    refresh_token = create_refresh_token(user.id)
    return jwt_token_schema.dump(dict(token=token, refresh_token=refresh_token))


def refresh():
    """Refresh token"""


def auth_oauthlogin(provider, register=None):
    """Handle Oauth login"""
    api_base_path = "/api/v1/"
    base_url = conf.get("webserver", "base_url") + api_base_path
    appbuilder = current_app.appbuilder
    if g.user is not None and g.user.is_authenticated:
        raise Unauthenticated(detail="Client already authenticated")
    state = jwt.encode(
        request.args.to_dict(flat=False),
        appbuilder.app.config["SECRET_KEY"],
        algorithm="HS256",
    )
    redirect_uri = base_url + f"authorized?provider={provider}"
    try:
        if register:
            session["register"] = True
        if provider == "twitter":
            return appbuilder.sm.oauth_remotes[provider].authorize_redirect(
                redirect_uri=redirect_uri + f"&state={state}"
            )
        else:
            return appbuilder.sm.oauth_remotes[provider].authorize_redirect(
                redirect_uri=redirect_uri,
                state=state.decode("ascii") if isinstance(state, bytes) else state,
            )
    except Exception:

        return
