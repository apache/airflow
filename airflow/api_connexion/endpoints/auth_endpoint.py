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

from flask import current_app, jsonify
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    get_jwt_identity,
    jwt_refresh_token_required,
    set_access_cookies,
    set_refresh_cookies,
    unset_jwt_cookies,
)

from airflow.api_connexion import security


def login():
    """User login"""
    ab_security_manager = current_app.appbuilder.sm
    user = ab_security_manager.current_user
    if user:
        access_token = create_access_token(user.id)
        refresh_token = create_refresh_token(user.id)
        resp = jsonify({"logged_in": True})
        set_access_cookies(resp, access_token)
        set_refresh_cookies(resp, refresh_token)
        return resp
    security.check_authentication()
    user = current_app.appbuilder.sm.current_user
    access_token = create_access_token(user.id)
    refresh_token = create_refresh_token(user.id)
    resp = jsonify({"logged_in": True})
    set_access_cookies(resp, access_token)
    set_refresh_cookies(resp, refresh_token)
    return resp


@jwt_refresh_token_required
def token_refresh():
    """Refresh token"""
    current_user = get_jwt_identity()
    access_token = create_access_token(identity=current_user)
    resp = jsonify({"fresh": True})
    set_access_cookies(resp, access_token)
    return resp


def logout():
    """Sign out"""
    resp = jsonify({'logged_out': True})
    unset_jwt_cookies(resp)
    return resp, 200
