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
from flask import current_app
from flask_appbuilder.security.sqla.models import User
from sqlalchemy import func

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.user_schema import (
    UserCollection,
    user_collection_item_schema,
    user_collection_schema,
)
from airflow.security import permissions


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_USER)])
def get_user(username):
    """Get a user"""
    ab_security_manager = current_app.appbuilder.sm
    user = ab_security_manager.find_user(username=username)
    if not user:
        raise NotFound(title="User not found", detail=f"The User with username `{username}` was not found")
    return user_collection_item_schema.dump(user)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_USER)])
@format_parameters({'limit': check_limit})
def get_users(limit, order_by='id', offset=None):
    """Get users"""
    appbuilder = current_app.appbuilder
    session = appbuilder.get_session
    total_entries = session.query(func.count(User.id)).scalar()
    to_replace = {"user_id": "id"}
    allowed_filter_attrs = [
        "user_id",
        'id',
        "first_name",
        "last_name",
        "user_name",
        "email",
        "is_active",
        "role",
    ]
    query = session.query(User)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    users = query.offset(offset).limit(limit).all()

    return user_collection_schema.dump(UserCollection(users=users, total_entries=total_entries))
