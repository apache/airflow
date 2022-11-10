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

from http import HTTPStatus

from flask import Response
from marshmallow import ValidationError
from sqlalchemy import func
from sqlalchemy.orm import Session

from airflow.api_connexion import security
from airflow.api_connexion.endpoints.request_dict import get_json_request_dict
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.variable_schema import variable_collection_schema, variable_schema
from airflow.api_connexion.types import UpdateMask
from airflow.models import Variable
from airflow.security import permissions
from airflow.utils.session import NEW_SESSION, provide_session


@security.requires_access([(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VARIABLE)])
def delete_variable(*, variable_key: str) -> Response:
    """Delete variable"""
    if Variable.delete(variable_key) == 0:
        raise NotFound("Variable not found")
    return Response(status=HTTPStatus.NO_CONTENT)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
@provide_session
def get_variable(*, variable_key: str, session: Session = NEW_SESSION) -> Response:
    """Get a variable by key"""
    var = session.query(Variable).filter(Variable.key == variable_key)
    if not var.count():
        raise NotFound("Variable not found")
    return variable_schema.dump(var.first())


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)])
@format_parameters({"limit": check_limit})
@provide_session
def get_variables(
    *,
    limit: int | None,
    order_by: str = "id",
    offset: int | None = None,
    session: Session = NEW_SESSION,
) -> Response:
    """Get all variable values"""
    total_entries = session.query(func.count(Variable.id)).scalar()
    to_replace = {"value": "val"}
    allowed_filter_attrs = ["value", "key", "id"]
    query = session.query(Variable)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    variables = query.offset(offset).limit(limit).all()
    return variable_collection_schema.dump(
        {
            "variables": variables,
            "total_entries": total_entries,
        }
    )


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_VARIABLE)])
def patch_variable(*, variable_key: str, update_mask: UpdateMask = None) -> Response:
    """Update a variable by key"""
    try:
        data = variable_schema.load(get_json_request_dict())
    except ValidationError as err:
        raise BadRequest("Invalid Variable schema", detail=str(err.messages))

    if data["key"] != variable_key:
        raise BadRequest("Invalid post body", detail="key from request body doesn't match uri parameter")

    if update_mask:
        if "key" in update_mask:
            raise BadRequest("key is a ready only field")
        if "value" not in update_mask:
            raise BadRequest("No field to update")

    Variable.set(data["key"], data["val"])
    return variable_schema.dump(data)


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE)])
def post_variables() -> Response:
    """Create a variable"""
    try:
        data = variable_schema.load(get_json_request_dict())

    except ValidationError as err:
        raise BadRequest("Invalid Variable schema", detail=str(err.messages))
    Variable.set(data["key"], data["val"])
    return variable_schema.dump(data)
