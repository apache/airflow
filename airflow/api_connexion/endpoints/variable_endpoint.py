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
from typing import TYPE_CHECKING

from flask import Response
from marshmallow import ValidationError
from sqlalchemy import func, select

from airflow.api_connexion import security
from airflow.api_connexion.endpoints.request_dict import get_json_request_dict
from airflow.api_connexion.endpoints.update_mask import extract_update_mask_data
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.variable_schema import variable_collection_schema, variable_schema
from airflow.models import Variable
from airflow.security import permissions
from airflow.utils.api_migration import mark_fastapi_migration_done
from airflow.utils.log.action_logger import action_event_from_permission
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.decorators import action_logging

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import UpdateMask

RESOURCE_EVENT_PREFIX = "variable"


@mark_fastapi_migration_done
@security.requires_access_variable("DELETE")
@action_logging(
    event=action_event_from_permission(
        prefix=RESOURCE_EVENT_PREFIX,
        permission=permissions.ACTION_CAN_DELETE,
    ),
)
def delete_variable(*, variable_key: str) -> Response:
    """Delete variable."""
    if Variable.delete(variable_key) == 0:
        raise NotFound("Variable not found")
    return Response(status=HTTPStatus.NO_CONTENT)


@mark_fastapi_migration_done
@security.requires_access_variable("GET")
@provide_session
def get_variable(*, variable_key: str, session: Session = NEW_SESSION) -> Response:
    """Get a variable by key."""
    var = session.scalar(select(Variable).where(Variable.key == variable_key).limit(1))
    if not var:
        raise NotFound("Variable not found", detail="Variable does not exist")
    return variable_schema.dump(var)


@security.requires_access_variable("GET")
@format_parameters({"limit": check_limit})
@provide_session
def get_variables(
    *,
    limit: int | None,
    order_by: str = "id",
    offset: int | None = None,
    session: Session = NEW_SESSION,
) -> Response:
    """Get all variable values."""
    total_entries = session.execute(select(func.count(Variable.id))).scalar()
    to_replace = {"value": "val"}
    allowed_sort_attrs = ["value", "key", "id"]
    query = select(Variable)
    query = apply_sorting(query, order_by, to_replace, allowed_sort_attrs)
    variables = session.scalars(query.offset(offset).limit(limit)).all()
    return variable_collection_schema.dump(
        {
            "variables": variables,
            "total_entries": total_entries,
        }
    )


@mark_fastapi_migration_done
@security.requires_access_variable("PUT")
@provide_session
@action_logging(
    event=action_event_from_permission(
        prefix=RESOURCE_EVENT_PREFIX,
        permission=permissions.ACTION_CAN_EDIT,
    ),
)
def patch_variable(
    *,
    variable_key: str,
    update_mask: UpdateMask = None,
    session: Session = NEW_SESSION,
) -> Response:
    """Update a variable by key."""
    try:
        data = variable_schema.load(get_json_request_dict())
    except ValidationError as err:
        raise BadRequest("Invalid Variable schema", detail=str(err.messages))

    if data["key"] != variable_key:
        raise BadRequest("Invalid post body", detail="key from request body doesn't match uri parameter")
    non_update_fields = ["key"]
    variable = session.scalar(select(Variable).filter_by(key=variable_key).limit(1))
    if not variable:
        raise NotFound("Variable not found", detail="Variable does not exist")
    if update_mask:
        data = extract_update_mask_data(update_mask, non_update_fields, data)
    for key, val in data.items():
        setattr(variable, key, val)
    session.add(variable)
    return variable_schema.dump(variable)


@security.requires_access_variable("POST")
@action_logging(
    event=action_event_from_permission(
        prefix=RESOURCE_EVENT_PREFIX,
        permission=permissions.ACTION_CAN_CREATE,
    ),
)
def post_variables() -> Response:
    """Create a variable."""
    try:
        data = variable_schema.load(get_json_request_dict())
    except ValidationError as err:
        raise BadRequest("Invalid Variable schema", detail=str(err.messages))
    Variable.set(data["key"], data["val"], description=data.get("description", None))
    return variable_schema.dump(data)
