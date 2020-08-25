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
from typing import List, Optional

from flask import Response, request
from marshmallow import ValidationError
from sqlalchemy import func

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.variable_schema import variable_collection_schema, variable_schema
from airflow.models import Variable
from airflow.utils.session import provide_session


@security.requires_authentication
def delete_variable(variable_key: str) -> Response:
    """
    Delete variable
    """
    if Variable.delete(variable_key) == 0:
        raise NotFound("Variable not found")
    return Response(status=204)


@security.requires_authentication
def get_variable(variable_key: str) -> Response:
    """
    Get a variables by key
    """
    try:
        var = Variable.get(variable_key)
    except KeyError:
        raise NotFound("Variable not found")
    return variable_schema.dump({"key": variable_key, "val": var})


@security.requires_authentication
@format_parameters({'limit': check_limit})
@provide_session
def get_variables(session, limit: Optional[int], offset: Optional[int] = None) -> Response:
    """
    Get all variable values
    """
    total_entries = session.query(func.count(Variable.id)).scalar()
    query = session.query(Variable).order_by(Variable.id)
    if offset:
        query = query.offset(offset)
    if limit:
        query = query.limit(limit)
    variables = query.all()
    return variable_collection_schema.dump({"variables": variables, "total_entries": total_entries,})


@security.requires_authentication
def patch_variable(variable_key: str, update_mask: Optional[List[str]] = None) -> Response:
    """
    Update a variable by key
    """
    try:
        data = variable_schema.load(request.json)
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
    return Response(status=204)


@security.requires_authentication
def post_variables() -> Response:
    """
    Create a variable
    """
    try:
        data = variable_schema.load(request.json)

    except ValidationError as err:
        raise BadRequest("Invalid Variable schema", detail=str(err.messages))
    Variable.set(data["key"], data["val"])
    return variable_schema.dump(data)
