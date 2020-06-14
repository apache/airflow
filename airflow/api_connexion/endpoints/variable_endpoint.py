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
from typing import Optional

from flask import Response, make_response, request
from marshmallow import ValidationError

from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.variable_schema import variable_collection_schema, variable_schema
from airflow.models import Variable
from airflow.utils.session import provide_session


def delete_variable(variable_key: str) -> Response:
    """
    Delete variable
    """
    Variable.delete(variable_key)
    return make_response('', 204)


def get_variable(variable_key: str) -> Response:
    """
    Get a variables by key
    """
    try:
        var = Variable.get(variable_key)
    except KeyError:
        raise NotFound("Variable not found")
    return variable_schema.dump({"key": variable_key, "val": var})


@provide_session
def get_variables(session, limit: Optional[int], offset: Optional[int] = None) -> Response:
    """
    Get all variable values
    """
    query = session.query(Variable)
    if offset:
        query = query.offset(offset)
    if limit:
        query = query.limit(limit)
    variables = query.all()
    return variable_collection_schema.dump({
        "variables": variables,
        "total_entries": len(variables),
    })


def patch_variable(variable_key: str):
    """
    Update a variable by key
    """
    try:
        var = variable_schema.loads(request.get_data())
    except ValidationError as err:
        raise BadRequest("Invalid Variable schema", detail=str(err.messages))

    if var.data["key"] != variable_key:
        raise BadRequest("Invalid post body", detail="key from request body doesn't match uri parameter")

    Variable.set(var.data["key"], var.data["val"])
    return make_response('', 204)


def post_variables():
    """
    Create a variable
    """
    try:
        var = variable_schema.loads(request.get_data())
    except ValidationError as err:
        raise BadRequest("Invalid Variable schema", detail=str(err.messages))
    Variable.set(var.data["key"], var.data["val"])
    return variable_schema.dump(var)
