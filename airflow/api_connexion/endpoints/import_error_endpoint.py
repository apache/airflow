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
from flask import request
from sqlalchemy import func

from airflow.api_connexion import parameters
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.schemas.error_schema import (
    ImportErrorCollection, import_error_collection_schema, import_error_schema,
)
from airflow.models.errors import ImportError  # pylint: disable=redefined-builtin
from airflow.utils.session import provide_session


def delete_import_error():
    """
    Delete an import error
    """
    raise NotImplementedError("Not implemented yet.")


@provide_session
def get_import_error(import_error_id, session):
    """
    Get an import error
    """
    query = session.query(ImportError)
    error = query.filter(ImportError.id == import_error_id).one_or_none()

    if error is None:
        raise NotFound("Import error not found")
    return import_error_schema.dump(error)


@provide_session
def get_import_errors(session):
    """
    Get all import errors
    """
    offset = request.args.get(parameters.page_offset, 0)
    limit = min(int(request.args.get(parameters.page_limit, 100)), 100)

    total_entries = session.query(func.count(ImportError.id)).scalar()
    query = session.query(ImportError).order_by(ImportError.id).offset(offset).limit(limit)

    import_errors = query.all()
    return import_error_collection_schema.dump(
        ImportErrorCollection(import_errors=import_errors, total_entries=total_entries)
    ).data
