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

from sqlalchemy import func
from sqlalchemy.orm import Session

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.dag_warning_schema import (
    DagWarningCollection,
    dag_warning_collection_schema,
    dag_warning_schema,
)
from airflow.api_connexion.types import APIResponse
from airflow.models.dagwarning import DagWarning as DagWarningModel
from airflow.security import permissions
from airflow.utils.session import NEW_SESSION, provide_session


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_WARNING)])
@provide_session
def get_dag_warning(*, dag_warning_id: int, session: Session = NEW_SESSION) -> APIResponse:
    """Get a DAG warning"""
    error = session.query(DagWarningModel).get(dag_warning_id)

    if error is None:
        raise NotFound(
            "Dag warning not found",
            detail=f"The DagWarning with dag_warning_id: `{dag_warning_id}` was not found",
        )
    return dag_warning_schema.dump(error)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_WARNING)])
@format_parameters({'limit': check_limit})
@provide_session
def get_dag_warnings(
    *,
    limit: int,
    offset: Optional[int] = None,
    order_by: str = "dag_warning_id",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all import errors"""
    to_replace = {"dag_warning_id": 'id'}
    allowed_filter_attrs = ['dag_warning_id', "timestamp", "warning_type", "message"]
    total_entries = session.query(func.count(DagWarningModel.id)).scalar()
    query = session.query(DagWarningModel)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    dag_warnings = query.offset(offset).limit(limit).all()
    return dag_warning_collection_schema.dump(
        DagWarningCollection(dag_warnings=dag_warnings, total_entries=total_entries)
    )
