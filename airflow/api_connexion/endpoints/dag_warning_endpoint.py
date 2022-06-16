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
def get_dag_warning(*, dag_id: str, warning_type: str, session: Session = NEW_SESSION) -> APIResponse:
    """Get a DAG warning"""
    entity = session.query(DagWarningModel).get((dag_id, warning_type))

    if entity is None:
        raise NotFound(
            "Dag warning not found",
            detail=f"The DagWarning with (dag_id, warning_type) = {(dag_id, warning_type)} was not found",
        )
    return dag_warning_schema.dump(entity)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_WARNING)])
@format_parameters({'limit': check_limit})
@provide_session
def get_dag_warnings(
    *,
    limit: int,
    offset: Optional[int] = None,
    order_by: str = "timestamp",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all import errors"""
    allowed_filter_attrs = ["dag_id", "warning_type", "message", "timestamp"]
    total_entries = session.query(func.count(DagWarningModel.dag_id)).scalar()
    query = session.query(DagWarningModel)
    query = apply_sorting(query=query, order_by=order_by, allowed_attrs=allowed_filter_attrs)
    dag_warnings = query.offset(offset).limit(limit).all()
    return dag_warning_collection_schema.dump(
        DagWarningCollection(dag_warnings=dag_warnings, total_entries=total_entries)
    )
