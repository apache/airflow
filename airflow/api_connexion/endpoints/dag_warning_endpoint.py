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

from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.api_connexion import security
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.dag_warning_schema import (
    DagWarningCollection,
    dag_warning_collection_schema,
)
from airflow.api_connexion.security import get_readable_dags
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.models.dagwarning import DagWarning as DagWarningModel
from airflow.utils.api_migration import mark_fastapi_migration_done
from airflow.utils.db import get_query_count
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse


@mark_fastapi_migration_done
@security.requires_access_dag("GET", DagAccessEntity.WARNING)
@format_parameters({"limit": check_limit})
@provide_session
def get_dag_warnings(
    *,
    limit: int,
    dag_id: str | None = None,
    warning_type: str | None = None,
    offset: int | None = None,
    order_by: str = "timestamp",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """
    Get DAG warnings.

    :param dag_id: the dag_id to optionally filter by
    :param warning_type: the warning type to optionally filter by
    """
    allowed_sort_attrs = ["dag_id", "warning_type", "message", "timestamp"]
    query = select(DagWarningModel)
    if dag_id:
        query = query.where(DagWarningModel.dag_id == dag_id)
    else:
        readable_dags = get_readable_dags()
        query = query.where(DagWarningModel.dag_id.in_(readable_dags))
    if warning_type:
        query = query.where(DagWarningModel.warning_type == warning_type)
    total_entries = get_query_count(query, session=session)
    query = apply_sorting(query=query, order_by=order_by, allowed_attrs=allowed_sort_attrs)
    dag_warnings = session.scalars(query.offset(offset).limit(limit)).all()
    return dag_warning_collection_schema.dump(
        DagWarningCollection(dag_warnings=dag_warnings, total_entries=total_entries)
    )
