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

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import (
    get_session,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    QueryDagIdInDagWarningFilter,
    QueryLimit,
    QueryOffset,
    QueryWarningTypeFilter,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.dag_warning import (
    DAGWarningCollectionResponse,
    DAGWarningResponse,
)
from airflow.models import DagWarning

dag_warning_router = AirflowRouter(tags=["DagWarning"])


@dag_warning_router.get("/dagWarnings", responses=create_openapi_http_exception_doc([401, 403]))
async def list_dag_warnings(
    dag_id: QueryDagIdInDagWarningFilter,
    warning_type: QueryWarningTypeFilter,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["dag_id", "warning_type", "message", "timestamp"], DagWarning).dynamic_depends()),
    ],
    session: Annotated[Session, Depends(get_session)],
) -> DAGWarningCollectionResponse:
    """Get a list of DAG warnings."""
    dag_warnings_select, total_entries = paginated_select(
        select(DagWarning), [warning_type, dag_id], order_by, offset, limit, session
    )

    dag_warnings = session.scalars(dag_warnings_select).all()

    return DAGWarningCollectionResponse(
        dag_warnings=[
            DAGWarningResponse.model_validate(dag_warning, from_attributes=True)
            for dag_warning in dag_warnings
        ],
        total_entries=total_entries,
    )
