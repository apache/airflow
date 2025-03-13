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

from typing import Annotated

from fastapi import Depends
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import (
    SessionDep,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    SortParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_warning import (
    DAGWarningCollectionResponse,
)
from airflow.api_fastapi.core_api.security import ReadableDagWarningsFilterDep, requires_access_dag
from airflow.models.dagwarning import DagWarning, DagWarningType

dag_warning_router = AirflowRouter(tags=["DagWarning"])


@dag_warning_router.get(
    "/dagWarnings",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.WARNING))],
)
def list_dag_warnings(
    dag_id: Annotated[FilterParam[str | None], Depends(filter_param_factory(DagWarning.dag_id, str | None))],
    warning_type: Annotated[
        FilterParam[DagWarningType | None],
        Depends(filter_param_factory(DagWarning.warning_type, DagWarningType | None)),
    ],
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["dag_id", "warning_type", "message", "timestamp"], DagWarning).dynamic_depends()),
    ],
    readable_dag_warning_filter: ReadableDagWarningsFilterDep,
    session: SessionDep,
) -> DAGWarningCollectionResponse:
    """Get a list of DAG warnings."""
    dag_warnings_select, total_entries = paginated_select(
        statement=select(DagWarning),
        filters=[warning_type, dag_id, readable_dag_warning_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    dag_warnings = session.scalars(dag_warnings_select)

    return DAGWarningCollectionResponse(
        dag_warnings=dag_warnings,
        total_entries=total_entries,
    )
