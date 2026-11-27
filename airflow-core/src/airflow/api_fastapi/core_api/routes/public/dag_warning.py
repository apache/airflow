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
from sqlalchemy import select, tuple_
from sqlalchemy.orm import joinedload

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
    DAGWarningResponse,
)
from airflow.api_fastapi.core_api.security import (
    REDACTED_FILE_CONTENT,
    ReadableDagWarningsFilterDep,
    requires_access_dag,
)
from airflow.models.dag import DagModel
from airflow.models.dagwarning import DagWarning, DagWarningType, DagWarningTypeValue

# Every other type, including importer-defined ones, is computed for the whole source file and
# stored on each Dag the file defines, so its message can describe any of them.
PER_DAG_WARNING_TYPES = frozenset(
    {
        DagWarningType.ASSET_CONFLICT.value,
        DagWarningType.DUPLICATE_DAG_ID.value,
        DagWarningType.NONEXISTENT_POOL.value,
        DagWarningType.TASK_GROUP_CYCLE.value,
    }
)

dag_warning_router = AirflowRouter(tags=["DagWarning"])


@dag_warning_router.get(
    "/dagWarnings",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.WARNING))],
)
def list_dag_warnings(
    dag_id: Annotated[FilterParam[str | None], Depends(filter_param_factory(DagWarning.dag_id, str | None))],
    warning_type: Annotated[
        FilterParam[DagWarningTypeValue | None],
        Depends(filter_param_factory(DagWarning.warning_type, DagWarningTypeValue | None)),
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
    """Get a list of Dag warnings."""
    dag_warnings_select, total_entries = paginated_select(
        statement=select(DagWarning).options(joinedload(DagWarning.dag_model)),
        filters=[warning_type, dag_id, readable_dag_warning_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    dag_warnings = session.scalars(dag_warnings_select).all()

    # Being able to read the Dag a file-level warning is attached to does not mean being able to
    # read every Dag of that file, so its message is redacted like the source of that file.
    file_level_warnings = [
        dag_warning
        for dag_warning in dag_warnings
        if dag_warning.warning_type not in PER_DAG_WARNING_TYPES and dag_warning.dag_model.relative_fileloc
    ]
    redacted_warnings: set[DagWarning] = set()
    if file_level_warnings:
        files_with_unreadable_dags = set(
            session.execute(
                select(DagModel.bundle_name, DagModel.relative_fileloc)
                .where(
                    tuple_(DagModel.bundle_name, DagModel.relative_fileloc).in_(
                        {(w.dag_model.bundle_name, w.dag_model.relative_fileloc) for w in file_level_warnings}
                    ),
                    DagModel.dag_id.not_in(readable_dag_warning_filter.value or set()),
                )
                .distinct()
            ).tuples()
        )
        redacted_warnings = {
            w
            for w in file_level_warnings
            if (w.dag_model.bundle_name, w.dag_model.relative_fileloc) in files_with_unreadable_dags
        }

    response_warnings = []
    for dag_warning in dag_warnings:
        response_warning = DAGWarningResponse.model_validate(dag_warning)
        if dag_warning in redacted_warnings:
            response_warning.message = REDACTED_FILE_CONTENT
        response_warnings.append(response_warning)

    return DAGWarningCollectionResponse(
        dag_warnings=response_warnings,
        total_entries=total_entries,
    )
