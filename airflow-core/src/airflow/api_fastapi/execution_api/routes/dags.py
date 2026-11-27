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

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import AsyncSessionDep
from airflow.api_fastapi.execution_api.datamodels.dags import DagResponse
from airflow.models.dag import DagModel, DagTag

router = APIRouter()


@router.get(
    "/{dag_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
    },
)
async def get_dag(
    dag_id: str,
    session: AsyncSessionDep,
) -> DagResponse:
    """Get a DAG."""
    dag_row = (
        await session.execute(
            select(
                DagModel.dag_id,
                DagModel.is_paused,
                DagModel.bundle_name,
                DagModel.bundle_version,
                DagModel.relative_fileloc,
                DagModel.owners,
                DagModel.next_dagrun,
            ).where(DagModel.dag_id == dag_id)
        )
    ).one_or_none()
    if dag_row is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"The Dag with dag_id: `{dag_id}` was not found",
            },
        )

    tags = (await session.scalars(select(DagTag.name).where(DagTag.dag_id == dag_id))).all()

    return DagResponse(
        dag_id=dag_row.dag_id,
        is_paused=dag_row.is_paused,
        bundle_name=dag_row.bundle_name,
        bundle_version=dag_row.bundle_version,
        relative_fileloc=dag_row.relative_fileloc,
        owners=dag_row.owners,
        tags=sorted(tags),
        next_dagrun=dag_row.next_dagrun,
    )
