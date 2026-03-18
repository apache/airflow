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

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.dags import DagResponse
from airflow.models.dag import DagModel

router = APIRouter()


@router.get(
    "/{dag_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
    },
)
def get_dag(
    dag_id: str,
    session: SessionDep,
) -> DagResponse:
    """Get a DAG."""
    dag_model: DagModel | None = session.get(DagModel, dag_id)
    if not dag_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"The Dag with dag_id: `{dag_id}` was not found",
            },
        )

    return DagResponse(
        dag_id=dag_model.dag_id,
        is_paused=dag_model.is_paused,
        bundle_name=dag_model.bundle_name,
        bundle_version=dag_model.bundle_version,
        relative_fileloc=dag_model.relative_fileloc,
        owners=dag_model.owners,
        tags=sorted(tag.name for tag in dag_model.tags),
        next_dagrun=dag_model.next_dagrun,
    )
