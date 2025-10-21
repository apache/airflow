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

import logging

from fastapi import APIRouter, HTTPException, status

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.dags import DagStateResponse
from airflow.models.dag import DagModel

router = APIRouter()


log = logging.getLogger(__name__)


@router.get(
    "/{dag_id}/state",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
    },
)
def get_dag_state(
    dag_id: str,
    session: SessionDep,
) -> DagStateResponse:
    """Get a DAG Run State."""
    dag_model: DagModel = session.get(DagModel, dag_id)
    if not dag_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"The Dag with dag_id: `{dag_id}` was not found",
            },
        )

    is_paused = False if dag_model.is_paused is None else dag_model.is_paused

    return DagStateResponse(is_paused=is_paused)
