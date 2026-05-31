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

from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.circuit_breakers import (
    TaskCircuitBreakerCollectionResponse,
    TaskCircuitBreakerResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.models.task_circuit_breaker import TaskCircuitBreaker

circuit_breakers_router = AirflowRouter(tags=["Circuit Breaker"], prefix="/dags/{dag_id}")


@circuit_breakers_router.get(
    "/tasks/{task_id}/circuitBreaker",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_circuit_breaker(
    dag_id: str,
    task_id: str,
    session: SessionDep,
) -> TaskCircuitBreakerResponse:
    """Get circuit breaker state for a task."""
    cb = session.get(TaskCircuitBreaker, (dag_id, task_id))
    if cb is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No circuit breaker record for dag_id=`{dag_id}`, task_id=`{task_id}`. "
            "The circuit has never fired.",
        )
    return cb


@circuit_breakers_router.post(
    "/tasks/{task_id}/circuitBreaker/reset",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def reset_circuit_breaker(
    dag_id: str,
    task_id: str,
    session: SessionDep,
) -> TaskCircuitBreakerResponse:
    """Manually close an open circuit breaker."""
    cb = session.get(TaskCircuitBreaker, (dag_id, task_id))
    if cb is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No circuit breaker record for dag_id=`{dag_id}`, task_id=`{task_id}`.",
        )
    cb.reset()
    session.flush()
    return cb


@circuit_breakers_router.get(
    "/circuitBreakers",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def list_circuit_breakers(
    dag_id: str,
    session: SessionDep,
) -> TaskCircuitBreakerCollectionResponse:
    """List all circuit breaker records for a Dag."""
    cbs = session.scalars(select(TaskCircuitBreaker).where(TaskCircuitBreaker.dag_id == dag_id)).all()
    return TaskCircuitBreakerCollectionResponse(
        circuit_breakers=list(cbs),
        total_entries=len(cbs),
    )
