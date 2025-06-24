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
from uuid import UUID

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from structlog.contextvars import bind_contextvars

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_dag
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.api_fastapi.core_api.datamodels.hitl import (
    AddHITLResponsePayload,
    HITLInputRequest,
    HITLResponse,
)
from airflow.providers.standard.models import HITLResponseModel
from airflow.providers.standard.operators.hitl import HITLOperator

hitl_router = AirflowRouter(tags=["HumanInTheLoop"])

log = structlog.get_logger(__name__)


@hitl_router.post(
    "/input-requests/{task_instance_id}/response",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def write_response(
    task_instance_id: UUID,
    add_response_payload: AddHITLResponsePayload,
    user: GetUserDep,
    session: SessionDep,
) -> HITLResponse:
    """Write an HITLResponse."""
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)
    ti = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not ti:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    existing_response = session.scalar(select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str))
    if existing_response:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"HITL Response exists for task task_instance id {ti_id_str}",
        )

    hitl_response_model = HITLResponseModel(
        ti_id=ti_id_str,
        content=add_response_payload.content,
        user_id=user.get_id(),
    )
    session.add(hitl_response_model)
    session.commit()
    return HITLResponse.model_validate(hitl_response_model)


@hitl_router.get(
    "/input-requests/{task_instance_id}",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def get_hitl_input_request(
    task_instance_id: UUID,
    session: SessionDep,
) -> HITLInputRequest:
    """Get a Human-in-the-loop input request of a specific task instance."""
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)
    ti = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not ti:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )
    dag = dag_bag.get_dag(ti.dag_id)
    if not dag:
        log.error("Task Instance not found")
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag {ti.dag_id} not found")
    task = dag.get_task(ti.task_id)
    if TYPE_CHECKING:
        assert isinstance(task, HITLOperator)
    return HITLInputRequest.from_task_instance(ti, task)


# @hitl_router.get(
#     "/input-requests/{task_instance_id}/response",
#     status_code=status.HTTP_201_CREATED,
#     responses=create_openapi_http_exception_doc(
#         [
#             status.HTTP_404_NOT_FOUND,
#             status.HTTP_409_CONFLICT,
#         ]
#     ),
#     dependencies=[
#         Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
#     ],
# )
# def get_hitl_input_requests(
#     session: SessionDep,
#     readable_ti_filter: ReadableTIFilterDep,
# ) -> HITLInputRequest:
#     """Get a Human-in-the-loop input request of a specific task instance."""
#     task_instance_select, total_entries = paginated_select(
#         statement=query,
#         filters=[
#             readable_ti_filter,
#         ],
#         session=session,
#     )
#     # ti_id_str = str(task_instance_id)
#     # bind_contextvars(ti_id=ti_id_str)
#     # ti = session.scalar(select(TI).where(TI.id == ti_id_str))
#     # if not ti:
#     #     log.error("Task Instance not found")
#     #     raise HTTPException(
#     #         status_code=status.HTTP_404_NOT_FOUND,
#     #         detail={
#     #             "reason": "not_found",
#     #             "message": "Task Instance not found",
#     #         },
#     #     )
#     # task = ti.task
#     # if TYPE_CHECKING:
#     #     assert isinstance(task, HITLOperator)
#     # return HITLInputRequest(
#     #     ti_id=ti_id_str,
#     #     options=task.options,
#     #     subject=task.subject,
#     #     body=task.body,
#     #     default=task.default,
#     #     params=task.params,
#     # )
