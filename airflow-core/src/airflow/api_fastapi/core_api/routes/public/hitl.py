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

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryHITLDetailBodySearch,
    QueryHITLDetailDagIdFilter,
    QueryHITLDetailDagIdPatternSearch,
    QueryHITLDetailDagRunIdFilter,
    QueryHITLDetailRespondedUserIdFilter,
    QueryHITLDetailRespondedUserNameFilter,
    QueryHITLDetailResponseReceivedFilter,
    QueryHITLDetailSubjectSearch,
    QueryHITLDetailTaskIdFilter,
    QueryHITLDetailTaskIdPatternSearch,
    QueryLimit,
    QueryOffset,
    QueryTIStateFilter,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLDetail,
    HITLDetailCollection,
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, ReadableTIFilterDep, requires_access_dag
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.models.hitl import HITLDetail as HITLDetailModel
from airflow.models.taskinstance import TaskInstance as TI

hitl_router = AirflowRouter(tags=["HumanInTheLoop"], prefix="/hitlDetails")

log = structlog.get_logger(__name__)


def _get_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int,
) -> TI:
    query = select(TI).where(
        TI.dag_id == dag_id,
        TI.run_id == dag_run_id,
        TI.task_id == task_id,
    )

    if map_index is not None:
        query = query.where(TI.map_index == map_index)

    task_instance = session.scalar(query)
    if task_instance is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, "
                f"task_id: `{task_id}` and map_index: `{map_index}` was not found"
            ),
        )

    return task_instance


def _update_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
    map_index: int,
) -> HITLDetailResponse:
    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )
    ti_id_str = str(task_instance.id)
    hitl_detail_model = session.scalar(select(HITLDetailModel).where(HITLDetailModel.ti_id == ti_id_str))
    if not hitl_detail_model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Human-in-the-loop detail does not exist for Task Instance with id {ti_id_str}",
        )

    if hitl_detail_model.response_received:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Human-in-the-loop detail has already been updated for Task Instance with id {ti_id_str} "
                "and is not allowed to write again."
            ),
        )

    user_id = user.get_id()
    user_name = user.get_name()
    if hitl_detail_model.respondents:
        if isinstance(user_id, int):
            # FabAuthManager (ab_user) store user id as integer, but common interface is string type
            user_id = str(user_id)
        if user_id not in hitl_detail_model.respondents:
            log.error("User=%s (id=%s) is not a respondent for the task", user_name, user_id)
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                f"User={user_name} (id={user_id}) is not a respondent for the task.",
            )

    hitl_detail_model.responded_user_id = user_id
    hitl_detail_model.responded_user_name = user_name
    hitl_detail_model.response_at = timezone.utcnow()
    hitl_detail_model.chosen_options = update_hitl_detail_payload.chosen_options
    hitl_detail_model.params_input = update_hitl_detail_payload.params_input
    session.add(hitl_detail_model)
    session.commit()
    return HITLDetailResponse.model_validate(hitl_detail_model)


def _get_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )

    ti_id_str = str(task_instance.id)
    hitl_detail_model = session.scalar(
        select(HITLDetailModel)
        .where(HITLDetailModel.ti_id == ti_id_str)
        .options(joinedload(HITLDetailModel.task_instance))
    )
    if not hitl_detail_model:
        log.error("Human-in-the-loop detail does not exist for Task Instance with id %s", ti_id_str)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Human-in-the-loop detail does not exist for Task Instance with id {ti_id_str}",
        )
    return HITLDetail.model_validate(hitl_detail_model)


@hitl_router.patch(
    "/{dag_id}/{dag_run_id}/{task_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.HITL_DETAIL)),
        Depends(action_logging()),
    ],
)
def update_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
    map_index: int = -1,
) -> HITLDetailResponse:
    """Update a Human-in-the-loop detail."""
    return _update_hitl_detail(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        update_hitl_detail_payload=update_hitl_detail_payload,
        user=user,
        map_index=map_index,
    )


@hitl_router.get(
    "/{dag_id}/{dag_run_id}/{task_id}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.HITL_DETAIL))],
)
def get_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int = -1,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    return _get_hitl_detail(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )


@hitl_router.get(
    "/",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.HITL_DETAIL))],
)
def get_hitl_details(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "ti_id",
                    "subject",
                    "response_at",
                    "task_instance.dag_id",
                    "task_instance.run_id",
                ],
                HITLDetailModel,
                to_replace={
                    "dag_id": TI.dag_id,
                    "run_id": TI.run_id,
                },
            ).dynamic_depends(),
        ),
    ],
    session: SessionDep,
    # ti related filter
    readable_ti_filter: ReadableTIFilterDep,
    dag_id: QueryHITLDetailDagIdFilter,
    dag_id_pattern: QueryHITLDetailDagIdPatternSearch,
    dag_run_id: QueryHITLDetailDagRunIdFilter,
    task_id: QueryHITLDetailTaskIdFilter,
    task_id_pattern: QueryHITLDetailTaskIdPatternSearch,
    ti_state: QueryTIStateFilter,
    # hitl detail related filter
    response_received: QueryHITLDetailResponseReceivedFilter,
    responded_user_id: QueryHITLDetailRespondedUserIdFilter,
    responded_user_name: QueryHITLDetailRespondedUserNameFilter,
    subject_patten: QueryHITLDetailSubjectSearch,
    body_patten: QueryHITLDetailBodySearch,
) -> HITLDetailCollection:
    """Get Human-in-the-loop details."""
    query = (
        select(HITLDetailModel)
        .join(TI, HITLDetailModel.ti_id == TI.id)
        .options(joinedload(HITLDetailModel.task_instance))
    )
    hitl_detail_select, total_entries = paginated_select(
        statement=query,
        filters=[
            # ti related filter
            readable_ti_filter,
            dag_id,
            dag_id_pattern,
            dag_run_id,
            task_id,
            task_id_pattern,
            ti_state,
            # hitl detail related filter
            response_received,
            responded_user_id,
            responded_user_name,
            subject_patten,
            body_patten,
        ],
        offset=offset,
        limit=limit,
        order_by=order_by,
        session=session,
    )

    hitl_details = session.scalars(hitl_detail_select)

    return HITLDetailCollection(
        hitl_details=hitl_details,
        total_entries=total_entries,
    )
