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

import structlog
from cadwyn import VersionedAPIRouter
from fastapi import status
from pydantic import TypeAdapter

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.triggerer import (
    NextTriggersBody,
    NextTriggersResponse,
    TriggerDetail,
    TriggerEventBody,
    TriggerFailureBody,
)
from airflow.configuration import conf
from airflow.executors.workloads.task import TaskInstanceDTO
from airflow.models.trigger import Trigger
from airflow.triggers.base import DiscrimatedTriggerEvent
from airflow.utils.helpers import log_filename_template_renderer

log = structlog.get_logger(__name__)

_trigger_event_adapter: TypeAdapter[DiscrimatedTriggerEvent] = TypeAdapter(DiscrimatedTriggerEvent)

router = VersionedAPIRouter(prefix="/triggerer")
trigger_router = VersionedAPIRouter(prefix="/triggers")


@router.post(
    "/{triggerer_id}/next-triggers",
    status_code=status.HTTP_200_OK,
    summary="Assign and fetch next triggers for a triggerer instance",
)
def get_next_triggers(
    triggerer_id: int,
    body: NextTriggersBody,
    session: SessionDep,
) -> NextTriggersResponse:
    health_check_threshold = conf.getint("triggerer", "triggerer_health_check_threshold")

    Trigger.assign_unassigned(triggerer_id, body.capacity, health_check_threshold, queues=body.queues)
    Trigger.clean_unused()

    ids = Trigger.ids_for_triggerer(triggerer_id, queues=body.queues)
    triggers = Trigger.bulk_fetch(set(ids))
    non_task_ids = list(Trigger.fetch_trigger_ids_with_non_task_associations())

    render_log_fname = log_filename_template_renderer()
    trigger_details: list[TriggerDetail] = []

    for trigger_id, trigger_orm in triggers.items():
        ti_dto: TaskInstanceDTO | None = None
        log_path: str | None = None
        timeout_after = None

        if trigger_orm.task_instance:
            ti = trigger_orm.task_instance
            if not ti.dag_version_id:
                log.warning(
                    "TaskInstance has no dag_version_id, skipping",
                    trigger_id=trigger_id,
                    ti_id=ti.id,
                )
                continue
            ti_dto = TaskInstanceDTO.model_validate(ti, from_attributes=True)
            log_path = render_log_fname(ti=ti)
            timeout_after = ti.trigger_timeout

        trigger_details.append(
            TriggerDetail(
                id=trigger_id,
                classpath=trigger_orm.classpath,
                encrypted_kwargs=trigger_orm.encrypted_kwargs,
                task_instance=ti_dto,
                log_path=log_path,
                timeout_after=timeout_after,
            )
        )

    return NextTriggersResponse(
        triggers=trigger_details,
        trigger_ids_with_non_task_associations=non_task_ids,
    )


@trigger_router.post(
    "/{trigger_id}/event",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Submit a trigger event to resume a deferred task instance",
)
def submit_trigger_event(
    trigger_id: int,
    body: TriggerEventBody,
    session: SessionDep,
) -> None:
    log.debug("Submitting trigger event", trigger_id=trigger_id)
    event = _trigger_event_adapter.validate_python(body.event)
    Trigger.submit_event(trigger_id=trigger_id, event=event)


@trigger_router.post(
    "/{trigger_id}/failure",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Report that a trigger failed without firing an event",
)
def submit_trigger_failure(
    trigger_id: int,
    body: TriggerFailureBody,
    session: SessionDep,
) -> None:
    log.debug("Submitting trigger failure", trigger_id=trigger_id)
    Trigger.submit_failure(trigger_id=trigger_id, exc=body.error)
