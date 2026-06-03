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
"""
Execution API routes for the triggerer (AIP-92).

These endpoints let a DB-free triggerer orchestrate over HTTP instead of accessing the
metadata database directly: claim triggers, fetch the corresponding runnable workloads,
and report events/failures/cleanup back to the database.
"""

from __future__ import annotations

import logging

from cadwyn import VersionedAPIRouter
from fastapi import status

from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.trigger import (
    TriggerEventBody,
    TriggerFailureBody,
    TriggerIdsResponse,
    TriggerLoadBody,
    TriggerWorkloadsBody,
    TriggerWorkloadsResponse,
)
from airflow.jobs.triggerer_workloads import build_run_trigger_workloads
from airflow.models.trigger import Trigger
from airflow.utils.helpers import log_filename_template_renderer

router = VersionedAPIRouter()

log = logging.getLogger(__name__)


@router.post("/load", status_code=status.HTTP_200_OK)
def load_triggers(body: TriggerLoadBody, session: SessionDep) -> TriggerIdsResponse:
    """Assign unassigned triggers to the triggerer and return its assigned trigger IDs."""
    queues = set(body.queues) if body.queues else None
    Trigger.assign_unassigned(
        body.triggerer_id,
        body.capacity,
        body.health_check_threshold,
        queues=queues,
        team_name=body.team_name,
        session=session,
    )
    trigger_ids = Trigger.ids_for_triggerer(
        body.triggerer_id,
        queues=queues,
        team_name=body.team_name,
        session=session,
    )
    return TriggerIdsResponse(trigger_ids=trigger_ids)


@router.post("/workloads", status_code=status.HTTP_200_OK)
def get_trigger_workloads(
    body: TriggerWorkloadsBody,
    session: SessionDep,
    dag_bag: DagBagDep,
) -> TriggerWorkloadsResponse:
    """Build the runnable ``RunTrigger`` workloads for the given trigger IDs."""
    render_log_fname = log_filename_template_renderer()
    built = build_run_trigger_workloads(
        set(body.trigger_ids),
        dag_bag=dag_bag,
        render_log_fname=render_log_fname,
        session=session,
    )
    return TriggerWorkloadsResponse(workloads=[w.model_dump(mode="json") for w in built])


@router.post("/{trigger_id}/event", status_code=status.HTTP_204_NO_CONTENT)
def submit_trigger_event(trigger_id: int, body: TriggerEventBody, session: SessionDep) -> None:
    """Submit an event for a trigger, resuming any deferred task instances waiting on it."""
    # ``body.payload`` is a serde (``airflow.sdk.serde.serialize``) output supplied by an
    # execution-scoped worker. The server stores it WITHOUT deserializing -- it is spliced
    # straight into the task instance's next_kwargs, and the worker deserializes it on resume.
    # This avoids running ``import_string`` on a worker-controlled body in the trusted api-server.
    Trigger.submit_event_serialized(trigger_id, body.payload, session=session)


@router.post("/{trigger_id}/failure", status_code=status.HTTP_204_NO_CONTENT)
def submit_trigger_failure(trigger_id: int, body: TriggerFailureBody, session: SessionDep) -> None:
    """Submit a failure for a trigger, re-scheduling dependent task instances to fail."""
    Trigger.submit_failure(trigger_id, body.error, session=session)


@router.post("/cleanup", status_code=status.HTTP_204_NO_CONTENT)
def cleanup_triggers(session: SessionDep) -> None:
    """Delete triggers that no longer have any task, asset, or callback depending on them."""
    Trigger.clean_unused(session=session)
