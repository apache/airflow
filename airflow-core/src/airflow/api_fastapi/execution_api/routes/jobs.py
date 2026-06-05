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
Execution API routes for ``Job`` registration and liveness (AIP-92).

A DB-free triggerer has no metadata-DB access of its own, but trigger assignment in
``Trigger.assign_unassigned`` keys off a live ``Job`` row (it skips triggerers whose
heartbeat is stale). These endpoints let such a triggerer register a ``Job`` over HTTP
and keep it alive, so its triggers are not reassigned to other triggerers.
"""

from __future__ import annotations

import logging

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, status

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.job import JobRegisterBody, JobRegisterResponse
from airflow.jobs.job import Job, JobState

router = VersionedAPIRouter()

log = logging.getLogger(__name__)


@router.post("", status_code=status.HTTP_201_CREATED)
def register_job(body: JobRegisterBody, session: SessionDep) -> JobRegisterResponse:
    """Create a started + heartbeated ``Job`` row and return its id."""
    # ``Job.__init__`` already stamps hostname/start_date/latest_heartbeat/unixname; we only
    # need to mark it RUNNING (mirrors ``Job.prepare_for_execution``) so it counts as alive.
    job = Job(job_type=body.job_type, state=JobState.RUNNING)
    # ``Job.__init__`` records the api-server's hostname; overwrite it with the registering
    # process's hostname so log retrieval (FileTaskHandler) reaches the right host.
    job.hostname = body.hostname
    session.add(job)
    session.flush()
    return JobRegisterResponse(job_id=job.id)


@router.post("/{job_id}/heartbeat", status_code=status.HTTP_204_NO_CONTENT)
def heartbeat_job(job_id: int, session: SessionDep) -> None:
    """Record a fresh heartbeat for the given ``Job`` so it stays alive."""
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")
    job.latest_heartbeat = timezone.utcnow()
    job.end_date = None


@router.post("/{job_id}/complete", status_code=status.HTTP_204_NO_CONTENT)
def complete_job(job_id: int, session: SessionDep) -> None:
    """Mark the given ``Job`` finished by stamping its end date (mirrors ``Job.complete_execution``)."""
    job = session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")
    job.end_date = timezone.utcnow()
