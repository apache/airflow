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

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Security, status
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.datamodels.job import (
    JobCompleteBody,
    JobHeartbeatResponse,
    JobRegisterBody,
    JobRegisterResponse,
)
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.security import (
    SESSION_UNCHECKED_SCOPE,
    CurrentTIToken,
    ExecutionAPIRoute,
    require_auth,
)
from airflow.configuration import conf
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job, JobState
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import JobTeam

router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
)


def _get_session_job(job_id: int, token: TIToken, *, session: Session) -> Job:
    job = session.scalar(select(Job).where(Job.id == job_id, Job.session_id == token.id))
    if job is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Job {job_id} not found for this session"},
        )
    return job


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(require_auth, scopes=[SESSION_UNCHECKED_SCOPE])],
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_403_FORBIDDEN, "A requested bundle is not granted to the session"),
            (status.HTTP_409_CONFLICT, "The session already has a running Job"),
        ]
    ),
)
def register_job(body: JobRegisterBody, session: SessionDep, token=CurrentTIToken) -> JobRegisterResponse:
    """
    Register the Job of a Dag processor session.

    A session has at most one Job. Registering again is refused while that Job is alive, and otherwise
    moves the session to a new Job, so a processor restarted with the same session can carry on.
    """
    requested = token.claims.dag_bundles if body.bundle_names is None else set(body.bundle_names)
    if ungranted := requested - token.claims.dag_bundles:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={"reason": "bundle_not_granted", "message": f"Bundles not granted: {sorted(ungranted)}"},
        )
    bundle_names = sorted(requested)

    previous = session.scalar(select(Job).where(Job.session_id == token.id).with_for_update())
    if previous is not None:
        if previous.end_date is None and previous.is_alive():
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail={"reason": "job_running", "message": f"Session already has running Job {previous.id}"},
            )
        previous.session_id = None
        session.flush()

    now = timezone.utcnow()
    # A core insert: Job.__init__ would stamp the API server's host and user and fire component listeners.
    try:
        session.execute(
            insert(Job).values(
                job_type=DagProcessorJobRunner.job_type,
                state=JobState.RUNNING,
                start_date=now,
                latest_heartbeat=now,
                hostname=body.hostname,
                unixname=body.unixname,
                bundle_names=bundle_names,
                session_id=token.id,
            )
        )
    except IntegrityError:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={"reason": "job_running", "message": "Session registered another Job concurrently"},
        )
    job_id = session.scalars(select(Job.id).where(Job.session_id == token.id)).one()

    if conf.getboolean("core", "multi_team"):
        team_names = DagBundleModel.get_team_names(bundle_names, session=session)
        session.add_all(
            JobTeam(job_id=job_id, team_name=team)
            for team in sorted({team for team in team_names.values() if team})
        )
    return JobRegisterResponse(job_id=job_id)


@router.post(
    "/{job_id}/heartbeat",
    responses=create_openapi_http_exception_doc(
        [(status.HTTP_404_NOT_FOUND, "Job not found for this session")]
    ),
)
def heartbeat_job(job_id: int, session: SessionDep, token=CurrentTIToken) -> JobHeartbeatResponse:
    """Record a heartbeat and return the Job state, which tells the processor whether to stop."""
    job = _get_session_job(job_id, token, session=session)
    job.latest_heartbeat = timezone.utcnow()
    return JobHeartbeatResponse(state=JobState(job.state))


@router.post(
    "/{job_id}/complete",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=[SESSION_UNCHECKED_SCOPE])],
    responses=create_openapi_http_exception_doc(
        [(status.HTTP_404_NOT_FOUND, "Job not found for this session")]
    ),
)
def complete_job(job_id: int, body: JobCompleteBody, session: SessionDep, token=CurrentTIToken) -> None:
    """
    Record the final state of the Job, which ends its session.

    Completing an already completed Job succeeds without changing it, so a retry after a lost response is safe.
    """
    job = _get_session_job(job_id, token, session=session)
    if job.end_date is None:
        job.end_date = timezone.utcnow()
        job.state = JobState(body.state.value)
