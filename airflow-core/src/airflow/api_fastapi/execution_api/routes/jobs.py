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

import svcs
from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Security, status
from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.datamodels.job import (
    DagParseTokenBody,
    DagParseTokenResponse,
    JobCompleteBody,
    JobHeartbeatResponse,
    JobRegisterBody,
    JobRegisterResponse,
)
from airflow.api_fastapi.execution_api.datamodels.token import ExecutionToken
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.security import (
    JOB_UNCHECKED_SCOPE,
    CurrentExecutionToken,
    ExecutionAPIRoute,
    require_auth,
)
from airflow.configuration import conf
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job, JobState
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import JobTeam

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.orm import Session

router = VersionedAPIRouter(route_class=ExecutionAPIRoute)

_JOB_NOT_FOUND = create_openapi_http_exception_doc(
    [(status.HTTP_404_NOT_FOUND, "Job not found for this token")]
)


def _issue_job_token(
    services: svcs.Container, token: ExecutionToken, job_id: int, bundle_names: list[str]
) -> str:
    generator: JWTGenerator = services.get(JWTGenerator)
    # Never outlive the session token, so that provisioning, by no longer renewing it, still ends access.
    remaining = (token.claims.exp or 0) - timezone.utcnow().timestamp()
    return generator.generate(
        extras={
            "sub": str(token.id),
            "scope": "dag_processor",
            "dag_bundles": bundle_names,
            "job_id": job_id,
        },
        valid_for=min(generator.valid_for, remaining),
    )


def _check_token_job(job_id: int, token: ExecutionToken) -> None:
    if job_id != token.claims.job_id:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Job {job_id} not found for this token"},
        )


def _get_registered_job(registration_id: UUID, *, session: Session) -> Job | None:
    return session.scalar(select(Job).where(Job.registration_id == registration_id).with_for_update())


def _resume_registration(
    job: Job, body: JobRegisterBody, bundle_names: list[str], token: ExecutionToken, services: svcs.Container
) -> JobRegisterResponse:
    """Return the Job an earlier registration created, with a fresh token, if that Job is still the session's."""
    if job.session_id != token.id or job.end_date is not None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={
                "reason": "registration_retired",
                "message": f"Registration {body.registration_id} has ended; a restarted processor uses a new one",
            },
        )
    if (job.hostname, job.unixname, job.bundle_names) != (body.hostname, body.unixname, bundle_names):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={
                "reason": "registration_conflict",
                "message": f"Registration {body.registration_id} was made with different details",
            },
        )
    return JobRegisterResponse(job_id=job.id, token=_issue_job_token(services, token, job.id, bundle_names))


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(require_auth, scopes=["token:dag_processor_session"])],
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_403_FORBIDDEN, "A requested bundle is not granted to the session"),
            (
                status.HTTP_409_CONFLICT,
                "The registration has ended, conflicts with an earlier one, or another process's Job is running",
            ),
        ]
    ),
)
def register_job(
    body: JobRegisterBody, session: SessionDep, token=CurrentExecutionToken, services=DepContainer
) -> JobRegisterResponse:
    """
    Register the Job of a Dag processor session in exchange for its management credential.

    A registration creates one Job. Repeating it while that Job is open returns the Job with a fresh token;
    once the Job completes or is replaced, the registration is refused for good. A new registration is
    refused while the session's Job is alive, and otherwise replaces it, which ends every token issued for
    the replaced Job.
    """
    requested = token.claims.dag_bundles if body.bundle_names is None else set(body.bundle_names)
    if ungranted := requested - token.claims.dag_bundles:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={"reason": "bundle_not_granted", "message": f"Bundles not granted: {sorted(ungranted)}"},
        )
    bundle_names = sorted(requested)

    if registered := _get_registered_job(body.registration_id, session=session):
        return _resume_registration(registered, body, bundle_names, token, services)

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
    try:
        with session.begin_nested():
            # A core insert: Job.__init__ would stamp the API server's host and user and fire listeners.
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
                    registration_id=body.registration_id,
                )
            )
    except IntegrityError:
        # A retry sent before the original request finished can lose the race to it; resume the winner.
        if registered := _get_registered_job(body.registration_id, session=session):
            return _resume_registration(registered, body, bundle_names, token, services)
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
    return JobRegisterResponse(job_id=job_id, token=_issue_job_token(services, token, job_id, bundle_names))


@router.post(
    "/{job_id}/heartbeat",
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
    responses=_JOB_NOT_FOUND,
)
def heartbeat_job(job_id: int, session: SessionDep, token=CurrentExecutionToken) -> JobHeartbeatResponse:
    """Record a heartbeat and return the Job state, which tells the processor whether to stop."""
    _check_token_job(job_id, token)
    job = session.scalars(select(Job).where(Job.id == job_id)).one()
    job.latest_heartbeat = timezone.utcnow()
    return JobHeartbeatResponse(state=JobState(job.state))


@router.post(
    "/{job_id}/complete",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:dag_processor", JOB_UNCHECKED_SCOPE])],
    responses=_JOB_NOT_FOUND,
)
def complete_job(
    job_id: int, body: JobCompleteBody, session: SessionDep, token=CurrentExecutionToken
) -> None:
    """
    Record the final state of the Job, which ends every token issued for it.

    The first completion wins. Repeating it succeeds without changing the Job, so a retry after a lost
    response is safe.
    """
    _check_token_job(job_id, token)
    owned_by_token = (Job.id == job_id) & (Job.session_id == token.id)
    # One conditional statement, so overlapping completions cannot overwrite the first accepted outcome.
    session.execute(
        update(Job)
        .where(owned_by_token, Job.end_date.is_(None))
        .values(end_date=timezone.utcnow(), state=JobState(body.state.value))
    )
    if session.scalar(select(Job.id).where(owned_by_token)) is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Job {job_id} not found for this token"},
        )


@router.post(
    "/{job_id}/parse-token",
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
    responses=_JOB_NOT_FOUND,
)
def exchange_parse_token(
    job_id: int, body: DagParseTokenBody, token=CurrentExecutionToken, services=DepContainer
) -> DagParseTokenResponse:
    """Bind runtime access to the bundle and file selected by the trusted processor manager."""
    _check_token_job(job_id, token)
    if body.bundle_name not in token.claims.dag_bundles:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Token is not granted this Dag bundle")
    generator: JWTGenerator = services.get(JWTGenerator)
    remaining = (token.claims.exp or 0) - timezone.utcnow().timestamp()
    if remaining <= 0:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Processor credential has expired")
    return DagParseTokenResponse(
        token=generator.generate(
            extras={
                "sub": str(body.attempt_id),
                "scope": "dag_parse",
                "session_id": str(token.id),
                "job_id": job_id,
                "dag_bundles": [body.bundle_name],
                "relative_fileloc": body.relative_fileloc,
            },
            valid_for=min(generator.valid_for, remaining),
        )
    )
