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

from typing import TYPE_CHECKING, cast

import svcs
from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Security, status
from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.dag_processor_tokens import (
    ExpiredDagProcessorToken,
    generate_dag_parse_token,
    generate_dag_processor_token,
)
from airflow.api_fastapi.execution_api.datamodels.job import (
    DagParseTokenBody,
    DagParseTokenResponse,
    JobCompleteBody,
    JobHeartbeatResponse,
    JobRegisterBody,
    JobRegisterResponse,
)
from airflow.api_fastapi.execution_api.datamodels.token import DagProcessorSessionToken, DagProcessorToken
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.security import (
    JOB_UNCHECKED_SCOPE,
    CurrentDagProcessorSessionToken,
    CurrentDagProcessorToken,
    ExecutionAPIRoute,
    require_auth,
)
from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import get_configured_bundle_team_names
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job, JobState
from airflow.models.team import JobTeam

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.engine import CursorResult
    from sqlalchemy.orm import Session

router = VersionedAPIRouter(route_class=ExecutionAPIRoute)

_JOB_NOT_FOUND = create_openapi_http_exception_doc(
    [(status.HTTP_404_NOT_FOUND, "Job not found for this token")]
)


class _RegistrationRace(Exception):
    """The Job insert lost to a concurrent registration of the same session or registration id."""


def _get_team_names(bundle_names: list[str]) -> list[str]:
    # From config like ``airflow dag-processor``: bundle rows may not be synced when a processor registers.
    if not conf.getboolean("core", "multi_team"):
        return []
    configured = get_configured_bundle_team_names()
    return sorted({team for name in bundle_names if (team := configured.get(name))})


def _expired_credential(error: ExpiredDagProcessorToken) -> HTTPException:
    return HTTPException(
        status.HTTP_403_FORBIDDEN, detail={"reason": "credential_expired", "message": str(error)}
    )


def _check_token_job(job_id: int, token: DagProcessorToken) -> None:
    if job_id != token.claims.job_id:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Job {job_id} not found for this token"},
        )


def _get_registered_job(registration_id: UUID, *, session: Session) -> Job | None:
    return session.scalar(select(Job).where(Job.registration_id == registration_id).with_for_update())


def _build_registration_response(
    job_id: int, bundle_names: list[str], token: DagProcessorSessionToken, services: svcs.Container
) -> JobRegisterResponse:
    try:
        issued = generate_dag_processor_token(
            services.get(JWTGenerator),
            session_id=token.id,
            job_id=job_id,
            bundle_names=bundle_names,
            session_expiry=token.claims.exp,
        )
    except ExpiredDagProcessorToken as error:
        raise _expired_credential(error) from error
    return JobRegisterResponse(job_id=job_id, token=issued.token, expires_in=issued.expires_in)


def _resume_registration(
    job: Job,
    body: JobRegisterBody,
    bundle_names: list[str],
    token: DagProcessorSessionToken,
    services: svcs.Container,
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
    return _build_registration_response(job.id, bundle_names, token, services)


@router.post(
    "/jobs",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Security(require_auth, scopes=["token:dag_processor_session"])],
    responses=create_openapi_http_exception_doc(
        [
            (
                status.HTTP_403_FORBIDDEN,
                "A requested bundle is not granted to the session, or its credential has expired",
            ),
            (
                status.HTTP_409_CONFLICT,
                "The registration has ended, conflicts with an earlier one, or another process's Job is running",
            ),
        ]
    ),
)
def register_job(
    body: JobRegisterBody,
    session: SessionDep,
    token: DagProcessorSessionToken = CurrentDagProcessorSessionToken,
    services: svcs.Container = DepContainer,
) -> JobRegisterResponse:
    """
    Register the Job of a Dag processor session in exchange for its management credential.

    A registration creates one Job. Repeating it while that Job is open returns the Job with a fresh token;
    once the Job completes or is replaced, the registration is refused for good. A new registration is
    refused while the session's Job is alive, and otherwise ends and replaces it, which ends every token
    issued for the replaced Job.
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

    team_names = _get_team_names(bundle_names)
    now = timezone.utcnow()
    previous = session.scalar(select(Job).where(Job.session_id == token.id).with_for_update())
    if previous is not None:
        if previous.end_date is None and previous.is_alive():
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail={"reason": "job_running", "message": f"Session already has running Job {previous.id}"},
            )
        if previous.end_date is None:
            previous.state = JobState.FAILED
            previous.end_date = now
        previous.session_id = None
        session.flush()

    try:
        with session.begin_nested():
            try:
                # A core insert: Job.__init__ would stamp the API server's host and user and fire listeners.
                inserted = session.execute(
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
            except IntegrityError as error:
                raise _RegistrationRace from error
            job_id = cast("CursorResult", inserted).inserted_primary_key[0]  # type: ignore[index]
            session.add_all(JobTeam(job_id=job_id, team_name=team) for team in team_names)
            session.flush()
            # Everything that can fail runs before the savepoint is released: under SQLite's legacy
            # transaction control the release commits, and a failure after it would keep a half-registered Job.
            response = _build_registration_response(job_id, bundle_names, token, services)
    except _RegistrationRace:
        # A retry sent before the original request finished can lose the race to it; resume the winner.
        if registered := _get_registered_job(body.registration_id, session=session):
            return _resume_registration(registered, body, bundle_names, token, services)
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={"reason": "job_running", "message": "Session registered another Job concurrently"},
        )
    return response


@router.post(
    "/jobs/{job_id}/heartbeat",
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
    responses=_JOB_NOT_FOUND,
)
def heartbeat_job(
    job_id: int, session: SessionDep, token: DagProcessorToken = CurrentDagProcessorToken
) -> JobHeartbeatResponse:
    """Record a heartbeat and return the Job state, which tells the processor whether to stop."""
    _check_token_job(job_id, token)
    job = session.scalars(select(Job).where(Job.id == job_id)).one()
    job.latest_heartbeat = timezone.utcnow()
    return JobHeartbeatResponse(state=JobState(job.state))


@router.post(
    "/jobs/{job_id}/complete",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Security(require_auth, scopes=["token:dag_processor", JOB_UNCHECKED_SCOPE])],
    responses=_JOB_NOT_FOUND,
)
def complete_job(
    job_id: int,
    body: JobCompleteBody,
    session: SessionDep,
    token: DagProcessorToken = CurrentDagProcessorToken,
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
    "/jobs/{job_id}/parse-token",
    dependencies=[Security(require_auth, scopes=["token:dag_processor"])],
    responses=_JOB_NOT_FOUND,
)
def exchange_parse_token(
    job_id: int,
    body: DagParseTokenBody,
    token: DagProcessorToken = CurrentDagProcessorToken,
    services: svcs.Container = DepContainer,
) -> DagParseTokenResponse:
    """Bind runtime access to the bundle and file selected by the trusted processor manager."""
    _check_token_job(job_id, token)
    if body.bundle_name not in token.claims.dag_bundles:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={"reason": "bundle_not_granted", "message": f"Bundle not granted: {body.bundle_name}"},
        )
    try:
        issued = generate_dag_parse_token(
            services.get(JWTGenerator),
            session_id=token.id,
            job_id=job_id,
            attempt_id=body.attempt_id,
            bundle_name=body.bundle_name,
            relative_fileloc=body.relative_fileloc,
            processor_expiry=token.claims.exp,
        )
    except ExpiredDagProcessorToken as error:
        raise _expired_credential(error) from error
    return DagParseTokenResponse(token=issued.token, expires_in=issued.expires_in)
