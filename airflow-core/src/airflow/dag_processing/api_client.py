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
"""Execution API client owned by a standalone Dag processor's manager loop."""

from __future__ import annotations

import math
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic
from typing import Any
from uuid import UUID

import httpx
import jwt
import structlog
from uuid6 import uuid7

from airflow.api_fastapi.execution_api.datamodels.job import (
    DagParseTokenBody,
    DagParseTokenResponse,
    JobCompleteBody,
    JobHeartbeatResponse,
    JobRegisterBody,
    JobRegisterResponse,
    JobState,
    TerminalJobState,
)
from airflow.api_fastapi.execution_api.versions import bundle
from airflow.sdk.api.client import BearerAuth, Client
from airflow.sdk.execution_time.comms import GetConnection, GetVariable, MaskSecret
from airflow.sdk.execution_time.request_handlers import handle_get_connection, handle_get_variable

log = structlog.get_logger(__name__)


@dataclass
class DagParseContext:
    """Credential cache owned by one parsing subprocess's supervisor."""

    request: DagParseTokenBody
    token: str | None = field(default=None, repr=False)
    expires_at: float = 0.0
    renew_at: float = 0.0


# Core owns the control contracts, so it speaks the version its datamodels match, as the Task SDK does
# for its own; runtime requests keep the SDK's negotiated version.
_JOB_API_HEADERS = {"Airflow-API-Version": bundle.version_values[0]}


class DagProcessorRegistrationRetired(RuntimeError):
    """The manager must restart; the Job ended or no longer belongs to this session."""


class DagProcessorJobAlreadyRunning(RuntimeError):
    """The session's previous Job has not completed or stopped heartbeating yet."""


def _get_error_reason(error: httpx.HTTPStatusError) -> str | None:
    try:
        payload = error.response.json()
    except ValueError:
        return None
    detail = payload.get("detail") if isinstance(payload, dict) else None
    return detail.get("reason") if isinstance(detail, dict) else None


class DagProcessorAPIClient(Client):
    """
    Register one processor process and use its Job token for subsequent API requests.

    Create one client per process start. Token rotation and registration retries retain that process's
    registration ID. Heartbeats return the server's Job state so the manager can stop its importers on
    ``RESTARTING``. Closing this client closes the HTTP connection pool; call ``complete_job`` explicitly
    to record the outcome before closing it.

    Wrap subprocess requests in ``use_parse`` and manager secret lookups in ``use_bundle``. Early renewal is
    best effort; ``restart_required`` tells the manager to drain and restart after registration retirement,
    or once a request finds the Job completed or replaced, which raises ``DagProcessorRegistrationRetired``.
    The existing token remains usable until expiry, subject to the API's ownership checks.

    After completion is attempted, only retries of that same completion are allowed. An expired token
    can be renewed if the Job is still open. Retirement raises ``DagProcessorRegistrationRetired``;
    it does not confirm which outcome was saved or whether another process replaced the Job.
    """

    def __init__(
        self,
        *,
        base_url: str,
        token_file: str | Path,
        hostname: str,
        unixname: str | None = None,
        bundle_names: list[str] | None = None,
        token_reload_interval: float = 30.0,
        **kwargs: Any,
    ):
        if not math.isfinite(token_reload_interval) or token_reload_interval < 0:
            raise ValueError("token_reload_interval must be finite and nonnegative")
        self._registration = JobRegisterBody(
            registration_id=uuid7(),
            hostname=hostname,
            unixname=unixname,
            bundle_names=bundle_names,
        )
        self._token_file = Path(token_file)
        self._token_reload_interval = token_reload_interval
        self._session_token: str | None = None
        self._reload_at = 0.0
        self._registered_with: str | None = None
        self._renew_at = 0.0
        self._expires_at = 0.0
        self._retry_renewal_at = 0.0
        self._restart_required = False
        self._bundle_context: ContextVar[str | None] = ContextVar("dag_processor_bundle", default=None)
        self._parse_context: ContextVar[DagParseContext | None] = ContextVar("dag_parse", default=None)
        self._job_id: int | None = None
        self._completion_state: TerminalJobState | None = None
        self._completed = False
        super().__init__(base_url=base_url, token="", **kwargs)

    @property
    def registration_id(self) -> UUID:
        return self._registration.registration_id

    @property
    def job_id(self) -> int | None:
        return self._job_id

    @property
    def restart_required(self) -> bool:
        return self._restart_required

    @contextmanager
    def use_bundle(self, bundle_name: str) -> Iterator[DagProcessorAPIClient]:
        """Select the bundle for one subprocess request, restoring the previous context afterwards."""
        if not bundle_name:
            raise ValueError("A Dag processor request needs a nonempty bundle name")
        context = self._bundle_context.set(bundle_name)
        try:
            yield self
        finally:
            self._bundle_context.reset(context)

    @contextmanager
    def use_parse(self, context: DagParseContext) -> Iterator[DagProcessorAPIClient]:
        """Answer a subprocess with its own credential; restore the manager context afterwards."""
        selected = self._parse_context.set(context)
        try:
            yield self
        finally:
            self._parse_context.reset(selected)

    def _get_parse_token(self, context: DagParseContext, *, retry: bool) -> str:
        if context.token is not None and monotonic() < context.expires_at:
            if monotonic() < context.renew_at:
                return context.token
            try:
                self._exchange_parse_token(context, retry=False, timeout=self._get_renewal_timeout())
            except (httpx.HTTPError, ValueError) as error:
                if isinstance(error, httpx.HTTPStatusError) and error.response.status_code < 500:
                    raise
                context.renew_at = monotonic() + 30
                log.warning(
                    "Unable to renew Dag parsing token",
                    job_id=self._job_id,
                    attempt_id=str(context.request.attempt_id),
                    error_type=type(error).__name__,
                )
            if monotonic() < context.expires_at:
                return context.token
        return self._exchange_parse_token(context, retry=retry)

    def _exchange_parse_token(
        self, context: DagParseContext, *, retry: bool, timeout: httpx.Timeout | None = None
    ) -> str:
        self._ensure_job_token(retry=retry)
        started_at = monotonic()
        response = super().request(
            "POST",
            f"jobs/{self._require_job_id()}/parse-token",
            json=context.request.model_dump(mode="json"),
            retry=retry,
            headers=_JOB_API_HEADERS,
            timeout=timeout or self.timeout,
        )
        parsed = DagParseTokenResponse.model_validate_json(response.content)
        try:
            claims = jwt.decode(parsed.token, options={"verify_signature": False})
            lifetime = float(claims["exp"]) - float(claims["iat"])
            if (
                not math.isfinite(lifetime)
                or lifetime <= 0
                or claims.get("scope") != "dag_parse"
                or claims.get("sub") != str(context.request.attempt_id)
                or claims.get("job_id") != self._job_id
                or claims.get("dag_bundles") != [context.request.bundle_name]
                or claims.get("relative_fileloc") != context.request.relative_fileloc
            ):
                raise ValueError
        except (jwt.PyJWTError, KeyError, TypeError, ValueError):
            raise ValueError("Token exchange returned an invalid Dag parsing token") from None
        context.token = parsed.token
        context.expires_at = started_at + lifetime
        context.renew_at = started_at + lifetime * 0.8
        return parsed.token

    def _update_auth(self, response: httpx.Response) -> None:
        # Task-token refresh headers cannot replace a provisioned or Job-bound credential.
        pass

    def _read_session_token(self, *, force: bool = False) -> str:
        now = monotonic()
        if self._session_token is None or force or now >= self._reload_at:
            token = self._token_file.read_text().strip()
            if not token:
                raise ValueError(f"Dag processor token file is empty: {self._token_file}")
            self._session_token = token
            self._reload_at = now + self._token_reload_interval
        return self._session_token

    def _check_can_run(self) -> None:
        if self._completion_state is not None:
            raise RuntimeError("The Dag processor Job is completing; only completion retries are allowed")

    def _require_job_id(self) -> int:
        if self._job_id is None:
            raise RuntimeError("Register the Dag processor Job before making API requests")
        return self._job_id

    def register_job(self, *, retry: bool = True) -> int:
        """
        Register this process, recover its registration, or renew its Job token.

        While the session's previous Job is still alive, raise ``DagProcessorJobAlreadyRunning``.
        Retrying with this client retains the registration ID.
        """
        self._check_can_run()
        try:
            return self._register_job(retry=retry)
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 409 and _get_error_reason(error) == "job_running":
                raise DagProcessorJobAlreadyRunning(
                    "The previous Dag processor Job is still alive"
                ) from error
            raise

    def _register_job(self, *, retry: bool, timeout: httpx.Timeout | None = None) -> int:
        if self.restart_required:
            raise DagProcessorRegistrationRetired(
                "The Dag processor registration has ended; restart required"
            )
        session_token = self._read_session_token(force=True)
        started_at = monotonic()
        body = self._registration.model_dump(mode="json")
        retried_auth = False
        while True:
            try:
                response = super().request(
                    "POST",
                    "jobs",
                    json=body,
                    auth=BearerAuth(session_token),
                    retry=retry,
                    headers=_JOB_API_HEADERS,
                    timeout=timeout or self.timeout,
                )
                break
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 409 and _get_error_reason(error) == "registration_retired":
                    self._restart_required = True
                    raise DagProcessorRegistrationRetired(
                        "The Dag processor registration has ended; restart required"
                    ) from error
                if error.response.status_code not in (401, 403) or retried_auth:
                    raise
                rotated = self._read_session_token(force=True)
                if rotated == session_token:
                    raise
                session_token = rotated
                retried_auth = True

        registered = JobRegisterResponse.model_validate_json(response.content)
        if self._job_id is not None and self._job_id != registered.job_id:
            raise RuntimeError("Registration returned a different Dag processor Job")
        try:
            # Unverified claims only schedule renewal; the API server remains the authority on validity.
            claims = jwt.decode(registered.token, options={"verify_signature": False})
            lifetime = float(claims["exp"]) - float(claims["iat"])
            if (
                not math.isfinite(lifetime)
                or lifetime <= 0
                or claims.get("scope") != "dag_processor"
                or claims.get("job_id") != registered.job_id
            ):
                raise ValueError
        except (jwt.PyJWTError, KeyError, TypeError, ValueError):
            raise ValueError("Registration returned an invalid Dag processor Job token") from None

        self._job_id = registered.job_id
        self.auth = BearerAuth(registered.token)
        self._registered_with = session_token
        self._renew_at = started_at + lifetime * 0.8
        self._expires_at = started_at + lifetime
        self._retry_renewal_at = 0.0
        return registered.job_id

    def _get_renewal_timeout(self) -> httpx.Timeout:
        return httpx.Timeout(
            **{
                key: min(value if value is not None else 1.0, 1.0)
                for key, value in self.timeout.as_dict().items()
            }
        )

    def _ensure_job_token(self, *, retry: bool) -> None:
        self._require_job_id()
        if monotonic() >= self._expires_at:
            self._register_job(retry=retry)
            return
        if self.restart_required or monotonic() < self._retry_renewal_at:
            return
        try:
            if self._read_session_token() != self._registered_with or monotonic() >= self._renew_at:
                # Do not spend the runtime request's retry budget on an optional early renewal.
                self._register_job(retry=False, timeout=self._get_renewal_timeout())
        except (httpx.HTTPError, OSError, ValueError, DagProcessorRegistrationRetired) as error:
            self._retry_renewal_at = monotonic() + 30
            log.warning(
                "Unable to renew Dag processor Job token",
                job_id=self._job_id,
                error_type=type(error).__name__,
                restart_required=self.restart_required,
            )
        if monotonic() >= self._expires_at:
            self._register_job(retry=retry)

    def request(self, *args, retry: bool = True, **kwargs) -> httpx.Response:
        """Use a parsing credential for subprocess requests, and the Job credential for manager work."""
        self._check_can_run()
        try:
            headers = httpx.Headers(kwargs.get("headers"))
            if context := self._parse_context.get():
                kwargs["auth"] = BearerAuth(self._get_parse_token(context, retry=retry))
                headers["Airflow-Dag-Bundle"] = context.request.bundle_name
            else:
                self._ensure_job_token(retry=retry)
                if bundle_name := self._bundle_context.get():
                    headers["Airflow-Dag-Bundle"] = bundle_name
            if kwargs.get("content") is not None:
                headers.setdefault("Content-Type", "application/json")
            kwargs["headers"] = headers
            return super().request(*args, retry=retry, **kwargs)
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 403 and _get_error_reason(error) == "job_closed":
                self._restart_required = True
                raise DagProcessorRegistrationRetired(
                    "The Dag processor Job has completed or been replaced; restart required"
                ) from error
            raise

    def heartbeat(self) -> JobState:
        """Heartbeat once; the manager's next iteration retries transport failures."""
        job_id = self._require_job_id()
        response = self.request("POST", f"jobs/{job_id}/heartbeat", retry=False, headers=_JOB_API_HEADERS)
        return JobHeartbeatResponse.model_validate_json(response.content).state

    def complete_job(self, state: TerminalJobState) -> None:
        """Complete this Job, retaining its identity and outcome across lost acknowledgments."""
        body = JobCompleteBody(state=state)
        job_id = self._require_job_id()
        if self._completion_state is not None and self._completion_state != body.state:
            raise ValueError("A Dag processor completion retry must keep the original outcome")
        if self._completed:
            return
        self._completion_state = body.state
        # A still-valid token can replay completion even after the Job closes; renewal cannot.
        retried_expiry = False
        while True:
            if monotonic() >= self._expires_at:
                self._register_job(retry=True)
            try:
                super().request(
                    "POST",
                    f"jobs/{job_id}/complete",
                    json=body.model_dump(mode="json"),
                    headers=_JOB_API_HEADERS,
                )
            except httpx.HTTPStatusError as error:
                if (
                    retried_expiry
                    or error.response.status_code not in (401, 403)
                    or monotonic() < self._expires_at
                ):
                    raise
                retried_expiry = True
            else:
                self._completed = True
                return


class DagProcessorSecretsComms:
    """
    Stand-in for ``SUPERVISOR_COMMS`` in the Dag processor manager process.

    Installed as ``task_runner.SUPERVISOR_COMMS``, it makes ``ensure_secrets_backend_loaded()`` choose
    ``ExecutionAPISecretsBackend``, whose connection and variable lookups it answers through the manager's
    client for the bundle selected with ``DagProcessorAPIClient.use_bundle``.
    """

    def __init__(self, client: DagProcessorAPIClient) -> None:
        self._client = client

    def send(self, msg: Any, **kwargs: Any) -> Any:
        if isinstance(msg, GetConnection):
            return handle_get_connection(self._client, msg)[0]
        if isinstance(msg, GetVariable):
            return handle_get_variable(self._client, msg)[0]
        if isinstance(msg, MaskSecret):
            # mask_secret has already masked the value in this process.
            return None
        raise TypeError(f"{type(msg).__name__} is not answered in the Dag processor manager process")
