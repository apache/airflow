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
"""Boat backend for :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`."""

from __future__ import annotations

import base64
import json
import logging
import math
import shlex
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.sandbox.base import (
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxTerminalError,
    _new_sandbox_name,
    _validate_positive_finite,
)
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from collections.abc import Iterator

    from boat_sdk import ApiClient
    from boat_sdk.api.boat_api import BoatApi

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

log = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://boat.dev/api/v1"
_MAX_COMMAND_TIMEOUT = 600
_MAX_ERROR_DETAIL = 300
_FILE_OP_TIMEOUT = 120.0
_HELPER_OUTPUT_CAP = 1024 * 1024
_READY_STATES = frozenset({"ready", "idle", "running"})
_MACHINE_TYPES = frozenset({"small", "default", "large"})


def _api_error_detail(body: Any) -> str:
    """
    Summarize the ``code``/``message`` pair Boat returns in a failed call's body.

    The status code alone hides reasons the task owner has to act on, such as an
    account that has no Boat subscription yet. Returns an empty string when the
    body is missing or is not the documented JSON error envelope.
    """
    if isinstance(body, bytes):
        with suppress(UnicodeDecodeError):
            body = body.decode("utf-8")
    if not isinstance(body, str):
        return ""
    try:
        payload = json.loads(body)
    except ValueError:
        return ""
    if not isinstance(payload, dict):
        return ""
    error = payload.get("error")
    if not isinstance(error, dict):
        error = payload
    parts = [
        value.strip()
        for key in ("code", "message")
        if isinstance(value := error.get(key), str) and value.strip()
    ]
    if len(parts) == 2 and parts[0] == parts[1]:
        del parts[1]
    detail = ": ".join(parts)
    return detail[:_MAX_ERROR_DETAIL]


@contextmanager
def _translate_boat_errors(
    operation: str, *, recoverable_statuses: frozenset[int] = frozenset()
) -> Iterator[None]:
    try:
        yield
    except SandboxError:
        raise
    except Exception as e:
        try:
            from boat_sdk.exceptions import ApiException
        except ImportError:
            raise SandboxTerminalError(
                'The Boat SDK is not installed. Install "apache-airflow-providers-common-ai[sandbox-boat]".'
            ) from e
        if isinstance(e, ApiException):
            status_code = e.status if isinstance(e.status, int) else None
            status = f" (HTTP {status_code})" if status_code is not None else ""
            detail = _api_error_detail(e.body)
            message = f"Boat could not {operation}{status}."
            if detail:
                message = f"{message} {detail}"
            if status_code in recoverable_statuses:
                raise SandboxError(message) from e
            raise SandboxTerminalError(message) from e
        raise SandboxTerminalError(f"Boat could not {operation}: {type(e).__name__}.") from e


def _bound_text(text: str, max_bytes: int, *, already_truncated: bool = False) -> tuple[str, bool]:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text, already_truncated
    return encoded[-max_bytes:].decode("utf-8", errors="ignore"), True


def _parse_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise SandboxTerminalError(f"The Boat connection extra {name} must be a boolean.")


class BoatSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent commands in a `Boat <https://docs.boat.dev/quickstart>`__ sandbox.

    Boat (formerly Ascii Box) is a hosted cloud-computer API: the Airflow worker
    needs only network access and an API key, with no local daemon or host
    virtualization. Credentials resolve lazily from an Airflow connection on
    first use.

    Connection fields: ``password`` is the Boat API key (required). ``host`` may
    override the API base URL. The extra may set ``timeout`` (request timeout in
    seconds) and ``no_env`` (withhold account secrets; default ``true``).

    Boat cannot enforce a deny-all or per-domain egress policy. ``create``
    therefore refuses a :class:`~airflow.providers.common.ai.sandbox.SandboxSpec`
    that asks for ``block_network=True`` or ``allow_egress_to``, preserving the
    fail-closed contract. Pass ``SandboxSpec(block_network=False)`` (and set
    that on :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`)
    when open egress is acceptable.

    Writes use Boat's native file API; reads use the inherited shell
    implementation, because the native read API takes no size parameter and
    would land a whole file in worker memory before ``max_bytes`` could reject
    it.

    :param boat_conn_id: Airflow connection ID for Boat. ``None`` lets the
        backend read ``BOAT_API_KEY`` (and optional ``BOAT_BASE_URL``) from the
        environment.
    :param machine_type: Boat machine size: ``small``, ``default``, or ``large``.
        Default ``"default"``.
    :param ttl_seconds: Server-side auto-stop TTL in seconds after which the
        sandbox is archived even if the worker never destroyed it. Default ``3600``.
    :param ready_timeout: Seconds to wait for a newly created sandbox to become
        ready. Default ``300``.
    :param no_env: When ``True`` (default), create a no-env sandbox that receives
        none of the account's stored secrets. ``None`` reads the connection extra
        and otherwise defaults to ``True``.
    """

    name = "boat"

    def __init__(
        self,
        boat_conn_id: str | None = "boat_default",
        *,
        machine_type: str = "default",
        ttl_seconds: int = 3600,
        ready_timeout: float = 300.0,
        no_env: bool | None = None,
    ) -> None:
        if machine_type not in _MACHINE_TYPES:
            raise ValueError(f"machine_type must be one of {sorted(_MACHINE_TYPES)}, got {machine_type!r}.")
        _validate_positive_finite(ttl_seconds, "ttl_seconds")
        # int() would floor a fractional value, and the API reads 0 as "never
        # auto-stop" -- silently discarding the only backstop against a leak.
        if int(ttl_seconds) != ttl_seconds:
            raise ValueError(f"ttl_seconds must be a whole number of seconds, got {ttl_seconds!r}.")
        _validate_positive_finite(ready_timeout, "ready_timeout")
        self._boat_conn_id = boat_conn_id
        self._machine_type = machine_type
        self._ttl_seconds = int(ttl_seconds)
        self._ready_timeout = ready_timeout
        self._no_env = no_env
        self._resolved_no_env = True if no_env is None else no_env
        self._request_timeout: float | None = None
        self._api_client: ApiClient | None = None
        self._boat_api: BoatApi | None = None

    def _get_api(self) -> BoatApi:
        if self._boat_api is not None:
            return self._boat_api
        with _translate_boat_errors("initialize its client"):
            import os

            from boat_sdk import ApiClient, Configuration
            from boat_sdk.api.boat_api import BoatApi

            if self._boat_conn_id is None:
                api_key = (os.environ.get("BOAT_API_KEY") or "").strip()
                if not api_key:
                    raise SandboxTerminalError(
                        "BOAT_API_KEY is not set; export it or pass an Airflow connection id."
                    )
                base_url = (os.environ.get("BOAT_BASE_URL") or _DEFAULT_BASE_URL).rstrip("/")
                request_timeout = 30.0
                no_env = True if self._no_env is None else self._no_env
            else:
                conn = BaseHook.get_connection(self._boat_conn_id)
                api_key = (conn.password or "").strip()
                if not api_key:
                    raise SandboxTerminalError(
                        f"Connection {self._boat_conn_id!r} has no password; set it to the Boat API key."
                    )
                base_url = (conn.host or _DEFAULT_BASE_URL).rstrip("/")
                if not base_url.startswith("http"):
                    base_url = f"https://{base_url}"
                extra = conn.extra_dejson
                request_timeout = extra.get("timeout", 30)
                try:
                    request_timeout = float(request_timeout)
                    _validate_positive_finite(request_timeout, "connection extra timeout")
                except (TypeError, ValueError) as e:
                    raise SandboxTerminalError(
                        "The Boat connection extra timeout must be a positive finite number."
                    ) from e
                if self._no_env is None:
                    no_env = _parse_bool(extra.get("no_env", True), "no_env")
                else:
                    no_env = self._no_env

            self._request_timeout = request_timeout
            self._resolved_no_env = no_env
            self._api_client = ApiClient(Configuration(host=base_url, access_token=api_key))
            self._boat_api = BoatApi(self._api_client)
            return self._boat_api

    def _http_timeout(self, seconds: float) -> float:
        configured = self._request_timeout if self._request_timeout is not None else 30.0
        return max(configured, seconds + 30.0)

    def _wait_until_ready(self, sandbox_id: str) -> None:
        from boat_sdk import wait_until_ready

        with _translate_boat_errors("wait for a sandbox to become ready"):
            wait_until_ready(
                self._get_api(),
                sandbox_id,
                timeout_seconds=self._ready_timeout,
                poll_interval_seconds=2.0,
            )

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        if spec is not None and spec.allow_egress_to:
            raise SandboxTerminalError(
                "The Boat backend cannot apply a per-domain egress allowlist. "
                "Drop allow_egress_to, or use a backend with per-domain network rules."
            )
        if spec is not None and spec.block_network:
            raise SandboxTerminalError(
                "The Boat backend cannot deny outbound network access. Pass "
                "SandboxSpec(block_network=False) when open egress is acceptable, or "
                "use a backend that can enforce a deny-all policy."
            )

        api = self._get_api()
        with _translate_boat_errors("create a sandbox"):
            from boat_sdk.models.create_sandbox_request import CreateSandboxRequest

            # The generated request models are typed by their wire aliases.
            created = api.create(
                create_sandbox_request=CreateSandboxRequest(
                    type=self._machine_type,
                    ttlSeconds=self._ttl_seconds,
                    noEnv=self._resolved_no_env,
                    env=dict(spec.env) if spec is not None and spec.env else None,
                ),
                _request_timeout=self._http_timeout(self._ready_timeout),
            )
            sandbox_id = created.sandbox.id
        try:
            self._name_sandbox(sandbox_id)
            self._wait_until_ready(sandbox_id)
        except BaseException:
            # The id has not reached the toolset yet, so nothing else can tear
            # this sandbox down. The server-side TTL would archive it eventually,
            # but that leaves a billed machine idling for an hour by default.
            with suppress(Exception):
                self.destroy(sandbox_id)
            raise
        return sandbox_id

    def _name_sandbox(self, sandbox_id: str) -> None:
        """
        Best-effort rename to the ``airflow-sandbox-`` prefix used for correlation.

        Failing to name a sandbox costs nothing at run time, so it must not fail
        the create -- but it does cost an operator sweeping for orphans later,
        which is why it is logged rather than silently dropped.
        """
        from boat_sdk.models.update_sandbox_request import UpdateSandboxRequest

        try:
            self._get_api().update(
                sandbox_id,
                UpdateSandboxRequest(name=_new_sandbox_name()),
                _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT),
            )
        except Exception:
            log.warning(
                "Could not name Boat sandbox %s; it keeps its server-assigned name and will not "
                "match an airflow-sandbox-* orphan sweep.",
                sandbox_id,
                exc_info=True,
            )

    def _destroy_after_timeout(self, sandbox: str) -> None:
        try:
            self.destroy(sandbox)
        except SandboxError as e:
            raise SandboxTerminalError(
                "The Boat command timed out and deletion of its sandbox could not be confirmed."
            ) from e

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        if timeout > _MAX_COMMAND_TIMEOUT:
            raise SandboxTerminalError(
                f"Boat commands are capped at {_MAX_COMMAND_TIMEOUT} seconds; got timeout={timeout}."
            )
        timeout_seconds = max(1, math.ceil(timeout))
        api = self._get_api()
        with _translate_boat_errors("run a sandbox command"):
            from boat_sdk.models.command_request import CommandRequest
            from boat_sdk.models.command_response import CommandResponse

            response = api.command(
                sandbox,
                CommandRequest(command=command, timeoutSeconds=timeout_seconds),
                _request_timeout=self._http_timeout(timeout),
            )
            result = response.actual_instance
        # The endpoint answers with a oneOf whose other branch is a detached
        # command, which this backend never asks for.
        if not isinstance(result, CommandResponse):
            raise SandboxTerminalError("Boat returned no result for the command.")

        stdout, out_truncated = _bound_text(
            result.stdout or "",
            max_output_bytes,
            already_truncated=bool(result.stdout_truncated),
        )
        stderr, err_truncated = _bound_text(
            result.stderr or "",
            max_output_bytes,
            already_truncated=bool(result.stderr_truncated),
        )
        if result.timed_out:
            self._destroy_after_timeout(sandbox)
            return SandboxExecResult(
                exit_code=-1,
                stdout=stdout,
                stderr=stderr,
                timed_out=True,
                stdout_truncated=out_truncated,
                stderr_truncated=err_truncated,
                sandbox_terminated=True,
            )
        return SandboxExecResult(
            exit_code=result.exit_code if result.exit_code is not None else -1,
            stdout=stdout,
            stderr=stderr,
            stdout_truncated=out_truncated,
            stderr_truncated=err_truncated,
        )

    def _run_helper(self, sandbox: str, script: str, *, operation: str) -> str:
        result = self.run_command(
            sandbox, script, timeout=_FILE_OP_TIMEOUT, max_output_bytes=_HELPER_OUTPUT_CAP
        )
        if result.timed_out or result.sandbox_terminated:
            raise SandboxTerminalError(f"The sandbox was destroyed after it timed out while {operation}.")
        if result.exit_code:
            raise SandboxError(result.stderr.strip() or f"Could not {operation}.")
        return result.stdout

    def _confirm_sandbox_exists(self, sandbox: str) -> None:
        with _translate_boat_errors("confirm that a sandbox still exists"):
            response = self._get_api().get(sandbox, _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT))
        state = response.sandbox.state
        if state not in _READY_STATES:
            raise SandboxTerminalError(f"Boat sandbox {sandbox!r} is not runnable (state={state!r}).")

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        quoted = shlex.quote(path)
        self._run_helper(
            sandbox,
            f'mkdir -p -- "$(dirname -- {quoted})"',
            operation=f"create the parent directory for {path!r}",
        )
        api = self._get_api()
        try:
            from boat_sdk.models.file_write_request import FileWriteRequest

            api.write_file(
                sandbox,
                FileWriteRequest(
                    path=path,
                    content=base64.b64encode(content).decode("ascii"),
                    encoding="base64",
                ),
                _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT),
            )
        except Exception as e:
            from boat_sdk.exceptions import ApiException

            if isinstance(e, ApiException) and e.status == 404:
                self._confirm_sandbox_exists(sandbox)
                raise SandboxError(f"Could not write {path!r} in the sandbox.") from e
            with _translate_boat_errors("write a sandbox file", recoverable_statuses=frozenset({400})):
                raise

    def list_directory(self, sandbox: str, path: str) -> list[tuple[str, bool]]:
        quoted = shlex.quote(path)
        listing = self._run_helper(
            sandbox,
            f"find -- {quoted} -maxdepth 1 -mindepth 1 -printf '%y %f\\0'",
            operation=f"list {path!r}",
        )
        entries: list[tuple[str, bool]] = []
        for record in listing.split("\0"):
            if not record:
                continue
            kind, _, name = record.partition(" ")
            if name:
                entries.append((name, kind == "d"))
        return entries

    def destroy(self, sandbox: str) -> None:
        """
        Request permanent deletion of the sandbox.

        The API accepts the request and returns before teardown finishes, so a
        successful return means accepted, not gone.
        """
        api = self._get_api()
        with _translate_boat_errors("delete a sandbox"):
            from boat_sdk.exceptions import ApiException

            param = api.api_client.param_serialize(
                method="DELETE",
                resource_path="/sandboxes/{sandboxId}",
                path_params={"sandboxId": sandbox},
                header_params={
                    "Accept": "application/json",
                    # The confirmation header kept its pre-rename name in the API.
                    "X-Ascii-Confirm-Delete": sandbox,
                },
                auth_settings=["BoatBearerAuth"],
            )
            try:
                response_data = api.api_client.call_api(
                    *param, _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT)
                )
                response_data.read()
                if response_data.status == 404:
                    return
                if not 200 <= response_data.status <= 299:
                    raise ApiException.from_response(http_resp=response_data, body=None, data=None)
            except ApiException as e:
                if e.status == 404:
                    return
                raise
