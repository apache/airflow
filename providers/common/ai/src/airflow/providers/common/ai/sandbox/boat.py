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
import os
import shlex
import time
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.sandbox.base import (
    _FILE_OP_OUTPUT_CAP,
    _FILE_OP_TIMEOUT,
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxTerminalError,
    _new_sandbox_name,
    _validate_positive_finite,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from boat_sdk.api.boat_api import BoatApi

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

log = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://boat.dev/api/v1"
_MAX_COMMAND_TIMEOUT = 600
# The deadline is enforced inside the guest by GNU ``timeout``, which sends SIGTERM and
# then SIGKILL this many seconds later. Boat's own deadline sits _SERVER_TIMEOUT_GRACE
# past that SIGKILL so the wrapper can still print what the command wrote: when Boat
# ends the call first, that output is lost and the sandbox has to be destroyed.
_KILL_AFTER = 5
_SERVER_TIMEOUT_GRACE = 10
# What ``timeout`` exits with when it stopped the command: 124 after SIGTERM, 137 when
# it had to escalate to SIGKILL.
_IN_GUEST_TIMEOUT_EXITS = frozenset({124, 137})
_MAX_ERROR_DETAIL = 300
_READY_STATES = frozenset({"ready", "idle", "running"})
_TERMINAL_STATES = frozenset({"archiving", "archived", "error"})
_READY_POLL_INTERVAL = 2.0
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
                'The Boat SDK is not installed. Install "apache-airflow-providers-common-ai[boat]".'
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


class BoatSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent commands in a `Boat <https://docs.boat.dev/quickstart>`__ sandbox.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Boat (formerly Ascii Box) is a hosted cloud-computer API: the Airflow worker
    needs only network access and an API key, with no local daemon or host
    virtualization.

    **Credentials are ambient.** On first use the backend reads ``BOAT_API_KEY``
    (required) and optional ``BOAT_BASE_URL`` from the worker environment. Modal
    reads a ``modal`` connection first, but that connection type is owned by the
    Modal provider; a ``boat`` connection type belongs in a future Boat provider,
    not in this one.

    Boat cannot enforce a deny-all, per-domain, or CIDR egress policy. ``create``
    therefore refuses a :class:`~airflow.providers.common.ai.sandbox.SandboxSpec`
    that asks for ``block_network=True``, ``allow_egress_to``, or
    ``allow_egress_to_cidrs``, preserving the fail-closed contract. Pass
    ``SandboxSpec(block_network=False)`` (and set that on
    :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`) when
    open egress is acceptable.

    Writes use Boat's native file API; reads use the inherited shell
    implementation, because the native read API takes no size parameter and
    would land a whole file in worker memory before ``max_bytes`` could reject
    it.

    :param machine_type: Boat machine size: ``small``, ``default``, or ``large``.
        Default ``"default"``.
    :param ttl_seconds: Server-side archive TTL in seconds after which the
        sandbox is archived even if the worker never destroyed it. Default ``3600``.
    :param ready_timeout: Seconds to wait for a newly created sandbox to become
        ready. Default ``300``.
    :param request_timeout: HTTP timeout in seconds for a Boat API call that answers
        at once, such as a status check or a delete, and the time added to the
        operation's own for a call that waits on one: a command's deadline, a
        create's ``ready_timeout``, or 120 seconds for a file write. Default ``30``.
    :param no_env: When ``True`` (default), create a no-env sandbox that receives
        none of the account's stored secrets.
    """

    name = "boat"

    def __init__(
        self,
        *,
        machine_type: str = "default",
        ttl_seconds: int = 3600,
        ready_timeout: float = 300.0,
        request_timeout: float = 30.0,
        no_env: bool = True,
    ) -> None:
        if machine_type not in _MACHINE_TYPES:
            raise ValueError(f"machine_type must be one of {sorted(_MACHINE_TYPES)}, got {machine_type!r}.")
        _validate_positive_finite(ttl_seconds, "ttl_seconds")
        # int() would floor a fractional value, and the API reads 0 as "never
        # archive" -- silently discarding the only backstop against a leak.
        if int(ttl_seconds) != ttl_seconds:
            raise ValueError(f"ttl_seconds must be a whole number of seconds, got {ttl_seconds!r}.")
        _validate_positive_finite(ready_timeout, "ready_timeout")
        _validate_positive_finite(request_timeout, "request_timeout")
        if not isinstance(no_env, bool):
            raise ValueError(f"no_env must be a boolean, got {no_env!r}.")
        self._machine_type = machine_type
        self._ttl_seconds = int(ttl_seconds)
        self._ready_timeout = ready_timeout
        self._request_timeout = request_timeout
        self._no_env = no_env
        self._boat_api: BoatApi | None = None
        self._sandbox_env: dict[str, dict[str, str]] = {}

    def _get_api(self) -> BoatApi:
        if self._boat_api is not None:
            return self._boat_api
        with _translate_boat_errors("initialize its client"):
            from boat_sdk import ApiClient, Configuration
            from boat_sdk.api.boat_api import BoatApi

            api_key = (os.environ.get("BOAT_API_KEY") or "").strip()
            if not api_key:
                raise SandboxTerminalError("BOAT_API_KEY is not set.")
            base_url = (os.environ.get("BOAT_BASE_URL") or _DEFAULT_BASE_URL).rstrip("/")
            self._boat_api = BoatApi(ApiClient(Configuration(host=base_url, access_token=api_key)))
            return self._boat_api

    def _http_timeout(self, seconds: float) -> float:
        """HTTP timeout for a call that waits ``seconds`` on an operation before it answers."""
        return seconds + self._request_timeout

    def _wait_until_ready(self, sandbox_id: str) -> None:
        # Not boat_sdk.wait_until_ready: it polls with no HTTP timeout, so one stalled
        # response would hold create past ready_timeout indefinitely.
        deadline = time.monotonic() + self._ready_timeout
        while (remaining := deadline - time.monotonic()) > 0:
            with _translate_boat_errors("wait for a sandbox to become ready"):
                response = self._get_api().get(
                    sandbox_id, _request_timeout=min(self._request_timeout, remaining)
                )
            state = response.sandbox.state
            if state in _READY_STATES:
                return
            if state in _TERMINAL_STATES:
                raise SandboxTerminalError(
                    f"Boat sandbox {sandbox_id!r} entered state {state!r} before it was ready."
                )
            time.sleep(min(_READY_POLL_INTERVAL, max(0.0, deadline - time.monotonic())))
        raise SandboxTerminalError(
            f"Boat sandbox {sandbox_id!r} was not ready within {self._ready_timeout:g} seconds."
        )

    @staticmethod
    def _check_spec(spec: SandboxSpec | None) -> None:
        """Refuse a spec this backend cannot carry faithfully, before anything is provisioned."""
        if spec is None:
            return
        if spec.owner is not None:
            # An owner exists so that a later task can attach to the sandbox, and the
            # ownership rules live in per-sandbox metadata this backend does not read
            # back, so recording one would promise an attach that cannot be checked.
            raise SandboxTerminalError(
                "SandboxSpec names an owner, but this backend keeps no per-sandbox metadata the "
                "ownership rules could be read back from, so a sandbox created here cannot be attached "
                "to from another task. Drop owner, or provision the sandbox on a backend that supports "
                "attaching, such as ModalSandboxBackend."
            )
        if spec.allow_egress_to:
            raise SandboxTerminalError(
                "The Boat backend cannot apply a per-domain egress allowlist. "
                "Drop allow_egress_to, or use a backend with per-domain network rules."
            )
        if spec.allow_egress_to_cidrs:
            raise SandboxTerminalError(
                "The Boat backend cannot apply a CIDR egress allowlist. "
                "Drop allow_egress_to_cidrs, or use a backend with network rules."
            )
        if spec.block_network:
            raise SandboxTerminalError(
                "The Boat backend cannot deny outbound network access. Pass "
                "SandboxSpec(block_network=False) when open egress is acceptable, or "
                "use a backend that can enforce a deny-all policy."
            )

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        self._check_spec(spec)
        env = dict(spec.env) if spec is not None and spec.env else {}
        api = self._get_api()
        with _translate_boat_errors("create a sandbox"):
            from boat_sdk.models.create_sandbox_request import CreateSandboxRequest

            # The generated request models are typed by their wire aliases.
            created = api.create(
                create_sandbox_request=CreateSandboxRequest(
                    type=self._machine_type,
                    ttlSeconds=self._ttl_seconds,
                    noEnv=self._no_env,
                    env=env or None,
                ),
                _request_timeout=self._http_timeout(self._ready_timeout),
            )
            sandbox_id = created.sandbox.id
        try:
            self._name_sandbox(sandbox_id)
            self._wait_until_ready(sandbox_id)
            self._sandbox_env[sandbox_id] = env
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
                _request_timeout=self._request_timeout,
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
            raise SandboxError(
                f"Boat commands are capped at {_MAX_COMMAND_TIMEOUT} seconds; got timeout={timeout}. "
                "Ask for a shorter timeout."
            )
        # GNU timeout reads 0 as "no timeout", so round up; a command near the API cap is
        # shortened so Boat's own deadline still fits above it.
        timeout_seconds = min(
            max(1, math.ceil(timeout)), _MAX_COMMAND_TIMEOUT - _KILL_AFTER - _SERVER_TIMEOUT_GRACE
        )
        server_timeout = timeout_seconds + _KILL_AFTER + _SERVER_TIMEOUT_GRACE
        api = self._get_api()
        env = self._sandbox_env.get(sandbox, {})
        if env:
            exports = "; ".join(
                f"export {shlex.quote(key)}={shlex.quote(value)}" for key, value in env.items()
            )
            command = f"{exports}; {command}"
        # wait's stderr is dropped because bash reports a SIGKILLed job there, quoting
        # the whole command line, exports included.
        command = (
            "tmp_dir=$(mktemp -d); trap 'rm -rf \"$tmp_dir\"' EXIT; "
            f"timeout --kill-after={_KILL_AFTER} {timeout_seconds} bash -c {shlex.quote(command)} "
            '>"$tmp_dir/stdout" 2>"$tmp_dir/stderr" & '
            'command_pid=$!; wait "$command_pid" 2>/dev/null; command_status=$?; '
            'cat "$tmp_dir/stdout"; cat "$tmp_dir/stderr" >&2; exit "$command_status"'
        )
        started = time.monotonic()
        with _translate_boat_errors("run a sandbox command"):
            from boat_sdk.models.command_request import CommandRequest
            from boat_sdk.models.command_response import CommandResponse

            response = api.command(
                sandbox,
                CommandRequest(command=command, timeoutSeconds=server_timeout),
                _request_timeout=self._http_timeout(server_timeout),
            )
            result = response.actual_instance
        elapsed = time.monotonic() - started
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
            # Boat's deadline means the in-guest one did not stop the command, so it
            # may still be running.
            self._destroy_after_timeout(sandbox)
            return SandboxExecResult(
                exit_code=-1,
                stdout=stdout,
                stderr=stderr,
                timed_out=True,
                stdout_truncated=out_truncated,
                stderr_truncated=err_truncated,
                sandbox_terminated=True,
                applied_timeout=float(timeout_seconds),
            )
        exit_code = result.exit_code if result.exit_code is not None else -1
        return SandboxExecResult(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            # 124 and 137 are also ordinary exits (an OOM kill is 137), so only one at or
            # past the deadline is timeout's.
            timed_out=exit_code in _IN_GUEST_TIMEOUT_EXITS and elapsed >= timeout_seconds,
            stdout_truncated=out_truncated,
            stderr_truncated=err_truncated,
            applied_timeout=float(timeout_seconds),
        )

    def _confirm_sandbox_exists(self, sandbox: str) -> None:
        with _translate_boat_errors("confirm that a sandbox still exists"):
            response = self._get_api().get(sandbox, _request_timeout=self._request_timeout)
        state = response.sandbox.state
        if state not in _READY_STATES:
            raise SandboxTerminalError(f"Boat sandbox {sandbox!r} is not runnable (state={state!r}).")

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        quoted = shlex.quote(path)
        result = self.run_command(
            sandbox,
            f'mkdir -p -- "$(dirname -- {quoted})"',
            timeout=_FILE_OP_TIMEOUT,
            max_output_bytes=_FILE_OP_OUTPUT_CAP,
        )
        if result.sandbox_terminated:
            raise SandboxTerminalError(
                f"The sandbox ended while the parent directory for {path!r} was being created."
            )
        if result.exit_code:
            raise SandboxError(
                result.stderr.strip() or f"Could not create the parent directory for {path!r}."
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

    def destroy(self, sandbox: str) -> None:
        """
        Request permanent deletion of the sandbox.

        The API accepts the request and returns before teardown finishes, so a
        successful return means accepted, not gone.
        """
        api = self._get_api()
        with _translate_boat_errors("delete a sandbox"):
            from boat_sdk.exceptions import ApiException

            try:
                # The API wants the sandbox id repeated as the delete confirmation.
                api.delete_sandbox(sandbox, sandbox, _request_timeout=self._request_timeout)
            except ApiException as e:
                if e.status != 404:
                    raise
        self._sandbox_env.pop(sandbox, None)
