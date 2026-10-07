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
import uuid
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

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
from airflow.providers.common.ai.sandbox.output import _tail_bytes
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from collections.abc import Iterator

    from boat_sdk.api.boat_api import BoatApi
    from boat_sdk.models.command_response import CommandResponse

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

log = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://boat.dev/api/v1"
# CommandRequest's bound on timeoutSeconds.
_MAX_COMMAND_TIMEOUT = 600
# The deadline is enforced inside the guest by GNU ``timeout``, which sends SIGTERM and
# then SIGKILL this many seconds later. Boat's own deadline sits _SERVER_TIMEOUT_GRACE
# past that SIGKILL so the wrapper can still print what the command wrote: when Boat
# ends the call first, that output is lost and the sandbox has to be destroyed.
_KILL_AFTER = 5
_SERVER_TIMEOUT_GRACE = 10
_MAX_ERROR_DETAIL = 300
_READY_STATES = frozenset({"ready", "idle", "running"})
_TERMINAL_STATES = frozenset({"archiving", "archived", "error", "cancelled"})
_READY_POLL_INTERVAL = 2.0
_MACHINE_TYPES = frozenset({"small", "default", "large"})
# CreateSandboxRequest's bound on ttlSeconds: 30 days.
_MAX_TTL_SECONDS = 2_592_000


def _parse_api_error(body: str | None) -> dict[str, Any]:
    """Return the error object of Boat's JSON error envelope, or ``{}`` when ``body`` is not one."""
    if body is None:
        return {}
    try:
        payload = json.loads(body)
    except ValueError:
        return {}
    if not isinstance(payload, dict):
        return {}
    error = payload.get("error")
    return error if isinstance(error, dict) else payload


def _api_error_detail(body: str | None) -> str:
    """
    Summarize the ``code``/``message`` pair Boat returns in a failed call's body.

    The status code alone hides reasons the task owner has to act on, such as an
    account that has no Boat subscription yet. Returns an empty string when the
    body is missing or is not the documented JSON error envelope.
    """
    error = _parse_api_error(body)
    parts = [
        value.strip()
        for key in ("code", "message")
        if isinstance(value := error.get(key), str) and value.strip()
    ]
    if len(parts) == 2 and parts[0] == parts[1]:
        del parts[1]
    detail = ": ".join(parts)
    return detail[:_MAX_ERROR_DETAIL]


def _may_have_lost_response(error: Exception) -> bool:
    """Whether a failed create may have reached Boat and only its answer was lost."""
    from boat_sdk.exceptions import ApiException
    from urllib3.exceptions import HTTPError

    if isinstance(error, ApiException):
        # Not status 0: with the client's Retry, a TLS failure arrives as urllib3's
        # MaxRetryError, so boat-sdk's status 0 is only a request it could not build.
        return isinstance(error.status, int) and error.status >= 500
    return isinstance(error, HTTPError)


def _is_idempotency_in_progress(error: Exception) -> bool:
    from boat_sdk.exceptions import ApiException

    return (
        isinstance(error, ApiException)
        and error.status == 409
        and _parse_api_error(error.body).get("code") == "idempotency_in_progress"
    )


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
    if len(text.encode("utf-8")) <= max_bytes:
        return text, already_truncated
    return _tail_bytes(text, max_bytes), True


class BoatSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent commands in a `Boat <https://docs.boat.dev/quickstart>`__ sandbox.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Boat (formerly Ascii Box) is a hosted cloud-computer API: the Airflow worker
    needs only network access and an API key, with no local daemon or host
    virtualization.

    Credentials come from a generic Airflow connection, resolved on first use:
    its ``password`` is the Boat API key, and its ``host``, when set, the API
    base URL. Without a connection, the backend reads ``BOAT_API_KEY`` (required)
    and optional ``BOAT_BASE_URL`` from the worker environment on first use.

    Every sandbox is created with Boat's ``noEnv`` flag, so it gets none of the
    account's stored environment variables, secret files or credentials, and
    cannot act on the account or its other sandboxes. ``SandboxSpec.env`` is the
    only environment it receives.

    Boat cannot enforce a deny-all, per-domain, or CIDR egress policy. ``create``
    therefore refuses a :class:`~airflow.providers.common.ai.sandbox.SandboxSpec`
    that asks for ``block_network=True``, ``allow_egress_to``, or
    ``allow_egress_to_cidrs``, preserving the fail-closed contract. Pass
    ``SandboxSpec(block_network=False)`` (and set that on
    :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`) when
    open egress is acceptable.

    Writes use Boat's native file API, which accepts only paths that resolve
    under ``/home/user`` (where relative paths land) or ``/tmp``. The parent
    directory is created first with ``mkdir -p`` in the guest, so a write
    anywhere else reaches the model as a recoverable error: ``mkdir``'s own
    when the guest user cannot create the parent directory, and one carrying
    Boat's ``invalid_path`` code when that directory exists or could be
    created. Reads use the inherited shell implementation, because the native
    read API takes no size parameter and would land a whole file in worker
    memory before ``max_bytes`` could reject it.

    :param boat_conn_id: Generic Airflow connection ID. Its ``password`` is the
        Boat API key and is required; its ``host``, when set, is the full API base
        URL with its scheme, such as ``https://boat.dev/api/v1``. ``None``
        (default) reads ``BOAT_API_KEY`` and optional ``BOAT_BASE_URL`` from the
        worker environment instead.
    :param machine_type: Boat machine size: ``small``, ``default``, or ``large``.
        Default ``"default"``.
    :param ttl_seconds: Server-side TTL in seconds after which Boat stops the
        sandbox even if the worker never destroyed it: a whole number from 1 to
        2592000 (30 days). The sandbox is created without snapshots, so stopping
        it erases its disk. Default ``3600``.
    :param ready_timeout: Seconds allowed for provisioning, from the create request
        until the sandbox is ready. A create request that times out, fails in
        transport, or gets a server error is sent once more with the same
        idempotency key, with what is left of ``ready_timeout`` plus
        ``request_timeout``, so provisioning takes at most their sum. Default ``300``.
    :param request_timeout: HTTP timeout in seconds for a Boat API call that answers
        at once, such as the create request, a status check or a delete, and the
        time added to the operation's own for a call that waits on one: a
        command's deadline, or 120 seconds for a file write. Default ``30``.
    """

    name = "boat"

    def __init__(
        self,
        *,
        boat_conn_id: str | None = None,
        machine_type: str = "default",
        ttl_seconds: int = 3600,
        ready_timeout: float = 300.0,
        request_timeout: float = 30.0,
    ) -> None:
        if machine_type not in _MACHINE_TYPES:
            raise ValueError(f"machine_type must be one of {sorted(_MACHINE_TYPES)}, got {machine_type!r}.")
        _validate_positive_finite(ttl_seconds, "ttl_seconds")
        # Refused rather than floored: int() would quietly shorten the TTL backstop,
        # and turn a sub-second value into 0, which the SDK only rejects at create time.
        if int(ttl_seconds) != ttl_seconds:
            raise ValueError(f"ttl_seconds must be a whole number of seconds, got {ttl_seconds!r}.")
        if ttl_seconds > _MAX_TTL_SECONDS:
            raise ValueError(
                f"ttl_seconds must be at most {_MAX_TTL_SECONDS} (30 days), got {ttl_seconds!r}."
            )
        _validate_positive_finite(ready_timeout, "ready_timeout")
        _validate_positive_finite(request_timeout, "request_timeout")
        self._boat_conn_id = boat_conn_id
        self._machine_type = machine_type
        self._ttl_seconds = int(ttl_seconds)
        self._ready_timeout = ready_timeout
        self._request_timeout = request_timeout
        self._boat_api: BoatApi | None = None
        self._sandbox_env: dict[str, dict[str, str]] = {}
        self._deleted_at_deadline: set[str] = set()

    def _get_api(self) -> BoatApi:
        if self._boat_api is not None:
            return self._boat_api
        with _translate_boat_errors("initialize its client"):
            from boat_sdk import ApiClient, Configuration
            from boat_sdk.api.boat_api import BoatApi
            from urllib3.util import Retry

            if self._boat_conn_id is None:
                api_key = (os.environ.get("BOAT_API_KEY") or "").strip()
                if not api_key:
                    raise SandboxTerminalError("BOAT_API_KEY is not set.")
                base_url = os.environ.get("BOAT_BASE_URL") or _DEFAULT_BASE_URL
            else:
                conn = BaseHook.get_connection(self._boat_conn_id)
                api_key = (conn.password or "").strip()
                if not api_key:
                    raise SandboxTerminalError(
                        f"Connection {self._boat_conn_id!r} has no password; set it to the Boat API key."
                    )
                base_url = conn.host or _DEFAULT_BASE_URL
                # A bare domain, which is what Airflow's connection URI form leaves in host,
                # would make urllib3 send the API key to it over plain HTTP.
                parts = urlsplit(base_url)
                if parts.scheme not in ("https", "http") or not parts.netloc:
                    raise SandboxTerminalError(
                        f"Connection {self._boat_conn_id!r} host {conn.host!r} has no http:// or https:// "
                        f"scheme; set it to the full Boat API base URL, such as {_DEFAULT_BASE_URL}, "
                        "or leave it empty for that default."
                    )
            # urllib3 would otherwise resend a request that stalled or failed up to three
            # times, each with the call's whole timeout, and sleep out a 413/429/503's
            # Retry-After between sends, so one call could outlast the deadline it was given.
            # Unlike retries=False, this still follows redirects.
            retries = Retry.DEFAULT.new(connect=0, read=0, other=0, respect_retry_after_header=False)
            self._boat_api = BoatApi(
                ApiClient(Configuration(host=base_url.rstrip("/"), access_token=api_key, retries=retries))
            )
            return self._boat_api

    def _http_timeout(self, seconds: float) -> float:
        """HTTP timeout for a call that waits ``seconds`` on an operation before it answers."""
        return seconds + self._request_timeout

    def _get_state(self, sandbox_id: str, *, timeout: float) -> str:
        """
        Return the state Boat reports for a sandbox, read from the raw response.

        Boat reports a cancelled sandbox once with only ``id``, ``state`` and
        ``error``, and the SDK's ``Sandbox`` model rejects that body for lacking
        fields it requires, so ``BoatApi.get`` would fail before the state could
        be seen.
        """
        from boat_sdk.exceptions import ApiException

        response = self._get_api().get_without_preload_content(sandbox_id, _request_timeout=timeout)
        if not 200 <= response.status <= 299:
            raise ApiException.from_response(http_resp=response, body=None, data=None)
        return json.loads(response.data)["sandbox"]["state"]

    def _wait_until_ready(self, sandbox_id: str, deadline: float) -> None:
        # Not boat_sdk.wait_until_ready: it polls with no HTTP timeout, so one stalled
        # response would hold create past ready_timeout indefinitely.
        while (remaining := deadline - time.monotonic()) > 0:
            with _translate_boat_errors("wait for a sandbox to become ready"):
                state = self._get_state(sandbox_id, timeout=min(self._request_timeout, remaining))
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
        # Taken once the client exists, so a slow connection lookup is not counted
        # against provisioning.
        api = self._get_api()
        deadline = time.monotonic() + self._ready_timeout
        sandbox_id = self._request_sandbox(api, env, deadline)
        try:
            self._name_sandbox(sandbox_id, deadline)
            self._wait_until_ready(sandbox_id, deadline)
            self._sandbox_env[sandbox_id] = env
        except BaseException:
            # The id has not reached the toolset yet, so nothing else can tear
            # this sandbox down. The server-side TTL would stop it eventually,
            # but that leaves a billed machine idling for an hour by default.
            with suppress(Exception):
                self.destroy(sandbox_id)
            raise
        return sandbox_id

    def _request_sandbox(self, api: BoatApi, env: dict[str, str], deadline: float) -> str:
        """
        Send the create request, retrying it once if its answer may have been lost.

        Both attempts carry the same idempotency key and body, for which Boat
        returns the sandbox the first one created rather than billing a second.
        The first attempt gets at most ``request_timeout``, so a lost answer
        leaves time to retry; a retry that arrives while Boat is still creating
        that sandbox is answered with 409 ``idempotency_in_progress``. The retry
        is sent even at the deadline, because only its answer names a sandbox
        the first request may have started, which ``create`` then deletes.
        """
        with _translate_boat_errors("create a sandbox"):
            from boat_sdk.models.create_sandbox_request import CreateSandboxRequest

            # The generated request models are typed by their wire aliases.
            request = CreateSandboxRequest(
                type=self._machine_type,
                ttlSeconds=self._ttl_seconds,
                noEnv=True,
                # Nothing here resumes or forks a sandbox, so snapshots would only
                # cost CPU and memory and keep a stopped sandbox's disk.
                snapshots=False,
                env=env or None,
            )
        idempotency_key = uuid.uuid4().hex
        http_timeout = min(self._request_timeout, deadline - time.monotonic())
        retried = False
        while True:
            try:
                return api.create(
                    idempotency_key=idempotency_key,
                    create_sandbox_request=request,
                    _request_timeout=http_timeout,
                ).sandbox.id
            except Exception as e:
                if retried and _is_idempotency_in_progress(e):
                    time.sleep(min(_READY_POLL_INTERVAL, max(0.0, deadline - time.monotonic())))
                    retry = time.monotonic() < deadline
                else:
                    retry = not retried and _may_have_lost_response(e)
                if not retry:
                    with _translate_boat_errors("create a sandbox"):
                        raise
            retried = True
            http_timeout = self._http_timeout(max(0.0, deadline - time.monotonic()))

    def _name_sandbox(self, sandbox_id: str, deadline: float) -> None:
        """
        Best-effort rename to the ``airflow-sandbox-`` prefix used for correlation.

        Failing to name a sandbox costs nothing at run time, so it must not fail
        the create -- but it does cost an operator sweeping for orphans later,
        which is why it is logged rather than silently dropped.
        """
        from boat_sdk.models.update_sandbox_request import UpdateSandboxRequest

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            # The wait for readiness fails at once, and create deletes the sandbox.
            return
        try:
            self._get_api().update(
                sandbox_id,
                UpdateSandboxRequest(name=_new_sandbox_name()),
                _request_timeout=min(self._request_timeout, remaining),
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
        self._deleted_at_deadline.add(sandbox)

    @contextmanager
    def _terminal_if_deleted_at_deadline(self, sandbox: str, what: str) -> Iterator[None]:
        """
        Make a failed file operation terminal when Boat's deadline deleted its sandbox.

        The inherited helpers look only at the exit status, so they would tell the
        model to retry against a sandbox that no longer exists.
        """
        try:
            yield
        except SandboxTerminalError:
            raise
        except SandboxError as e:
            if sandbox in self._deleted_at_deadline:
                raise SandboxTerminalError(f"The sandbox ended while {what}.") from e
            raise

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        # GNU timeout reads 0 as "no timeout", so round up. Any longer request is shortened
        # so Boat's own deadline, above this one, still fits under the API cap; the result's
        # applied_timeout tells the model what the command got.
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
        # A login shell, like the one Boat runs this wrapper in, so the functions and
        # unexported variables the guest's profile sets still reach the command, with
        # the exports applied after them. wait's stderr is dropped because bash reports
        # a SIGKILLed job there, quoting the whole command line, exports included.
        # Only the tail of each spool is replayed: a command can spool gigabytes before
        # its deadline, and replaying all of it can outlast Boat's own deadline, which
        # deletes the sandbox. The extra byte is how _bound_text sees that bytes were cut.
        replay_bytes = max_output_bytes + 1
        command = (
            "tmp_dir=$(mktemp -d); trap 'rm -rf \"$tmp_dir\"' EXIT; "
            f"timeout --kill-after={_KILL_AFTER} {timeout_seconds} bash -lc {shlex.quote(command)} "
            '>"$tmp_dir/stdout" 2>"$tmp_dir/stderr" & '
            'command_pid=$!; wait "$command_pid" 2>/dev/null; command_status=$?; '
            f'tail -c {replay_bytes} "$tmp_dir/stdout"; tail -c {replay_bytes} "$tmp_dir/stderr" >&2; '
            'exit "$command_status"'
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
        stderr_text = result.stderr or ""
        if result.exit_code is None and not result.timed_out:
            # Boat reports no status when the wrapper shell itself was killed, which also
            # loses what the command printed; a bare -1 would tell the model nothing.
            note = self._describe_missing_exit_status(result)
            stderr_text = f"{stderr_text.rstrip()}\n{note}" if stderr_text.strip() else note
        stderr, err_truncated = _bound_text(
            stderr_text,
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
            # timeout exits 124 after its SIGTERM and 137 once it escalates to SIGKILL,
            # _KILL_AFTER later. Both are also ordinary exits, and an OOM kill is 137, so
            # only one that late, and not one Boat reports as an OOM kill, is timeout's.
            timed_out=not result.oom_killed
            and (
                (exit_code == 124 and elapsed >= timeout_seconds)
                or (exit_code == 137 and elapsed >= timeout_seconds + _KILL_AFTER)
            ),
            stdout_truncated=out_truncated,
            stderr_truncated=err_truncated,
            applied_timeout=float(timeout_seconds),
        )

    @staticmethod
    def _describe_missing_exit_status(result: CommandResponse) -> str:
        if result.oom_killed:
            killed_by = f" by {result.signal}" if result.signal else ""
            return f"The command was killed{killed_by} when the sandbox ran out of memory."
        if result.signal:
            return f"The command was killed by {result.signal}."
        return "The command ended without reporting an exit status."

    def _confirm_sandbox_exists(self, sandbox: str) -> None:
        with _translate_boat_errors("confirm that a sandbox still exists"):
            state = self._get_state(sandbox, timeout=self._request_timeout)
        if state not in _READY_STATES:
            raise SandboxTerminalError(f"Boat sandbox {sandbox!r} is not runnable (state={state!r}).")

    def read_file(self, sandbox: str, path: str, *, max_bytes: int) -> bytes:
        with self._terminal_if_deleted_at_deadline(sandbox, f"{path!r} was being read"):
            return super().read_file(sandbox, path, max_bytes=max_bytes)

    def list_directory(self, sandbox: str, path: str) -> list[tuple[str, bool]]:
        with self._terminal_if_deleted_at_deadline(sandbox, f"{path!r} was being listed"):
            return super().list_directory(sandbox, path)

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
