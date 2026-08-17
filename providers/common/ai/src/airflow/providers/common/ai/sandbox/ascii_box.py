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
"""Ascii Box backend for :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`."""

from __future__ import annotations

import base64
import binascii
import math
import shlex
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.sandbox.base import (
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxFileTooLargeError,
    SandboxTerminalError,
    _new_sandbox_name,
    _validate_positive_finite,
)
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ascii_box_sdk import ApiClient
    from ascii_box_sdk.api.box_api import BoxApi

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

_DEFAULT_BASE_URL = "https://ascii.dev/api/box/v1"
_MAX_COMMAND_TIMEOUT = 600
_FILE_OP_TIMEOUT = 120.0
_HELPER_OUTPUT_CAP = 1024 * 1024
_READY_STATES = frozenset({"ready", "idle", "running"})
_MACHINE_TYPES = frozenset({"small", "default", "large"})


@contextmanager
def _translate_ascii_box_errors(
    operation: str, *, recoverable_statuses: frozenset[int] = frozenset()
) -> Iterator[None]:
    try:
        yield
    except SandboxError:
        raise
    except Exception as e:
        try:
            from ascii_box_sdk.exceptions import ApiException
        except ImportError:
            raise SandboxTerminalError(
                "The Ascii Box SDK is not installed. Install "
                '"apache-airflow-providers-common-ai[sandbox-ascii-box]".'
            ) from e
        if isinstance(e, ApiException):
            status_code = e.status if isinstance(e.status, int) else None
            status = f" (HTTP {status_code})" if status_code is not None else ""
            message = f"Ascii Box could not {operation}{status}."
            if status_code in recoverable_statuses:
                raise SandboxError(message) from e
            raise SandboxTerminalError(message) from e
        raise SandboxTerminalError(f"Ascii Box could not {operation}: {type(e).__name__}.") from e


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
    raise SandboxTerminalError(f"The Ascii Box connection extra {name} must be a boolean.")


class AsciiBoxSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent commands in an `Ascii Box <https://docs.ascii.dev/box/quickstart>`__.

    Box is a hosted cloud computer API: the Airflow worker needs only network
    access and an API key, with no local daemon or host virtualization.
    Credentials resolve lazily from an Airflow connection on first use.

    Connection fields: ``password`` is the Box API key (required). ``host`` may
    override the API base URL. The extra may set ``timeout`` (request timeout in
    seconds) and ``no_env`` (withhold account secrets; default ``true``).

    Box cannot enforce a deny-all or per-domain egress policy. ``create``
    therefore refuses a :class:`~airflow.providers.common.ai.sandbox.SandboxSpec`
    that asks for ``block_network=True`` or ``allow_egress_to``, preserving the
    fail-closed contract. Pass ``SandboxSpec(block_network=False)`` (and set
    that on :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`)
    when open egress is acceptable.

    File paths must resolve under ``/home/user`` or ``/tmp``. Reads and writes
    use Box's native file APIs; directory listings fall back to shell ``find``.

    :param box_conn_id: Airflow connection ID for Ascii Box. ``None`` lets the
        backend read ``BOX_API_KEY`` (and optional ``BOX_BASE_URL``) from the
        environment.
    :param machine_type: Box machine size: ``small``, ``default``, or ``large``.
        Default ``"default"``.
    :param ttl_seconds: Server-side auto-stop TTL in seconds after which the Box
        is archived even if the worker never destroyed it. Default ``3600``.
    :param ready_timeout: Seconds to wait for a newly created Box to become ready.
        Default ``300``.
    :param no_env: When ``True`` (default), create a no-env Box that receives none
        of the account's stored secrets. ``None`` reads the connection extra and
        otherwise defaults to ``True``.
    """

    name = "ascii-box"

    def __init__(
        self,
        box_conn_id: str | None = "ascii_box_default",
        *,
        machine_type: str = "default",
        ttl_seconds: int = 3600,
        ready_timeout: float = 300.0,
        no_env: bool | None = None,
    ) -> None:
        if machine_type not in _MACHINE_TYPES:
            raise ValueError(f"machine_type must be one of {sorted(_MACHINE_TYPES)}, got {machine_type!r}.")
        _validate_positive_finite(ttl_seconds, "ttl_seconds")
        _validate_positive_finite(ready_timeout, "ready_timeout")
        self._box_conn_id = box_conn_id
        self._machine_type = machine_type
        self._ttl_seconds = int(ttl_seconds)
        self._ready_timeout = ready_timeout
        self._no_env = no_env
        self._resolved_no_env = True if no_env is None else no_env
        self._request_timeout: float | None = None
        self._api_client: ApiClient | None = None
        self._box_api: BoxApi | None = None

    def _get_api(self) -> BoxApi:
        if self._box_api is not None:
            return self._box_api
        with _translate_ascii_box_errors("initialize its client"):
            import os

            from ascii_box_sdk import ApiClient, Configuration
            from ascii_box_sdk.api.box_api import BoxApi

            if self._box_conn_id is None:
                api_key = (os.environ.get("BOX_API_KEY") or "").strip()
                if not api_key:
                    raise SandboxTerminalError(
                        "BOX_API_KEY is not set; export it or pass an Airflow connection id."
                    )
                base_url = (os.environ.get("BOX_BASE_URL") or _DEFAULT_BASE_URL).rstrip("/")
                request_timeout = 30.0
                no_env = True if self._no_env is None else self._no_env
            else:
                conn = BaseHook.get_connection(self._box_conn_id)
                api_key = (conn.password or "").strip()
                if not api_key:
                    raise SandboxTerminalError(
                        f"Connection {self._box_conn_id!r} has no password; set it to the Ascii Box API key."
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
                        "The Ascii Box connection extra timeout must be a positive finite number."
                    ) from e
                if self._no_env is None:
                    no_env = _parse_bool(extra.get("no_env", True), "no_env")
                else:
                    no_env = self._no_env

            self._request_timeout = request_timeout
            self._resolved_no_env = no_env
            self._api_client = ApiClient(Configuration(host=base_url, access_token=api_key))
            self._box_api = BoxApi(self._api_client)
            return self._box_api

    def _http_timeout(self, seconds: float) -> float:
        configured = self._request_timeout if self._request_timeout is not None else 30.0
        return max(configured, seconds + 30.0)

    def _wait_until_ready(self, box_id: str) -> None:
        from ascii_box_sdk import wait_until_ready

        with _translate_ascii_box_errors("wait for a sandbox to become ready"):
            wait_until_ready(
                self._get_api(),
                box_id,
                timeout_seconds=self._ready_timeout,
                poll_interval_seconds=2.0,
            )

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        if spec is not None and spec.allow_egress_to:
            raise SandboxTerminalError(
                "The Ascii Box backend cannot apply a per-domain egress allowlist. "
                "Drop allow_egress_to, or use a backend with per-domain network rules."
            )
        if spec is not None and spec.block_network:
            raise SandboxTerminalError(
                "The Ascii Box backend cannot deny outbound network access. Pass "
                "SandboxSpec(block_network=False) when open egress is acceptable, or "
                "use a backend that can enforce a deny-all policy."
            )

        api = self._get_api()
        with _translate_ascii_box_errors("create a sandbox"):
            from ascii_box_sdk.models.create_box_request import CreateBoxRequest
            from ascii_box_sdk.models.update_box_request import UpdateBoxRequest

            created = api.create(
                CreateBoxRequest(
                    type=self._machine_type,
                    ttl_seconds=self._ttl_seconds,
                    no_env=self._resolved_no_env,
                    env=dict(spec.env) if spec is not None and spec.env else None,
                ),
                _request_timeout=self._http_timeout(self._ready_timeout),
            )
            box_id = created.box.id
            with suppress(Exception):
                api.update(
                    box_id,
                    UpdateBoxRequest(name=_new_sandbox_name()),
                    _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT),
                )
        self._wait_until_ready(box_id)
        return box_id

    def _destroy_after_timeout(self, sandbox: str) -> None:
        try:
            self.destroy(sandbox)
        except SandboxError as e:
            raise SandboxTerminalError(
                "The Ascii Box command timed out and deletion of its sandbox could not be confirmed."
            ) from e

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        if timeout > _MAX_COMMAND_TIMEOUT:
            raise SandboxTerminalError(
                f"Ascii Box commands are capped at {_MAX_COMMAND_TIMEOUT} seconds; got timeout={timeout}."
            )
        timeout_seconds = max(1, min(_MAX_COMMAND_TIMEOUT, math.ceil(timeout)))
        api = self._get_api()
        with _translate_ascii_box_errors("run a sandbox command"):
            from ascii_box_sdk.models.command_request import CommandRequest

            result = api.command(
                sandbox,
                CommandRequest(command=command, timeout_seconds=timeout_seconds),
                _request_timeout=self._http_timeout(timeout),
            )

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
        with _translate_ascii_box_errors("confirm that a sandbox still exists"):
            box = self._get_api().get(sandbox, _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT)).box
        if box.state not in _READY_STATES:
            raise SandboxTerminalError(
                f"Ascii Box sandbox {sandbox!r} is not runnable (state={box.state!r})."
            )

    def read_file(self, sandbox: str, path: str, *, max_bytes: int) -> bytes:
        _validate_positive_finite(max_bytes, "max_bytes")
        api = self._get_api()
        try:
            response = api.read_file(
                sandbox,
                path,
                encoding="base64",
                _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT),
            )
        except Exception as e:
            from ascii_box_sdk.exceptions import ApiException

            if isinstance(e, ApiException) and e.status == 404:
                self._confirm_sandbox_exists(sandbox)
                raise SandboxError(f"{path!r} does not exist in the sandbox, or is not readable.") from e
            with _translate_ascii_box_errors("read a sandbox file", recoverable_statuses=frozenset({400})):
                raise

        size = getattr(response, "size", None)
        if isinstance(size, int) and size > max_bytes:
            raise SandboxFileTooLargeError(path, size, max_bytes)
        try:
            data = base64.b64decode(response.content, validate=False)
        except (binascii.Error, ValueError) as e:
            raise SandboxError(f"Could not decode {path!r} from the sandbox.") from e
        if len(data) > max_bytes:
            raise SandboxFileTooLargeError(path, len(data), max_bytes)
        return data

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        quoted = shlex.quote(path)
        self._run_helper(
            sandbox,
            f'mkdir -p -- "$(dirname -- {quoted})"',
            operation=f"create the parent directory for {path!r}",
        )
        api = self._get_api()
        try:
            from ascii_box_sdk.models.file_write_request import FileWriteRequest

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
            from ascii_box_sdk.exceptions import ApiException

            if isinstance(e, ApiException) and e.status == 404:
                self._confirm_sandbox_exists(sandbox)
                raise SandboxError(f"Could not write {path!r} in the sandbox.") from e
            with _translate_ascii_box_errors("write a sandbox file", recoverable_statuses=frozenset({400})):
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
        api = self._get_api()
        with _translate_ascii_box_errors("delete a sandbox"):
            from ascii_box_sdk.exceptions import ApiException

            param = api.api_client.param_serialize(
                method="DELETE",
                resource_path="/boxes/{boxId}",
                path_params={"boxId": sandbox},
                header_params={
                    "Accept": "application/json",
                    "X-Ascii-Confirm-Delete": sandbox,
                },
                auth_settings=["BoxBearerAuth"],
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
