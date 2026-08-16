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
"""islo.dev microVM backend for :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`."""

from __future__ import annotations

import math
import shlex
import time
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from islo import Islo
from islo.core.api_error import ApiError
from islo.errors import NotFoundError

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

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

_TERMINAL_EXEC_STATUSES = frozenset({"completed", "failed", "timeout"})
_POLL_INITIAL = 0.2
_POLL_MAX = 2.0
_POLL_BACKOFF = 1.5
_FILE_OP_TIMEOUT = 120.0
_HELPER_OUTPUT_CAP = 1024 * 1024
_COMMAND_WRAPPER = """\
umask 077
dir="${TMPDIR:-/tmp}/airflow-sandbox-$$"
mkdir "$dir" || exit 70
trap 'rm -rf "$dir"' EXIT HUP INT TERM
mkfifo "$dir/out" "$dir/err" "$dir/out-tail" "$dir/err-tail" || exit 70
tail -c "$2" <"$dir/out-tail" >"$dir/out-result" &
out_tail_pid=$!
tail -c "$2" <"$dir/err-tail" >"$dir/err-result" &
err_tail_pid=$!
tee "$dir/out-tail" <"$dir/out" | wc -c >"$dir/out-count" &
out_drain_pid=$!
tee "$dir/err-tail" <"$dir/err" | wc -c >"$dir/err-count" &
err_drain_pid=$!
sh -lc "$1" >"$dir/out" 2>"$dir/err"
status=$?
wait "$out_drain_pid" "$err_drain_pid" "$out_tail_pid" "$err_tail_pid" || exit 70
if [ "$(cat "$dir/out-count")" -gt "$2" ]; then printf '1\\n'; else printf '0\\n'; fi
cat "$dir/out-result"
if [ "$(cat "$dir/err-count")" -gt "$2" ]; then printf '1\\n' >&2; else printf '0\\n' >&2; fi
cat "$dir/err-result" >&2
exit "$status"
"""


@contextmanager
def _translate_islo_errors(operation: str) -> Iterator[None]:
    try:
        yield
    except SandboxError:
        raise
    except ApiError as e:
        status = f" (HTTP {e.status_code})" if e.status_code is not None else ""
        raise SandboxTerminalError(f"Islo could not {operation}{status}.") from e
    except Exception as e:
        raise SandboxTerminalError(f"Islo could not {operation}: {type(e).__name__}.") from e


def _bound_result_stream(text: str, max_bytes: int, *, server_truncated: bool) -> tuple[str, bool]:
    flag, separator, payload = text.partition("\n")
    if separator and flag in {"0", "1"}:
        truncated = flag == "1" or server_truncated
    else:
        payload = text
        truncated = True

    encoded = payload.encode("utf-8")
    if len(encoded) > max_bytes:
        payload = encoded[-max_bytes:].decode("utf-8", errors="ignore")
        truncated = True
    return payload, truncated


class IsloSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent commands in an `islo.dev <https://islo.dev>`__ microVM.

    Islo is a hosted API with no local daemon or host-virtualisation requirement,
    so this backend works from an Airflow worker running in a container.
    Credentials resolve lazily from an Airflow connection on first use.

    Connection fields: ``password`` is the Islo API key (required), ``host`` the
    compute URL (optional), and the extra may set ``base_url`` and ``timeout``
    (request timeout in seconds).

    File reads and writes use Islo's native streaming APIs. Directory listings
    and command-output bounding require common Unix command-line tools in the
    sandbox image, including ``sh``, ``mkfifo``, ``tail``, ``tee`` and a ``find``
    implementation with ``-printf`` support.

    :param islo_conn_id: Airflow connection ID for Islo. ``None`` lets the SDK
        resolve credentials from its own environment variables (``ISLO_API_KEY``).
    :param image: Sandbox image. ``None`` (default) uses the server default.
    :param vcpus: Number of virtual CPUs. ``None`` uses the server default.
    :param memory_mb: Memory in MB. ``None`` uses the server default.
    :param delete_after: Server-side TTL in seconds after which the sandbox is
        deleted even if the worker never got to destroy it. Default ``3600``.
    """

    name = "islo"

    def __init__(
        self,
        islo_conn_id: str | None = "islo_default",
        *,
        image: str | None = None,
        vcpus: int | None = None,
        memory_mb: int | None = None,
        delete_after: int = 3600,
    ) -> None:
        _validate_positive_finite(delete_after, "delete_after")
        if vcpus is not None:
            _validate_positive_finite(vcpus, "vcpus")
        if memory_mb is not None:
            _validate_positive_finite(memory_mb, "memory_mb")
        if image == "":
            raise ValueError("image must not be empty.")
        self._islo_conn_id = islo_conn_id
        self._image = image
        self._vcpus = vcpus
        self._memory_mb = memory_mb
        self._delete_after = delete_after
        self._client: Islo | None = None

    def _get_client(self) -> Islo:
        if self._client is not None:
            return self._client
        with _translate_islo_errors("initialize its client"):
            if self._islo_conn_id is None:
                self._client = Islo()
                return self._client
            conn = BaseHook.get_connection(self._islo_conn_id)
            api_key = (conn.password or "").strip()
            if not api_key:
                raise SandboxTerminalError(
                    f"Connection {self._islo_conn_id!r} has no password; set it to the Islo API key."
                )
            kwargs: dict[str, Any] = {"api_key": api_key}
            if conn.host:
                kwargs["compute_url"] = conn.host
            extra = conn.extra_dejson
            if extra.get("base_url"):
                kwargs["base_url"] = extra["base_url"]
            if extra.get("timeout") is not None:
                try:
                    request_timeout = float(extra["timeout"])
                    _validate_positive_finite(request_timeout, "connection extra timeout")
                except (TypeError, ValueError) as e:
                    raise SandboxTerminalError(
                        "The Islo connection extra timeout must be a positive finite number."
                    ) from e
                kwargs["timeout"] = request_timeout
            self._client = Islo(**kwargs)
            return self._client

    @staticmethod
    def _request_options(*, timeout: float, chunk_size: int | None = None) -> dict[str, int]:
        options = {"timeout_in_seconds": max(1, math.ceil(timeout)), "max_retries": 0}
        if chunk_size is not None:
            options["chunk_size"] = chunk_size
        return options

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        from islo.types import LifecyclePolicy

        if spec is not None and spec.allow_egress_to:
            raise SandboxTerminalError(
                "The Islo backend cannot apply a per-domain egress allowlist; it can only turn "
                "outbound access on or off. Drop allow_egress_to, or use a backend with "
                "per-domain network rules."
            )
        kwargs: dict[str, Any] = {
            "internet_enabled": False if spec is None else not spec.block_network,
            "lifecycle": LifecyclePolicy(delete_after=self._delete_after),
        }
        if self._image is not None:
            kwargs["image"] = self._image
        if self._vcpus is not None:
            kwargs["vcpus"] = self._vcpus
        if self._memory_mb is not None:
            kwargs["memory_mb"] = self._memory_mb
        if spec is not None and spec.env:
            kwargs["env"] = dict(spec.env)
        with _translate_islo_errors("create a sandbox"):
            sandbox = self._get_client().sandboxes.create_sandbox(
                name=_new_sandbox_name(),
                request_options=self._request_options(timeout=_FILE_OP_TIMEOUT),
                **kwargs,
            )
        return sandbox.name

    def _await_exec(self, sandbox: str, exec_id: str, *, deadline: float) -> Any:
        client = self._get_client()
        interval = _POLL_INITIAL
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            with _translate_islo_errors("poll a sandbox command"):
                result = client.sandboxes.get_exec_result(
                    sandbox,
                    exec_id,
                    request_options=self._request_options(timeout=remaining),
                )
            if result.status in _TERMINAL_EXEC_STATUSES:
                return result
            time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
            interval = min(interval * _POLL_BACKOFF, _POLL_MAX)
        return None

    def _destroy_after_timeout(self, sandbox: str) -> None:
        try:
            self.destroy(sandbox)
        except SandboxError as e:
            raise SandboxTerminalError(
                "The Islo command timed out and deletion of its sandbox could not be confirmed."
            ) from e

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        client = self._get_client()
        deadline = time.monotonic() + timeout
        with _translate_islo_errors("start a sandbox command"):
            response = client.sandboxes.exec_in_sandbox(
                sandbox,
                command=["sh", "-c", _COMMAND_WRAPPER, "airflow-sandbox", command, str(max_output_bytes)],
                timeout_secs=max(1, math.ceil(timeout)),
                request_options=self._request_options(timeout=timeout),
            )
        result = self._await_exec(sandbox, response.exec_id, deadline=deadline)
        if result is None:
            self._destroy_after_timeout(sandbox)
            return SandboxExecResult(
                exit_code=-1, stdout="", stderr="", timed_out=True, sandbox_terminated=True
            )

        server_truncated = bool(getattr(result, "truncated", False))
        stdout, out_truncated = _bound_result_stream(
            result.stdout or "", max_output_bytes, server_truncated=server_truncated
        )
        stderr, err_truncated = _bound_result_stream(
            result.stderr or "", max_output_bytes, server_truncated=server_truncated
        )
        if result.status == "timeout":
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

    def _raise_file_not_found(self, sandbox: str, path: str, error: NotFoundError) -> None:
        with _translate_islo_errors("check a sandbox after a missing file response"):
            self._get_client().sandboxes.get_sandbox(
                sandbox, request_options=self._request_options(timeout=_FILE_OP_TIMEOUT)
            )
        raise SandboxError(f"{path!r} does not exist in the sandbox, or is not readable.") from error

    def read_file(self, sandbox: str, path: str, *, max_bytes: int) -> bytes:
        _validate_positive_finite(max_bytes, "max_bytes")
        chunks = None
        data = bytearray()
        try:
            chunks = self._get_client().sandboxes.download_file(
                sandbox,
                path=path,
                request_options=self._request_options(
                    timeout=_FILE_OP_TIMEOUT, chunk_size=min(65536, max_bytes + 1)
                ),
            )
            for chunk in chunks:
                data.extend(chunk[: max_bytes + 1 - len(data)])
                if len(data) > max_bytes:
                    raise SandboxFileTooLargeError(path, len(data), max_bytes)
        except SandboxFileTooLargeError:
            raise
        except NotFoundError as e:
            self._raise_file_not_found(sandbox, path, e)
        except Exception:
            with _translate_islo_errors("download a sandbox file"):
                raise
        finally:
            close = getattr(chunks, "close", None)
            if close is not None:
                with suppress(Exception):
                    close()
        return bytes(data)

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        quoted = shlex.quote(path)
        self._run_helper(
            sandbox,
            f'mkdir -p -- "$(dirname -- {quoted})"',
            operation=f"create the parent directory for {path!r}",
        )
        with _translate_islo_errors("upload a sandbox file"):
            self._get_client().sandboxes.upload_file(
                sandbox,
                path=path,
                file=("upload", content, "application/octet-stream"),
                request_options=self._request_options(timeout=_FILE_OP_TIMEOUT),
            )

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
        try:
            self._get_client().sandboxes.delete_sandbox(
                sandbox_name=sandbox,
                request_options=self._request_options(timeout=_FILE_OP_TIMEOUT),
            )
        except NotFoundError:
            return
        except Exception:
            with _translate_islo_errors("delete a sandbox"):
                raise
