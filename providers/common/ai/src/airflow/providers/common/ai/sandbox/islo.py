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

import logging
import math
import shlex
import time
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.hooks.islo import IsloHook
from airflow.providers.common.ai.sandbox.base import (
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxFileTooLargeError,
    SandboxTerminalError,
    _new_sandbox_name,
    _validate_positive_finite,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from islo import Islo
    from islo.errors import NotFoundError

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

log = logging.getLogger(__name__)

# The vendor types ``status`` as a plain string on a model that allows extra
# fields, so the vocabulary is open-ended. Track the statuses that mean "not
# finished yet" instead of the ones that mean "finished": an unrecognised status
# then reads as terminal, which surfaces a failure, rather than as still-running,
# which would poll to the deadline and cost the agent its sandbox.
_RUNNING_EXEC_STATUSES = frozenset({"pending", "queued", "starting", "running"})
# Sandbox statuses that cannot serve a request. Same open vocabulary, opposite
# bias: after a missing-file response the sandbox was reachable a moment ago, so
# an unknown status reads as usable and only a known-dead one fails the task.
_UNUSABLE_SANDBOX_STATUSES = frozenset({"stopping", "stopped", "deleting", "deleted", "error", "failed"})
_AUTO_RESUME_POLICIES = frozenset({"never", "on_activity"})
_POLL_INITIAL = 0.2
_POLL_MAX = 2.0
_POLL_BACKOFF = 1.5
# A poll request's HTTP timeout never drops below this: the last poll before
# the deadline must not get a one-second budget that a slow API turns into a
# task failure instead of the timeout result.
_POLL_HTTP_TIMEOUT_MIN = 5.0
_FILE_OP_TIMEOUT = 120.0
# Measured against the compute API: each stream is capped at exactly this many
# bytes with the tail kept, and one ``truncated`` flag covers both streams. The
# wrapper never asks for more than this per stream, so the server's cap is not
# reached by wrapper output and its flag stays a fallback.
_SERVER_STREAM_CAP = 1024 * 1024
_HELPER_OUTPUT_CAP = _SERVER_STREAM_CAP - 1
# Runs the agent's command with each stream captured to a scratch file, then
# emits only the last ``$2`` bytes of each. The tail is what the model needs (a
# traceback and the exit status live at the end), and bounding inside the guest
# keeps the transfer and the worker's copy at the caller's budget rather than
# the server's 1 MiB.
#
# ``sh -c`` rather than a login shell: the spec's variables are the process
# environment of every exec, and ``/etc/profile`` would run after them and win
# for anything it also exports.
#
# Deliberately free of fifos, background jobs and ``wait``: a command that
# backgrounds a process hands it the capture descriptor, so anything waiting for
# end-of-input would block until that process exits -- ``sleep 20 & echo
# started`` took 20s in a real microVM before this. Redirecting to files means
# only the foreground command is waited on. The cost is that the scratch file
# grows with total output, on the sandbox's own ephemeral disk.
_COMMAND_WRAPPER = """\
dir="${TMPDIR:-/tmp}/airflow-sandbox-$$"
mkdir -m 700 "$dir" || exit 70
trap 'rm -rf "$dir"' EXIT
trap 'rm -rf "$dir"; exit 143' HUP INT TERM
sh -c "$1" >"$dir/out" 2>"$dir/err"
status=$?
tail -c "$2" <"$dir/out"
tail -c "$2" <"$dir/err" >&2
exit "$status"
"""


@contextmanager
def _translate_islo_errors(operation: str) -> Iterator[None]:
    try:
        yield
    except SandboxError:
        raise
    except Exception as e:
        try:
            from islo.core.api_error import ApiError
        except ImportError:
            raise SandboxTerminalError(
                'The Islo SDK is not installed. Install "apache-airflow-providers-common-ai[sandbox-islo]".'
            ) from e
        if isinstance(e, ApiError):
            status = f" (HTTP {e.status_code})" if e.status_code is not None else ""
            raise SandboxTerminalError(f"Islo could not {operation}{status}.") from e
        raise SandboxTerminalError(f"Islo could not {operation}: {type(e).__name__}.") from e


def _is_transient_error(error: Exception) -> bool:
    """Whether a failed call says nothing about the command: a 5xx, a 429, or no response at all."""
    import httpx
    from islo.core.api_error import ApiError

    if isinstance(error, ApiError):
        return error.status_code is None or error.status_code == 429 or error.status_code >= 500
    return isinstance(error, httpx.TransportError)


def _bound_result_stream(text: str, max_bytes: int, *, server_truncated: bool) -> tuple[str, bool]:
    """
    Trim one stream to ``max_bytes``, keeping the tail, and report whether bytes were dropped.

    The sandbox is asked for one byte more than the budget, so a stream that
    comes back over budget is the signal that the guest had more to give. The
    server's own flag covers both streams at once, so it is only attributed to a
    stream that sits at the server's cap.
    """
    encoded = text.encode("utf-8", errors="surrogatepass")
    truncated = server_truncated and len(encoded) >= _SERVER_STREAM_CAP
    if len(encoded) > max_bytes:
        encoded = encoded[-max_bytes:]
        truncated = True
        # A byte-aligned cut usually lands mid-record, and the model must never
        # be handed a fragment presented as a whole line.
        newline = encoded.find(b"\n")
        if newline != -1:
            encoded = encoded[newline + 1 :]
    return encoded.decode("utf-8", errors="replace"), truncated


class IsloSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent commands in an `islo.dev <https://islo.dev>`__ microVM.

    Islo is a hosted API with no local daemon or host-virtualization requirement,
    so this backend works from an Airflow worker running in a container.
    Credentials resolve lazily on first use through
    :class:`~airflow.providers.common.ai.hooks.islo.IsloHook` and its ``islo``
    connection type.

    File reads and writes use Islo's native streaming APIs. Directory listings
    and command-output bounding need ``sh``, ``tail``, ``stat`` and GNU ``find``
    in the sandbox image, which the server default image and any Debian or
    Ubuntu based image provide. Each command's output is captured to a scratch
    file in the sandbox and only its last ``max_output_bytes`` are returned, so
    the worker sees a bounded tail while the sandbox's own ephemeral disk
    absorbs the rest.

    Islo sets ``PATH`` for every command itself and drops a ``PATH`` given at
    creation, so a spec that names it is refused rather than silently ignored.

    :param islo_conn_id: Airflow connection ID for Islo. ``None`` lets the SDK
        resolve credentials from its own environment variables (``ISLO_API_KEY``,
        ``ISLO_BASE_URL``, ``ISLO_COMPUTE_URL``).
    :param image: Sandbox image. ``None`` (default) uses the server default.
    :param vcpus: Number of virtual CPUs. ``None`` uses the server default.
    :param memory_mb: Memory in MB. ``None`` uses the server default.
    :param pause_after_idle: Seconds without a command or file operation after
        which the server pauses the microVM and releases its compute. ``None``
        disables it. Default ``600``.
    :param auto_resume: ``"on_activity"`` (default) resumes a paused sandbox on
        the next command or file operation; ``"never"`` leaves it paused, and the
        backend then treats a paused sandbox as unusable.
    :param delete_after: Seconds after *creation* at which the server deletes the
        sandbox whether or not it is in use. ``None`` disables it. Default
        ``86400``.
    """

    name = "islo"

    def __init__(
        self,
        islo_conn_id: str | None = "islo_default",
        *,
        image: str | None = None,
        vcpus: int | None = None,
        memory_mb: int | None = None,
        pause_after_idle: int | None = 600,
        auto_resume: str = "on_activity",
        delete_after: int | None = 86400,
    ) -> None:
        if pause_after_idle is not None:
            _validate_positive_finite(pause_after_idle, "pause_after_idle")
        if delete_after is not None:
            _validate_positive_finite(delete_after, "delete_after")
        if auto_resume not in _AUTO_RESUME_POLICIES:
            raise ValueError(
                f"auto_resume must be one of {sorted(_AUTO_RESUME_POLICIES)}, got {auto_resume!r}."
            )
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
        self._pause_after_idle = pause_after_idle
        self._auto_resume = auto_resume
        self._delete_after = delete_after
        self._client: Islo | None = None

    def _get_client(self) -> Islo:
        if self._client is not None:
            return self._client
        with _translate_islo_errors("initialize its client"):
            if self._islo_conn_id is None:
                from islo import Islo

                self._client = Islo()
            else:
                try:
                    self._client = IsloHook(islo_conn_id=self._islo_conn_id).get_conn()
                except ValueError as e:
                    raise SandboxTerminalError(str(e)) from e
        return self._client

    @staticmethod
    def _request_options(
        *, timeout: float, chunk_size: int | None = None, max_retries: int | None = None
    ) -> dict[str, int]:
        # ``max_retries`` is left to the SDK's default unless asked for: passing
        # 0 would switch off the two transport retries it does on its own.
        options = {"timeout_in_seconds": max(1, math.ceil(timeout))}
        if chunk_size is not None:
            options["chunk_size"] = chunk_size
        if max_retries is not None:
            options["max_retries"] = max_retries
        return options

    def _ensure_sandbox_usable(self, info: Any) -> None:
        status = getattr(info, "status", None)
        unusable = getattr(info, "deleted_at", None) is not None or status in _UNUSABLE_SANDBOX_STATUSES
        if status == "paused" and self._auto_resume != "on_activity":
            unusable = True
        if unusable:
            raise SandboxTerminalError(
                f"Islo sandbox {getattr(info, 'name', '?')!r} cannot serve requests (status={status!r})."
            )

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        if spec is not None and spec.allow_egress_to:
            raise SandboxTerminalError(
                "The Islo backend cannot apply a per-domain egress allowlist; it can only turn "
                "outbound access on or off. Drop allow_egress_to, or use a backend with "
                "per-domain network rules."
            )
        if spec is not None and spec.env and "PATH" in spec.env:
            raise SandboxTerminalError(
                "Islo sets PATH for every command itself and drops a PATH given at creation; "
                "remove PATH from SandboxSpec.env."
            )
        with _translate_islo_errors("create a sandbox"):
            from islo.types import AutoResumePolicy, LifecyclePolicy

            kwargs: dict[str, Any] = {
                "internet_enabled": False if spec is None else not spec.block_network,
                "lifecycle": LifecyclePolicy(
                    pause_after_idle=self._pause_after_idle,
                    auto_resume=AutoResumePolicy(self._auto_resume),
                    delete_after=self._delete_after,
                ),
            }
            if self._image is not None:
                kwargs["image"] = self._image
            if self._vcpus is not None:
                kwargs["vcpus"] = self._vcpus
            if self._memory_mb is not None:
                kwargs["memory_mb"] = self._memory_mb
            if spec is not None and spec.env:
                # Verified against a live microVM: variables set here are the
                # process environment of every later exec.
                kwargs["env"] = dict(spec.env)
            # Bind the name before the call. If creation fails after the server
            # provisioned the microVM -- a response timeout, a reset, a 5xx --
            # this is the only handle that can still delete it, and without it
            # the leak is neither cleanable nor traceable to a run.
            name = _new_sandbox_name()
            try:
                sandbox = self._get_client().sandboxes.create_sandbox(
                    name=name,
                    request_options=self._request_options(timeout=_FILE_OP_TIMEOUT),
                    **kwargs,
                )
            except BaseException:
                with suppress(Exception):
                    self.destroy(name)
                raise
        try:
            self._ensure_sandbox_usable(sandbox)
        except SandboxTerminalError:
            with suppress(Exception):
                self.destroy(name)
            raise
        return sandbox.name

    def _await_exec(self, sandbox: str, exec_id: str, *, deadline: float) -> Any:
        client = self._get_client()
        interval = _POLL_INITIAL
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                result = client.sandboxes.get_exec_result(
                    sandbox,
                    exec_id,
                    request_options=self._request_options(timeout=max(_POLL_HTTP_TIMEOUT_MIN, remaining)),
                )
            except Exception as e:
                if not _is_transient_error(e):
                    with _translate_islo_errors("poll a sandbox command"):
                        raise
                # One failed poll says nothing about the command; the deadline decides.
                last_error = e
            else:
                last_error = None
                if result.status not in _RUNNING_EXEC_STATUSES:
                    return result
            time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
            interval = min(interval * _POLL_BACKOFF, _POLL_MAX)
        if last_error is not None:
            # Nothing was heard after the last failure, so "timed out" would be a guess.
            with _translate_islo_errors("poll a sandbox command"):
                raise last_error
        return None

    def _destroy_after_timeout(self, sandbox: str) -> None:
        try:
            self.destroy(sandbox)
        except SandboxError:
            # Warn rather than fail the task: the command merely ran long, and
            # the lifecycle policy reclaims the microVM whether or not this call
            # landed. Failing here would turn a timeout the model can react to
            # into a task failure over a transient error.
            log.warning(
                "Timed out running a command in Islo sandbox %s and could not confirm its deletion; "
                "the server-side lifecycle policy will reclaim it.",
                sandbox,
                exc_info=True,
            )

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        client = self._get_client()
        # The server returns at most _SERVER_STREAM_CAP bytes per stream, so a
        # larger budget cannot be honoured and is clamped below it.
        budget = min(max_output_bytes, _SERVER_STREAM_CAP - 1)
        deadline = time.monotonic() + timeout
        with _translate_islo_errors("start a sandbox command"):
            response = client.sandboxes.exec_in_sandbox(
                sandbox,
                # One byte over the budget, so a stream that comes back over it
                # is proof the guest had more to give.
                command=[
                    "sh",
                    "-c",
                    _COMMAND_WRAPPER,
                    "airflow-sandbox",
                    command,
                    str(budget + 1),
                ],
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
            result.stdout or "", budget, server_truncated=server_truncated
        )
        stderr, err_truncated = _bound_result_stream(
            result.stderr or "", budget, server_truncated=server_truncated
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

    def _run_helper(self, sandbox: str, script: str, *, operation: str) -> SandboxExecResult:
        result = self.run_command(
            sandbox, script, timeout=_FILE_OP_TIMEOUT, max_output_bytes=_HELPER_OUTPUT_CAP
        )
        if result.timed_out or result.sandbox_terminated:
            raise SandboxTerminalError(f"The sandbox was destroyed after it timed out while {operation}.")
        if result.exit_code:
            raise SandboxError(result.stderr.strip() or f"Could not {operation}.")
        return result

    def _raise_file_not_found(self, sandbox: str, path: str, error: NotFoundError) -> None:
        with _translate_islo_errors("check a sandbox after a missing file response"):
            info = self._get_client().sandboxes.get_sandbox(
                sandbox, request_options=self._request_options(timeout=_FILE_OP_TIMEOUT)
            )
        self._ensure_sandbox_usable(info)
        raise SandboxError(f"{path!r} does not exist in the sandbox, or is not readable.") from error

    def _get_file_size(self, sandbox: str, path: str) -> int | None:
        """Ask the guest for the file's size; ``None`` when it cannot say."""
        try:
            result = self._run_helper(
                sandbox, f"stat -Lc %s -- {shlex.quote(path)}", operation=f"size {path!r}"
            )
            return int(result.stdout.strip())
        except SandboxTerminalError:
            raise
        except (SandboxError, ValueError):
            return None

    def read_file(self, sandbox: str, path: str, *, max_bytes: int) -> bytes:
        _validate_positive_finite(max_bytes, "max_bytes")
        client = self._get_client()
        from islo.errors import NotFoundError

        chunks = None
        data = bytearray()
        over_budget = False
        try:
            chunks = client.sandboxes.download_file(
                sandbox,
                path=path,
                request_options=self._request_options(
                    timeout=_FILE_OP_TIMEOUT, chunk_size=min(65536, max_bytes + 1)
                ),
            )
            for chunk in chunks:
                data.extend(chunk[: max_bytes + 1 - len(data)])
                if len(data) > max_bytes:
                    over_budget = True
                    break
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
        if over_budget:
            # The download API has no size endpoint, so the guest's own stat
            # supplies the number the model plans around. Without it, say only
            # what is known rather than report the budget back as the size.
            size = self._get_file_size(sandbox, path)
            if size is None:
                raise SandboxError(
                    f"{path!r} is larger than the {max_bytes} byte read limit. Read just the part you "
                    "need with a shell command instead (e.g. head, tail, sed -n, or grep)."
                )
            raise SandboxFileTooLargeError(path, max(size, len(data)), max_bytes)
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
        result = self._run_helper(
            sandbox,
            f"find -- {quoted} -maxdepth 1 -mindepth 1 -printf '%y %f\\0'",
            operation=f"list {path!r}",
        )
        records = result.stdout.split("\0")
        if result.stdout_truncated and records:
            # Both the wrapper and the server keep the tail, so the cut is at
            # the head: drop the leading record rather than report a mangled
            # entry name the model cannot open.
            records = records[1:]
        entries: list[tuple[str, bool]] = []
        for record in records:
            if not record:
                continue
            kind, _, name = record.partition(" ")
            if name:
                entries.append((name, kind == "d"))
        return entries

    def destroy(self, sandbox: str) -> None:
        client = self._get_client()
        from islo.errors import NotFoundError

        try:
            client.sandboxes.delete_sandbox(
                sandbox_name=sandbox,
                # Deletion is idempotent, and it is the one call whose failure
                # strands a microVM, so make the SDK's retries explicit here.
                request_options=self._request_options(timeout=_FILE_OP_TIMEOUT, max_retries=2),
            )
        except NotFoundError:
            return
        except Exception:
            with _translate_islo_errors("delete a sandbox"):
                raise
