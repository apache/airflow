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
"""OpenSandbox backend for :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`."""

from __future__ import annotations

import logging
import posixpath
import threading
import time
from contextlib import contextmanager, suppress
from datetime import timedelta
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
    from collections.abc import Callable, Iterator

    from opensandbox import SandboxSync
    from opensandbox.config import ConnectionConfigSync
    from opensandbox.models.sandboxes import NetworkPolicy

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

log = logging.getLogger(__name__)

# Wall-clock allowance past the per-command budget before a streaming call is
# treated as hung. execd enforces the budget itself; this covers the case where
# its events stop arriving at all, which the SDK's read-timeout-free SSE client
# would otherwise wait on forever.
_EXEC_GRACE = 30.0
# A listing is metadata, never bulk content. Past this many entries the model is
# told to narrow the path instead of the worker holding the whole array.
_LIST_DIRECTORY_MAX_ENTRIES = 10_000
# Stamped on every sandbox so an operator on a shared server can find Airflow's
# through ``SandboxFilter(metadata=...)``; the sbx backend uses the sandbox name
# for the same purpose.
_CREATED_BY_METADATA = {"created-by": "airflow"}


def _get_status_code(error: Exception) -> int | None:
    status_code = getattr(error, "status_code", None)
    return status_code if isinstance(status_code, int) else None


@contextmanager
def _translate_opensandbox_errors(
    operation: str, *, recoverable_statuses: frozenset[int] = frozenset()
) -> Iterator[None]:
    try:
        yield
    except SandboxError:
        raise
    except Exception as e:
        try:
            from opensandbox.exceptions import SandboxApiException
        except ImportError:
            raise SandboxTerminalError(
                "The OpenSandbox SDK is not installed. Install "
                '"apache-airflow-providers-common-ai[sandbox-opensandbox]".'
            ) from e
        status_code = _get_status_code(e) if isinstance(e, SandboxApiException) else None
        status = f" (HTTP {status_code})" if status_code is not None else ""
        message = f"OpenSandbox could not {operation}{status}."
        if status_code in recoverable_statuses:
            raise SandboxError(message) from e
        raise SandboxTerminalError(message) from e


class _BoundedTail:
    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._data = bytearray()
        self.truncated = False

    def add_text(self, text: str) -> None:
        self._data.extend(text.encode("utf-8"))
        if len(self._data) > self._max_bytes:
            del self._data[: len(self._data) - self._max_bytes]
            self.truncated = True

    def add_message(self, message: Any) -> None:
        # execd streams one message per output line with the delimiter stripped,
        # so the newline has to be put back or every line runs together. A blank
        # line already arrives as "\n", hence the guard.
        text = message.text
        self.add_text(text if text.endswith("\n") else text + "\n")

    def get_text(self) -> str:
        return bytes(self._data).decode("utf-8", errors="ignore")


class _CallStillRunning(Exception):
    """The call handed to :func:`_call_with_deadline` outlived its deadline."""


def _call_with_deadline(fn: Callable[[], Any], deadline: float) -> Any:
    """
    Run ``fn`` on a daemon thread and wait at most ``deadline`` seconds for it.

    A daemon thread so a call that never returns cannot hold up interpreter
    exit; the caller is expected to make it return by destroying the sandbox.
    """
    outcome: dict[str, Any] = {}

    def target() -> None:
        try:
            outcome["result"] = fn()
        except BaseException as e:
            outcome["error"] = e

    thread = threading.Thread(target=target, name="opensandbox-command", daemon=True)
    thread.start()
    thread.join(deadline)
    if thread.is_alive():
        raise _CallStillRunning
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


def _parse_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise SandboxTerminalError(f"The OpenSandbox connection extra {name} must be a boolean.")


class OpenSandboxBackend(SandboxBackend):
    """
    Run sandbox tools through an OpenSandbox server.

    OpenSandbox supports Docker and Kubernetes runtimes behind the same API.
    Airflow workers need only network access to that API; the OpenSandbox
    deployment owns container provisioning and isolation.

    A generic Airflow connection supplies the server configuration. ``host``
    and ``port`` identify the lifecycle API, ``schema`` selects ``http`` or
    ``https``, and ``password`` carries the optional API key. Connection extras
    may set ``request_timeout`` and ``use_server_proxy``.

    The create API accepts a network policy whether or not the server runs the
    egress sidecar that enforces it, so after creating a sandbox with a
    deny-by-default policy the backend reads the enforced policy back and
    destroys the sandbox if it differs from what
    :class:`~airflow.providers.common.ai.sandbox.SandboxSpec` asked for. The
    fail-closed contract is this backend's to keep, not the server's.

    Command deadlines are enforced by execd. If its event stream stalls, the
    call is abandoned ``_EXEC_GRACE`` seconds past the budget, the sandbox is
    destroyed to end it, and the result reports ``timed_out`` with
    ``sandbox_terminated`` so the toolset provisions a fresh one. Output is
    streamed and each stream is kept to ``max_output_bytes`` on the worker, with
    one caveat: the SDK reassembles a whole output line before handing it over,
    so a single line with no newline in it is resident in full first.

    :param opensandbox_conn_id: Generic Airflow connection ID. ``None`` lets the
        SDK resolve ``OPEN_SANDBOX_DOMAIN`` and ``OPEN_SANDBOX_API_KEY``.
    :param image: Container image used for each sandbox.
    :param cpu: OpenSandbox CPU resource limit.
    :param memory: OpenSandbox memory resource limit.
    :param sandbox_timeout: Server-side sandbox lifetime in seconds.
    :param ready_timeout: Seconds to wait for a newly created sandbox to become healthy.
    :param use_server_proxy: Route sandbox service calls through the lifecycle
        server. ``None`` reads the connection extra and otherwise defaults to ``True``.
    """

    name = "opensandbox"

    def __init__(
        self,
        opensandbox_conn_id: str | None = "opensandbox_default",
        *,
        image: str = "python:3.12-slim",
        cpu: str = "1",
        memory: str = "2Gi",
        sandbox_timeout: float = 3600.0,
        ready_timeout: float = 120.0,
        use_server_proxy: bool | None = None,
    ) -> None:
        if not image:
            raise ValueError("image must not be empty.")
        if not cpu:
            raise ValueError("cpu must not be empty.")
        if not memory:
            raise ValueError("memory must not be empty.")
        _validate_positive_finite(sandbox_timeout, "sandbox_timeout")
        _validate_positive_finite(ready_timeout, "ready_timeout")
        self._opensandbox_conn_id = opensandbox_conn_id
        self._image = image
        self._resource = {"cpu": cpu, "memory": memory}
        self._sandbox_timeout = sandbox_timeout
        self._ready_timeout = ready_timeout
        self._use_server_proxy = use_server_proxy
        self._connection_config: ConnectionConfigSync | None = None
        self._sandboxes: dict[str, SandboxSync] = {}

    def _get_connection_config(self) -> ConnectionConfigSync:
        if self._connection_config is not None:
            return self._connection_config
        with _translate_opensandbox_errors("initialize its client"):
            from opensandbox.config import ConnectionConfigSync

            if self._opensandbox_conn_id is None:
                self._connection_config = ConnectionConfigSync(
                    use_server_proxy=True if self._use_server_proxy is None else self._use_server_proxy
                )
                return self._connection_config

            conn = BaseHook.get_connection(self._opensandbox_conn_id)
            if not conn.host:
                # The SDK would otherwise fall back to localhost:8080 and point
                # the worker at itself.
                raise SandboxTerminalError(
                    f"Connection {self._opensandbox_conn_id!r} has no host; set it to the OpenSandbox "
                    "server address, or pass opensandbox_conn_id=None to use OPEN_SANDBOX_DOMAIN."
                )
            extra = conn.extra_dejson
            request_timeout = extra.get("request_timeout", 30)
            try:
                request_timeout = float(request_timeout)
                _validate_positive_finite(request_timeout, "connection extra request_timeout")
            except (TypeError, ValueError) as e:
                raise SandboxTerminalError(
                    "The OpenSandbox connection extra request_timeout must be a positive finite number."
                ) from e

            use_server_proxy = self._use_server_proxy
            if use_server_proxy is None:
                value = extra.get("use_server_proxy", True)
                use_server_proxy = _parse_bool(value, "use_server_proxy")

            domain = conn.host
            if conn.port:
                domain = f"{domain}:{conn.port}"
            self._connection_config = ConnectionConfigSync(
                api_key=conn.password or None,
                domain=domain,
                protocol=conn.schema or "http",
                request_timeout=timedelta(seconds=request_timeout),
                use_server_proxy=use_server_proxy,
            )
            return self._connection_config

    @staticmethod
    def _get_network_policy(spec: SandboxSpec | None) -> NetworkPolicy | None:
        if spec is None:
            return None
        if not spec.block_network and spec.allow_egress_to:
            raise SandboxTerminalError(
                "SandboxSpec.allow_egress_to only narrows a deny-by-default policy; "
                "set block_network=True or remove the allowlist."
            )
        from opensandbox.models.sandboxes import NetworkPolicy, NetworkRule

        rules = [NetworkRule(action="allow", target=target) for target in spec.allow_egress_to or ()]
        # default_action is declared under its wire alias. populate_by_name means both
        # spellings work at runtime, but only the alias is in the typed signature.
        return NetworkPolicy(
            defaultAction="deny" if spec.block_network else "allow",
            egress=rules or None,
        )

    @staticmethod
    def _verify_network_policy(sandbox: SandboxSync, requested: NetworkPolicy) -> None:
        wanted = {rule.target for rule in requested.egress or ()}
        try:
            enforced = sandbox.get_egress_policy()
            allowed = {rule.target for rule in enforced.egress or () if rule.action == "allow"}
            matches = enforced.default_action == "deny" and allowed == wanted
            detail = f"enforced policy is default {enforced.default_action!r} with allow {sorted(allowed)}"
        except Exception as e:
            matches = False
            detail = f"the policy could not be read back ({type(e).__name__})"
        if matches:
            return
        with suppress(Exception):
            sandbox.destroy()
        raise SandboxTerminalError(
            f"OpenSandbox did not enforce the requested network policy ({detail}), so the sandbox was "
            "destroyed. The server may be running without its egress sidecar."
        )

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        with _translate_opensandbox_errors("create a sandbox"):
            from opensandbox import SandboxSync

            network_policy = self._get_network_policy(spec)
            sandbox = SandboxSync.create(
                self._image,
                timeout=timedelta(seconds=self._sandbox_timeout),
                ready_timeout=timedelta(seconds=self._ready_timeout),
                env=dict(spec.env) if spec is not None and spec.env else None,
                metadata={**_CREATED_BY_METADATA, "name": _new_sandbox_name()},
                resource=dict(self._resource),
                network_policy=network_policy,
                connection_config=self._get_connection_config(),
            )
        if network_policy is not None and network_policy.default_action == "deny":
            self._verify_network_policy(sandbox, network_policy)
        self._sandboxes[sandbox.id] = sandbox
        return sandbox.id

    def _get_sandbox(self, sandbox_id: str) -> SandboxSync:
        if sandbox := self._sandboxes.get(sandbox_id):
            return sandbox
        with _translate_opensandbox_errors("connect to a sandbox"):
            from opensandbox import SandboxSync

            sandbox = SandboxSync.connect(
                sandbox_id,
                connection_config=self._get_connection_config(),
                connect_timeout=timedelta(seconds=self._ready_timeout),
            )
        self._sandboxes[sandbox_id] = sandbox
        return sandbox

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        # Reconnecting an uncached handle polls for readiness, which is not the
        # command's time to spend against ``timeout``.
        sandbox_client = self._get_sandbox(sandbox)
        stdout = _BoundedTail(max_output_bytes)
        stderr = _BoundedTail(max_output_bytes)
        started = time.monotonic()
        with _translate_opensandbox_errors("run a sandbox command"):
            from opensandbox.models.execd import RunCommandOpts
            from opensandbox.models.execd_sync import ExecutionHandlersSync

            opts = RunCommandOpts(timeout=timedelta(seconds=timeout))
            handlers = ExecutionHandlersSync(
                on_stdout=stdout.add_message,
                on_stderr=stderr.add_message,
                skip_accumulation=True,
            )
            try:
                execution = _call_with_deadline(
                    lambda: sandbox_client.commands.run(command, opts=opts, handlers=handlers),
                    timeout + _EXEC_GRACE,
                )
            except _CallStillRunning:
                return self._abandon_command(sandbox, stdout, stderr)
        elapsed = time.monotonic() - started

        exit_code = execution.exit_code
        if exit_code is None:
            # execd reports no exit status for a foreground run. The SDK parses
            # one out of the free-text ``error.value`` and yields None when that
            # text is prose, so None means "unknown", never "sandbox unusable".
            exit_code = 1 if execution.error is not None else -1
        if execution.error is not None:
            if not stderr.get_text():
                stderr.add_text("\n".join(execution.error.traceback) or execution.error.value)
        elif execution.exit_code is None:
            stderr.add_text("The command ended without reporting an exit status.")
        return SandboxExecResult(
            exit_code=exit_code,
            stdout=stdout.get_text(),
            stderr=stderr.get_text(),
            timed_out=exit_code != 0 and elapsed >= timeout,
            stdout_truncated=stdout.truncated,
            stderr_truncated=stderr.truncated,
        )

    def _abandon_command(self, sandbox: str, stdout: _BoundedTail, stderr: _BoundedTail) -> SandboxExecResult:
        # Destroying the sandbox is what ends the stalled stream. If that fails
        # the server-side lifetime reclaims it; either way this sandbox is not
        # one to reuse, and the toolset is told so.
        try:
            self.destroy(sandbox)
        except SandboxError:
            log.warning(
                "Timed out running a command in OpenSandbox sandbox %s and could not destroy it; "
                "its server-side lifetime will reclaim it",
                sandbox,
                exc_info=True,
            )
        return SandboxExecResult(
            exit_code=-1,
            stdout=stdout.get_text(),
            stderr=stderr.get_text(),
            timed_out=True,
            stdout_truncated=stdout.truncated,
            stderr_truncated=stderr.truncated,
            sandbox_terminated=True,
        )

    @staticmethod
    def _confirm_sandbox_exists(sandbox: SandboxSync) -> None:
        with _translate_opensandbox_errors("confirm that a sandbox still exists"):
            sandbox.get_info()

    @staticmethod
    def _get_file_size(sandbox: SandboxSync, path: str, *, at_least: int) -> int:
        """Return the file's size for the too-large message, never less than what was already read."""
        try:
            info = sandbox.files.get_file_info([path])
            entry = info.get(path) or next(iter(info.values()))
        except Exception:
            return at_least
        return max(entry.size, at_least)

    def read_file(self, sandbox: str, path: str, *, max_bytes: int) -> bytes:
        _validate_positive_finite(max_bytes, "max_bytes")
        sandbox_client = self._get_sandbox(sandbox)
        chunks = None
        data = bytearray()
        try:
            chunks = sandbox_client.files.read_bytes_stream(
                path,
                chunk_size=min(65536, max_bytes + 1),
                range_header=f"bytes=0-{max_bytes}",
            )
            for chunk in chunks:
                data.extend(chunk[: max_bytes + 1 - len(data)])
                if len(data) > max_bytes:
                    size = self._get_file_size(sandbox_client, path, at_least=len(data))
                    raise SandboxFileTooLargeError(path, size, max_bytes)
        except SandboxFileTooLargeError:
            raise
        except Exception as e:
            if _get_status_code(e) == 404:
                self._confirm_sandbox_exists(sandbox_client)
                raise SandboxError(f"{path!r} does not exist in the sandbox, or is not readable.") from e
            with _translate_opensandbox_errors("read a sandbox file", recoverable_statuses=frozenset({400})):
                raise
        finally:
            close = getattr(chunks, "close", None)
            if close is not None:
                with suppress(Exception):
                    close()
        return bytes(data)

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        sandbox_client = self._get_sandbox(sandbox)
        try:
            from opensandbox.models.filesystem import WriteEntry

            parent = posixpath.dirname(path)
            if parent and parent != "/":
                sandbox_client.files.create_directories([WriteEntry(path=parent, mode=755)])
            sandbox_client.files.write_file(path, content, mode=644)
        except Exception as e:
            if _get_status_code(e) == 404:
                self._confirm_sandbox_exists(sandbox_client)
                raise SandboxError(f"Could not write {path!r} in the sandbox.") from e
            with _translate_opensandbox_errors("write a sandbox file", recoverable_statuses=frozenset({400})):
                raise

    def list_directory(self, sandbox: str, path: str) -> list[tuple[str, bool]]:
        sandbox_client = self._get_sandbox(sandbox)
        try:
            from opensandbox.models.filesystem import DirectoryListEntry

            entries = sandbox_client.files.list_directory(DirectoryListEntry(path=path, depth=1))
        except Exception as e:
            if _get_status_code(e) == 404:
                self._confirm_sandbox_exists(sandbox_client)
                raise SandboxError(f"{path!r} does not exist in the sandbox, or is not readable.") from e
            with _translate_opensandbox_errors(
                "list a sandbox directory", recoverable_statuses=frozenset({400})
            ):
                raise
        # The SDK has parsed the whole listing by now; the cap bounds what goes
        # any further and gives the model something to do about it.
        if len(entries) > _LIST_DIRECTORY_MAX_ENTRIES:
            raise SandboxError(
                f"{path!r} has more than {_LIST_DIRECTORY_MAX_ENTRIES} entries; list a subdirectory "
                "instead, or use a shell command such as `ls | head`."
            )
        return [
            (posixpath.basename(entry.path.rstrip("/")), entry.entry_type == "directory") for entry in entries
        ]

    def destroy(self, sandbox: str) -> None:
        sandbox_client = self._sandboxes.pop(sandbox, None)
        try:
            if sandbox_client is None:
                from opensandbox import SandboxSync

                # No readiness poll: a paused or unhealthy sandbox is still one
                # that must be destroyable, and waiting on it only delays that.
                sandbox_client = SandboxSync.connect(
                    sandbox,
                    connection_config=self._get_connection_config(),
                    connect_timeout=timedelta(seconds=self._ready_timeout),
                    skip_health_check=True,
                )
            sandbox_client.destroy()
        except Exception as e:
            if _get_status_code(e) == 404:
                return
            with _translate_opensandbox_errors("destroy a sandbox"):
                raise
