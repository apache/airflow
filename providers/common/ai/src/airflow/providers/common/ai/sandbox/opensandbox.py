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

import posixpath
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
    _validate_positive_finite,
)
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from collections.abc import Iterator

    from opensandbox import SandboxSync
    from opensandbox.config import ConnectionConfigSync
    from opensandbox.models.sandboxes import NetworkPolicy

    from airflow.providers.common.ai.sandbox.base import SandboxSpec


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
        self.add_text(message.text)

    def get_text(self) -> str:
        return bytes(self._data).decode("utf-8", errors="ignore")


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

    Strict network policy requires the OpenSandbox egress sidecar. The server
    rejects a requested policy when that component or runtime support is
    unavailable, preserving :class:`~airflow.providers.common.ai.sandbox.SandboxSpec`'s
    fail-closed contract.

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
            if domain and conn.port:
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
        return NetworkPolicy(
            default_action="deny" if spec.block_network else "allow",
            egress=rules or None,
        )

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        with _translate_opensandbox_errors("create a sandbox"):
            from opensandbox import SandboxSync

            sandbox = SandboxSync.create(
                self._image,
                timeout=timedelta(seconds=self._sandbox_timeout),
                ready_timeout=timedelta(seconds=self._ready_timeout),
                env=dict(spec.env) if spec is not None and spec.env else None,
                resource=dict(self._resource),
                network_policy=self._get_network_policy(spec),
                connection_config=self._get_connection_config(),
            )
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
        stdout = _BoundedTail(max_output_bytes)
        stderr = _BoundedTail(max_output_bytes)
        started = time.monotonic()
        with _translate_opensandbox_errors("run a sandbox command"):
            from opensandbox.models.execd import RunCommandOpts
            from opensandbox.models.execd_sync import ExecutionHandlersSync

            execution = self._get_sandbox(sandbox).commands.run(
                command,
                opts=RunCommandOpts(timeout=timedelta(seconds=timeout)),
                handlers=ExecutionHandlersSync(
                    on_stdout=stdout.add_message,
                    on_stderr=stderr.add_message,
                    skip_accumulation=True,
                ),
            )

        if execution.exit_code is None:
            raise SandboxTerminalError("OpenSandbox returned no terminal status for the command.")
        if execution.error is not None and not stderr.get_text():
            details = "\n".join(execution.error.traceback) or execution.error.value
            stderr.add_text(details)
        timed_out = execution.exit_code < 0 and time.monotonic() - started >= timeout
        return SandboxExecResult(
            exit_code=execution.exit_code,
            stdout=stdout.get_text(),
            stderr=stderr.get_text(),
            timed_out=timed_out,
            stdout_truncated=stdout.truncated,
            stderr_truncated=stderr.truncated,
        )

    @staticmethod
    def _confirm_sandbox_exists(sandbox: SandboxSync) -> None:
        with _translate_opensandbox_errors("confirm that a sandbox still exists"):
            sandbox.get_info()

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
                    raise SandboxFileTooLargeError(path, len(data), max_bytes)
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
        return [
            (posixpath.basename(entry.path.rstrip("/")), entry.entry_type == "directory") for entry in entries
        ]

    def destroy(self, sandbox: str) -> None:
        sandbox_client = self._sandboxes.pop(sandbox, None)
        try:
            if sandbox_client is None:
                from opensandbox import SandboxSync

                sandbox_client = SandboxSync.connect(
                    sandbox,
                    connection_config=self._get_connection_config(),
                    connect_timeout=timedelta(seconds=self._ready_timeout),
                )
            sandbox_client.destroy()
        except Exception as e:
            if _get_status_code(e) == 404:
                return
            with _translate_opensandbox_errors("destroy a sandbox"):
                raise
