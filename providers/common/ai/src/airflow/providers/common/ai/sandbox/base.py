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
"""Vendor-neutral contract for running agent commands and file operations in an isolated sandbox."""

from __future__ import annotations

import base64
import binascii
import math
import shlex
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class SandboxError(Exception):
    """
    A sandbox operation failed in a way the agent may be able to work around.

    The toolset turns this into a ``ModelRetry`` so the model can adjust and try
    again within the run (a bad path, a command the image cannot run).
    """


class SandboxTerminalError(SandboxError):
    """
    The sandbox is unusable and retrying the same call cannot succeed.

    Credentials were rejected, the daemon is unreachable, the sandbox is gone.
    The toolset lets this propagate and fail the task, so Airflow's own retry
    handles it rather than the model burning its retry budget.
    """


class SandboxFileTooLargeError(SandboxError):
    """A file is larger than the caller's read budget, so it was not transferred."""

    def __init__(self, path: str, size_bytes: int, max_bytes: int) -> None:
        self.path = path
        self.size_bytes = size_bytes
        self.max_bytes = max_bytes
        super().__init__(f"{path!r} is {size_bytes} bytes, over the {max_bytes} byte read limit.")


# Bounded budget for the shell helpers behind the default file operations.
_FILE_OP_TIMEOUT = 120.0
# They return a status or a listing, never bulk content, so a small cap bounds
# what a hostile guest can push into worker memory.
_FILE_OP_OUTPUT_CAP = 1024 * 1024


def _validate_positive_finite(value: float, name: str) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number, got {value!r}.")


def _new_sandbox_name() -> str:
    """Generate a unique sandbox name, ``airflow-sandbox-`` prefixed for correlation and cleanup."""
    return f"airflow-sandbox-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True)
class SandboxSpec:
    """
    What a single sandbox should be provisioned with.

    Passed to :meth:`SandboxBackend.create`. Every field is optional and a
    backend may not be able to honor all of them; a backend that cannot enforce
    a field it was given must raise rather than silently ignore it, so a DAG
    author never believes a restriction is in force when it is not.

    :param env: Environment variables to set inside the sandbox. Airflow never
        populates this itself -- the DAG author decides what, if anything, the
        sandbox is given. Anything placed here is visible to model-generated
        code, so scope it to what that code legitimately needs.
    :param block_network: Deny all outbound network access. Defaults to ``True``:
        an isolated sandbox that cannot phone home is the safe starting point,
        and egress is opened deliberately.
    :param allow_egress_to: Hostnames the sandbox may reach when
        ``block_network`` is ``True``. An empty or unset value with
        ``block_network=True`` means no egress at all.
    """

    env: Mapping[str, str] | None = None
    block_network: bool = True
    allow_egress_to: Sequence[str] | None = None


@dataclass(frozen=True)
class SandboxExecResult:
    """
    Outcome of one command executed inside a sandbox.

    ``timed_out`` means the command hit the budget, so ``exit_code`` carries no
    meaning. ``stdout_truncated`` / ``stderr_truncated`` mean the backend
    dropped bytes while reading that stream, before any model-facing formatting.
    ``sandbox_terminated`` means the backend destroyed the sandbox to stop the
    command, so the toolset must provision a fresh one before the next call.
    """

    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool = False
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    sandbox_terminated: bool = False


class SandboxBackend(ABC):
    """
    Contract for running commands and file operations in an isolated sandbox.

    The lifecycle is create -> (any number of operations) -> destroy, driven by
    :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`.
    The four operation methods are named after the four tools the toolset
    exposes, so the mapping from a model-facing tool to the backend call behind
    it is literal; ``create`` and ``destroy`` are lifecycle and have no tool.

    Implementations must be cheap to construct, because constructors run at
    Dag-parse time: resolve credentials and open connections lazily, on first
    use. ``destroy`` must be idempotent -- destroying an already-gone sandbox is
    not an error. All methods are synchronous; the toolset offloads them to a
    thread, so a call may block for as long as its timeout allows.

    Raise :class:`SandboxError` for a failure the model could work around, and
    :class:`SandboxTerminalError` for one it cannot.
    """

    name: ClassVar[str]
    """Short backend identifier (e.g. ``"sbx"``), used in the toolset id."""

    @abstractmethod
    def create(self, *, spec: SandboxSpec | None = None) -> str:
        """
        Provision one sandbox and return its handle (name or id).

        ``spec`` of ``None`` means "no requirements stated": the backend applies
        its own defaults and makes no guarantee. It is not the same as a default
        :class:`SandboxSpec`, which is an explicit request for an isolated
        sandbox. The toolset always sends a concrete spec, so ``None`` only
        reaches a backend a caller drives directly.

        Raise :class:`SandboxTerminalError` if ``spec`` asks for something this
        backend cannot enforce, rather than provisioning something weaker than
        was asked for. It is terminal rather than recoverable because it states
        a configuration fact the model cannot see and cannot fix by retrying.
        """

    @abstractmethod
    def run_command(
        self,
        sandbox: str,
        command: str,
        *,
        timeout: float,
        max_output_bytes: int,
    ) -> SandboxExecResult:
        """
        Run ``command`` through a shell in the sandbox, bounded by ``timeout`` seconds.

        ``max_output_bytes`` bounds what the backend retains *per stream* while
        reading, so unbounded command output cannot exhaust worker memory before
        the toolset gets a chance to format it.
        """

    # ------------------------------------------------------------------
    # File operations.
    #
    # Concrete, not abstract: every one of these is expressible as a shell
    # command, so a backend only has to implement ``run_command`` to get all
    # three. Override them when the vendor exposes a native file API, which
    # avoids base64 expansion, the command-line length ceiling, and the guest
    # needing coreutils at all.
    # ------------------------------------------------------------------

    # Reserved exit status for "the path is not readable", distinct from any
    # status the guest's own command might return.
    _MISSING_PATH_STATUS = 66

    def read_file(self, sandbox: str, path: str, *, max_bytes: int) -> bytes:
        """
        Read a file from the sandbox.

        Raise :class:`SandboxFileTooLargeError` instead of transferring a file
        larger than ``max_bytes``.
        """
        quoted = shlex.quote(path)
        # One command, with the cap enforced inside the guest by ``head -c``.
        # Sizing in a separate call would be both a TOCTOU window and useless
        # against anything ``stat`` reports as zero-length -- character devices,
        # FIFOs, procfs -- which stream without end when read. ``stat`` failing
        # is an error in its own right: without the explicit exit, a missing
        # path yields an empty ``base64`` and reads back as an empty file.
        script = (
            f"sz=$(stat -Lc %s -- {quoted} 2>/dev/null) || exit {self._MISSING_PATH_STATUS}; "
            f'printf "%s\n" "$sz"; '
            f"head -c {max_bytes + 1} -- {quoted} | base64"
        )
        # base64 expands by 4/3 and adds line breaks; twice the budget plus slack
        # bounds the transfer while leaving room to detect the overflow byte.
        result = self.run_command(
            sandbox, script, timeout=_FILE_OP_TIMEOUT, max_output_bytes=max_bytes * 2 + 4096
        )
        if result.exit_code == self._MISSING_PATH_STATUS:
            raise SandboxError(f"{path!r} does not exist in the sandbox, or is not readable.")
        if result.exit_code:
            raise SandboxError(result.stderr.strip() or f"Could not read {path!r}.")
        reported, _, encoded = result.stdout.partition("\n")
        try:
            data = base64.b64decode(encoded, validate=False)
        except (binascii.Error, ValueError) as e:
            raise SandboxError(f"Could not decode {path!r} from the sandbox.") from e
        if len(data) > max_bytes:
            # ``head`` handed back the sentinel byte, so the file is over budget.
            # A streaming source reports 0, in which case the true size is
            # unknown but irrelevant.
            try:
                size = int(reported.strip())
            except ValueError:
                size = 0
            raise SandboxFileTooLargeError(path, max(size, len(data)), max_bytes)
        return data

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        """
        Write ``content`` to ``path`` in the sandbox, creating parent directories.

        The payload rides in the command itself, so this default is bounded by
        the guest's command-line length. A backend that can stream stdin or
        upload directly should override.
        """
        quoted = shlex.quote(path)
        payload = base64.b64encode(content).decode()
        script = (
            f'mkdir -p -- "$(dirname -- {quoted})" && printf %s {shlex.quote(payload)} | base64 -d > {quoted}'
        )
        result = self.run_command(
            sandbox, script, timeout=_FILE_OP_TIMEOUT, max_output_bytes=_FILE_OP_OUTPUT_CAP
        )
        if result.exit_code:
            raise SandboxError(result.stderr.strip() or f"Could not write {path!r}.")

    def list_directory(self, sandbox: str, path: str) -> list[tuple[str, bool]]:
        """Return ``(name, is_dir)`` for each entry in a sandbox directory."""
        quoted = shlex.quote(path)
        # NUL-separated: a filename may legally contain a newline, and the agent
        # can create one itself, which a line-based listing would split into two
        # entries that neither it nor the model can then open.
        result = self.run_command(
            sandbox,
            f"find -- {quoted} -maxdepth 1 -mindepth 1 -printf '%y %f\\0'",
            timeout=_FILE_OP_TIMEOUT,
            max_output_bytes=_FILE_OP_OUTPUT_CAP,
        )
        if result.exit_code:
            raise SandboxError(result.stderr.strip() or f"Could not list {path!r}.")
        entries: list[tuple[str, bool]] = []
        for record in result.stdout.split("\0"):
            if not record:
                continue
            kind, _, name = record.partition(" ")
            if not name:
                continue
            # find's %y is a single type character: 'd' for a directory.
            entries.append((name, kind == "d"))
        return entries

    @abstractmethod
    def destroy(self, sandbox: str) -> None:
        """Tear down the sandbox. Must be idempotent."""
