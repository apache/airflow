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
"""Toolset giving an agent shell and file access inside an isolated sandbox, off the worker."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import logging
import math
import sys
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, NamedTuple, Self

from fsspec.implementations.local import LocalFileSystem
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

from airflow.providers.common.ai.sandbox.base import (
    AttachableSandboxBackend,
    SandboxError,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
    _validate_positive_finite,
    dag_run_owner,
    is_sandbox_handle,
)
from airflow.providers.common.ai.sandbox.output import (
    format_size,
    render_file_window,
    truncate_output,
)
from airflow.providers.common.ai.utils.masking import mask_secrets
from airflow.providers.common.ai.utils.tool_definition import (
    build_args_validator,
    code_arg_kwargs,
    return_schema_kwargs,
)
from airflow.providers.common.ai.utils.toolset_base import AirflowToolset
from airflow.providers.common.compat.sdk import ObjectStoragePath, get_current_context

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from pydantic_ai._run_context import RunContext

    from airflow.providers.common.ai.sandbox.base import SandboxBackend

log = logging.getLogger(__name__)

# Releasing a claim on an attached sandbox is retried this many times, this far apart,
# before the run gives up and logs: a claim left behind blocks other tasks from the sandbox.
_RELEASE_ATTEMPTS = 3
_RELEASE_RETRY_DELAY = 1.0

# Runs backend.create off the event loop. Its threads start on first use, not at import.
_provisioning = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="sandbox-create")

RUN_COMMAND = "run_command"


def _sentence(error: BaseException) -> str:
    """Return an exception's message ending in exactly one full stop, to join into a longer one."""
    return f"{str(error).rstrip('.')}."


# Ends the name of the staging key an export is copied to before it is moved into place,
# so a consumer globbing its destination's own extension never matches one.
_PARTIAL_SUFFIX = ".partial"


class _Identity(NamedTuple):
    """Who an attaching run presents as: the owner it expects on the sandbox, and the holder it claims to be."""

    owner: str
    holder: str


READ_FILE = "read_file"
WRITE_FILE = "write_file"
LIST_DIRECTORY = "list_directory"

_SCHEMAS: dict[str, dict[str, Any]] = {
    RUN_COMMAND: {
        "type": "object",
        "properties": {
            "command": {"type": "string", "description": "Shell command to run."},
            "timeout_seconds": {
                "type": ["number", "null"],
                "description": "Maximum seconds to wait. Defaults to the configured timeout.",
            },
        },
        "required": ["command"],
    },
    READ_FILE: {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Path to the file inside the sandbox."},
            "offset": {
                "type": ["integer", "null"],
                "description": "Line number to start reading from (1-indexed).",
            },
            "limit": {"type": ["integer", "null"], "description": "Maximum number of lines to read."},
        },
        "required": ["path"],
    },
    WRITE_FILE: {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Path to the file inside the sandbox."},
            "content": {"type": "string", "description": "Text to write."},
        },
        "required": ["path", "content"],
    },
    LIST_DIRECTORY: {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Directory to list. Defaults to the working directory.",
            },
        },
        "required": [],
    },
}

_DESCRIPTIONS = {
    RUN_COMMAND: (
        "Run a shell command inside an isolated sandbox and return its output. Pipes, "
        "redirection, && and globs work. A non-zero exit is reported, not raised, so read "
        "stderr and fix your command. The sandbox is separate from the Airflow worker and "
        "is not given Airflow's connections, variables, or environment. Relative paths "
        "resolve against the sandbox's working directory; run 'pwd' if you need to know it."
    ),
    READ_FILE: (
        "Read a text file from the sandbox. Long files are truncated and the result tells "
        "you the next offset to continue from."
    ),
    WRITE_FILE: "Write text to a file in the sandbox, creating parent directories as needed.",
    LIST_DIRECTORY: "List the entries in a sandbox directory. Directories are shown with a trailing slash.",
}


class SandboxToolset(AirflowToolset):
    """
    Give an agent shell and file access inside a disposable sandbox, off the Airflow worker.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Exposes four tools -- ``run_command``, ``read_file``, ``write_file`` and
    ``list_directory`` -- against a sandbox provisioned by the given
    :class:`~airflow.providers.common.ai.sandbox.SandboxBackend`. The same four
    names and shapes are what pydantic-ai's own sandbox capabilities use, so a
    model that has seen one already knows this one.

    **What the boundary covers.** Only what these tools do runs in the sandbox.
    The agent loop, the LLM calls, and every other toolset on the same agent
    still run in the Airflow worker process with its credentials. This contains
    model-written code; it does not contain the agent. See the toolsets
    documentation for the full picture of which boundary protects what.

    The sandbox is created lazily on the first tool call, shared by every call
    within one agent run, and destroyed when that run ends. A run that never
    calls a tool never provisions one. Files persist between calls in a run;
    each ``run_command`` is a fresh shell, so shell variables do not.

    **Or the sandbox is someone else's.** With ``attach_to`` set to the handle
    another task provisioned, the toolset uses that sandbox for the run and does
    not destroy it: the task that created it decides its environment, its network
    policy and its lifetime, and reads out whatever the agent left behind. The
    handle alone is not enough. The sandbox has to carry the owner the toolset
    presents, by default the current Dag run, so a wrong handle from an upstream
    XCom is refused rather than used, and one agent run holds a sandbox at a time.
    ``attach_to`` is templated when the toolset is passed through
    ``AgentOperator(toolsets=...)``, wherever it sits in that list, which is how
    the handle travels from the provisioning task:
    ``attach_to="{{ ti.xcom_pull('provision') }}"``.

    **Files the agent builds can leave.** ``exports`` maps a path in the sandbox to
    an object-storage destination, and when the run ends the toolset copies each
    file there before it destroys the sandbox. The copy streams through the worker
    in bounded pieces and never passes through the model's context or XCom, so a
    parquet file, a chart, or a trained model is as easy to hand downstream as a
    line of text. The destinations are templated the same way ``attach_to`` is. A
    promised file that cannot be exported fails the task, and a failed run exports
    nothing; a sandbox that cannot be destroyed afterwards does not fail the task.

    A non-zero exit or a timeout is normal tool output -- the model reads it and
    corrects itself. A recoverable sandbox failure becomes a bounded retry. Only
    a terminal failure (credentials rejected, daemon unreachable) fails the task,
    so Airflow's own retry handles it.

    :param backend: Backend that provisions and drives the sandbox.
    :param spec: What to provision the sandbox with -- environment variables and
        network policy. Defaults to no environment and no egress.
    :param default_command_timeout: Seconds allowed for a ``run_command`` call
        when the model does not ask for one. Default ``60``.
    :param max_command_timeout: Hard ceiling in seconds for any single command,
        including a model-supplied ``timeout_seconds``. Default ``300``.
    :param max_output_lines: Maximum lines retained per output stream or file
        read. Default ``2000``.
    :param max_output_bytes: Maximum bytes retained per output stream or file
        read. Default 50 KiB. Whichever cap is reached first wins.
    :param max_read_bytes: Largest file ``read_file`` will transfer. Default
        5 MiB; larger files are refused with a hint to slice them in the shell.
    :param tool_prefix: Prefix for the four tool names, e.g. ``"local"`` gives
        ``local_run_command``. Set this when one agent has more than one
        ``SandboxToolset``, since duplicate tool names are rejected.
    :param attach_to: Handle of a sandbox another task provisioned, to use instead
        of creating one. Needs a backend that can find a sandbox from another
        process (an :class:`~airflow.providers.common.ai.sandbox.AttachableSandboxBackend`,
        which ``ModalSandboxBackend`` is and ``SbxSandboxBackend`` is not), and
        cannot be combined with ``spec``, since the sandbox is already provisioned.
        The toolset never destroys an attached sandbox.
    :param exports: Files to copy out of the sandbox when the run ends, as a
        mapping from a path in the sandbox (relative paths resolve the way the
        ``read_file`` tool resolves them) to an object-storage URL such as
        ``"s3://bucket/{{ run_id }}/report.parquet"``, anything
        :class:`~airflow.sdk.ObjectStoragePath` can open. Only a regular file is
        exported. Cannot be combined with ``attach_to``: the task that owns an
        attached sandbox collects its files itself.
    :param export_conn_id: Airflow connection for the export destinations, or
        ``None`` for the storage's default credentials. Only meaningful with
        ``exports``.
    :param max_export_bytes: Largest file an export will copy. Default 1 GiB.
    :param owner: The owner the attached sandbox must carry. Defaults to the Dag
        run the task is part of, which is what a provisioning task in the same run
        stamps with ``SandboxSpec(owner=dag_run_owner(context))``. Set it only when
        the sandbox was provisioned under another name, or when the toolset runs
        outside an Airflow task. Only meaningful with ``attach_to``.
    """

    # Rendered, on a copy, by AgentOperator. Deliberately not ``template_fields``, which
    # Airflow's templater would render in place wherever the toolset is nested.
    agent_template_fields: Sequence[str] = ("attach_to", "_exports", "_export_conn_id")

    def __init__(
        self,
        backend: SandboxBackend,
        *,
        spec: SandboxSpec | None = None,
        default_command_timeout: float = 60.0,
        max_command_timeout: float = 300.0,
        max_output_lines: int = 2000,
        max_output_bytes: int = 50 * 1024,
        max_read_bytes: int = 5 * 1024 * 1024,
        tool_prefix: str = "",
        attach_to: str | None = None,
        owner: str | None = None,
        exports: Mapping[str, str] | None = None,
        export_conn_id: str | None = None,
        max_export_bytes: int = 1024 * 1024 * 1024,
    ) -> None:
        _validate_positive_finite(default_command_timeout, "default_command_timeout")
        _validate_positive_finite(max_command_timeout, "max_command_timeout")
        _validate_positive_finite(max_output_lines, "max_output_lines")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        _validate_positive_finite(max_read_bytes, "max_read_bytes")
        _validate_positive_finite(max_export_bytes, "max_export_bytes")
        if default_command_timeout > max_command_timeout:
            raise ValueError(
                f"default_command_timeout ({default_command_timeout}) must not exceed "
                f"max_command_timeout ({max_command_timeout})."
            )
        if tool_prefix and not tool_prefix.isidentifier():
            # The prefixed names are rendered as Python function signatures under
            # code mode, so a name that is not an identifier breaks there.
            raise ValueError(f"tool_prefix must be a valid Python identifier, got {tool_prefix!r}.")
        if attach_to is not None:
            if not isinstance(backend, AttachableSandboxBackend):
                raise ValueError(
                    f"attach_to needs a backend that can find a sandbox from another task, and "
                    f"{backend.name!r} cannot: it is not an AttachableSandboxBackend. Drop attach_to and "
                    "let the toolset provision its own sandbox, or provision on a backend that can be "
                    "attached to, such as ModalSandboxBackend."
                )
            if spec is not None:
                raise ValueError(
                    "spec cannot be combined with attach_to: the sandbox is already provisioned, so its "
                    "environment and network policy belong to the task that created it."
                )
            if not attach_to:
                raise ValueError("attach_to must be a sandbox handle, not an empty string.")
            if exports:
                raise ValueError(
                    "exports cannot be combined with attach_to: the sandbox belongs to the task that "
                    "created it, and that task reads out whatever the agent left behind."
                )
        elif owner is not None:
            raise ValueError("owner only applies together with attach_to.")
        elif spec is not None and spec.owner is not None:
            # An owner exists so that a later task can attach; a sandbox this toolset
            # provisions is destroyed when the run ends, so nothing ever could.
            raise ValueError(
                "SandboxSpec.owner is for a sandbox another task attaches to. The toolset destroys the "
                "sandbox it provisions itself when the run ends, so an owner on it would mean nothing; "
                "provision the sandbox in a task and pass its handle as attach_to instead."
            )
        for path, destination in (exports or {}).items():
            if not path or not destination:
                raise ValueError(
                    f"exports maps a sandbox path to a destination, and neither may be empty; got "
                    f"{path!r} -> {destination!r}."
                )
        if export_conn_id is not None and not exports:
            raise ValueError("export_conn_id only applies together with exports.")
        self._backend = backend
        self.attach_to = attach_to
        # Fixed here, not re-derived from ``attach_to`` later: the templater rewrites
        # that attribute at run time, and a handle that renders to nothing must fail
        # the run rather than turn this into a toolset that provisions its own sandbox.
        self._attach_mode = attach_to is not None
        # The same object as ``_backend``, narrowed once for the attach and release paths.
        self._attachable = backend if isinstance(backend, AttachableSandboxBackend) else None
        self._owner = owner
        # Never None: the documented default is "no environment, no egress", and a
        # backend reads None as "no requirements stated". Passing None through would
        # silently skip the contract check and hand back an unrestricted sandbox,
        # which is the opposite of what the default promises.
        self._spec = spec if spec is not None else SandboxSpec()
        self._default_command_timeout = default_command_timeout
        self._max_command_timeout = max_command_timeout
        self._max_output_lines = int(max_output_lines)
        self._max_output_bytes = int(max_output_bytes)
        self._max_read_bytes = int(max_read_bytes)
        self._exports = dict(exports or {})
        self._export_conn_id = export_conn_id
        self._max_export_bytes = int(max_export_bytes)
        self._tool_prefix = tool_prefix
        self._sandbox: str | None = None
        self._create_lock = threading.Lock()
        self._create_future: concurrent.futures.Future[str] | None = None
        # Set while attached: who this run claimed the sandbox as, when the sandbox ends
        # on this process's clock (None when the creator recorded no lifetime), and what
        # the tool description says about it, fixed at attach so it is the same on every
        # step and provider prompt caching keeps working.
        self._holder: str | None = None
        self._expires_at: float | None = None
        self._attach_note: str | None = None
        # Tools run only between enter and exit, so nothing outside a run can provision a
        # sandbox that no exit will destroy, or use one it has not claimed.
        self._open = False
        # The exception already being handled when a run entered, if any. The run was
        # started from inside that handler, so finding it again at exit says nothing
        # about how the run went (see ``_run_failed``).
        self._handling_at_enter: BaseException | None = None
        # Set on an instance that has handed its runs to copies (see ``for_run``). It
        # never holds a sandbox itself, so it has nothing to export or to fail over.
        self._forked = False

    @property
    def id(self) -> str:
        suffix = f"-{self._tool_prefix}" if self._tool_prefix else ""
        return f"sandbox-{self._backend.name}{suffix}"

    def _tool_name(self, base: str) -> str:
        return f"{self._tool_prefix}_{base}" if self._tool_prefix else base

    def _base_name(self, tool_name: str) -> str | None:
        if not self._tool_prefix:
            return tool_name if tool_name in _SCHEMAS else None
        prefix = f"{self._tool_prefix}_"
        if not tool_name.startswith(prefix):
            return None
        base = tool_name[len(prefix) :]
        return base if base in _SCHEMAS else None

    async def for_run(self, ctx: RunContext[Any]) -> AbstractToolset[Any]:
        # pydantic-ai shares one toolset instance across runs, but each run holds
        # its own sandbox in ``_sandbox``/``_create_future``. Hand every run a fresh
        # instance so concurrent runs never share a sandbox or destroy each
        # other's. The backend keys all state by unique sandbox handle, so
        # sharing the backend itself is safe. ``type(self)`` so a subclass does
        # not silently degrade to this class on every run.
        self._forked = True
        return type(self)(
            self._backend,
            # Attach mode refuses a spec, and the default one filled in above is
            # not the author's, so it is not handed back.
            spec=None if self._attach_mode else self._spec,
            default_command_timeout=self._default_command_timeout,
            max_command_timeout=self._max_command_timeout,
            max_output_lines=self._max_output_lines,
            max_output_bytes=self._max_output_bytes,
            max_read_bytes=self._max_read_bytes,
            tool_prefix=self._tool_prefix,
            attach_to=self._attached_handle if self._attach_mode else None,
            owner=self._owner,
            exports=self._exports or None,
            export_conn_id=self._export_conn_id,
            max_export_bytes=self._max_export_bytes,
        )

    @property
    def _attached_handle(self) -> str:
        """
        The rendered handle, checked, because the templater bypasses the constructor.

        ``attach_to`` is templated so the handle can come from an upstream XCom, and
        an XCom that was never pushed renders to ``None`` under native rendering or to
        the string ``"None"`` otherwise. Neither is a sandbox, and silently falling back
        to provisioning one would run the agent in an empty workspace with no error.
        """
        handle = self.attach_to
        if not is_sandbox_handle(handle):
            raise SandboxTerminalError(
                f"attach_to rendered to {handle!r}, which is not a sandbox handle. The task that "
                "provisions the sandbox pushed nothing, or this task does not depend on it and ran "
                "first; check the upstream task and the XCom it returns."
            )
        return handle

    async def __aenter__(self) -> Self:
        # An owned sandbox is provisioned lazily on first use, not here: a durable
        # replay that only serves cached tool results must not provision one, and
        # nothing leaks if the run fails before any tool executes. An attached one is
        # claimed now, so a wrong handle or a held sandbox fails the run before the
        # model has spent anything, and the tool descriptions can state the lifetime.
        # Read here, not in the thread: the exception being handled belongs to this frame.
        self._handling_at_enter = sys.exc_info()[1]
        await asyncio.to_thread(self._open_run)
        return self

    def _open_run(self) -> None:
        if self._attach_mode:
            self._attach(self._attached_handle)
        else:
            self._check_export_destinations()
        self._open = True

    def _check_export_destinations(self) -> None:
        """
        Refuse a rendered destination that is not a URL, before the model spends anything.

        The destinations are templated, and a value that rendered to nothing reads as a
        path relative to the worker's working directory, where the file would land and
        the export report success. The same failure shape ``_attached_handle`` guards.
        """
        for path, destination in self._exports.items():
            if not isinstance(destination, str) or "://" not in destination:
                raise SandboxTerminalError(
                    f"exports[{path!r}] rendered to {destination!r}, which is not a storage URL such as "
                    "s3://bucket/key or file:///path. Check the template or the value it reads."
                )

    def _attach(self, handle: str) -> None:
        owner, holder = self._identity()
        backend = self._attachable_backend
        try:
            attached = backend.attach(handle, owner=owner, holder=holder)
        except SandboxTerminalError:
            raise
        except SandboxError as e:
            # Nothing the model does can change whether this sandbox can be attached
            # to, so a recoverable label here is one it could not act on.
            raise SandboxTerminalError(
                f"Could not attach to sandbox {handle!r} on backend {backend.name!r}: {e}"
            ) from e
        self._sandbox = handle
        self._holder = holder
        remaining = attached.remaining_lifetime
        self._expires_at = None if remaining is None else time.monotonic() + remaining
        self._attach_note = self._describe_attached(attached.network, remaining)
        log.info(
            "Attached to sandbox %s on backend %s as %s; %s of its lifetime remain",
            handle,
            backend.name,
            holder,
            "an unknown number of seconds" if remaining is None else f"{remaining:.0f}s",
        )

    @classmethod
    def _describe_attached(cls, network: SandboxSpec | None, remaining_lifetime: float | None) -> str:
        whose = (
            "This sandbox was set up by an earlier task, and your files stay in it after this run "
            "for a later task to collect."
        )
        policy = (
            cls._describe_network(network)
            if network is not None
            else "Its network access is whatever the task that set it up allowed; test before relying on it."
        )
        if remaining_lifetime is None:
            clock = "How long it has left is not known."
        elif remaining_lifetime < 60:
            clock = "Under a minute of its lifetime remained when this run began, so finish up."
        else:
            minutes = round(remaining_lifetime / 60)
            unit = "minute" if minutes == 1 else "minutes"
            clock = f"About {minutes} {unit} of its lifetime remained when this run began."
        return f"{whose} {policy} {clock}"

    @property
    def _attachable_backend(self) -> AttachableSandboxBackend:
        if self._attachable is None:
            # The constructor refuses attach_to on any other backend, so this is a
            # programming error, not a run-time condition.
            raise RuntimeError("attach mode on a backend that cannot attach")
        return self._attachable

    def _identity(self) -> _Identity:
        """
        Who this run presents as.

        Inside an Airflow task the owner defaults to the Dag run and the holder is the
        task instance without its try number, so a retry counts as the same holder and
        finds the files of an attempt that died without releasing, while a different
        task, another Dag run of the same task under a shared owner, or another map
        index is refused. Outside a task there is nothing to derive either from, so the
        owner has to be given and stands for both.
        """
        try:
            context = get_current_context()
        except RuntimeError:
            context = None
        if context is None:
            if self._owner is None:
                raise SandboxTerminalError(
                    "attach_to needs an owner. Inside an Airflow task the Dag run is the owner by "
                    "default; outside one, pass owner=... matching the SandboxSpec.owner the sandbox "
                    "was provisioned with."
                )
            return _Identity(owner=self._owner, holder=self._owner)
        ti = context["ti"]
        run = dag_run_owner(context)
        holder = f"{run}/{ti.task_id}"
        if ti.map_index is not None and ti.map_index >= 0:
            holder = f"{holder}[{ti.map_index}]"
        return _Identity(owner=self._owner if self._owner is not None else run, holder=holder)

    async def __aexit__(self, *args: Any) -> bool | None:
        # Read here, not in the thread: the exception being handled belongs to this frame.
        run_failed = args[0] is not None or self._run_failed
        await asyncio.to_thread(self._close, run_failed=run_failed)
        return None

    @property
    def _run_failed(self) -> bool:
        """
        Whether the run this toolset served ended in an exception.

        pydantic-ai exits its toolsets through an exit stack that passes no exception to
        them, whether the run succeeded or not, so the exception being handled at exit is
        the only record of a failure. One that was already being handled when the run
        entered is not it: that run was started from inside an ``except`` block, and it
        may well have succeeded.
        """
        handling = sys.exc_info()[1]
        return handling is not None and handling is not self._handling_at_enter

    def __enter__(self) -> Self:
        """Own the sandbox's lifetime from synchronous code, such as a task running a native agent."""
        self._open_run()
        return self

    def __exit__(self, *args: Any) -> bool | None:
        # A with statement passes the exception that ended the block, so it is the answer.
        self._close(run_failed=args[0] is not None)
        return None

    def _close(self, *, run_failed: bool) -> None:
        with self._create_lock:
            self._open = False
            pending = self._create_future
        sandbox = self._sandbox
        if sandbox is None and pending is not None:
            # The block ended while a tool was still provisioning the sandbox, such as when
            # the task timed out: wait for it, so it is destroyed rather than left running.
            try:
                sandbox = pending.result()
            except Exception:
                sandbox = None
        holder = self._holder
        # Clear first: an instance entered again must never reuse a sandbox whose
        # cleanup was attempted.
        self._sandbox = None
        self._holder = None
        self._expires_at = None
        self._attach_note = None
        if self._attach_mode:
            # Not ours to destroy. Give up the claim so the next run, or the task that
            # created the sandbox, finds it free.
            if holder is not None:
                self._release(self._attached_handle, holder)
            return
        export = bool(self._exports) and not run_failed
        if sandbox is None:
            if export and not self._forked:
                # The run never called a tool, so it never provisioned a sandbox, or the
                # sandbox ended under its last command and nothing replaced it. Either
                # way the files it was to leave behind do not exist.
                raise SandboxTerminalError(
                    "The run ended with no sandbox standing, so none of the files it was to export "
                    f"exist: {', '.join(repr(path) for path in self._exports)}."
                )
            return
        try:
            if export:
                self._export(sandbox)
        finally:
            self._destroy(sandbox)

    def _export(self, sandbox: str) -> None:
        """
        Copy every file in ``exports`` out of the sandbox, failing the task on the first that cannot be.

        Before teardown, and never best effort: a task that promised a file and did not
        deliver it must fail, or its downstream task finds nothing and cannot tell why.

        Each file is copied to a staging key next to its destination, and only once every
        file has been copied are they moved into place. A failed export therefore leaves
        every destination as it was, including one an earlier run filled, rather than
        holding a truncated copy or a part of this run's set.
        """
        staged: list[tuple[ObjectStoragePath, ObjectStoragePath]] = []
        for path, destination in self._exports.items():
            target = self._export_target(destination)
            partial = target.with_name(f"{target.name}.{uuid.uuid4().hex[:12]}{_PARTIAL_SUFFIX}")
            try:
                if isinstance(target.fs, LocalFileSystem):
                    # A per-run destination names a directory nothing has created yet. Only
                    # locally: on object storage a key needs no parent, and an s3fs mkdir
                    # can create a bucket.
                    target.parent.mkdir(parents=True, exist_ok=True)
                staged.append((partial, target))
                with partial.open("wb") as stream:
                    size = self._backend.export_file(sandbox, path, stream, max_bytes=self._max_export_bytes)
            except Exception as e:
                raise self._export_failed(
                    staged,
                    f"Could not export {path!r} from sandbox {sandbox} "
                    f"on backend {self._backend.name!r} to {destination}: {_sentence(e)}",
                ) from e
            log.info("Exported %s from sandbox %s (%s)", path, sandbox, format_size(size))
        published: list[str] = []
        for partial, target in staged:
            try:
                # Within one store: a rename on a local disk, a server-side copy on object
                # storage, so the bytes do not pass through the worker a second time.
                partial.move(target)
            except Exception as e:
                done = f" Already in place: {', '.join(published)}." if published else ""
                raise self._export_failed(
                    staged, f"Could not move the export into {target}: {_sentence(e)}{done}"
                ) from e
            published.append(str(target))
            log.info("Published %s", target)

    @staticmethod
    def _export_failed(
        staged: list[tuple[ObjectStoragePath, ObjectStoragePath]], message: str
    ) -> SandboxTerminalError:
        """Remove the staging keys of a failed export, and build the error that names any left behind."""
        left_behind = []
        for partial, _ in staged:
            try:
                partial.unlink(missing_ok=True)
            except Exception:
                log.warning("Could not remove %s after a failed export", partial, exc_info=True)
                left_behind.append(str(partial))
        note = f" Left behind, to delete by hand: {', '.join(left_behind)}." if left_behind else ""
        return SandboxTerminalError(f"{message}{note}")

    def _export_target(self, destination: str) -> ObjectStoragePath:
        # ``conn_id`` only when one is set: on Airflow 3.0 and 3.1 an explicit None
        # discards a connection named in the URL itself (``s3://conn@bucket/key``).
        if self._export_conn_id is None:
            return ObjectStoragePath(destination)
        return ObjectStoragePath(destination, conn_id=self._export_conn_id)

    def _destroy(self, sandbox: str) -> None:
        try:
            self._backend.destroy(sandbox)
        except Exception:
            # The model work is finished and paid for by this point, so a teardown
            # blip must not turn a successful run into a task failure. Log loudly
            # and let the backend's own backstop (a server-side TTL, or an
            # operator sweep) reclaim it.
            log.warning(
                "Failed to destroy sandbox %s on backend %s; it may need manual cleanup",
                sandbox,
                self._backend.name,
                exc_info=True,
            )

    def _release(self, handle: str, holder: str) -> None:
        backend = self._attachable_backend
        # A claim left behind blocks every other task from the sandbox until its lifetime
        # ends, so a blip in the tag service is worth a few more tries before giving up.
        for attempt in range(1, _RELEASE_ATTEMPTS + 1):
            try:
                backend.release(handle, holder=holder)
                return
            except SandboxTerminalError:
                # The sandbox is gone, and the claim went with it.
                log.info("Sandbox %s on backend %s has ended; nothing to release", handle, backend.name)
                return
            except Exception:
                if attempt == _RELEASE_ATTEMPTS:
                    # The run is finished and paid for, so this cannot fail the task. Name
                    # the holder, which is what a later refusal will name too.
                    log.warning(
                        "Failed to release sandbox %s on backend %s after %d attempts; it stays marked "
                        "as held by %s, and only that holder, or the task that created the sandbox, "
                        "can use it",
                        handle,
                        backend.name,
                        _RELEASE_ATTEMPTS,
                        holder,
                        exc_info=True,
                    )
                    return
            time.sleep(_RELEASE_RETRY_DELAY)

    async def _ensure_sandbox(self) -> str:
        if self._sandbox is not None:
            return self._sandbox
        if self._attach_mode:
            # Reached only after the attached sandbox ended: there is nothing to provision,
            # because the sandbox was never this toolset's to create.
            raise SandboxTerminalError(
                f"Not attached to sandbox {self.attach_to!r}: the sandbox ended. The toolset does not "
                "provision a replacement for a sandbox another task owns."
            )
        # A native framework can make the first calls from several threads, each with an
        # event loop of its own, so the one creation they share is a thread-safe future
        # that any loop can await, not a task bound to the loop that started it.
        with self._create_lock:
            # Checked again under the lock: another thread may have finished creating it.
            if self._sandbox is not None:
                return self._sandbox
            if self._create_future is None:
                self._create_future = _provisioning.submit(
                    contextvars.copy_context().run, self._backend.create, spec=self._spec
                )
            create_future = self._create_future
        creating = asyncio.wrap_future(create_future)
        try:
            sandbox = await asyncio.shield(creating)
        except asyncio.CancelledError:
            # A thread cannot be cancelled. Wait until it publishes the handle so
            # __aexit__ can destroy a sandbox created during cancellation.
            self._sandbox = await creating
            raise
        except SandboxTerminalError:
            raise
        except SandboxError as e:
            # Provisioning takes only the spec, which the model cannot see or change,
            # so no retry the model makes can turn a failed create into a working
            # sandbox. Fail the task and let Airflow's retry try the provisioning
            # again, rather than spending the model's retry budget on it.
            raise SandboxTerminalError(
                f"Could not provision a sandbox on backend {self._backend.name!r}: {e}"
            ) from e
        else:
            with self._create_lock:
                if not self._open:
                    # The block ended while this call waited; closing destroys the sandbox.
                    raise SandboxTerminalError("The sandbox was closed while it was being provisioned.")
                self._sandbox = sandbox
            return sandbox
        finally:
            with self._create_lock:
                if self._create_future is create_future:
                    self._create_future = None

    @property
    def _network_note(self) -> str:
        """
        The sandbox's egress, described in the one place the model reliably reads.

        Without it the model has to discover the policy by failing: measured, a
        ``pip install`` in a sandbox that denies egress costs a turn and returns a DNS
        error, and under an allowlist a reach for plain HTTP burns the whole command
        budget and returns only a timeout, which reads as "my command was slow". The
        spec is known here, so say it instead.

        An attached sandbox was provisioned by someone else, so the note is built once at
        attach from what the backend recorded about it: whose it is, the network policy
        it was created with, and the clock it is on.
        """
        if self._attach_mode:
            return self._attach_note or "This sandbox was set up by an earlier task."
        return self._describe_network(self._spec)

    @property
    def _export_note(self) -> str:
        """Which files leave the sandbox when the run ends, so the model writes them where they are collected."""
        if not self._exports:
            return ""
        paths = ", ".join(self._exports)
        return (
            f" When this run ends, these files are copied out of the sandbox and everything else is "
            f"discarded, so write your results to them before you finish: {paths}."
        )

    @staticmethod
    def _describe_network(spec: SandboxSpec) -> str:
        if not spec.block_network:
            return "This sandbox has outbound network access."
        hosts = list(spec.allow_egress_to or ())
        cidrs = list(spec.allow_egress_to_cidrs or ())
        if not hosts and not cidrs:
            return (
                "This sandbox has NO network access, including DNS, so installing packages "
                "or downloading anything will fail. Work with what the image already has."
            )
        by_name = f"these hosts, over HTTPS on port 443 only: {', '.join(hosts)}"
        by_address = f"these address ranges, on any port: {', '.join(cidrs)}"
        if hosts and cidrs:
            return (
                f"This sandbox reaches only {by_name}; and {by_address}. Anything else will fail. "
                "The hosts accept HTTPS only, so plain HTTP to them fails; the address ranges accept "
                "any port. Hostnames resolve, but only listed hosts and addresses answer."
            )
        if hosts:
            return (
                f"This sandbox reaches only {by_name}. Anything else, and plain HTTP to any host, will fail."
            )
        return (
            f"This sandbox reaches only {by_address}. Anything else will fail; hostnames still resolve, "
            "but only those addresses answer."
        )

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}
        for base, schema in _SCHEMAS.items():
            # sequential=True: every tool shares one sandbox and later calls
            # depend on files earlier ones wrote, so they must not interleave.
            # return_schema "string": each returns text, so code mode renders
            # `-> str` rather than `-> Any`.
            # run_command additionally carries code_arg metadata so code mode
            # leaves it native instead of folding a shell surface into run_code.
            extra = code_arg_kwargs("command", "shell") if base == RUN_COMMAND else {}
            name = self._tool_name(base)
            description = _DESCRIPTIONS[base]
            if base == RUN_COMMAND:
                description = f"{description} {self._network_note}{self._export_note}"
            tool_def = ToolDefinition(
                name=name,
                description=description,
                parameters_json_schema=schema,
                sequential=True,
                **return_schema_kwargs({"type": "string"}),
                **extra,
            )
            tools[name] = ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=2,
                args_validator=build_args_validator(schema),
            )
        return tools

    async def execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        *,
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        base = self._base_name(name)
        if base is None:
            raise ValueError(f"Unknown tool: {name!r}")
        if not self._open:
            raise SandboxTerminalError(
                "SandboxToolset is not open. Use it inside `with sandbox:` or `async with sandbox:`, "
                "so the sandbox it provisions is destroyed when the block ends."
            )
        # Backend calls are synchronous and a command can take minutes, so offload
        # them to a thread instead of blocking the event loop.
        sandbox = await self._ensure_sandbox()
        try:
            if base == RUN_COMMAND:
                return await self._run_command(sandbox, tool_args)
            if base == READ_FILE:
                return await self._read_file(sandbox, tool_args)
            if base == WRITE_FILE:
                return await self._write_file(sandbox, tool_args)
            return await self._list_directory(sandbox, tool_args)
        except SandboxTerminalError:
            # Retrying cannot help: the sandbox is gone or the credentials are
            # bad. Fail the task so Airflow retries the whole thing.
            raise
        except SandboxFileTooLargeError as e:
            raise ModelRetry(
                f"{e.path!r} is {format_size(e.size_bytes)}, over the "
                f"{format_size(e.max_bytes)} read limit. Read just the part you need with a "
                "shell command instead (e.g. head, tail, sed -n, or grep)."
            ) from e
        except SandboxError as e:
            raise ModelRetry(f"The {name} tool failed: {e}") from e

    def _command_timeout(self, requested: float | None) -> float:
        if requested is None:
            budget = self._default_command_timeout
        elif not math.isfinite(requested) or requested <= 0:
            # Reject rather than silently clamping: a surprise "[timed out after
            # 1s]" would hide the model's own mistake from it.
            raise ModelRetry(f"timeout_seconds must be greater than 0, got {requested}.")
        else:
            budget = min(requested, self._max_command_timeout)
        if self._expires_at is not None:
            # An attached sandbox ends on its creator's clock, whatever backend it is on.
            # A command that could not finish inside that is shortened to what is left,
            # so it runs the work that fits instead of dying half way through; the
            # result then reports the budget it really had.
            budget = min(budget, max(1.0, self._expires_at - time.monotonic()))
        return budget

    async def _run_command(self, sandbox: str, tool_args: dict[str, Any]) -> str:
        timeout = self._command_timeout(tool_args.get("timeout_seconds"))
        result = await asyncio.to_thread(
            self._backend.run_command,
            sandbox,
            tool_args["command"],
            timeout=timeout,
            max_output_bytes=self._max_output_bytes,
        )
        if result.sandbox_terminated:
            self._sandbox = None
            if self._attach_mode:
                # An owned sandbox is replaced on the next call; an attached one cannot be,
                # and the model would be told its files are gone while nothing can follow.
                raise SandboxTerminalError(
                    f"The sandbox this task attached to, {self.attach_to!r}, stopped while running a "
                    "command. The task that created it decides its lifetime, so nothing can be "
                    "provisioned in its place."
                )
        # Truncate each stream separately and attach its label afterwards, so the
        # markers always survive and a large stderr cannot crowd out stdout.
        parts: list[str] = []
        if result.stdout:
            parts.append(f"[stdout]\n{self._truncate(result.stdout, result.stdout_truncated)}")
        if result.stderr:
            parts.append(f"[stderr]\n{self._truncate(result.stderr, result.stderr_truncated)}")
        output = "\n".join(parts) if parts else "(no output)"
        notes: list[str] = []
        if result.timed_out:
            # What the command actually got, which a backend may have had to shorten.
            # Telling the model the number it asked for would send it back with a bigger
            # one when the real constraint was never its request.
            applied = result.applied_timeout if result.applied_timeout is not None else timeout
            notes.append(f"[timed out after {applied:g}s]")
        elif result.exit_code:
            notes.append(f"[exit code: {result.exit_code}]")
        if result.sandbox_terminated:
            # Said whatever else happened, not only after a timeout: a backend can lose
            # the sandbox under an ordinary-looking failure too, and the model has to
            # learn that its files are gone from somewhere.
            notes.append("[sandbox was replaced; files from earlier calls are gone]")
        if notes:
            return f"{output}\n{' '.join(notes)}"
        return output

    def _truncate(self, text: str, already_truncated: bool) -> str:
        # Masked before it is cut, or a secret split at the cut would no longer match.
        return truncate_output(
            mask_secrets(text),
            max_lines=self._max_output_lines,
            max_bytes=self._max_output_bytes,
            already_truncated=already_truncated,
        )

    async def _read_file(self, sandbox: str, tool_args: dict[str, Any]) -> str:
        data = await asyncio.to_thread(
            self._backend.read_file,
            sandbox,
            tool_args["path"],
            max_bytes=self._max_read_bytes,
        )
        return render_file_window(
            mask_secrets(data),
            offset=tool_args.get("offset"),
            limit=tool_args.get("limit"),
            max_lines=self._max_output_lines,
            max_bytes=self._max_output_bytes,
        )

    async def _write_file(self, sandbox: str, tool_args: dict[str, Any]) -> str:
        path = tool_args["path"]
        try:
            data = tool_args["content"].encode("utf-8")
        except UnicodeEncodeError as e:
            # Reachable when a provider's pre-parsed tool arguments carry an
            # unpaired surrogate. A model mistake, so retry rather than fail.
            raise ModelRetry(
                "content contains characters that cannot be encoded as UTF-8 (unpaired surrogates)."
            ) from e
        await asyncio.to_thread(self._backend.write_file, sandbox, path, data)
        return f"Wrote {len(data)} bytes to {path!r}."

    async def _list_directory(self, sandbox: str, tool_args: dict[str, Any]) -> str:
        path = tool_args.get("path") or "."
        entries = await asyncio.to_thread(self._backend.list_directory, sandbox, path)
        if not entries:
            return "(empty)"
        # Sort by name before adding the "/" suffix so directories keep plain name
        # order ("/" sorts after "-" and ".", which would misplace suffixed names).
        listing = "\n".join(f"{name}/" if is_dir else name for name, is_dir in sorted(entries))
        # Bounded like every other tool result: an unpacked dataset or a
        # node_modules is tens of thousands of names, which would blow the
        # model's context mid-run after everything before it has been paid for.
        return truncate_output(
            listing,
            max_lines=self._max_output_lines,
            max_bytes=self._max_output_bytes,
        )
