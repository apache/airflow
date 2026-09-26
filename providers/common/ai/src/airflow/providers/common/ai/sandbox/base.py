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
import json
import math
import shlex
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, TypeGuard

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# Tags a backend that supports attaching stamps on a sandbox. ``airflow_`` prefixed so an
# author's own tags are unlikely to collide, and overwritten when they do; shared across
# backends so the ownership rules in :class:`AttachableSandboxBackend` mean the same thing
# everywhere.
OWNER_TAG = "airflow_owner"
"""Who the sandbox was provisioned for: the value of :attr:`SandboxSpec.owner`."""
HOLDER_TAG = "airflow_holder"
"""The agent task currently holding the sandbox, set on attach and cleared on release."""
EXPIRES_AT_TAG = "airflow_expires_at"
"""Unix time, in whole seconds, at which the backend will end the sandbox."""
NETWORK_TAG = "airflow_network"
"""The network policy the sandbox was provisioned with, as :func:`encode_network_policy` writes it."""
WORKDIR_TAG = "airflow_workdir"
"""The working directory the sandbox was provisioned with, when the creator chose one."""


def is_sandbox_handle(value: object) -> TypeGuard[str]:
    """
    Whether ``value`` can name a sandbox at all.

    A handle travels between tasks by XCom and template, so the shapes a missing one
    takes are known: ``None`` from a missing XCom under native rendering, the string
    ``"None"`` from the default Jinja environment, and the empty string.
    """
    return isinstance(value, str) and bool(value) and value != "None"


class SandboxError(Exception):
    """
    A sandbox operation failed in a way the agent may be able to work around.

    The toolset turns this into a ``ModelRetry`` so the model can adjust and try
    again within the run (a bad path, a command the image cannot run). Raised
    from :meth:`SandboxBackend.create` it is treated as terminal instead, since
    the model cannot influence provisioning.
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
    :param allow_egress_to_cidrs: IPv4 address ranges, in CIDR notation such as
        ``"203.0.113.0/24"`` or ``"203.0.113.7/32"``, the sandbox may reach when
        ``block_network`` is ``True``, on any port and protocol. This is the
        right field for one service at a fixed public address; it cannot serve
        a package registry behind a CDN, whose addresses rotate, and a hosted
        backend cannot reach private (RFC 1918) addresses at all. A backend that
        enforces it does so at the address layer, which is a stronger guarantee
        than a hostname list gives, so it needs no opt-in. Both lists may be set
        together; how a backend combines them, and what that costs, is the
        backend's to document.
    :param owner: Who the sandbox is for, when a task provisions it for an agent
        task to attach to later. A :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`
        attaching to the sandbox has to present the same value, and by default it
        presents the Dag run it is part of, so the provisioning task in the same
        run writes ``owner=dag_run_owner(context)``. Unset for a sandbox nobody
        will attach to. A backend that cannot record it must refuse it.
    """

    env: Mapping[str, str] | None = None
    block_network: bool = True
    allow_egress_to: Sequence[str] | None = None
    allow_egress_to_cidrs: Sequence[str] | None = None
    owner: str | None = None


def dag_run_owner(context: Mapping[str, Any]) -> str:
    """
    Return the owner token naming the Dag run a task is part of: ``"<dag_id>/<run_id>"``.

    This is what a ``SandboxToolset`` presents when it attaches to a sandbox
    without an explicit ``owner``, so a task provisioning a sandbox for an agent
    task in the same Dag run stamps it with ``SandboxSpec(owner=dag_run_owner(context))``.
    The pair is unique across the deployment where a bare ``run_id`` is not: two
    Dags on the same schedule share their run ids. ``context`` is the task context,
    as a ``@task`` receives it in ``**context`` or ``get_current_context`` returns it.
    """
    ti = context["ti"]
    return f"{ti.dag_id}/{ti.run_id}"


def encode_network_policy(spec: SandboxSpec) -> str:
    """
    Serialize a spec's network policy for a sandbox tag, so an attaching toolset can read it back.

    The toolset tells the model what the sandbox can reach, because a model that has to
    discover a denied network by failing wastes a turn, or a whole command budget. An
    attached sandbox was provisioned under a spec the toolset never sees, so the backend
    records the policy on the sandbox at create and :func:`decode_network_policy` turns
    it back into a spec. Compact JSON with sorted keys, so the same policy always encodes
    the same way.
    """
    return json.dumps(
        {
            "block_network": spec.block_network,
            "allow_egress_to": list(spec.allow_egress_to or ()),
            "allow_egress_to_cidrs": list(spec.allow_egress_to_cidrs or ()),
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def decode_network_policy(value: str | None) -> SandboxSpec | None:
    """Read a :data:`NETWORK_TAG` value back into a spec carrying only the network fields, or ``None``."""
    if not value:
        return None
    try:
        policy = json.loads(value)
        return SandboxSpec(
            block_network=bool(policy["block_network"]),
            allow_egress_to=[str(host) for host in policy["allow_egress_to"]] or None,
            allow_egress_to_cidrs=[str(cidr) for cidr in policy["allow_egress_to_cidrs"]] or None,
        )
    except (ValueError, KeyError, TypeError):
        # Written by something other than a backend of this contract. Better to say
        # nothing about the network than to describe one that was never asked for.
        return None


@dataclass(frozen=True)
class AttachedSandbox:
    """
    What :meth:`AttachableSandboxBackend.attach` reports about the sandbox it just claimed.

    ``remaining_lifetime`` is in seconds, or ``None`` when the creator recorded no expiry,
    in which case nothing is claimed about it. ``network`` is the policy the sandbox was
    provisioned with and ``workdir`` its working directory, each ``None`` when the
    creator recorded nothing.
    """

    remaining_lifetime: float | None
    network: SandboxSpec | None
    workdir: str | None


@dataclass(frozen=True)
class SandboxExecResult:
    """
    Outcome of one command executed inside a sandbox.

    ``timed_out`` means the command hit the budget, so ``exit_code`` carries no
    meaning. ``stdout_truncated`` / ``stderr_truncated`` mean the backend
    dropped bytes while reading that stream, before any model-facing formatting.
    ``sandbox_terminated`` means the backend destroyed the sandbox to stop the
    command, so the toolset must provision a fresh one before the next call.

    ``applied_timeout`` is the deadline the backend actually gave the command,
    when that differs from the one it was asked for -- a backend may have to
    shorten it, for instance to fit what is left of a sandbox's life. ``None``
    means the requested deadline was used as given. The toolset reports this
    rather than the request, so a model that times out is told the budget it
    really had and can ask for something that fits.
    """

    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool = False
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    sandbox_terminated: bool = False
    applied_timeout: float | None = None


class SandboxBackend(ABC):
    """
    Contract for running commands and file operations in an isolated sandbox.

    The lifecycle is create -> (any number of operations) -> destroy, driven by
    :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`.
    The four operation methods are named after the four tools the toolset
    exposes, so the mapping from a model-facing tool to the backend call behind
    it is literal; ``create`` and ``destroy`` are lifecycle and have no tool. A
    backend whose sandboxes can be found again from another process implements
    :class:`AttachableSandboxBackend` instead, which adds the ownership rules a
    task-provisioned sandbox needs.

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

        Every failure raised here is terminal, whichever class carries it. The
        model has no input into provisioning, so a :class:`SandboxError` from
        ``create`` is not something it can work around; the toolset re-raises one
        as :class:`SandboxTerminalError` and fails the task, so Airflow's retry
        attempts the provisioning again.
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

    # Reserved exit statuses for "the path is not readable" and "the path is a
    # directory", distinct from any status the guest's own command might return.
    _MISSING_PATH_STATUS = 66
    _IS_DIRECTORY_STATUS = 67

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
        # path yields an empty ``base64`` and reads back as an empty file. A
        # directory needs its own check for the same reason: ``stat`` succeeds
        # on it, ``head`` fails but the pipeline's status is ``base64``'s, so
        # without it a directory reads back as an empty file too.
        script = (
            f"sz=$(stat -Lc %s -- {quoted} 2>/dev/null) || exit {self._MISSING_PATH_STATUS}; "
            f"[ -d {quoted} ] && exit {self._IS_DIRECTORY_STATUS}; "
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
        if result.exit_code == self._IS_DIRECTORY_STATUS:
            raise SandboxError(f"{path!r} is a directory. Use list_directory to see what is in it.")
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


class AttachableSandboxBackend(SandboxBackend):
    """
    A backend whose sandboxes outlive the process that created them and can be found again.

    This is what lets one task provision a sandbox and a later agent task use it:
    the provisioning task stamps the sandbox with :attr:`SandboxSpec.owner`, the
    :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset` attaches
    with ``attach_to=<handle>``, and the task that created the sandbox destroys it.
    A backend that has no way to reach a sandbox from another process, such as
    ``sbx``, stays a plain :class:`SandboxBackend` and the toolset refuses
    ``attach_to`` for it at construction.

    Two primitives are the vendor's to implement: :meth:`read_tags` and
    :meth:`write_tags`. The rules are written once, here, on top of them:

    * **Who may attach.** A sandbox is attached only if its :data:`OWNER_TAG`
      equals the owner the toolset presents. A bare handle is never enough, so a
      wrong handle from an upstream XCom is refused rather than used. This stops a
      run reaching the wrong sandbox by mistake and gives attribution; it is not a
      boundary between authors, since anyone holding the vendor credential can
      rewrite the tags or drive the sandbox without the toolset.
    * **One holder at a time.** Attaching stamps :data:`HOLDER_TAG` with the
      attaching task and releasing clears it. A second, different holder is refused
      while the first is attached. The same holder may attach again, so a retry of
      the agent task finds its files after an attempt that died without releasing.
      The claim is a plain read-then-write over the vendor's tags, re-read after the
      write to catch a competing writer, so two tasks attaching in the same instant
      can both pass; it stops the sequential mistakes, not a race, and a Dag that
      needs a workspace per task provisions one per task.
    * **The lifetime is the creator's.** ``create`` stamps :data:`EXPIRES_AT_TAG`,
      and :meth:`attach` reports what is left so the toolset can bound its commands
      and tell the model the clock it is on. ``create`` also records the network
      policy under :data:`NETWORK_TAG`, so the toolset can tell the model what the
      sandbox reaches, and the working directory under :data:`WORKDIR_TAG`, so the
      attaching backend resolves relative paths where the creator's shell does.
    """

    @abstractmethod
    def read_tags(self, sandbox: str) -> Mapping[str, str]:
        """
        Return the tags on a sandbox.

        Raise :class:`SandboxTerminalError` if the sandbox does not exist or has
        ended: attaching to it can never succeed.
        """

    @abstractmethod
    def write_tags(self, sandbox: str, tags: Mapping[str, str]) -> None:
        """Replace the sandbox's tags with ``tags``."""

    def attach(self, sandbox: str, *, owner: str, holder: str) -> AttachedSandbox:
        """
        Claim ``sandbox`` for ``holder`` and report what the creator recorded about it.

        Raises :class:`SandboxTerminalError` if the sandbox is not owned by ``owner``
        or is held by someone else; either is a fact about the Dag that no retry of
        the agent task can change.
        """
        tags = self.read_tags(sandbox)
        actual_owner = tags.get(OWNER_TAG)
        if actual_owner != owner:
            carried = f"it carries owner {actual_owner!r}" if actual_owner else "it carries no owner"
            raise SandboxTerminalError(
                f"Sandbox {sandbox!r} on backend {self.name!r} is not owned by {owner!r}: {carried}. "
                "A sandbox can be attached to only by the owner it was provisioned for. Provision it "
                "with SandboxSpec(owner=dag_run_owner(context)) from a task in the Dag run that will "
                "attach to it, or give the toolset the owner the provisioning task used."
            )
        held_by = tags.get(HOLDER_TAG)
        if held_by and held_by != holder:
            raise SandboxTerminalError(
                f"Sandbox {sandbox!r} is already held by {held_by!r}, and one agent run uses a sandbox "
                "at a time. Provision a sandbox per task that needs its own workspace, or make the "
                "tasks run in sequence."
            )
        if held_by != holder:
            # A claim already in our name (a retry after an attempt that died) needs no
            # write. Otherwise write, then re-read: the vendor offers no conditional
            # write, so a competing attach that landed between our read and our write
            # shows up here, and one of the two backs off instead of both proceeding.
            self.write_tags(sandbox, {**tags, HOLDER_TAG: holder})
            written = self.read_tags(sandbox).get(HOLDER_TAG)
            if written != holder:
                raise SandboxTerminalError(
                    f"Sandbox {sandbox!r} was claimed by {written!r} while {holder!r} was attaching to "
                    "it. Two agent runs asked for the same sandbox at once; provision one per task."
                )
        return AttachedSandbox(
            remaining_lifetime=self._remaining(tags.get(EXPIRES_AT_TAG)),
            network=decode_network_policy(tags.get(NETWORK_TAG)),
            workdir=tags.get(WORKDIR_TAG) or None,
        )

    @staticmethod
    def _remaining(expires_at: str | None) -> float | None:
        if expires_at is None:
            return None
        try:
            return max(0.0, float(expires_at) - time.time())
        except ValueError:
            return None

    def release(self, sandbox: str, *, holder: str) -> None:
        """
        Give up ``holder``'s claim on ``sandbox`` so another run may attach.

        Idempotent, and never destroys anything: the sandbox belongs to the task
        that created it. A claim held by someone else is left alone.
        """
        tags = dict(self.read_tags(sandbox))
        if tags.get(HOLDER_TAG) != holder:
            return
        del tags[HOLDER_TAG]
        self.write_tags(sandbox, tags)
