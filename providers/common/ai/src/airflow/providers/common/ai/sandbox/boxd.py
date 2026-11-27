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
"""boxd microVM backend for :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`."""

from __future__ import annotations

import logging
import math
import os
import time
import uuid
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, NoReturn

from airflow.providers.common.ai.sandbox.base import (
    EXPIRES_AT_TAG,
    NETWORK_TAG,
    OWNER_TAG,
    AttachableSandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxSpec,
    SandboxTerminalError,
    _validate_positive_finite,
    encode_network_policy,
)
from airflow.providers.common.compat.sdk import BaseHook

# ``boxd`` is the optional ``[boxd]`` extra and the package exports this backend
# eagerly, so the SDK is imported where it is first used, never at module level.
if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from boxd import Boxd

log = logging.getLogger(__name__)

# The deadline is enforced inside the guest by GNU ``timeout``, which sends SIGTERM and
# then SIGKILL this many seconds later. The SDK call is given up on _CALL_GRACE seconds
# after that, so the wrapper still has time to print what the command wrote.
_KILL_AFTER = 5
_CALL_GRACE = 30
# boxd documents new egress rules as in force within about a second, so create waits a
# little longer before it hands over a machine with an allowlist.
_EGRESS_SETTLE = 3.0
# A boxd label value holds at most this many characters, and no control characters.
_MAX_LABEL_VALUE = 256
# Machine states from which a machine never serves another request.
_ENDED_STATUSES = frozenset({"destroyed", "failed"})
# Runs the agent's command under GNU ``timeout`` with each stream captured to a scratch
# file, then emits only the last ``$2`` bytes of each, so the transfer and the worker's
# copy stay at the caller's budget whatever the command prints. Redirecting to files
# rather than pipes means only the foreground command is waited on: ``sleep 20 & echo
# started`` returns at once. ``timeout`` puts the command in a process group of its own
# and signals the whole group, so a pipeline is stopped as a unit.
_COMMAND_WRAPPER = """\
dir=$(mktemp -d "${TMPDIR:-/tmp}/airflow-sandbox-XXXXXX") || exit 70
trap 'rm -rf "$dir"' EXIT
trap 'rm -rf "$dir"; exit 143' HUP INT TERM
timeout --kill-after="$4" "$3" bash -c "$1" >"$dir/out" 2>"$dir/err" </dev/null
status=$?
tail -c "$2" "$dir/out"
tail -c "$2" "$dir/err" >&2
exit "$status"
"""


def _describe(error: Exception) -> str:
    message = getattr(error, "message", None)
    if isinstance(message, str) and message.strip():
        return message.strip().rstrip(".")
    return type(error).__name__


@contextmanager
def _translate_boxd_errors(operation: str) -> Iterator[None]:
    """Turn anything the SDK raises into a terminal error naming the operation."""
    try:
        yield
    except SandboxError:
        raise
    except Exception as e:
        try:
            from boxd import AuthenticationError
        except ImportError:
            raise SandboxTerminalError(
                'The boxd SDK is not installed. Install "apache-airflow-providers-common-ai[boxd]".'
            ) from e
        if isinstance(e, AuthenticationError):
            raise SandboxTerminalError(
                f"boxd rejected the API key when asked to {operation}: {_describe(e)}."
            ) from e
        raise SandboxTerminalError(f"boxd could not {operation}: {_describe(e)}.") from e


def _raise_translated(error: Exception, operation: str) -> NoReturn:
    """Raise ``error`` the way :func:`_translate_boxd_errors` would have, for an error caught elsewhere."""
    with _translate_boxd_errors(operation):
        raise error


def _tail_stream(data: str | bytes, max_bytes: int) -> tuple[str, bool]:
    """
    Keep the last ``max_bytes`` of one stream, decoded, and report whether bytes were dropped.

    The wrapper hands back one byte more than the budget, so a stream that comes back
    over it is the sign the command printed more.
    """
    # ``encoding="buffer"`` asks for bytes, but ``ExecResult`` is typed to carry either.
    if isinstance(data, str):
        data = data.encode()
    truncated = len(data) > max_bytes
    if truncated:
        data = data[-max_bytes:]
    return data.decode("utf-8", errors="replace"), truncated


class BoxdSandboxBackend(AttachableSandboxBackend):
    """
    Sandbox backend that runs agent commands in a `boxd <https://boxd.sh/?ref=airflow>`__ microVM.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    boxd is a hosted API for KVM microVMs with a persistent disk. The worker only speaks
    to the API, with no local daemon or host virtualization, so this backend works from
    an Airflow worker running in a container.

    Credentials come from a generic Airflow connection, resolved on first use: its
    ``password`` is the boxd API key. Without a connection, the backend reads
    ``BOXD_API_KEY`` from the worker environment. Either way the key is required: the
    SDK can also authenticate as the machine it runs on, and a worker that happens to run
    on a boxd machine must not act as that machine's owner without being told to. The SDK
    reads ``BOXD_REGION`` (from boxd 0.3.0) and ``BOXD_BASE_URL`` from the worker
    environment too, to pick the region or the cluster.

    Every sandbox is an *isolated* machine: it has no metadata endpoint, no in-machine
    ``boxd`` CLI and none of the account's integration tokens, so model-generated code
    cannot act on the account or its other machines, and it cannot reach the
    organization's other machines over the private network. ``SandboxSpec`` network
    rules become the machine's egress allowlist, set when the machine is created, before
    anything runs in it.

    Sandboxes can be attached to from another task: the ownership tags live in the
    machine's labels, which any process holding the API key can read.

    Commands run as the machine's user ``boxd`` in its home directory, ``/home/boxd``,
    through ``bash``. The image needs ``bash``, ``mktemp``, ``rm`` and ``tail`` for the
    command wrapper, GNU ``timeout`` for the deadline, and ``stat``, ``head``, ``base64``
    and GNU ``find`` for file reads, exports and listings. The default image (Ubuntu
    24.04) and any snapshot of a machine made from it have them.

    :param boxd_conn_id: Generic Airflow connection ID whose ``password`` is the boxd API
        key. ``None`` (default) reads ``BOXD_API_KEY`` from the worker environment.
    :param size: Machine size as ``"<vcpu>x<memory GiB>"``, such as ``"2x8"``. ``None``
        (default) uses the organization's default size.
    :param snapshot: Name of a boxd snapshot to boot each sandbox from, for an image with
        your tools already installed. ``None`` (default) uses the default image.
    :param delete_after: Seconds after creation at which boxd deletes the machine,
        whether or not it is in use. This is the backstop for a worker killed mid-run,
        and the lifetime an attaching toolset is told about. ``None`` disables it.
        Default ``3600``.
    :param name_prefix: Prefix of each machine's name, which is followed by ``-`` and a
        random suffix. Machine names are unique within the organization and shown in its
        console. Default ``"airflow-sandbox"``.
    :param labels: Extra labels to set on every machine, such as ``{"team": "data"}``,
        to find them with ``boxd machine list --label``. The ``airflow_*`` labels the
        backend sets overwrite any of the same name.
    :param ready_timeout: Seconds allowed from the create request until the machine
        answers a command. Default ``120``.
    """

    name = "boxd"

    def __init__(
        self,
        *,
        boxd_conn_id: str | None = None,
        size: str | None = None,
        snapshot: str | None = None,
        delete_after: int | None = 3600,
        name_prefix: str = "airflow-sandbox",
        labels: Mapping[str, str] | None = None,
        ready_timeout: float = 120.0,
    ) -> None:
        if delete_after is not None:
            _validate_positive_finite(delete_after, "delete_after")
            # Refused rather than floored: int() would quietly shorten the backstop.
            if int(delete_after) != delete_after:
                raise ValueError(f"delete_after must be a whole number of seconds, got {delete_after!r}.")
        _validate_positive_finite(ready_timeout, "ready_timeout")
        for value, what in ((size, "size"), (snapshot, "snapshot")):
            if value is not None and not value.strip():
                raise ValueError(f"{what} must not be empty; pass None for the default.")
        if not name_prefix.strip():
            raise ValueError("name_prefix must not be empty.")
        self._boxd_conn_id = boxd_conn_id
        self._size = size
        self._snapshot = snapshot
        self._delete_after = None if delete_after is None else int(delete_after)
        self._name_prefix = name_prefix.strip()
        self._labels = dict(labels) if labels else {}
        self._ready_timeout = ready_timeout
        self._client: Boxd | None = None

    def _get_client(self) -> Boxd:
        if self._client is not None:
            return self._client
        with _translate_boxd_errors("initialize its client"):
            from boxd import Boxd

            if self._boxd_conn_id is None:
                api_key = (os.environ.get("BOXD_API_KEY") or "").strip()
                if not api_key:
                    raise SandboxTerminalError(
                        "BOXD_API_KEY is not set in the worker environment; set it, or pass "
                        "boxd_conn_id naming a connection whose password is the boxd API key."
                    )
            else:
                conn = BaseHook.get_connection(self._boxd_conn_id)
                api_key = (conn.password or "").strip()
                if not api_key:
                    raise SandboxTerminalError(
                        f"Connection {self._boxd_conn_id!r} has no password; set it to the boxd API key."
                    )
            self._client = Boxd(api_key=api_key)
        return self._client

    @staticmethod
    def _build_egress_entries(spec: SandboxSpec) -> list[str]:
        """Return the egress allowlist that enforces ``spec``; empty means unrestricted."""
        if not spec.block_network:
            return []
        entries = [*(spec.allow_egress_to or ()), *(spec.allow_egress_to_cidrs or ())]
        # ``none`` alone keeps the allowlist on with nothing in it.
        return entries or ["none"]

    @staticmethod
    def _check_spec(spec: SandboxSpec) -> None:
        """Refuse a spec this backend cannot carry faithfully, before anything is provisioned."""
        if not spec.block_network and (spec.allow_egress_to or spec.allow_egress_to_cidrs):
            raise SandboxTerminalError(
                "SandboxSpec names an egress allowlist but leaves block_network False, which opens "
                "outbound access to everything. Leave block_network at its default of True to "
                "restrict egress to the allowlist, or drop the allowlist."
            )
        if spec.env:
            not_strings = sorted(
                str(key)
                for key, value in spec.env.items()
                if not isinstance(key, str) or not isinstance(value, str)
            )
            if not_strings:
                raise SandboxTerminalError(
                    f"SandboxSpec.env must map strings to strings; these entries do not: {not_strings}. "
                    "Convert the value at the Dag level, e.g. str(port), so what reaches the sandbox "
                    "is what you meant."
                )
        if spec.owner is not None:
            _check_label_value(OWNER_TAG, spec.owner)

    def _create_labels(self, spec: SandboxSpec) -> dict[str, str]:
        """Return the labels a new machine carries: the author's, then the ones this backend reserves."""
        labels = dict(self._labels)
        if self._delete_after is not None:
            labels[EXPIRES_AT_TAG] = str(int(time.time()) + self._delete_after)
        network = encode_network_policy(spec)
        if len(network) <= _MAX_LABEL_VALUE:
            labels[NETWORK_TAG] = network
        else:
            # The allowlist is still enforced; only the description an attaching toolset
            # gives the model is lost, and it then tells the model to test first.
            log.warning(
                "The network policy is %d characters, over boxd's %d character label limit, so it is "
                "not recorded on the machine; an agent attaching to it is not told what it can reach.",
                len(network),
                _MAX_LABEL_VALUE,
            )
            labels.pop(NETWORK_TAG, None)
        if spec.owner is not None:
            labels[OWNER_TAG] = spec.owner
        else:
            labels.pop(OWNER_TAG, None)
        return labels

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        if spec is None:
            # No requirements stated: apply the isolated default rather than an open machine.
            spec = SandboxSpec()
        self._check_spec(spec)
        client = self._get_client()
        egress = self._build_egress_entries(spec)
        with _translate_boxd_errors("create a sandbox"):
            from boxd.spec import snapshot, spec as machine_spec

            builder = machine_spec().isolated().label(self._create_labels(spec))
            if spec.env:
                builder = builder.env(dict(spec.env))
            if egress:
                builder = builder.egress(*egress)
            if self._size is not None:
                builder = builder.size(self._size)
            if self._snapshot is not None:
                builder = builder.from_(snapshot(self._snapshot))
        # Bound before the call. If the create fails after the server made the machine
        # -- a lost answer, a retried request that then meets the name it took -- the
        # name is the only handle that can still delete it.
        name = f"{self._name_prefix}-{uuid.uuid4().hex[:12]}"
        try:
            with _translate_boxd_errors("create a sandbox"):
                machine = client.machines.create(name, spec=builder, delete_after=self._delete_after)
            self._check_created(machine, egress)
            with _translate_boxd_errors("wait for a sandbox to start"):
                client.machines.wait_until_ready(machine.id, timeout=self._ready_timeout)
            if egress:
                time.sleep(_EGRESS_SETTLE)
        except BaseException:
            with suppress(Exception):
                self.destroy(name)
            raise
        return machine.id

    @staticmethod
    def _check_created(machine: Any, egress: list[str]) -> None:
        """Fail closed when the machine boxd answered with does not carry the policy asked for."""
        # Only whether a policy is in force is compared: the server may normalize the
        # entries themselves, such as an internationalized name into its ASCII form.
        if egress and not machine.egress_allow:
            raise SandboxTerminalError(
                f"boxd created machine {machine.name!r} without the egress allowlist the SandboxSpec "
                "asks for, so it was deleted."
            )
        if not (machine.spec or {}).get("isolated"):
            raise SandboxTerminalError(
                f"boxd created machine {machine.name!r} without isolation, so it was deleted."
            )

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        # GNU timeout reads 0 as "no timeout", so round up.
        timeout_seconds = max(1, math.ceil(timeout))
        client = self._get_client()
        started = time.monotonic()
        try:
            result = client.machines.exec(
                sandbox,
                # One byte over the budget, so a stream that comes back over it is proof
                # the command printed more.
                [
                    "sh",
                    "-c",
                    _COMMAND_WRAPPER,
                    "airflow-sandbox",
                    command,
                    str(int(max_output_bytes) + 1),
                    str(timeout_seconds),
                    str(_KILL_AFTER),
                ],
                timeout=timeout_seconds + _KILL_AFTER + _CALL_GRACE,
                encoding="buffer",
            )
        except Exception as e:
            elapsed = time.monotonic() - started
            if elapsed >= timeout_seconds + _KILL_AFTER + _CALL_GRACE:
                # The deadline inside the guest did not end the command, so it may still be
                # running and holding the machine; there is nothing left to trust in it.
                self._destroy_after_timeout(sandbox)
                return SandboxExecResult(
                    exit_code=-1,
                    stdout="",
                    stderr="",
                    timed_out=True,
                    sandbox_terminated=True,
                    applied_timeout=float(timeout_seconds),
                )
            _raise_translated(e, "run a sandbox command")
        elapsed = time.monotonic() - started
        stdout, out_truncated = _tail_stream(result.stdout, int(max_output_bytes))
        stderr, err_truncated = _tail_stream(result.stderr, int(max_output_bytes))
        exit_code = result.exit_code
        return SandboxExecResult(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            # timeout exits 124 after its SIGTERM and 137 once it escalates to SIGKILL,
            # _KILL_AFTER later. Both are also ordinary exits, so only one that late is timeout's.
            timed_out=(exit_code == 124 and elapsed >= timeout_seconds)
            or (exit_code == 137 and elapsed >= timeout_seconds + _KILL_AFTER),
            stdout_truncated=out_truncated,
            stderr_truncated=err_truncated,
            applied_timeout=float(timeout_seconds),
        )

    def _destroy_after_timeout(self, sandbox: str) -> None:
        try:
            self.destroy(sandbox)
        except SandboxError as e:
            raise SandboxTerminalError(
                "A boxd command outlived its deadline and deletion of its sandbox could not be confirmed."
            ) from e

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        """
        Override: write through boxd's file API, which creates missing parent directories.

        The inherited default carries the payload in the command line, which a file of
        more than about 100 KiB overflows.
        """
        client = self._get_client()
        try:
            client.machines.fs.write(sandbox, path, content)
        except Exception as e:
            from boxd import MachineFilesystemError

            if isinstance(e, MachineFilesystemError):
                # Raised inside the machine (a missing or unwritable directory, a path
                # through a file), so the model can pick another path.
                raise SandboxError(f"Could not write {path!r}: {_describe(e)}.") from e
            _raise_translated(e, "write a sandbox file")

    def destroy(self, sandbox: str) -> None:
        client = self._get_client()
        try:
            client.machines.delete(sandbox)
        except Exception as e:
            from boxd import NotFoundError

            if isinstance(e, NotFoundError):
                return
            _raise_translated(e, "delete a sandbox")

    def read_tags(self, sandbox: str) -> dict[str, str]:
        client = self._get_client()
        with _translate_boxd_errors(f"find sandbox {sandbox!r}"):
            machine = client.machines.get(sandbox)
        if machine.status in _ENDED_STATUSES:
            raise SandboxTerminalError(f"boxd machine {sandbox!r} has ended (status={machine.status!r}).")
        return dict(machine.labels)

    def write_tags(self, sandbox: str, tags: Mapping[str, str]) -> None:
        for key, value in tags.items():
            _check_label_value(key, value)
        client = self._get_client()
        with _translate_boxd_errors(f"label sandbox {sandbox!r}"):
            client.machines.update_labels(sandbox, dict(tags), replace_all=True)


def _check_label_value(key: str, value: str) -> None:
    if len(value) > _MAX_LABEL_VALUE:
        raise SandboxTerminalError(
            f"The {key} label would be {len(value)} characters, and a boxd label holds at most "
            f"{_MAX_LABEL_VALUE}. Use a shorter Dag id, run id or task id, or pass a shorter owner."
        )
