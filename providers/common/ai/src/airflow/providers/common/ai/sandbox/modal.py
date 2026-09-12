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
"""Modal hosted-sandbox backend for the SandboxToolset."""

from __future__ import annotations

import ipaddress
import logging
import math
import posixpath
import re
import threading
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Literal

try:
    import modal
except ModuleNotFoundError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    # Importing this module directly without the extra installed is how the provider
    # verifier walks every submodule, and how a Dag author reaching past the package
    # __getattr__ gets here. Both want the optional-feature signal, not ImportError.
    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.sandbox.base import (
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxTerminalError,
    _new_sandbox_name,
    _validate_positive_finite,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from airflow.providers.common.ai.sandbox.base import SandboxSpec

log = logging.getLogger(__name__)

DEFAULT_IMAGE = "python:3.12-slim"
DEFAULT_APP_NAME = "airflow-sandbox"
# Modal creates this directory if the image does not carry it, so the agent gets a
# writable working directory rather than "/".
DEFAULT_WORKDIR = "/workspace"
DEFAULT_SANDBOX_TIMEOUT = 3600
# Off by default, as it is in Modal. One sandbox serves a whole agent run and nothing
# keeps it warm between tool calls, so a gap for model generation, another toolset's
# work, or a human review pause would look idle and reclaim it mid-run, losing every
# file the agent had written. ``sandbox_timeout`` already bounds what a leak can cost.
DEFAULT_IDLE_TIMEOUT = None

# Extra wall-clock beyond a command's own deadline before we stop waiting on the
# stream readers. Modal enforces the deadline itself, so this only has to absorb
# the round trip that reports it.
_DRAIN_GRACE = 30.0
# Bounded budget for the small helper commands behind the inherited file operations.
_FILE_OP_TIMEOUT = 120.0
# Modal reports a command that hit its deadline as this exit status, in place of
# raising: modal/container_process.py's poll/wait catch ExecTimeoutError and set it.
# No exit status a guest command can return collides with it.
_DEADLINE_RETURNCODE = -1
# Modal maps a signal death to 128 + signal, and a sandbox dying under a command surfaces
# as one of these too, which is why they are worth a liveness question.
_SIGNAL_EXIT_BASE = 128
# 128 + SIGKILL. Either the guest was killed inside a living sandbox, or the deadline was
# enforced by a kill rather than reported as -1; see _hit_the_deadline.
_SIGKILL_EXIT = 137
# Budget for that question. It runs `true`, so anything slower than this means the answer
# is not going to arrive usefully either way.
_LIVENESS_TIMEOUT = 10

# A plain hostname, optionally with a single leading "*." wildcard label, which is what
# Modal matches an SNI value against. Modal itself sends the strings on without
# validating them, so anything else -- a URL, a host:port, a bare "*" -- would be
# accepted here and then silently match nothing (or, for "*", everything).
_ALLOWED_HOSTNAME = re.compile(
    r"^(?:\*\.)?(?!-)[A-Za-z0-9-]{1,63}(?<!-)(?:\.(?!-)[A-Za-z0-9-]{1,63}(?<!-))*$"
)

EgressEnforcement = Literal["strict", "sni"]


def _is_tls_hostname(value: object) -> bool:
    """Whether ``value`` is a name a TLS handshake could actually present."""
    if not isinstance(value, str) or not _ALLOWED_HOSTNAME.match(value):
        return False
    with suppress(ValueError):
        ipaddress.ip_address(value)
        # An address is not a name. A client sends no SNI for one, so allowlisting it
        # would allow nothing while reading like a restriction.
        return False
    # A final label of digits is not a hostname either (RFC 1123), which also catches a
    # bare port number written on its own.
    return not value.rsplit(".", 1)[-1].isdigit()


class ModalSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent commands in a `Modal <https://modal.com>`__ sandbox.

    Each sandbox is a gVisor-isolated container in Modal's infrastructure, provisioned
    over the API. Nothing has to be installed on the Airflow worker and model-written
    code never executes on the worker host, which makes this the backend to reach for
    on Kubernetes, where :class:`~airflow.providers.common.ai.sandbox.SbxSandboxBackend`
    cannot run at all.

    Modal reclaims a sandbox at ``sandbox_timeout`` whatever happens to the worker, so a
    worker killed outright cannot leak one. Sandboxes are also named ``airflow-sandbox-*``
    and carry whatever ``tags`` you set, so they can be found in Modal's dashboard. Those
    tags are fixed when the backend is constructed, which is Dag-parse time, so they can
    identify the Dag but not the run, task or map index that leaked one.

    **Credentials are ambient.** Modal is authenticated the same way its CLI is: run
    ``modal token new`` once to write ``~/.modal.toml``, or set ``MODAL_TOKEN_ID`` and
    ``MODAL_TOKEN_SECRET`` in the worker environment. Nothing is read until the first
    sandbox is created, so a Dag file that constructs this backend parses without
    credentials present.

    **What the network policy can and cannot promise.** ``SandboxSpec(block_network=True)``
    maps to Modal's own ``block_network``, which drops all outbound traffic including DNS.
    A :class:`~airflow.providers.common.ai.sandbox.SandboxSpec` that also names
    ``allow_egress_to`` is **refused by default**, because Modal cannot combine an
    allowlist with ``block_network`` and its hostname allowlist is enforced by matching
    the TLS SNI: it permits TLS on port 443 to the listed hosts, blocks other hosts, and
    leaves DNS resolution open for every hostname. Sandbox code can therefore still reach
    a non-listed host that shares a TLS endpoint with a listed one, and can still carry
    data out through DNS queries. Pass ``egress_enforcement="sni"`` to accept that and
    have the allowlist applied.

    **A timeout does not cost you the sandbox.** Modal stops the command server-side and
    the sandbox stays usable, so files written by earlier calls survive and the model can
    inspect them. That differs from ``sbx``, which has to destroy the sandbox to be sure
    a command stopped.

    The image must provide ``sh``, and ``stat``, ``head`` and ``base64`` for
    :meth:`read_file`, which deliberately stays on the base class's shell implementation
    (see that method). Any Debian or Ubuntu based image, including ``python:*-slim``, does.

    :param image: Registry tag for the sandbox image, or a prepared ``modal.Image``.
        Default ``"python:3.12-slim"``. An image carrying the packages an agent needs is
        the alternative to opening egress so it can install them:
        ``modal.Image.from_registry("python:3.12-slim").pip_install("pandas")``.
    :param app_name: Modal app the sandboxes are created under. Default
        ``"airflow-sandbox"``.
    :param create_app_if_missing: Create the Modal app when it does not exist yet.
        Default ``True``.
    :param sandbox_timeout: Maximum lifetime in seconds of a sandbox before Modal shuts
        it down. Bounds the whole sandbox, where a ``run_command`` timeout bounds one
        command. Default ``3600``; Modal's own default of 300 is below a plausible agent
        run.
    :param idle_timeout: Seconds of inactivity after which Modal reclaims the sandbox.
        Default ``None``, meaning ``sandbox_timeout`` is the only bound. Set it only if
        you want tighter cost control and know your agent's pace: one sandbox serves a
        whole run, nothing keeps it warm between tool calls, and a gap for model
        generation or a human review would be reclaimed with every file in it.
    :param workdir: Working directory for commands, created if the image lacks it.
        Default ``"/workspace"``. ``None`` uses the image's own default, in which case
        the backend has to ask the sandbox where that is before its first file
        operation, so prefer stating it.
    :param cpu: CPU cores to request. ``None`` uses Modal's default. Takes effect: a
        sandbox created with ``cpu=4`` reports 4 from ``nproc``, though ``/proc/cpuinfo``
        still lists the host's cores.
    :param memory: Memory in MiB to request. ``None`` uses Modal's default. **A request,
        not a ceiling**: a sandbox created with ``memory=512`` allocated 1.5 GiB without
        complaint when measured, so this schedules the sandbox somewhere with room and does
        not bound what model-written code can take. Nothing inside the sandbox can see it
        either -- ``/proc/meminfo`` reports the host's memory.
    :param gpu: GPU specification, e.g. ``"A10G"``. ``None`` requests none. Billed at a
        different rate, so set it deliberately.
    :param region: Region or regions to run in. ``None`` lets Modal choose. Validated by
        Modal, so an unrecognized region fails the task rather than falling back.
    :param cloud: Cloud provider to run on. ``None`` lets Modal choose. Validated the same
        way.
    :param tags: Extra Modal tags to set on every sandbox, e.g. ``{"dag_id": "my_dag"}``.
        You can query them, which is the point: ``modal.Sandbox.list(app_id=..., tags={"dag_id":
        "my_dag"})`` returns exactly the sandboxes carrying them, which is how an operator
        finds what a Dag left behind. ``airflow_sandbox`` is set by this backend and will
        overwrite a key of that name.
    :param egress_enforcement: ``"strict"`` (default) refuses a ``SandboxSpec`` that
        names ``allow_egress_to``, because Modal cannot enforce a hostname allowlist
        below TLS. ``"sni"`` accepts it and applies Modal's SNI-matched allowlist, with
        the limits described above.
    """

    name = "modal"

    def __init__(
        self,
        *,
        image: str | modal.Image = DEFAULT_IMAGE,
        app_name: str = DEFAULT_APP_NAME,
        create_app_if_missing: bool = True,
        sandbox_timeout: int = DEFAULT_SANDBOX_TIMEOUT,
        idle_timeout: int | None = DEFAULT_IDLE_TIMEOUT,
        workdir: str | None = DEFAULT_WORKDIR,
        cpu: float | None = None,
        memory: int | None = None,
        gpu: str | None = None,
        region: str | Sequence[str] | None = None,
        cloud: str | None = None,
        tags: Mapping[str, str] | None = None,
        egress_enforcement: EgressEnforcement = "strict",
    ) -> None:
        if isinstance(image, str) and not image:
            raise ValueError("image must not be empty.")
        if not app_name:
            raise ValueError("app_name must not be empty.")
        _validate_positive_finite(sandbox_timeout, "sandbox_timeout")
        if sandbox_timeout < _FILE_OP_TIMEOUT:
            # The inherited file operations run helper commands with a fixed budget, and
            # a command may not outlive its sandbox. A shorter lifetime would make
            # read_file fail every time with advice the model cannot act on, since that
            # tool has no timeout argument to shorten.
            raise ValueError(
                f"sandbox_timeout must be at least {_FILE_OP_TIMEOUT:g} seconds, the budget the "
                f"file operations run with, got {sandbox_timeout}."
            )
        if idle_timeout is not None:
            _validate_positive_finite(idle_timeout, "idle_timeout")
            if idle_timeout > sandbox_timeout:
                # An idle timeout past the sandbox's own lifetime could never fire.
                raise ValueError(
                    f"idle_timeout ({idle_timeout}) must not exceed sandbox_timeout ({sandbox_timeout})."
                )
        if workdir is not None and not workdir.startswith("/"):
            raise ValueError(f"workdir must be an absolute path, got {workdir!r}.")
        if cpu is not None:
            _validate_positive_finite(cpu, "cpu")
        if memory is not None:
            _validate_positive_finite(memory, "memory")
        if egress_enforcement not in ("strict", "sni"):
            raise ValueError(f"egress_enforcement must be 'strict' or 'sni', got {egress_enforcement!r}.")
        self._image = image
        self._app_name = app_name
        self._create_app_if_missing = create_app_if_missing
        self._sandbox_timeout = int(sandbox_timeout)
        self._idle_timeout = None if idle_timeout is None else int(idle_timeout)
        self._workdir = workdir
        self._cpu = cpu
        self._memory = None if memory is None else int(memory)
        self._gpu = gpu
        self._region = region
        self._cloud = cloud
        self._tags = dict(tags) if tags else {}
        self._egress_enforcement: EgressEnforcement = egress_enforcement
        # Keyed by sandbox handle, so one backend instance shared by concurrent agent
        # runs never has two of them touching the same entry.
        self._sandboxes: dict[str, modal.Sandbox] = {}
        self._workdirs: dict[str, str] = {}
        self._expiries: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Lifecycle.
    # ------------------------------------------------------------------

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        # Both refuse a spec this backend cannot carry faithfully, before anything is
        # provisioned, so a rejected spec never costs a sandbox.
        network = self._network_kwargs(spec)
        environment = self._environment(spec)
        name = _new_sandbox_name()
        try:
            # Looked up per create rather than memoized on the instance: the lookup is
            # idempotent server-side, and one round trip per sandbox is cheaper than the
            # locking a shared cache would need, given the toolset shares one backend
            # across concurrent runs.
            app = modal.App.lookup(self._app_name, create_if_missing=self._create_app_if_missing)
            # A registry tag is the common case; a prepared Image is how packages get
            # into the sandbox without opening egress to fetch them.
            image = modal.Image.from_registry(self._image) if isinstance(self._image, str) else self._image
            sandbox = modal.Sandbox.create(
                app=app,
                image=image,
                name=name,
                tags={**self._tags, "airflow_sandbox": name},
                timeout=self._sandbox_timeout,
                idle_timeout=self._idle_timeout,
                workdir=self._workdir,
                env=environment,
                cpu=self._cpu,
                memory=self._memory,
                gpu=self._gpu,
                region=self._region,
                cloud=self._cloud,
                **network,
            )
        except modal.exception.Error as e:
            error = self._as_sandbox_error(e)
            if isinstance(error, SandboxTerminalError):
                raise error from e
            # Everything that escapes create is terminal, whatever its shape. The toolset
            # provisions the sandbox outside the block that turns a recoverable error into
            # a ModelRetry, so a recoverable label here is one the model never sees, and
            # could not act on if it did: no prompt fixes a bad image tag or an
            # unreachable control plane. Airflow's own retry is the right handler.
            raise SandboxTerminalError(f"Could not create a Modal sandbox: {error}") from e
        # Nothing is applied after Sandbox.create returns: the environment, the network
        # policy, the working directory and the tags are all create-time arguments. So
        # there is no window in which a live sandbox exists half-configured, and no
        # cleanup block to go with it. Anything added here later needs one.
        handle = sandbox.object_id
        self._sandboxes[handle] = sandbox
        # When this sandbox stops existing, so a command that could not finish inside its
        # remaining life is refused up front instead of dying half way through.
        self._expiries[handle] = time.monotonic() + self._sandbox_timeout
        return handle

    def destroy(self, sandbox: str) -> None:
        """
        Ask Modal to terminate the sandbox, without waiting for it to finish stopping.

        The request is not blocking on purpose. Measured against modal 1.5.5 in September
        2026, ``terminate()`` returned in under 0.2s while ``terminate(wait=True)`` took 31
        seconds, and this runs in the teardown path of every agent run, where that would be
        pure added latency. The sandbox stops shortly afterwards and shows an exit status
        within about 35 seconds; ``sandbox_timeout`` is the backstop if the request never
        lands at all.
        """
        handle = self._sandboxes.pop(sandbox, None)
        self._workdirs.pop(sandbox, None)
        self._expiries.pop(sandbox, None)
        try:
            target = handle if handle is not None else modal.Sandbox.from_id(sandbox)
            target.terminate()
        except modal.exception.NotFoundError:
            # Already gone, which is what destroy is for. Idempotent by contract.
            pass
        except modal.exception.Error:
            # Terminating a sandbox that is already finished, or a control-plane blip.
            # Modal reclaims it at sandbox_timeout or idle_timeout either way, so log
            # and let the toolset finish its run.
            log.warning("Could not terminate Modal sandbox %s", sandbox, exc_info=True)

    # ------------------------------------------------------------------
    # Commands.
    # ------------------------------------------------------------------

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        # Modal takes whole-second deadlines and reads 0 as "no timeout", so round up.
        seconds = max(1, math.ceil(timeout))
        if seconds > self._sandbox_timeout:
            # Against the whole lifetime, which is a fact the Dag author chose and can
            # change. Recoverable, because the model can also just ask for less.
            raise SandboxError(
                f"A {seconds}s command does not fit this sandbox's {self._sandbox_timeout}s "
                "lifetime. Ask for a shorter timeout, or raise sandbox_timeout on the backend."
            )
        # Against what is LEFT, the answer is to shorten the deadline rather than refuse.
        # Refusing here would be unactionable: the file operations inherited from the base
        # class run on a fixed internal budget that neither the model nor the Dag author
        # can lower, so once a sandbox had less of that budget left than the budget itself
        # -- which happens to every sandbox eventually, whatever its lifetime -- read_file
        # would fail for the rest of its life with advice nobody could follow. A clamped
        # deadline runs the work that fits, and if the sandbox does end underneath it, the
        # liveness check below reports that plainly instead.
        remaining = self._remaining_lifetime(sandbox)
        if remaining is not None and seconds > remaining:
            log.debug(
                "Shortening a %ss command to the %.0fs sandbox %s has left",
                seconds,
                remaining,
                sandbox,
            )
            seconds = max(1, int(remaining))
        handle = self._handle(sandbox)
        started = time.monotonic()
        try:
            process = handle.exec("sh", "-c", command, timeout=seconds, text=False)
        except modal.exception.Error as e:
            raise self._as_sandbox_error(e, sandbox=sandbox) from e

        cap = int(max_output_bytes)
        stdout, stderr = bytearray(), bytearray()
        out_truncated, err_truncated = [False], [False]
        # Drained in threads, so a command that fills one stream while we read the other
        # cannot wedge the call, and so a stream that never ends cannot outlive the
        # deadline: the joins below are bounded and the threads are daemons.
        drains = [
            threading.Thread(
                target=self._drain, args=(process.stdout, stdout, out_truncated, cap), daemon=True
            ),
            threading.Thread(
                target=self._drain, args=(process.stderr, stderr, err_truncated, cap), daemon=True
            ),
        ]
        for thread in drains:
            thread.start()

        try:
            returncode = process.wait()
        except modal.exception.ExecTimeoutError:
            # Not raised by modal 1.5.x, which reports the deadline as an exit status
            # instead, carrying its own note that it should probably raise. Handled so
            # this keeps working if it starts to.
            returncode = _DEADLINE_RETURNCODE
        except modal.exception.Error as e:
            raise self._as_sandbox_error(e, sandbox=sandbox) from e
        # Measured from here rather than from the start of the command: the streams have
        # only the round trip left to deliver, and a command that finished in a second
        # must not leave a background writer holding the fd for the rest of its budget.
        deadline = time.monotonic() + _DRAIN_GRACE
        for thread in drains:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in drains):
            # Something still holds a stream open. Report what arrived and mark it short
            # rather than blocking past the caller's budget.
            out_truncated[0] = err_truncated[0] = True

        elapsed = time.monotonic() - started
        # A sandbox that died under the command reports the same way an ordinary failure
        # does, so ask. Only for a deadline or a signal exit, which is where a death can
        # hide: an ordinary non-zero exit costs no round trip.
        terminated = False
        if returncode == _DEADLINE_RETURNCODE or returncode >= _SIGNAL_EXIT_BASE:
            terminated = not self._still_alive(sandbox)
            if terminated:
                # The handle is dead, so drop it rather than hand it to a later call.
                self._sandboxes.pop(sandbox, None)
                self._workdirs.pop(sandbox, None)
                self._expiries.pop(sandbox, None)

        return SandboxExecResult(
            exit_code=returncode,
            stdout=stdout.decode(errors="replace"),
            stderr=stderr.decode(errors="replace"),
            timed_out=self._hit_the_deadline(returncode, elapsed=elapsed, budget=seconds),
            stdout_truncated=out_truncated[0],
            stderr_truncated=err_truncated[0],
            sandbox_terminated=terminated,
            # Whole seconds, and shortened when the sandbox had less life left than the
            # command asked for, so the model hears the budget it actually had.
            applied_timeout=seconds,
        )

    @staticmethod
    def _hit_the_deadline(returncode: int, *, elapsed: float, budget: int) -> bool:
        """
        Whether the command was stopped by its deadline rather than by something inside it.

        Modal usually reports a deadline as ``-1``, but not always: the same ``sleep 30``
        at a two second budget has been observed returning ``137`` instead, at the same
        elapsed time, from the same SDK version. Depending on the status alone therefore
        loses about a quarter of deadlines in the environments where the race shows, and
        the model reads a bare ``[exit code: 137]`` for a command that simply ran long.

        Elapsed time separates the two cleanly, which is what ``sbx`` already does with the
        same ambiguity. Measured: a deadline lands at or just past its budget (2.02-2.97s
        against 2s), while a guest killing itself lands nowhere near it (0.04s against 30s),
        including ``sleep 3; kill -9`` at 3.03s of a 30s budget. What is left over is a real
        SIGKILL that happens to arrive after the budget, an OOM at the very end of a long
        command, which no signal available here can tell apart.

        Worth knowing before trusting the second branch: the ``137`` deadline has been seen
        three times, all inside one ten-minute window on a host whose Docker daemon was
        half-wedged at the time, and 40 consecutive deadlines since have all been ``-1``,
        including 16 in that same container image and Python version. Host load is the
        plausible trigger, which is to say the race appears exactly when a worker is busy
        -- so the branch is worth its two lines even though it cannot be summoned on
        demand. It is unit-covered and costs nothing when it never fires.
        """
        if returncode == _DEADLINE_RETURNCODE:
            return True
        return returncode == _SIGKILL_EXIT and elapsed >= budget

    def _still_alive(self, sandbox: str) -> bool:
        """
        Whether the sandbox can still run a command, asked by running the cheapest one.

        ``poll()`` is the obvious question and the wrong one: measured against modal
        1.5.5, a sandbox terminated mid-command still polled ``None`` while a command in
        it was already impossible. A trivial ``exec`` answers truthfully within about two
        tenths of a second, either returning 0 or raising -- ``NotFoundError`` for a
        terminated sandbox, ``ConflictError`` for one shutting down at its own timeout.
        """
        handle = self._sandboxes.get(sandbox)
        if handle is None:
            return False
        try:
            handle.exec("sh", "-c", "true", timeout=_LIVENESS_TIMEOUT, text=True).wait()
        except modal.exception.Error:
            return False
        except Exception:
            # Nothing here is worth failing a command that already produced its output.
            log.debug("Could not check whether Modal sandbox %s is alive", sandbox, exc_info=True)
            return True
        return True

    @staticmethod
    def _drain(stream, buf: bytearray, flag: list[bool], cap: int) -> None:
        """Read a stream to its end, retaining at most ``cap`` bytes from the tail."""
        # Keep the tail: the model reads this to fix its own command, and a traceback
        # plus the exit status live at the end. The toolset's formatter promises a tail
        # too, so retaining the head would hand it a window that no longer has one.
        try:
            for chunk in stream:
                buf.extend(chunk)
                if len(buf) > cap:
                    del buf[: len(buf) - cap]
                    flag[0] = True
                    # Drop the leading partial line so no fragment is presented as a
                    # whole record -- but only when dropping it is cheap. One line longer
                    # than the budget has its newline at the very end, so dropping through
                    # it would leave nothing at all, and the toolset labels only a
                    # non-empty stream: the model would read "(no output)" for a command
                    # that produced megabytes. A long line followed by a short one is the
                    # same trade in miniature, handing back a few bytes of a 50 KiB
                    # window. The formatter already marks output as cut, so a partial
                    # first line misleads nobody, while throwing away most of the window
                    # to avoid one does. Half the window is the line between the two.
                    newline = buf.find(b"\n")
                    if newline != -1 and len(buf) - (newline + 1) >= cap // 2:
                        del buf[: newline + 1]
        except Exception:
            # The sandbox died mid-stream, or the deadline cut the connection. What the
            # command actually did is decided by ``wait()``, which classifies the same
            # failure with the exit status in hand, so record the loss and stop reading.
            flag[0] = True
            log.debug("Modal sandbox stream ended early", exc_info=True)

    # ------------------------------------------------------------------
    # File operations.
    #
    # ``read_file`` is deliberately NOT overridden. Modal's ``filesystem.read_bytes``
    # takes no length, so honoring ``max_bytes`` through it means stat-then-read: the
    # TOCTOU window the base class documents avoiding, and no protection at all against
    # anything ``stat`` reports as zero-length. Measured against modal 1.5.5, a
    # ``read_bytes("/dev/zero")`` never returned and ``stat`` called that device
    # ``size=0``, while a 200 MB regular file came back whole with no server-side
    # ceiling. The base class's ``head -c`` runs inside the guest and cannot be lied to.
    # ------------------------------------------------------------------

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        """
        Override: write through Modal's filesystem API instead of a shell command.

        The base implementation carries the payload in the command itself, so the guest's
        command-line length caps it. ``write_bytes`` streams the content and creates
        parent directories itself.

        One behavioral difference this buys, worth knowing before pointing an agent at a
        tree of symlinks: writing to a path that is a symlink **replaces the link with a
        regular file** and leaves the original target untouched, where a shell redirect
        (what ``sbx`` and the base class do) follows the link and writes through it.
        """
        handle = self._handle(sandbox)
        target = self._absolute(sandbox, path)
        try:
            handle.filesystem.write_bytes(content, target)
        except modal.exception.Error as e:
            raise self._as_sandbox_error(e, sandbox=sandbox) from e

    def list_directory(self, sandbox: str, path: str) -> list[tuple[str, bool]]:
        """
        Override: list through Modal's filesystem API instead of ``find``.

        Returns structured entries, so nothing has to be parsed out of shell output and
        the image needs nothing from GNU find. Modal's filesystem calls do run a helper binary
        inside the sandbox, but Modal injects it rather than expecting it in the image.

        A symlink reports ``is_dir()`` false whatever it points at, because Modal types it
        as a symlink rather than as its target. That matches the base class's
        ``find -printf '%y'``, which does not follow links either, so a directory reached by a link
        lists without a trailing slash on both backends.
        """
        handle = self._handle(sandbox)
        target = self._absolute(sandbox, path)
        try:
            entries = handle.filesystem.list_files(target)
        except modal.exception.Error as e:
            raise self._as_sandbox_error(e, sandbox=sandbox) from e
        return [(entry.name, entry.is_dir()) for entry in entries]

    # ------------------------------------------------------------------
    # Internals.
    # ------------------------------------------------------------------

    def _network_kwargs(self, spec: SandboxSpec | None) -> dict[str, object]:
        """
        Map a spec's network policy onto ``Sandbox.create`` arguments, or refuse it.

        Raises :class:`SandboxTerminalError` rather than provisioning something weaker
        than was asked for: an unenforceable spec is a Dag-author or Deployment-Manager
        fact that the model cannot see and cannot fix by trying again.
        """
        if spec is None:
            # "No requirements stated" -- see SandboxBackend.create. The toolset always
            # sends a concrete spec, so this is the direct-caller path.
            return {}
        if spec.allow_egress_to:
            if not spec.block_network:
                raise SandboxTerminalError(
                    "SandboxSpec names an egress allowlist but leaves block_network False, "
                    "which asks for an open network and an allowlist at the same time. Set "
                    "block_network=True to restrict egress to allow_egress_to, or drop "
                    "allow_egress_to to leave the network open."
                )
            if self._egress_enforcement != "sni":
                raise SandboxTerminalError(
                    "SandboxSpec names an egress allowlist, which Modal can only enforce by "
                    "matching the TLS SNI: it allows TLS on port 443 to those hosts, but "
                    "leaves DNS open for every hostname and cannot stop a host that shares a "
                    "TLS endpoint with an allowed one from being reached. Pass "
                    "ModalSandboxBackend(egress_enforcement='sni') to accept that, or use "
                    "SandboxSpec(block_network=True) with no allowlist, which Modal enforces "
                    "exactly and which also blocks DNS."
                )
            # block_network is deliberately not set alongside this: Modal rejects the
            # combination outright, and an allowlist on its own already blocks every
            # host that is not on it.
            return {"outbound_domain_allowlist": self._hostnames(spec.allow_egress_to)}
        if spec.block_network:
            return {"block_network": True}
        return {}

    @staticmethod
    def _hostnames(allow_egress_to: Sequence[str]) -> list[str]:
        """
        Check that every entry is a hostname Modal can actually match, or refuse the spec.

        Modal passes these strings to its API without validating them, and matches them
        against the hostname in the TLS handshake. So a URL, a ``host:port``, or a path
        would be accepted and then match nothing at all, and a bare ``"*"`` is a wildcard
        that matches everything. Either way the Dag author would be told egress is
        restricted while it is not restricted the way they wrote it, which is the belief
        the contract exists to protect.
        """
        if isinstance(allow_egress_to, str):
            # A str is a Sequence[str], so this would otherwise be read one character at a
            # time, and a name without dots would pass every character check and then
            # allow nothing at all.
            raise SandboxTerminalError(
                "SandboxSpec.allow_egress_to must be a sequence of hostnames, not one string: "
                f"{allow_egress_to!r} would be read a character at a time. Wrap it in a list."
            )
        rejected = [host for host in allow_egress_to if not _is_tls_hostname(host)]
        if rejected:
            raise SandboxTerminalError(
                f"SandboxSpec.allow_egress_to must contain bare hostnames, optionally with a "
                f"leading '*.' wildcard label, because Modal matches them against the hostname "
                f"in the TLS handshake. These entries are not hostnames and would silently match "
                f"nothing, or everything: {rejected}. Write 'pypi.org' or '*.pythonhosted.org', "
                f"not a URL, a host:port, or a bare '*'."
            )
        return list(allow_egress_to)

    @staticmethod
    def _environment(spec: SandboxSpec | None) -> dict[str, str] | None:
        """
        Return the environment to inject, refusing anything Modal cannot carry.

        ``SandboxSpec.env`` is typed as strings but nothing enforces that, and Modal's
        environment is strings too, so an ``int`` would fail somewhere deeper and less
        legibly. Refuse it here with the offending keys named.
        """
        if spec is None or not spec.env:
            return None
        rejected = sorted(
            str(key)
            for key, value in spec.env.items()
            if not isinstance(key, str) or not isinstance(value, str)
        )
        if rejected:
            raise SandboxTerminalError(
                f"SandboxSpec.env must map strings to strings; these entries do not: {rejected}. "
                "Convert the value at the Dag level, e.g. str(port), so what reaches the sandbox "
                "is what you meant."
            )
        return dict(spec.env)

    def _remaining_lifetime(self, sandbox: str) -> float | None:
        """
        Seconds left before Modal ends this sandbox, or ``None`` if we did not create it.

        A handle from elsewhere has a lifetime this backend cannot know, so nothing is
        claimed about it rather than a number being invented.
        """
        expiry = self._expiries.get(sandbox)
        return None if expiry is None else max(0.0, expiry - time.monotonic())

    def _handle(self, sandbox: str) -> modal.Sandbox:
        """Return the live sandbox object for a handle, looking it up if not cached."""
        cached = self._sandboxes.get(sandbox)
        if cached is not None:
            return cached
        try:
            # Reachable when the handle came from another backend instance, or from a
            # caller that created the sandbox elsewhere.
            found = modal.Sandbox.from_id(sandbox)
        except modal.exception.Error as e:
            raise self._as_sandbox_error(e, sandbox=sandbox) from e
        self._sandboxes[sandbox] = found
        return found

    def _absolute(self, sandbox: str, path: str) -> str:
        """
        Resolve ``path`` for Modal's filesystem API, which only accepts absolute paths.

        Resolved against the sandbox's working directory, which is also what the shell
        behind ``run_command`` and the inherited ``read_file`` resolve against, so both
        views of the filesystem agree on what a relative path means.

        ``.`` and doubled separators are collapsed, since they mean the same thing to
        everyone. ``..`` is deliberately left alone: resolving it here would be a lexical
        answer, and a shell resolves it physically, so the two disagree the moment a
        symlink is in the path. Leaving it in place hands one path to both, and if Modal
        will not take it the author hears about it instead of writing to a file nobody
        asked for.
        """
        joined = path if path.startswith("/") else posixpath.join(self._working_directory(sandbox), path)
        return "/" + "/".join(segment for segment in joined.split("/") if segment not in ("", "."))

    def _working_directory(self, sandbox: str) -> str:
        if self._workdir is not None:
            return self._workdir
        cached = self._workdirs.get(sandbox)
        if cached is not None:
            return cached
        # Only reachable when the backend was built with workdir=None, so the image
        # decides. Asked once per sandbox and remembered.
        result = self.run_command(sandbox, "pwd", timeout=_FILE_OP_TIMEOUT, max_output_bytes=4096)
        resolved = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else ""
        if result.exit_code or not resolved.startswith("/"):
            raise SandboxError(
                "Could not determine the sandbox working directory, so a relative path "
                "cannot be resolved. Use an absolute path, or set workdir on the backend."
            )
        self._workdirs[sandbox] = resolved
        return resolved

    def _as_sandbox_error(self, error: modal.exception.Error, *, sandbox: str | None = None) -> SandboxError:
        """
        Translate a Modal error into the contract's two kinds.

        Recoverable errors reach the model as a retry; terminal ones fail the task so
        Airflow's own retry handles them. Order matters here: ``ConflictError`` is a
        subclass of ``InvalidError``, and ``SandboxTimeoutError`` shares a ``TimeoutError``
        parent with ``ExecTimeoutError``, which means the opposite thing.
        """
        if isinstance(error, modal.exception.ExecTimeoutError):
            # A command deadline, which run_command reports as output rather than an
            # error. Only reachable if it escapes from somewhere that cannot report it.
            return SandboxError(f"The command hit its deadline: {error}")
        if isinstance(error, modal.exception.AuthError):
            return SandboxTerminalError(
                "Modal rejected the credentials. Run 'modal token new', or set "
                f"MODAL_TOKEN_ID and MODAL_TOKEN_SECRET on the worker: {error}"
            )
        if isinstance(error, modal.exception.PermissionDeniedError):
            # A sibling of AuthError rather than a subclass, and just as unfixable by
            # anything the model could say: the workspace does not allow this.
            return SandboxTerminalError(f"Modal refused the operation: {error}")
        if isinstance(
            error,
            (
                modal.exception.SandboxTerminatedError,
                modal.exception.SandboxTimeoutError,
                modal.exception.NotFoundError,
            ),
        ):
            if sandbox is None:
                # Reached from create, where no sandbox exists yet, so a missing app or
                # image must not be reported as a sandbox that died.
                return SandboxTerminalError(f"Modal could not find what the request named: {error}")
            return SandboxTerminalError(
                f"The Modal sandbox is gone: it was terminated, or it reached its sandbox_timeout. {error}"
            )
        if isinstance(error, modal.exception.SandboxFilesystemError):
            # A bad path, a directory where a file was expected, a permission the guest
            # does not have: all things the model can correct.
            return SandboxError(str(error))
        if isinstance(error, modal.exception.ConflictError):
            # A subclass of InvalidError, so it has to be caught first, and it means
            # something different: observed as "Modal Sandbox is shutting down", which is
            # the sandbox going away rather than the request being wrong.
            return SandboxTerminalError(f"The Modal sandbox is shutting down: {error}")
        if isinstance(error, modal.exception.InvalidError):
            # Modal rejected the request itself. Retrying the same call cannot help.
            return SandboxTerminalError(f"Modal rejected the request: {error}")
        # Everything else -- a connection blip, an unmapped server error -- is ambiguous
        # on its own, so ask whether the sandbox is still there before deciding.
        if sandbox is not None and self._has_finished(sandbox):
            return SandboxTerminalError(f"The Modal sandbox is no longer running: {error}")
        return SandboxError(str(error))

    def _has_finished(self, sandbox: str) -> bool:
        """
        Whether the sandbox has stopped, used to classify an otherwise ambiguous error.

        The answer can lag reality: measured against modal 1.5.5, a sandbox that has just
        been terminated still polls as running for around 35 seconds. So an ambiguous error
        arriving in that window is treated as recoverable, the model retries once, and the
        next call gets the unambiguous ``SandboxTerminatedError`` and fails the task. One
        wasted retry is the right trade against blocking to find out sooner. Where the
        answer has to be right now -- deciding whether a command's sandbox died under it --
        :meth:`_still_alive` asks by running a command instead, which does not lag.
        """
        handle = self._sandboxes.get(sandbox)
        if handle is None:
            return False
        with suppress(Exception):
            return handle.poll() is not None
        return False
