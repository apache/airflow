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
"""Docker Sandboxes (``sbx``) microVM backend for the SandboxToolset."""

from __future__ import annotations

import base64
import logging
import math
import os
import shlex
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from contextlib import suppress
from typing import TYPE_CHECKING, Literal

from airflow.providers.common.ai.sandbox.base import (
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxTerminalError,
    _new_sandbox_name,
    _validate_positive_finite,
)

if TYPE_CHECKING:
    from airflow.providers.common.ai.sandbox.base import SandboxSpec

# Extra wall-clock beyond the per-command budget to absorb CLI and microVM
# round-trip overhead before we treat the sbx call itself as hung.
_EXEC_GRACE = 30.0
# Grace after the timeout fires before the command is force-killed (SIGKILL) if it
# ignored SIGTERM. Kept well under _EXEC_GRACE so the outer call still returns.
_KILL_AFTER = 10
# Bounded budget for the small helper commands behind the file tools.
_FILE_OP_TIMEOUT = 120.0
# Helpers return a status or a directory listing, never bulk file content, so a
# small cap is enough to bound what a hostile guest can push into worker memory.
_HELPER_OUTPUT_CAP = 1024 * 1024

log = logging.getLogger(__name__)

HostNetworkPolicy = Literal["unknown", "deny-all", "allow-all"]


class SbxSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent commands in a Docker Sandboxes (``sbx``) microVM.

    Drives the ``sbx`` CLI: ``create`` provisions a per-session microVM, ``exec``
    runs commands in it, and ``rm`` tears it down. Each sandbox is a microVM with
    its own kernel, so agent code is isolated by a hardware boundary rather than a
    shared kernel. Effective isolation still depends on the image, host policy,
    and resource limits.

    **Use this for local development, not production.** Docker Sandboxes is
    built for running coding agents against a checkout on your own machine, so
    driving it from an Airflow worker is off-label use. A production worker would
    need the ``sbx`` binary on the host, an authenticated Docker account
    (``sbx login``), a one-time ``sbx policy init``, and on Linux, KVM or nested
    virtualization -- which an unprivileged container cannot provide. No hosted
    backend ships with the provider yet; add one behind :class:`SandboxBackend`
    if you need Kubernetes.

    **Network policy is a host-level setting, not a per-sandbox one.** ``sbx``
    governs egress through ``sbx policy``, so this backend cannot apply a
    per-sandbox rule. Rather than let a DAG author believe a
    :class:`~airflow.providers.common.ai.sandbox.SandboxSpec` restriction is in
    force when it is not, ``create`` refuses a spec it cannot honor unless the
    Deployment Manager states the host policy through ``host_network_policy``.

    **Orphans are not reclaimed automatically.** There is no server-side TTL to
    fall back on: if the worker is killed outright, the microVM and its workspace
    directory survive. Sandboxes are named ``airflow-sandbox-*`` so an operator
    can find and remove them; budget for that sweep before running this at scale.

    The template image must provide GNU coreutils ``timeout``, ``base64``, ``stat``,
    ``find``, ``mkdir`` and ``dirname``, which the command and file tools use. Any Debian or Ubuntu based
    image, including ``python:*-slim``, does.

    :param image: Container image for the sandbox (``sbx --template``).
        Default ``"python:3.12-slim"``.
    :param memory: Memory limit in binary units (e.g. ``"2g"``). ``sbx`` enforces a
        1 GiB minimum. Default ``"2g"``.
    :param cpus: Number of CPUs to allocate. ``None`` (default) uses the ``sbx``
        default, which is all host CPUs.
    :param sbx_path: Path to the ``sbx`` binary. Default ``"sbx"``.
    :param create_timeout: Seconds to allow for provisioning; first-run microVM
        boot plus an image pull can be slow. Default ``600``.
    :param host_network_policy: What ``sbx policy`` is set to on this host.
        ``"unknown"`` (default) makes ``create`` refuse any spec that asks for a
        network guarantee. Set ``"deny-all"`` after running
        ``sbx policy init deny-all``, or ``"allow-all"`` to state that egress is
        open and have specs requesting isolation refused.
    """

    name = "sbx"

    def __init__(
        self,
        *,
        image: str = "python:3.12-slim",
        memory: str = "2g",
        cpus: int | None = None,
        sbx_path: str = "sbx",
        create_timeout: float = 600.0,
        host_network_policy: HostNetworkPolicy = "unknown",
    ) -> None:
        if not image:
            raise ValueError("image must not be empty.")
        if not memory:
            raise ValueError("memory must not be empty.")
        if cpus is not None:
            _validate_positive_finite(cpus, "cpus")
        if not sbx_path:
            raise ValueError("sbx_path must not be empty.")
        _validate_positive_finite(create_timeout, "create_timeout")
        if host_network_policy not in ("unknown", "deny-all", "allow-all"):
            raise ValueError(
                "host_network_policy must be 'unknown', 'deny-all', or 'allow-all', "
                f"got {host_network_policy!r}."
            )
        self._image = image
        self._memory = memory
        self._cpus = cpus
        self._sbx_path = sbx_path
        self._create_timeout = create_timeout
        self._host_network_policy = host_network_policy
        # Each sandbox mounts a throwaway host workspace; remember it so destroy can
        # remove it. ``sbx create`` requires a workspace path but the agent never
        # needs host files, so an empty temp dir keeps the host untouched.
        self._workspaces: dict[str, str] = {}

    def _check_spec(self, spec: SandboxSpec | None) -> None:
        """
        Refuse a spec this backend cannot actually enforce.

        Raises :class:`SandboxTerminalError`, not :class:`SandboxError`: an
        unenforceable spec is a Dag-author or Deployment-Manager configuration
        fact. The model cannot see it and cannot fix it by trying again, so the
        task must fail rather than the model burning retries on it.
        """
        if spec is None:
            # "No requirements stated" -- see SandboxBackend.create. The toolset
            # always sends a concrete spec, so this is the direct-caller path.
            return
        if spec.allow_egress_to and self._host_network_policy != "deny-all":
            # A per-sandbox allow rule only means anything on top of a deny-all
            # global policy; against an open host policy it grants nothing and
            # would imply a restriction that is not there.
            raise SandboxTerminalError(
                "SandboxSpec names an egress allowlist, but a per-sandbox allow rule only "
                "narrows a deny-all host policy. Run 'sbx policy init deny-all' on the worker "
                "host and pass host_network_policy='deny-all'."
            )
        if spec.block_network and self._host_network_policy != "deny-all":
            raise SandboxTerminalError(
                "SandboxSpec asks for no network egress, but this backend cannot enforce that "
                "per sandbox and the host policy has not been declared. Run 'sbx policy init "
                "deny-all' on the worker host and pass host_network_policy='deny-all', or pass "
                "SandboxSpec(block_network=False) to acknowledge that egress is open."
            )

    def _run_cli(
        self, args: list[str], *, timeout: float, stdin: bytes | None = None
    ) -> subprocess.CompletedProcess[bytes]:
        # Lifecycle only (create/rm), where output is a short status or error.
        # Anything carrying guest-controlled output goes through _exec_capped,
        # which bounds what reaches worker memory.
        return subprocess.run(
            [self._sbx_path, *args],
            input=stdin,
            capture_output=True,
            timeout=timeout,
            check=False,
        )

    def _exec_capped_bytes(
        self, args: list[str], *, timeout: float, max_output_bytes: int, stdin: bytes | None = None
    ) -> tuple[int, bytearray, bytearray, bool, bool]:
        """
        Run the CLI keeping at most ``max_output_bytes`` per stream, as raw bytes.

        Everything the guest can influence flows through here. ``subprocess.run``
        would buffer the whole stream and exhaust worker memory -- a guest can
        emit gigabytes from ``/dev/zero`` in seconds -- so drain incrementally and
        keep only the cap. Returns
        ``(returncode, stdout, stderr, out_truncated, err_truncated)``.
        """
        deadline = time.monotonic() + timeout
        with subprocess.Popen(
            [self._sbx_path, *args],
            stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # Own process group so a command that forks on the host is killed with
            # its children rather than surviving to hold the pipes open.
            start_new_session=True,
        ) as proc:

            def drain(stream, buf: bytearray, flag: list[bool]) -> None:
                # Keep the tail, not the head: the model reads this to fix its own
                # command, and a traceback plus the exit status live at the end.
                # Retaining the head would also hand the formatter -- which
                # promises a tail -- a window that no longer contains one.
                # The pipes are closed underneath us when the deadline abandons a
                # thread, which surfaces as ValueError/OSError on the next read.
                with suppress(ValueError, OSError):
                    for chunk in iter(lambda: stream.read(65536), b""):
                        buf.extend(chunk)
                        if len(buf) > max_output_bytes:
                            del buf[: len(buf) - max_output_bytes]
                            flag[0] = True
                            # Drop the leading partial line so the caller never
                            # sees a fragment presented as a whole record.
                            newline = buf.find(b"\n")
                            if newline != -1:
                                del buf[: newline + 1]

            def feed() -> None:
                # Guarded: the guest may exit before reading all of stdin, and a
                # broken pipe here must not take down the call.
                with suppress(BrokenPipeError, ValueError, OSError):
                    proc.stdin.write(stdin)  # type: ignore[union-attr,arg-type]
                    proc.stdin.close()  # type: ignore[union-attr]

            out, err = bytearray(), bytearray()
            out_trunc, err_trunc = [False], [False]
            # Daemon threads: a drain can only finish when the pipe reaches EOF,
            # and a process the command backgrounded inherits the write end, so
            # EOF may never come. They must not be able to outlive the process.
            threads = [
                threading.Thread(target=drain, args=(proc.stdout, out, out_trunc), daemon=True),
                threading.Thread(target=drain, args=(proc.stderr, err, err_trunc), daemon=True),
            ]
            if stdin is not None:
                threads.append(threading.Thread(target=feed, daemon=True))
            for t in threads:
                t.start()
            try:
                proc.wait(timeout=timeout)
            finally:
                if proc.poll() is None:
                    with suppress(OSError):
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    proc.kill()
                    proc.wait()
                # Bounded join, for the same reason the threads are daemons. A
                # backgrounded grandchild holding the pipe open would otherwise
                # make this outlive the caller's timeout without limit, and the
                # command would be reported as a clean success long after its
                # budget expired.
                for t in threads:
                    t.join(timeout=max(0.0, deadline - time.monotonic()))
                if any(t.is_alive() for t in threads):
                    # Something still holds the pipe. Report what was collected
                    # and mark it short rather than blocking on it.
                    out_trunc[0] = err_trunc[0] = True
            return (proc.returncode, out, err, out_trunc[0], err_trunc[0])

    def _exec_capped(
        self, args: list[str], *, timeout: float, max_output_bytes: int
    ) -> tuple[int, str, str, bool, bool]:
        """Text wrapper over :meth:`_exec_capped_bytes` for the command tool."""
        code, out, err, out_trunc, err_trunc = self._exec_capped_bytes(
            args, timeout=timeout, max_output_bytes=max_output_bytes
        )
        return (code, out.decode(errors="replace"), err.decode(errors="replace"), out_trunc, err_trunc)

    def create(self, *, spec: SandboxSpec | None = None) -> str:
        self._check_spec(spec)
        if shutil.which(self._sbx_path) is None:
            raise SandboxTerminalError(
                f"The {self._sbx_path!r} binary was not found on PATH. Install Docker Sandboxes "
                "(https://docs.docker.com/ai/sandboxes/) and run 'sbx policy init' once. Note "
                "that this backend needs a host that can run microVMs, which usually rules out "
                "an unprivileged container."
            )
        name = _new_sandbox_name()
        workspace = tempfile.mkdtemp(prefix="airflow-sandbox-ws-")
        args = ["create", "--name", name, "--memory", self._memory, "--template", self._image]
        if self._cpus is not None:
            args += ["--cpus", str(int(self._cpus))]
        args += ["shell", workspace]
        try:
            result = self._run_cli(args, timeout=self._create_timeout)
            if result.returncode != 0:
                raise SandboxTerminalError(
                    f"'sbx create' failed ({result.returncode}): "
                    f"{result.stderr.decode(errors='replace').strip()}"
                )
            if spec is not None and spec.allow_egress_to:
                # Scoped to this sandbox: "sbx policy allow network --sandbox NAME"
                # adds to the local policy rather than the global one, so it
                # cannot widen egress for anything else.
                allow = self._run_cli(
                    ["policy", "allow", "network", "--sandbox", name, *spec.allow_egress_to],
                    timeout=_FILE_OP_TIMEOUT,
                )
                if allow.returncode != 0:
                    raise SandboxTerminalError(
                        "Could not apply the SandboxSpec egress allowlist: "
                        f"{allow.stderr.decode(errors='replace').strip()}"
                    )
            if spec is not None and spec.env:
                # sbx has no create-time env flag, so persist the variables into
                # the login profile: every later `sh -lc` picks them up.
                exports = "\n".join(f"export {key}={shlex.quote(value)}" for key, value in spec.env.items())
                code, _, stderr, _, _ = self._exec_capped_bytes(
                    ["exec", "-i", name, "sh", "-c", "cat >> /etc/profile"],
                    timeout=_FILE_OP_TIMEOUT,
                    max_output_bytes=_HELPER_OUTPUT_CAP,
                    stdin=f"\n{exports}\n".encode(),
                )
                if code != 0:
                    raise SandboxTerminalError(
                        "Could not apply SandboxSpec.env to the sandbox: "
                        f"{stderr.decode(errors='replace').strip()}"
                    )
        except BaseException:
            # A timeout, a nonzero exit, a partial provision, or a failure applying
            # the spec all leave a possibly-orphaned microVM and a workspace
            # tempdir. sbx has no server-side TTL to fall back on, so best-effort
            # clean both here or they survive until an operator notices.
            with suppress(Exception):
                self._run_cli(["rm", "-f", name], timeout=120.0)
            shutil.rmtree(workspace, ignore_errors=True)
            raise
        self._workspaces[name] = workspace
        return name

    def run_command(
        self, sandbox: str, command: str, *, timeout: float, max_output_bytes: int
    ) -> SandboxExecResult:
        _validate_positive_finite(timeout, "timeout")
        # Round up: GNU timeout treats 0 as "no timeout", so a sub-second value must
        # not truncate to it.
        seconds = max(1, math.ceil(timeout))
        # GNU ``timeout`` exits 124 when the budget is hit and the command dies to the
        # SIGTERM; if the command ignores it, ``--kill-after`` escalates to SIGKILL at
        # seconds + _KILL_AFTER and ``timeout`` exits 137 instead. 137 is ambiguous
        # with an OOM kill, so it only counts as a timeout when the call also outlived
        # the escalation point.
        exec_args = [
            "exec",
            sandbox,
            "timeout",
            f"--kill-after={_KILL_AFTER}",
            str(seconds),
            "sh",
            "-lc",
            command,
        ]
        start = time.monotonic()
        try:
            returncode, stdout, stderr, out_trunc, err_trunc = self._exec_capped(
                exec_args, timeout=timeout + _EXEC_GRACE, max_output_bytes=max_output_bytes
            )
        except subprocess.TimeoutExpired:
            # The CLI never returned: the command may still be running in the
            # shared microVM. Destroy it so it cannot continue, and tell the
            # toolset to provision a fresh one either way -- this sandbox is not
            # safe to reuse. A destroy that fails leaves it running with no TTL
            # to reclaim it, so say so rather than reporting a clean teardown.
            try:
                self.destroy(sandbox)
            except Exception:
                log.warning(
                    "Timed out running a command in sandbox %s and could not destroy it; "
                    "it may still be running and will need manual cleanup",
                    sandbox,
                    exc_info=True,
                )
            return SandboxExecResult(
                exit_code=-1, stdout="", stderr="", timed_out=True, sandbox_terminated=True
            )
        elapsed = time.monotonic() - start
        return SandboxExecResult(
            exit_code=returncode,
            stdout=stdout,
            stderr=stderr,
            timed_out=returncode == 124 or (returncode == 137 and elapsed >= seconds + _KILL_AFTER),
            stdout_truncated=out_trunc,
            stderr_truncated=err_trunc,
        )

    def write_file(self, sandbox: str, path: str, content: bytes) -> None:
        """
        Override: send the payload on stdin instead of in the command.

        The base implementation embeds the content in the command itself, which
        the guest's command-line length caps. ``sbx exec`` accepts stdin, so a
        large file needs no such ceiling here.
        """
        quoted = shlex.quote(path)
        code, _, stderr, _, _ = self._exec_capped_bytes(
            [
                "exec",
                "-i",
                sandbox,
                "sh",
                "-c",
                f'mkdir -p -- "$(dirname -- {quoted})" && base64 -d > {quoted}',
            ],
            timeout=_FILE_OP_TIMEOUT,
            max_output_bytes=_HELPER_OUTPUT_CAP,
            stdin=base64.b64encode(content),
        )
        if code:
            raise SandboxError(stderr.decode(errors="replace").strip() or f"Could not write {path!r}.")

    def destroy(self, sandbox: str) -> None:
        # Already-gone is fine -- 'sbx rm -f' exits nonzero for a missing sandbox,
        # which we ignore so destroy stays idempotent.
        try:
            self._run_cli(["rm", "-f", sandbox], timeout=120.0)
        except subprocess.TimeoutExpired:
            pass
        finally:
            workspace = self._workspaces.pop(sandbox, None)
            if workspace is not None:
                shutil.rmtree(workspace, ignore_errors=True)
