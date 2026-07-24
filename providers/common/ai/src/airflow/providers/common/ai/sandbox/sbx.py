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

import math
import shutil
import subprocess
import tempfile
import threading
import time
from contextlib import suppress

from airflow.providers.common.ai.sandbox.base import (
    _MAX_OUTPUT_CHARS,
    SandboxBackend,
    SandboxResult,
    _new_sandbox_name,
    _validate_positive_finite,
)
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

# Extra wall-clock beyond the per-command budget to absorb CLI + microVM
# round-trip overhead before we treat the sbx call itself as hung.
_EXEC_GRACE = 30.0
# Grace after the timeout fires before the command is force-killed (SIGKILL) if
# it ignored SIGTERM. Kept well under _EXEC_GRACE so the outer call still returns.
_KILL_AFTER = 10


class SbxSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent code in a Docker Sandboxes (``sbx``) microVM.

    Drives the ``sbx`` CLI: ``create`` provisions a per-session microVM, ``exec``
    runs commands in it (``docker exec`` semantics), and ``rm`` tears it down.
    Each sandbox is a genuine microVM with its own kernel, so agent code is far
    better isolated than a shared-kernel container — the same isolation grade as
    :class:`~airflow.providers.common.ai.sandbox.IsloSandboxBackend`, but local.

    Requires the ``sbx`` binary on ``PATH`` (``brew install docker/tap/sbx`` /
    ``winget install Docker.sbx``) and a one-time ``sbx policy init`` on the host;
    it is a Deployment Manager prerequisite, not something Airflow configures. The
    template image must provide the GNU coreutils ``timeout`` binary, which
    enforces the per-command timeout (any Debian/Ubuntu-based image, including
    ``python:*-slim``, does).

    Airflow injects none of its context, connections, or worker environment into
    the sandbox; a custom template image may still bundle its own tools. Outbound
    network egress is governed by the host ``sbx policy`` (a Deployment Manager
    setting), not by this backend — run ``sbx policy init deny-all`` for a
    no-egress default.

    :param image: Container image for the sandbox (``sbx --template``).
        Default ``"python:3.12-slim"``.
    :param memory: Memory limit in binary units (e.g. ``"2g"``). ``sbx`` enforces
        a 1 GiB minimum. Default ``"2g"``.
    :param cpus: Number of CPUs to allocate. ``None`` (default) uses the ``sbx``
        default (all host CPUs).
    :param sbx_path: Path to the ``sbx`` binary. Default ``"sbx"``.
    :param create_timeout: Seconds to allow for provisioning (first-run microVM
        boot plus image pull can be slow). Default ``600``.
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
        self._image = image
        self._memory = memory
        self._cpus = cpus
        self._sbx_path = sbx_path
        self._create_timeout = create_timeout
        # Each sandbox mounts a throwaway host workspace; remember it so destroy
        # can remove it. ``sbx create`` requires a workspace path but the agent
        # never needs host files, so an empty temp dir keeps the host untouched.
        self._workspaces: dict[str, str] = {}

    def _run_cli(self, args: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        # Only for small, bounded output (create/rm). Command execution uses
        # _exec_capped, which bounds memory against unbounded agent output.
        return subprocess.run(
            [self._sbx_path, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

    def _exec_capped(self, args: list[str], *, timeout: float) -> tuple[int, str, str, bool]:
        """
        Run the CLI with the output retained bounded to ``_MAX_OUTPUT_CHARS`` per stream.

        Agent code can print unbounded output; ``subprocess.run`` would buffer
        all of it and OOM the worker, so drain incrementally and keep only the
        cap. Returns ``(returncode, stdout, stderr, truncated)``.
        """
        proc = subprocess.Popen(
            [self._sbx_path, *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        def drain(stream, buf: bytearray, flag: list[bool]) -> None:
            for chunk in iter(lambda: stream.read(65536), b""):
                room = _MAX_OUTPUT_CHARS - len(buf)
                if room > 0:
                    buf.extend(chunk[:room])
                if len(chunk) > room:
                    flag[0] = True

        out, err = bytearray(), bytearray()
        out_trunc, err_trunc = [False], [False]
        threads = [
            threading.Thread(target=drain, args=(proc.stdout, out, out_trunc)),
            threading.Thread(target=drain, args=(proc.stderr, err, err_trunc)),
        ]
        for t in threads:
            t.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            for t in threads:
                t.join()
            raise
        for t in threads:
            t.join()
        return (
            proc.returncode,
            out.decode(errors="replace"),
            err.decode(errors="replace"),
            out_trunc[0] or err_trunc[0],
        )

    def create(self) -> str:
        if shutil.which(self._sbx_path) is None:
            raise AirflowOptionalProviderFeatureException(
                f"The {self._sbx_path!r} binary was not found on PATH. Install Docker Sandboxes "
                "(https://docs.docker.com/ai/sandboxes/) and run 'sbx policy init' once."
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
                raise RuntimeError(f"'sbx create' failed ({result.returncode}): {result.stderr.strip()}")
        except BaseException:
            # A timeout, a nonzero exit, or a partial provision all leave a
            # possibly-orphaned microVM and a workspace tempdir. Best-effort
            # clean both so nothing leaks on the host or in sbx.
            with suppress(Exception):
                self._run_cli(["rm", "-f", name], timeout=120.0)
            shutil.rmtree(workspace, ignore_errors=True)
            raise
        self._workspaces[name] = workspace
        return name

    def run(self, sandbox: str, command: list[str], *, timeout: float) -> SandboxResult:
        _validate_positive_finite(timeout, "timeout")
        # Round up: GNU timeout treats 0 as "no timeout", so a sub-second value
        # must not truncate to it.
        seconds = max(1, math.ceil(timeout))
        # GNU ``timeout`` exits 124 when the budget is hit and the command dies to
        # the SIGTERM; if the command ignores it, ``--kill-after`` escalates to
        # SIGKILL and ``timeout`` exits 137 instead — ambiguous with an OOM kill,
        # so 137 counts as a timeout only when the call also outlived the budget.
        exec_args = [
            "exec",
            sandbox,
            "timeout",
            f"--kill-after={_KILL_AFTER}",
            str(seconds),
            *command,
        ]
        start = time.monotonic()
        try:
            returncode, stdout, stderr, truncated = self._exec_capped(
                exec_args, timeout=timeout + _EXEC_GRACE
            )
        except subprocess.TimeoutExpired:
            # The CLI never returned: the command may still be running in the
            # shared microVM. Destroy it so it cannot continue, and tell the
            # toolset to provision a fresh one.
            self.destroy(sandbox)
            return SandboxResult(
                exit_code=-1,
                stdout="",
                stderr="",
                timed_out=True,
                sandbox_terminated=True,
            )
        elapsed = time.monotonic() - start
        return SandboxResult(
            exit_code=returncode,
            stdout=stdout,
            stderr=stderr,
            timed_out=returncode == 124 or (returncode == 137 and elapsed >= seconds),
            truncated=truncated,
        )

    def destroy(self, sandbox: str) -> None:
        # Already-gone is fine — 'sbx rm -f' exits nonzero for a missing sandbox,
        # which we ignore so destroy stays idempotent.
        try:
            self._run_cli(["rm", "-f", sandbox], timeout=120.0)
        except subprocess.TimeoutExpired:
            pass
        finally:
            workspace = self._workspaces.pop(sandbox, None)
            if workspace is not None:
                shutil.rmtree(workspace, ignore_errors=True)
