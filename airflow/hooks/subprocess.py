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
from __future__ import annotations

import contextlib
import os
import shutil
import signal
import time
from collections import namedtuple
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir, mkdtemp

from airflow.hooks.base import BaseHook
from airflow.utils.platform import IS_WINDOWS

SubprocessResult = namedtuple("SubprocessResult", ["exit_code", "output"])


class SubprocessHook(BaseHook):
    """Hook for running processes with the ``subprocess`` module."""

    def __init__(self, **kwargs) -> None:
        self.sub_process: Popen[bytes] | None = None
        super().__init__(**kwargs)

    def run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        output_encoding: str = "utf-8",
        cwd: str | None = None,
    ) -> SubprocessResult:
        """
        Execute the command.

        If ``cwd`` is None, execute the command in a temporary directory which will be cleaned afterwards.
        If ``env`` is not supplied, ``os.environ`` is passed

        :param command: the command to run
        :param env: Optional dict containing environment variables to be made available to the shell
            environment in which ``command`` will be executed.  If omitted, ``os.environ`` will be used.
            Note, that in case you have Sentry configured, original variables from the environment
            will also be passed to the subprocess with ``SUBPROCESS_`` prefix. See
            :doc:`/administration-and-deployment/logging-monitoring/errors` for details.
        :param output_encoding: encoding to use for decoding stdout
        :param cwd: Working directory to run the command in.
            If None (default), the command is run in a temporary directory.
        :return: :class:`namedtuple` containing ``exit_code`` and ``output``, the last line from stderr
            or stdout
        """
        self.log.info("Tmp dir root location: %s", gettempdir())

        safe_cleanup = False

        with contextlib.ExitStack() as stack:
            if cwd is None:
                # TemporaryDirectory will call shutil.rmtree() internally when the context exits. On Windows,
                # shutil.rmtree() is unreliable and there is a race condition, where even after self.sub_process.wait(),
                # the process will still be holding onto the directory causing an exception. The work-around is
                # to call shutil.rmtree() in a retry loop. If we're not running under Windows, shutil.rmtree() is
                # reliable and no retry loop is needed.

                prefix = "airflowtmp"

                if IS_WINDOWS:
                    cwd = mkdtemp(prefix=prefix)
                    safe_cleanup = True
                else:
                    cwd = stack.enter_context(TemporaryDirectory(prefix=prefix))

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info("Running command: %s", command)

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec if not IS_WINDOWS else None,
            )

            self.log.info("Output:")
            line = ""
            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")
            if self.sub_process.stdout is not None:
                for raw_line in iter(self.sub_process.stdout.readline, b""):
                    line = raw_line.decode(output_encoding, errors="backslashreplace").rstrip()
                    self.log.info("%s", line)

            self.sub_process.wait()
            self.log.info("Command exited with return code %s", self.sub_process.returncode)

            return_code: int = self.sub_process.returncode

        if safe_cleanup and cwd is not None:
            for retry in range(3):
                try:
                    shutil.rmtree(cwd)
                    if retry > 0:
                        self.log.info("Removed temporary directory %s on retry #%s", cwd, retry)
                    break
                except PermissionError:
                    if retry == 2:
                        self.log.warning("Could not remove temporary directory %s", cwd)
                        raise
                    else:
                        time.sleep(1)

        return SubprocessResult(exit_code=return_code, output=line)

    def send_sigterm(self):
        """Send SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info("Sending SIGTERM signal to process group")
        if self.sub_process and hasattr(self.sub_process, "pid"):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)
