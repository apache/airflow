#
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
"""Utilities initializing and managing Go modules."""

from __future__ import annotations

import logging
import os
import shlex
import subprocess


def _execute_in_subprocess(cmd: list[str], cwd: str | None = None, env: dict[str, str] | None = None) -> None:
    """
    Execute a process and stream output to logger.

    :param cmd: command and arguments to run
    :param cwd: Current working directory passed to the Popen constructor
    :param env: Additional environment variables to set for the subprocess.
    """
    log = logging.getLogger(__name__)

    log.info("Executing cmd: %s", " ".join(shlex.quote(c) for c in cmd))
    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        close_fds=True,
        cwd=cwd,
        env=env,
    ) as proc:
        log.info("Output:")
        if proc.stdout:
            with proc.stdout:
                for line in iter(proc.stdout.readline, b""):
                    log.info("%s", line.decode().rstrip())

        exit_code = proc.wait()
    if exit_code != 0:
        raise subprocess.CalledProcessError(exit_code, cmd)


def init_module(go_module_name: str, go_module_path: str) -> None:
    """
    Initialize a Go module.

    If a ``go.mod`` file already exists, this function will do nothing.

    :param go_module_name: The name of the Go module to initialize.
    :param go_module_path: The path to the directory containing the Go module.
    """
    if os.path.isfile(os.path.join(go_module_path, "go.mod")):
        return
    go_mod_init_cmd = ["go", "mod", "init", go_module_name]
    _execute_in_subprocess(go_mod_init_cmd, cwd=go_module_path)


def install_dependencies(go_module_path: str) -> None:
    """
    Install dependencies for a Go module.

    :param go_module_path: The path to the directory containing the Go module.
    """
    go_mod_tidy = ["go", "mod", "tidy"]
    _execute_in_subprocess(go_mod_tidy, cwd=go_module_path)
