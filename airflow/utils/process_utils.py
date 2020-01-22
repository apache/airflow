# -*- coding: utf-8 -*-
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
#
"""
Utilities for running process with writing output to logger
"""
import shlex
import subprocess
from typing import List

from airflow import LoggingMixin

log = LoggingMixin().log


def execute_in_subprocess(cmd: List[str]):
    """
    Execute a process and stream output to logger

    :param cmd: command and arguments to run
    :type cmd: List[str]
    """
    log.info("Executing cmd: %s", " ".join([shlex.quote(c) for c in cmd]))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        close_fds=True
    )
    log.info("Output:")
    with proc.stdout:
        for line in iter(proc.stdout.readline, b''):
            log.info("%s", line.decode().rstrip())

    exit_code = proc.wait()
    if exit_code != 0:
        raise subprocess.CalledProcessError(exit_code, cmd)
