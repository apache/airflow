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
Utilities for running or stopping processes
"""
import errno
import logging
import os
import shlex
import signal
import subprocess
from contextlib import contextmanager
from typing import Dict, List

import psutil

from airflow.configuration import conf

log = logging.getLogger(__name__)

# When killing processes, time to wait after issuing a SIGTERM before issuing a
# SIGKILL.
DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM = conf.getint(
    'core', 'KILLED_TASK_CLEANUP_TIME'
)


def reap_process_group(pgid, logger, sig=signal.SIGTERM, timeout=DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM):
    """
    Tries really hard to terminate all processes in the group (including grandchildren). Will send
    sig (SIGTERM) to the process group of pid. If any process is alive after timeout
    a SIGKILL will be send.

    :param pgid: process group id to kill
    :param logger: log handler
    :param sig: signal type
    :param timeout: how much time a process has to terminate
    """
    returncodes = {}

    def on_terminate(p):
        logger.info("Process %s (%s) terminated with exit code %s", p, p.pid, p.returncode)
        returncodes[p.pid] = p.returncode

    def signal_procs(sig):
        try:
            os.killpg(pgid, sig)
        except OSError as err:
            # If operation not permitted error is thrown due to run_as_user,
            # use sudo -n(--non-interactive) to kill the process
            if err.errno == errno.EPERM:
                subprocess.check_call(
                    ["sudo", "-n", "kill", "-" + str(sig)] + [str(p.pid) for p in children]
                )
            else:
                raise

    if pgid == os.getpgid(0):
        raise RuntimeError("I refuse to kill myself")

    try:
        parent = psutil.Process(pgid)

        children = parent.children(recursive=True)
        children.append(parent)
    except psutil.NoSuchProcess:
        # The process already exited, but maybe it's children haven't.
        children = []
        for proc in psutil.process_iter():
            try:
                if os.getpgid(proc.pid) == pgid and proc.pid != 0:
                    children.append(proc)
            except OSError:
                pass

    logger.info("Sending %s to GPID %s", sig, pgid)
    try:
        signal_procs(sig)
    except OSError as err:
        # No such process, which means there is no such process group - our job
        # is done
        if err.errno == errno.ESRCH:
            return returncodes

    _, alive = psutil.wait_procs(children, timeout=timeout, callback=on_terminate)

    if alive:
        for proc in alive:
            logger.warning("process %s did not respond to SIGTERM. Trying SIGKILL", proc)

        try:
            signal_procs(signal.SIGKILL)
        except OSError as err:
            if err.errno != errno.ESRCH:
                raise

        _, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
        if alive:
            for proc in alive:
                logger.error("Process %s (%s) could not be killed. Giving up.", proc, proc.pid)
    return returncodes


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


def kill_child_processes_by_pids(pids_to_kill: List[int], timeout: int = 5) -> None:
    """
    Kills child processes for the current process.
 
    First, it sends the SIGTERM signal, and after the time specified by the `timeout` parameter, sends
    the SIGKILL signal, if the process is still alive.

    :param pids_to_kill: List of PID to be killed.
    :type pids_to_kill: List[int]
    :param timeout: The time to wait before sending the SIGKILL signal.
    :type timeout: Optional[int]
    """
    this_process = psutil.Process(os.getpid())
    # Only check child processes to ensure that we don't have a case
    # where we kill the wrong process because a child process died
    # but the PID got reused.
    child_processes = [
        x for x in this_process.children(recursive=True) if x.is_running() and x.pid in pids_to_kill
    ]

    # First try SIGTERM
    for child in child_processes:
        log.info("Terminating child PID: %s", child.pid)
        child.terminate()

    log.info("Waiting up to %s seconds for processes to exit...", timeout)
    try:
        psutil.wait_procs(
            child_processes, timeout=timeout, callback=lambda x: log.info("Terminated PID %s", x.pid)
        )
    except psutil.TimeoutExpired:
        log.debug("Ran out of time while waiting for processes to exit")

    # Then SIGKILL
    child_processes = [
        x for x in this_process.children(recursive=True) if x.is_running() and x.pid in pids_to_kill
    ]
    if child_processes:
        log.info("SIGKILL processes that did not terminate gracefully")
        for child in child_processes:
            log.info("Killing child PID: %s", child.pid)
            child.kill()
            child.wait()


@contextmanager
def patch_environ(new_env_variables: Dict[str, str]):
    """
    Sets environment variables in context. After leaving the context, it restores its original state.

    :param new_env_variables: Environment variables to set
    """
    current_env_state = {
        key: os.environ.get(key)
        for key in new_env_variables.keys()
    }
    os.environ.update(new_env_variables)
    try:  # pylint: disable=too-many-nested-blocks
        yield
    finally:
        for key, old_value in current_env_state.items():
            if old_value is None:
                if key in os.environ:
                    del os.environ[key]
            else:
                os.environ[key] = old_value
