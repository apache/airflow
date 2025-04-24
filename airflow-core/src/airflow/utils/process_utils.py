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
"""Utilities for running or stopping processes."""

from __future__ import annotations

import array
import errno
import logging
import os
import select
import shlex
import signal
import subprocess
import sys

from airflow.utils.platform import IS_WINDOWS

if not IS_WINDOWS:
    import fcntl
    import pty
    import termios
    import tty

from collections.abc import Generator
from contextlib import contextmanager

import psutil
from lockfile.pidlockfile import PIDLockFile

from airflow.configuration import conf
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

# When killing processes, time to wait after issuing a SIGTERM before issuing a
# SIGKILL.
DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM = conf.getint("core", "KILLED_TASK_CLEANUP_TIME")


def reap_process_group(
    process_group_id: int,
    logger,
    sig: signal.Signals = signal.SIGTERM,
    timeout: int = DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM,
) -> dict[int, int]:
    """
    Send sig (SIGTERM) to the process group of pid.

    Tries really hard to terminate all processes in the group (including grandchildren). Will send
    sig (SIGTERM) to the process group of pid. If any process is alive after timeout
    a SIGKILL will be send.

    :param process_group_id: process group id to kill.
           The process that wants to create the group should run
           `airflow.utils.process_utils.set_new_process_group()` as the first command
           it executes which will set group id = process_id. Effectively the process that is the
           "root" of the group has pid = gid and all other processes in the group have different
           pids but the same gid (equal the pid of the root process)
    :param logger: log handler
    :param sig: signal type
    :param timeout: how much time a process has to terminate
    """
    returncodes = {}

    def on_terminate(p):
        logger.info("Process %s (%s) terminated with exit code %s", p, p.pid, p.returncode)
        returncodes[p.pid] = p.returncode

    def signal_procs(sig):
        if IS_WINDOWS:
            return
        try:
            logger.info("Sending the signal %s to group %s", sig, process_group_id)
            os.killpg(process_group_id, sig)
        except OSError as err_killpg:
            # If operation not permitted error is thrown due to run_as_user,
            # use sudo -n(--non-interactive) to kill the process
            if err_killpg.errno == errno.EPERM:
                subprocess.check_call(
                    ["sudo", "-n", "kill", "-" + str(int(sig))]
                    + [str(p.pid) for p in all_processes_in_the_group]
                )
            elif err_killpg.errno == errno.ESRCH:
                # There is a rare condition that the process has not managed yet to change its process
                # group. In this case os.killpg fails with ESRCH error
                # So we additionally send a kill signal to the process itself.
                logger.info(
                    "Sending the signal %s to process %s as process group is missing.", sig, process_group_id
                )
                try:
                    os.kill(process_group_id, sig)
                except OSError as err_kill:
                    if err_kill.errno == errno.EPERM:
                        subprocess.check_call(["sudo", "-n", "kill", "-" + str(process_group_id)])
                    else:
                        raise
            else:
                raise

    if not IS_WINDOWS and process_group_id == os.getpgid(0):
        raise RuntimeError("I refuse to kill myself")

    try:
        parent = psutil.Process(process_group_id)

        all_processes_in_the_group = parent.children(recursive=True)
        all_processes_in_the_group.append(parent)
    except psutil.NoSuchProcess:
        # The process already exited, but maybe its children haven't.
        all_processes_in_the_group = []
        for proc in psutil.process_iter():
            try:
                if os.getpgid(proc.pid) == process_group_id and proc.pid != 0:
                    all_processes_in_the_group.append(proc)
            except OSError:
                pass

    logger.info(
        "Sending %s to group %s. PIDs of all processes in the group: %s",
        sig,
        process_group_id,
        [p.pid for p in all_processes_in_the_group],
    )
    try:
        signal_procs(sig)
    except OSError as err:
        # No such process, which means there is no such process group - our job
        # is done
        if err.errno == errno.ESRCH:
            return returncodes

    _, alive = psutil.wait_procs(all_processes_in_the_group, timeout=timeout, callback=on_terminate)

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


def execute_in_subprocess(cmd: list[str], cwd: str | None = None, env: dict | None = None) -> None:
    """
    Execute a process and stream output to logger.

    :param cmd: command and arguments to run
    :param cwd: Current working directory passed to the Popen constructor
    :param env: Additional environment variables to set for the subprocess. If None,
        the subprocess will inherit the current environment variables of the parent process
        (``os.environ``)
    """
    execute_in_subprocess_with_kwargs(cmd, cwd=cwd, env=env)


def execute_in_subprocess_with_kwargs(cmd: list[str], **kwargs) -> None:
    """
    Execute a process and stream output to logger.

    :param cmd: command and arguments to run

    All other keyword args will be passed directly to subprocess.Popen
    """
    log.info("Executing cmd: %s", " ".join(shlex.quote(c) for c in cmd))
    with subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0, close_fds=True, **kwargs
    ) as proc:
        log.info("Output:")
        if proc.stdout:
            with proc.stdout:
                for line in iter(proc.stdout.readline, b""):
                    log.info("%s", line.decode().rstrip())

        exit_code = proc.wait()
    if exit_code != 0:
        raise subprocess.CalledProcessError(exit_code, cmd)


def execute_interactive(cmd: list[str], **kwargs) -> None:
    """
    Run the new command as a subprocess.

    Runs the new command as a subprocess and ensures that the terminal's state is restored to its original
    state after the process is completed e.g. if the subprocess hides the cursor, it will be restored after
    the process is completed.
    """
    log.info("Executing cmd: %s", " ".join(shlex.quote(c) for c in cmd))

    old_tty = termios.tcgetattr(sys.stdin)
    old_sigint_handler = signal.getsignal(signal.SIGINT)
    old_winch_handler = signal.getsignal(signal.SIGWINCH)
    tty.setcbreak(sys.stdin.fileno())

    # open pseudo-terminal to interact with subprocess
    primary_fd, secondary_fd = pty.openpty()

    try:
        with subprocess.Popen(
            cmd,
            stdin=secondary_fd,
            stdout=secondary_fd,
            stderr=secondary_fd,
            universal_newlines=True,
            **kwargs,
        ) as proc:
            # ignore SIGINT in the parent process
            def _sighandler(sig, frame):
                proc.send_signal(sig)

            def _sigwinch(sig, frame):
                # On Py3.11+ we could use termios.tcgetwinsize/tcsetwinsize instead
                buf = array.array("h", [0, 0, 0, 0])

                fcntl.ioctl(pty.STDOUT_FILENO, termios.TIOCGWINSZ, buf, True)
                fcntl.ioctl(secondary_fd, termios.TIOCSWINSZ, buf)

            # Set the initial size too
            _sigwinch(signal.SIGWINCH, None)

            signal.signal(signal.SIGINT, _sighandler)
            signal.signal(signal.SIGWINCH, _sigwinch)
            while proc.poll() is None:
                readable_fbs, _, _ = select.select([sys.stdin, primary_fd], [], [], 0)
                if sys.stdin in readable_fbs:
                    input_data = os.read(sys.stdin.fileno(), 10240)
                    os.write(primary_fd, input_data)
                if primary_fd in readable_fbs:
                    output_data = os.read(primary_fd, 10240)
                    if output_data:
                        os.write(pty.STDOUT_FILENO, output_data)
    finally:
        # restore tty settings back
        signal.signal(signal.SIGINT, old_sigint_handler)
        signal.signal(signal.SIGWINCH, old_winch_handler)
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_tty)


def kill_child_processes_by_pids(pids_to_kill: list[int], timeout: int = 5) -> None:
    """
    Kills child processes for the current process.

    First, it sends the SIGTERM signal, and after the time specified by the `timeout` parameter, sends
    the SIGKILL signal, if the process is still alive.

    :param pids_to_kill: List of PID to be killed.
    :param timeout: The time to wait before sending the SIGKILL signal.
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
def patch_environ(new_env_variables: dict[str, str]) -> Generator[None, None, None]:
    """
    Set environment variables in context.

    After leaving the context, it restores its original state.
    :param new_env_variables: Environment variables to set
    """
    current_env_state = {key: os.environ.get(key) for key in new_env_variables}
    os.environ.update(new_env_variables)
    try:
        yield
    finally:
        for key, old_value in current_env_state.items():
            if old_value is None:
                if key in os.environ:
                    del os.environ[key]
            else:
                os.environ[key] = old_value


def check_if_pidfile_process_is_running(pid_file: str, process_name: str):
    """
    Check if a pidfile already exists and process is still running.

    If process is dead then pidfile is removed.

    :param pid_file: path to the pidfile
    :param process_name: name used in exception if process is up and
        running
    """
    pid_lock_file = PIDLockFile(path=pid_file)
    # If file exists
    if pid_lock_file.is_locked():
        # Read the pid
        pid = pid_lock_file.read_pid()
        if pid is None:
            return
        try:
            # Check if process is still running
            proc = psutil.Process(pid)
            if proc.is_running():
                raise AirflowException(f"The {process_name} is already running under PID {pid}.")
        except psutil.NoSuchProcess:
            # If process is dead remove the pidfile
            pid_lock_file.break_lock()


def set_new_process_group() -> None:
    """
    Try to set current process to a new process group.

    That makes it easy to kill all sub-process of this at the OS-level,
    rather than having to iterate the child processes.

    If current process was spawned by system call ``exec()``, the current
    process group is kept.
    """
    if os.getpid() == os.getsid(0):
        # If PID = SID than process a session leader, and it is not possible to change process group
        return

    os.setpgid(0, 0)
