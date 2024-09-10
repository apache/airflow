"""
Module dedicated to common operations regarding calling subprocesses
"""

import logging
import shlex
import subprocess
import time
from typing import List, Optional, Union

from utils.network_utils import check_if_port_is_being_listened_on


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


# this method was copied from apache/airflow master
def execute_in_subprocess(cmd: Union[str, List[str]]) -> None:
    """
    Execute a process and stream output to logger

    :param cmd: command and arguments to run
    :type cmd: Union[str, List[str]]

    :raises: subprocess.CalledProcessError: if the called process exited with an error
    """
    if isinstance(cmd, List):

        log.info("Executing cmd: %s", " ".join([shlex.quote(c) for c in cmd]))
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            close_fds=True,
        )
    else:

        log.info("Executing cmd: %s", cmd)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            close_fds=True,
            shell=True,
        )

    log.info("Output:")
    if proc.stdout:
        with proc.stdout:
            for line in iter(proc.stdout.readline, b""):
                log.info("%s", line.decode().rstrip())

    exit_code = proc.wait()
    if exit_code != 0:
        raise subprocess.CalledProcessError(exit_code, cmd)


def get_subprocess_status(process: subprocess.Popen) -> Optional[int]:
    """
    Returns subprocess status.

    :param process: subprocess to check.
    :type process: subprocess.Popen

    :return: subprocess exit status or None if it has not finished yet.
    :rtype: Optional[int]
    """

    log.info("Checking status of subprocess %s", process.pid)
    poll = process.poll()
    if poll is None:
        log.info("Subprocess %s is still executing.", process.pid)
    return poll


def start_port_forwarding_process(cmd: List[str], port: int, max_time_to_wait: int) -> subprocess.Popen:
    """ "
    Starts a port forwarding process in the background and waits until port is listened on for a
    maximum amount of time. Note that this function does not handle killing of this process.

    :param cmd: port forwarding command to execute.
    :type cmd: List[str]
    :param port: port on which process will be listening.
    :type port: int
    :param max_time_to_wait: maximum amount of time to wait for port forwarding to start.
    :type max_time_to_wait: int

    :return: the started port forwarding process.
    :rtype: subprocess.Popen

    :raises:
        TimeoutError: if forwarding was not started in the specified time.
        subprocess.CalledProcessError: if port forwarding process has finished prematurely.
    """

    log.info("Executing cmd: %s", " ".join([shlex.quote(c) for c in cmd]))

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    time_left = max_time_to_wait
    time_to_sleep = 10
    while time_left >= 0:
        if check_if_port_is_being_listened_on(port):
            break
        log.info(
            "Waiting for port %d to be listened on. " "Sleeping for for %.2f seconds.",
            port,
            time_to_sleep,
        )
        time.sleep(time_to_sleep)
        time_left = time_left - time_to_sleep

    if time_left < 0 and not check_if_port_is_being_listened_on(port):
        raise TimeoutError(
            f"Port forwarding was not opened in specified time " f"of {max_time_to_wait} seconds."
        )

    # if port is open BUT given process has finished then raise an error as apparently some other
    # process have opened this port
    if proc.poll() is not None:
        log.error("Port forwarding process finished prematurely.")
        # TODO: decide if contents of standard output should be printed here
        # if proc.stdout:
        #     with proc.stdout:
        #         for line in iter(proc.stdout.readline, b""):
        #             log.info("%s", line.decode().rstrip())
        raise subprocess.CalledProcessError(proc.returncode, proc.args, None, None)

    return proc
