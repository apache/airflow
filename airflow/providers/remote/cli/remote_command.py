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

import logging
import os
import platform
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from subprocess import Popen
from time import sleep

import psutil
from lockfile.pidlockfile import read_pid_from_pidfile, remove_existing_pidfile, write_pid_to_pidfile

from airflow import __version__ as airflow_version, settings
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.cli.cli_config import ARG_PID, ARG_VERBOSE, ActionCommand, Arg
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.remote import __version__ as remote_provider_version
from airflow.providers.remote.models.remote_job import RemoteJob
from airflow.providers.remote.models.remote_logs import RemoteLogs
from airflow.providers.remote.models.remote_worker import RemoteWorker, RemoteWorkerState
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.state import TaskInstanceState

logger = logging.getLogger(__name__)
REMOTE_WORKER_PROCESS_NAME = "remote-worker"
REMOTE_WORKER_HEADER = "\n".join(
    [
        r"   ___                 __        _      __         __",
        r"  / _ \___ __ _  ___  / /____   | | /| / /__  ____/ /_____ ____",
        r" / , _/ -_)  ' \/ _ \/ __/ -_)  | |/ |/ / _ \/ __/  '_/ -_) __/",
        r"/_/|_|\__/_/_/_/\___/\__/\__/   |__/|__/\___/_/ /_/\_\\__/_/",
        r"",
    ]
)


@providers_configuration_loaded
def force_use_internal_api_on_remote_worker():
    """
    Force that environment is made for internal API w/o need to declare outside.

    This is only needed for remote worker and need to be made before click CLI wrapper
    is started as the CLI wrapper attempts to make a DB connection which will fail before
    function call can influence. On remote worker we need to "patch" the env before start.
    """
    if "airflow" in sys.argv[0] and sys.argv[1:3] == ["remote", "worker"]:
        api_url = conf.get("remote", "api_url")
        if not api_url:
            raise SystemExit("Error: API URL is not configured, please correct configuration.")
        logger.info("Starting worker with API endpoint %s", api_url)
        # export remote API to be used for internal API
        os.environ["AIRFLOW_ENABLE_AIP_44"] = "True"
        os.environ["AIRFLOW__CORE__INTERNAL_API_URL"] = api_url
        InternalApiConfig.set_use_internal_api("remote-worker")
        os.environ["AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION"] = "False"


force_use_internal_api_on_remote_worker()


def _hostname() -> str:
    if platform.system() == "Windows":
        return platform.uname().node
    else:
        return os.uname()[1]


def _get_sysinfo() -> dict:
    """Produce the sysinfo from worker to post to central site."""
    return {
        "airflow_version": airflow_version,
        "remote_provider_version": remote_provider_version,
    }


def _pid_file_path(pid_file: str | None) -> str:
    pid_file_path, _, _, _ = cli_utils.setup_locations(process=REMOTE_WORKER_PROCESS_NAME, pid=pid_file)
    return pid_file_path


@dataclass
class _Job:
    """Holds all information for a task/job to be executed as bundle."""

    remote_job: RemoteJob
    process: Popen
    logfile: Path
    logsize: int
    """Last size of log file, point of last chunk push."""


class _RemoteWorkerCli:
    """Runner instance which executes the remote worker."""

    jobs: list[_Job] = []
    """List of jobs that the worker is running currently."""
    last_hb: datetime | None = None
    """Timestamp of last heart beat sent to server."""
    drain: bool = False
    """Flag if job processing should be completed and no new jobs fetched for a graceful stop/shutdown."""

    def __init__(
        self,
        pid_file_path: Path,
        hostname: str,
        queues: list[str] | None,
        concurrency: int,
        job_poll_interval: int,
        heartbeat_interval: int,
    ):
        self.pid_file_path = pid_file_path
        self.job_poll_interval = job_poll_interval
        self.hb_interval = heartbeat_interval
        self.hostname = hostname
        self.queues = queues
        self.concurrency = concurrency

    @staticmethod
    def signal_handler(sig, frame):
        logger.info("Request to show down remote worker received, waiting for jobs to complete.")
        _RemoteWorkerCli.drain = True

    def start(self):
        """Start the execution in a loop until terminated."""
        try:
            self.last_hb = RemoteWorker.register_worker(
                self.hostname, RemoteWorkerState.STARTING, self.queues, _get_sysinfo()
            ).last_update
        except AirflowException as e:
            if "404:NOT FOUND" in str(e):
                raise SystemExit("Error: API endpoint is not ready, please set [remote] api_enabled=True.")
            raise SystemExit(str(e))
        write_pid_to_pidfile(self.pid_file_path)
        signal.signal(signal.SIGINT, _RemoteWorkerCli.signal_handler)
        try:
            while not _RemoteWorkerCli.drain or self.jobs:
                self.loop()

            logger.info("Quitting worker, signal being offline.")
            RemoteWorker.set_state(self.hostname, RemoteWorkerState.OFFLINE, 0, _get_sysinfo())
        finally:
            remove_existing_pidfile(self.pid_file_path)

    def loop(self):
        """Run a loop of scheduling and monitoring tasks."""
        new_job = False
        if not _RemoteWorkerCli.drain and len(self.jobs) < self.concurrency:
            new_job = self.fetch_job()
        self.check_running_jobs()

        if _RemoteWorkerCli.drain or datetime.now().timestamp() - self.last_hb.timestamp() > self.hb_interval:
            self.heartbeat()
            self.last_hb = datetime.now()

        if not new_job:
            self.interruptible_sleep()

    def fetch_job(self) -> bool:
        """Fetch and start a new job from central site."""
        logger.debug("Attempting to fetch a new job...")
        remote_job = RemoteJob.reserve_task(self.hostname, self.queues)
        if remote_job:
            logger.info("Received job: %s", remote_job)
            env = os.environ.copy()
            env["AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION"] = "True"
            env["AIRFLOW__CORE__INTERNAL_API_URL"] = conf.get("remote", "api_url")
            env["_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"] = "1"
            process = Popen(remote_job.command, close_fds=True, env=env)
            logfile = RemoteLogs.logfile_path(remote_job.key)
            self.jobs.append(_Job(remote_job, process, logfile, 0))
            RemoteJob.set_state(remote_job.key, TaskInstanceState.RUNNING)
            return True

        logger.info("No new job to process%s", f", {len(self.jobs)} still running" if self.jobs else "")
        return False

    def check_running_jobs(self) -> None:
        """Check which of the running tasks/jobs are completed and report back."""
        for i in range(len(self.jobs) - 1, -1, -1):
            job = self.jobs[i]
            job.process.poll()
            if job.process.returncode is not None:
                self.jobs.remove(job)
                if job.process.returncode == 0:
                    logger.info("Job completed: %s", job.remote_job)
                    RemoteJob.set_state(job.remote_job.key, TaskInstanceState.SUCCESS)
                else:
                    logger.error("Job failed: %s", job.remote_job)
                    RemoteJob.set_state(job.remote_job.key, TaskInstanceState.FAILED)
            if job.logfile.exists() and job.logfile.stat().st_size > job.logsize:
                with job.logfile.open("r") as logfile:
                    logfile.seek(job.logsize, os.SEEK_SET)
                    logdata = logfile.read()
                    RemoteLogs.push_logs(
                        task=job.remote_job.key,
                        log_chunk_time=datetime.now(),
                        log_chunk_data=logdata,
                    )
                    job.logsize += len(logdata)

    def heartbeat(self) -> None:
        """Report liveness state of worker to central site with stats."""
        state = (
            (RemoteWorkerState.TERMINATING if _RemoteWorkerCli.drain else RemoteWorkerState.RUNNING)
            if self.jobs
            else RemoteWorkerState.IDLE
        )
        sysinfo = _get_sysinfo()
        RemoteWorker.set_state(self.hostname, state, len(self.jobs), sysinfo)

    def interruptible_sleep(self):
        """Sleeps but stops sleeping if drain is made."""
        drain_before_sleep = _RemoteWorkerCli.drain
        for _ in range(0, self.job_poll_interval * 10):
            sleep(0.1)
            if drain_before_sleep != _RemoteWorkerCli.drain:
                return


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def worker(args):
    """Start Airflow Remote worker."""
    print(settings.HEADER)
    print(REMOTE_WORKER_HEADER)

    remote_worker = _RemoteWorkerCli(
        pid_file_path=_pid_file_path(args.pid),
        hostname=args.remote_hostname or _hostname(),
        queues=args.queues.split(",") if args.queues else None,
        concurrency=args.concurrency,
        job_poll_interval=conf.getint("remote", "job_poll_interval"),
        heartbeat_interval=conf.getint("remote", "heartbeat_interval"),
    )
    remote_worker.start()


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def stop(args):
    """Stop a running Airflow Remote worker."""
    pid = read_pid_from_pidfile(_pid_file_path(args.pid))
    # Send SIGINT
    if pid:
        logger.warning("Sending SIGINT to worker pid %i.", pid)
        worker_process = psutil.Process(pid)
        worker_process.send_signal(signal.SIGINT)
    else:
        logger.warning("Could not find PID of worker.")


ARG_CONCURRENCY = Arg(
    ("-c", "--concurrency"),
    type=int,
    help="The number of worker processes",
    default=conf.getint("remote", "worker_concurrency", fallback=8),
)
ARG_QUEUES = Arg(
    ("-q", "--queues"),
    help="Comma delimited list of queues to serve, serve all queues if not provided.",
)
ARG_REMOTE_HOSTNAME = Arg(
    ("-H", "--remote-hostname"),
    help="Set the hostname of worker if you have multiple workers on a single machine",
)
REMOTE_COMMANDS: list[ActionCommand] = [
    ActionCommand(
        name=worker.__name__,
        help=worker.__doc__,
        func=worker,
        args=(
            ARG_CONCURRENCY,
            ARG_QUEUES,
            ARG_REMOTE_HOSTNAME,
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name=stop.__name__,
        help=stop.__doc__,
        func=stop,
        args=(
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
]
