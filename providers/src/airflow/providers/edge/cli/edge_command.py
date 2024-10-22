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
from airflow.providers.edge import __version__ as edge_provider_version
from airflow.providers.edge.models.edge_job import EdgeJob
from airflow.providers.edge.models.edge_logs import EdgeLogs
from airflow.providers.edge.models.edge_worker import EdgeWorker, EdgeWorkerState
from airflow.utils import cli as cli_utils
from airflow.utils.platform import IS_WINDOWS
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.state import TaskInstanceState

logger = logging.getLogger(__name__)
EDGE_WORKER_PROCESS_NAME = "edge-worker"
EDGE_WORKER_HEADER = "\n".join(
    [
        r"   ____   __           _      __         __",
        r"  / __/__/ /__ ____   | | /| / /__  ____/ /_____ ____",
        r" / _// _  / _ `/ -_)  | |/ |/ / _ \/ __/  '_/ -_) __/",
        r"/___/\_,_/\_, /\__/   |__/|__/\___/_/ /_/\_\\__/_/",
        r"         /___/",
        r"",
    ]
)


@providers_configuration_loaded
def force_use_internal_api_on_edge_worker():
    """
    Ensure that the environment is configured for the internal API without needing to declare it outside.

    This is only required for an Edge worker and must to be done before the Click CLI wrapper is initiated.
    That is because the CLI wrapper will attempt to establish a DB connection, which will fail before the
    function call can take effect. In an Edge worker, we need to "patch" the environment before starting.
    """
    if "airflow" in sys.argv[0] and sys.argv[1:3] == ["edge", "worker"]:
        api_url = conf.get("edge", "api_url")
        if not api_url:
            raise SystemExit("Error: API URL is not configured, please correct configuration.")
        logger.info("Starting worker with API endpoint %s", api_url)
        # export Edge API to be used for internal API
        os.environ["AIRFLOW_ENABLE_AIP_44"] = "True"
        os.environ["AIRFLOW__CORE__INTERNAL_API_URL"] = api_url
        InternalApiConfig.set_use_internal_api("edge-worker")
        # Disable mini-scheduler post task execution and leave next task schedule to core scheduler
        os.environ["AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION"] = "False"


force_use_internal_api_on_edge_worker()


def _hostname() -> str:
    if IS_WINDOWS:
        return platform.uname().node
    else:
        return os.uname()[1]


def _pid_file_path(pid_file: str | None) -> str:
    return cli_utils.setup_locations(process=EDGE_WORKER_PROCESS_NAME, pid=pid_file)[0]

def _write_pid_to_pidfile(pid_file_path):
    """
    Write PIDs for Edge Workers to disk,
    ensuring that orphanded PID files from crashed instance are handled.
    """
    if pid_file_path.exists():
        # Handle existing PID files on disk
        logger.info("An existing PID file has been found: %s.", pid_file_path)
        pid_stored_in_pid_file = read_pid_from_pidfile(pid_file_path)
        if os.getpid() == pid_stored_in_pid_file:
            raise SystemExit("A PID file has already been written")
        else:
            # PID file was written by dead or already running instance
            if psutil.pid_exists(pid_stored_in_pid_file):
                # case 1: another instance uses the same path for its PID file
                raise SystemExit(
                    f"The PID file {pid_file_path} contains the PID of another running process. "
                    "Configuration issue: edge worker instance must use different PID file paths!"
                )
            else:
                # case 2: previous instance crashed without cleaning up its PID file
                logger.info("PID file is orphaned. Cleaning up.")
                pid_file_path.unlink()
    logger.debug("PID file written to %s.", pid_file_path)
    write_pid_to_pidfile(pid_file_path)


@dataclass
class _Job:
    """Holds all information for a task/job to be executed as bundle."""

    edge_job: EdgeJob
    process: Popen
    logfile: Path
    logsize: int
    """Last size of log file, point of last chunk push."""


class _EdgeWorkerCli:
    """Runner instance which executes the Edge Worker."""

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
        logger.info("Request to show down Edge Worker received, waiting for jobs to complete.")
        _EdgeWorkerCli.drain = True

    def _get_sysinfo(self) -> dict:
        """Produce the sysinfo from worker to post to central site."""
        return {
            "airflow_version": airflow_version,
            "edge_provider_version": edge_provider_version,
            "concurrency": self.concurrency,
        }

    def start(self):
        """Start the execution in a loop until terminated."""
        try:
            self.last_hb = EdgeWorker.register_worker(
                self.hostname, EdgeWorkerState.STARTING, self.queues, self._get_sysinfo()
            ).last_update
        except AirflowException as e:
            if "404:NOT FOUND" in str(e):
                raise SystemExit("Error: API endpoint is not ready, please set [edge] api_enabled=True.")
            raise SystemExit(str(e))
        _write_pid_to_pidfile(self.pid_file_path)
        signal.signal(signal.SIGINT, _EdgeWorkerCli.signal_handler)
        try:
            while not _EdgeWorkerCli.drain or self.jobs:
                self.loop()

            logger.info("Quitting worker, signal being offline.")
            EdgeWorker.set_state(self.hostname, EdgeWorkerState.OFFLINE, 0, self._get_sysinfo())
        finally:
            remove_existing_pidfile(self.pid_file_path)

    def loop(self):
        """Run a loop of scheduling and monitoring tasks."""
        new_job = False
        if not _EdgeWorkerCli.drain and len(self.jobs) < self.concurrency:
            new_job = self.fetch_job()
        self.check_running_jobs()

        if _EdgeWorkerCli.drain or datetime.now().timestamp() - self.last_hb.timestamp() > self.hb_interval:
            self.heartbeat()
            self.last_hb = datetime.now()

        if not new_job:
            self.interruptible_sleep()

    def fetch_job(self) -> bool:
        """Fetch and start a new job from central site."""
        logger.debug("Attempting to fetch a new job...")
        edge_job = EdgeJob.reserve_task(self.hostname, self.queues)
        if edge_job:
            logger.info("Received job: %s", edge_job)
            env = os.environ.copy()
            env["AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION"] = "True"
            env["AIRFLOW__CORE__INTERNAL_API_URL"] = conf.get("edge", "api_url")
            env["_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"] = "1"
            process = Popen(edge_job.command, close_fds=True, env=env)
            logfile = EdgeLogs.logfile_path(edge_job.key)
            self.jobs.append(_Job(edge_job, process, logfile, 0))
            EdgeJob.set_state(edge_job.key, TaskInstanceState.RUNNING)
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
                    logger.info("Job completed: %s", job.edge_job)
                    EdgeJob.set_state(job.edge_job.key, TaskInstanceState.SUCCESS)
                else:
                    logger.error("Job failed: %s", job.edge_job)
                    EdgeJob.set_state(job.edge_job.key, TaskInstanceState.FAILED)
            if job.logfile.exists() and job.logfile.stat().st_size > job.logsize:
                with job.logfile.open("r") as logfile:
                    logfile.seek(job.logsize, os.SEEK_SET)
                    logdata = logfile.read()
                    EdgeLogs.push_logs(
                        task=job.edge_job.key,
                        log_chunk_time=datetime.now(),
                        log_chunk_data=logdata,
                    )
                    job.logsize += len(logdata)

    def heartbeat(self) -> None:
        """Report liveness state of worker to central site with stats."""
        state = (
            (EdgeWorkerState.TERMINATING if _EdgeWorkerCli.drain else EdgeWorkerState.RUNNING)
            if self.jobs
            else EdgeWorkerState.IDLE
        )
        sysinfo = self._get_sysinfo()
        self.queues = EdgeWorker.set_state(self.hostname, state, len(self.jobs), sysinfo)

    def interruptible_sleep(self):
        """Sleeps but stops sleeping if drain is made."""
        drain_before_sleep = _EdgeWorkerCli.drain
        for _ in range(0, self.job_poll_interval * 10):
            sleep(0.1)
            if drain_before_sleep != _EdgeWorkerCli.drain:
                return


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def worker(args):
    """Start Airflow Edge Worker."""
    print(settings.HEADER)
    print(EDGE_WORKER_HEADER)

    edge_worker = _EdgeWorkerCli(
        pid_file_path=_pid_file_path(args.pid),
        hostname=args.edge_hostname or _hostname(),
        queues=args.queues.split(",") if args.queues else None,
        concurrency=args.concurrency,
        job_poll_interval=conf.getint("edge", "job_poll_interval"),
        heartbeat_interval=conf.getint("edge", "heartbeat_interval"),
    )
    edge_worker.start()


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def stop(args):
    """Stop a running Airflow Edge Worker."""
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
    default=conf.getint("edge", "worker_concurrency", fallback=8),
)
ARG_QUEUES = Arg(
    ("-q", "--queues"),
    help="Comma delimited list of queues to serve, serve all queues if not provided.",
)
ARG_EDGE_HOSTNAME = Arg(
    ("-H", "--edge-hostname"),
    help="Set the hostname of worker if you have multiple workers on a single machine",
)
EDGE_COMMANDS: list[ActionCommand] = [
    ActionCommand(
        name=worker.__name__,
        help=worker.__doc__,
        func=worker,
        args=(
            ARG_CONCURRENCY,
            ARG_QUEUES,
            ARG_EDGE_HOSTNAME,
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
