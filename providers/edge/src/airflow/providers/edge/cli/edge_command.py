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
from http import HTTPStatus
from multiprocessing import Process
from pathlib import Path
from subprocess import Popen
from time import sleep
from typing import TYPE_CHECKING

import psutil
from lockfile.pidlockfile import read_pid_from_pidfile, remove_existing_pidfile, write_pid_to_pidfile
from requests import HTTPError

from airflow import __version__ as airflow_version, settings
from airflow.cli.cli_config import ARG_PID, ARG_VERBOSE, ActionCommand, Arg
from airflow.configuration import conf
from airflow.providers.edge import __version__ as edge_provider_version
from airflow.providers.edge.cli.api_client import (
    jobs_fetch,
    jobs_set_state,
    logs_logfile_path,
    logs_push,
    worker_register,
    worker_set_state,
)
from airflow.providers.edge.models.edge_worker import EdgeWorkerState, EdgeWorkerVersionException
from airflow.providers.edge.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils import cli as cli_utils, timezone
from airflow.utils.platform import IS_WINDOWS
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.providers.edge.worker_api.datamodels import EdgeJobFetched

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
    # export Edge API to be used for internal API
    os.environ["_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"] = "1"
    os.environ["AIRFLOW_ENABLE_AIP_44"] = "True"
    if "airflow" in sys.argv[0] and sys.argv[1:3] == ["edge", "worker"]:
        api_url = conf.get("edge", "api_url")
        if not api_url:
            raise SystemExit("Error: API URL is not configured, please correct configuration.")
        logger.info("Starting worker with API endpoint %s", api_url)
        os.environ["AIRFLOW__CORE__INTERNAL_API_URL"] = api_url


force_use_internal_api_on_edge_worker()


def _hostname() -> str:
    if IS_WINDOWS:
        return platform.uname().node
    else:
        return os.uname()[1]


def _pid_file_path(pid_file: str | None) -> str:
    return cli_utils.setup_locations(process=EDGE_WORKER_PROCESS_NAME, pid=pid_file)[0]


def _write_pid_to_pidfile(pid_file_path: str):
    """Write PIDs for Edge Workers to disk, handling existing PID files."""
    if Path(pid_file_path).exists():
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
                remove_existing_pidfile(pid_file_path)
    logger.debug("PID file written to %s.", pid_file_path)
    write_pid_to_pidfile(pid_file_path)


@dataclass
class _Job:
    """Holds all information for a task/job to be executed as bundle."""

    edge_job: EdgeJobFetched
    process: Popen | Process
    logfile: Path
    logsize: int
    """Last size of log file, point of last chunk push."""

    @property
    def is_running(self) -> bool:
        """Check if the job is still running."""
        if isinstance(self.process, Popen):
            self.process.poll()
            return self.process.returncode is None
        return self.process.exitcode is None

    @property
    def is_success(self) -> bool:
        """Check if the job was successful."""
        if isinstance(self.process, Popen):
            return self.process.returncode == 0
        return self.process.exitcode == 0


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
        pid_file_path: str,
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
        self.free_concurrency = concurrency

    @staticmethod
    def signal_handler(sig, frame):
        logger.info("Request to shut down Edge Worker received, waiting for jobs to complete.")
        _EdgeWorkerCli.drain = True

    def shutdown_handler(self, sig, frame):
        logger.info("SIGTERM received. Terminating all jobs and quit")
        for job in self.jobs:
            os.killpg(job.process.pid, signal.SIGTERM)
        _EdgeWorkerCli.drain = True

    def _get_sysinfo(self) -> dict:
        """Produce the sysinfo from worker to post to central site."""
        return {
            "airflow_version": airflow_version,
            "edge_provider_version": edge_provider_version,
            "concurrency": self.concurrency,
            "free_concurrency": self.free_concurrency,
        }

    def _launch_job_af3(self, edge_job: EdgeJobFetched) -> tuple[Process, Path]:
        if TYPE_CHECKING:
            from airflow.executors.workloads import ExecuteTask

        def _run_job_via_supervisor(
            workload: ExecuteTask,
        ) -> int:
            from setproctitle import setproctitle

            from airflow.sdk.execution_time.supervisor import supervise

            # Ignore ctrl-c in this process -- we don't want to kill _this_ one. we let tasks run to completion
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            logger.info("Worker starting up pid=%d", os.getpid())
            setproctitle(f"airflow edge worker: {workload.ti.key}")

            try:
                supervise(
                    # This is the "wrong" ti type, but it duck types the same. TODO: Create a protocol for this.
                    # Same like in airflow/executors/local_executor.py:_execute_work()
                    ti=workload.ti,  # type: ignore[arg-type]
                    dag_rel_path=workload.dag_rel_path,
                    bundle_info=workload.bundle_info,
                    token=workload.token,
                    server=conf.get(
                        "workers", "execution_api_server_url", fallback="http://localhost:9091/execution/"
                    ),
                    log_path=workload.log_path,
                )
                return 0
            except Exception as e:
                logger.exception("Task execution failed: %s", e)
                return 1

        workload: ExecuteTask = edge_job.command
        process = Process(
            target=_run_job_via_supervisor,
            kwargs={"workload": workload},
        )
        process.start()
        base_log_folder = conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
        if TYPE_CHECKING:
            assert workload.log_path  # We need to assume this is defined in here
        logfile = Path(base_log_folder, workload.log_path)
        return process, logfile

    def _launch_job_af2_10(self, edge_job: EdgeJobFetched) -> tuple[Popen, Path]:
        """Compatibility for Airflow 2.10 Launch."""
        env = os.environ.copy()
        env["AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION"] = "True"
        env["AIRFLOW__CORE__INTERNAL_API_URL"] = conf.get("edge", "api_url")
        env["_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"] = "1"
        command: list[str] = edge_job.command  # type: ignore[assignment]
        process = Popen(command, close_fds=True, env=env, start_new_session=True)
        logfile = logs_logfile_path(edge_job.key)
        return process, logfile

    def _launch_job(self, edge_job: EdgeJobFetched):
        """Get the received job executed."""
        process: Popen | Process
        if AIRFLOW_V_3_0_PLUS:
            process, logfile = self._launch_job_af3(edge_job)
        else:
            # Airflow 2.10
            process, logfile = self._launch_job_af2_10(edge_job)
        self.jobs.append(_Job(edge_job, process, logfile, 0))

    def start(self):
        """Start the execution in a loop until terminated."""
        try:
            self.last_hb = worker_register(
                self.hostname, EdgeWorkerState.STARTING, self.queues, self._get_sysinfo()
            )
        except EdgeWorkerVersionException as e:
            logger.info("Version mismatch of Edge worker and Core. Shutting down worker.")
            raise SystemExit(str(e))
        except HTTPError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                raise SystemExit("Error: API endpoint is not ready, please set [edge] api_enabled=True.")
            raise SystemExit(str(e))
        _write_pid_to_pidfile(self.pid_file_path)
        signal.signal(signal.SIGINT, _EdgeWorkerCli.signal_handler)
        signal.signal(signal.SIGTERM, self.shutdown_handler)
        try:
            while not _EdgeWorkerCli.drain or self.jobs:
                self.loop()

            logger.info("Quitting worker, signal being offline.")
            try:
                worker_set_state(self.hostname, EdgeWorkerState.OFFLINE, 0, self.queues, self._get_sysinfo())
            except EdgeWorkerVersionException:
                logger.info("Version mismatch of Edge worker and Core. Quitting worker anyway.")
        finally:
            remove_existing_pidfile(self.pid_file_path)

    def loop(self):
        """Run a loop of scheduling and monitoring tasks."""
        new_job = False
        if not _EdgeWorkerCli.drain and self.free_concurrency > 0:
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
        edge_job = jobs_fetch(self.hostname, self.queues, self.free_concurrency)
        if edge_job:
            logger.info("Received job: %s", edge_job)
            self._launch_job(edge_job)
            jobs_set_state(edge_job.key, TaskInstanceState.RUNNING)
            return True

        logger.info("No new job to process%s", f", {len(self.jobs)} still running" if self.jobs else "")
        return False

    def check_running_jobs(self) -> None:
        """Check which of the running tasks/jobs are completed and report back."""
        used_concurrency = 0
        for i in range(len(self.jobs) - 1, -1, -1):
            job = self.jobs[i]
            if not job.is_running:
                self.jobs.remove(job)
                if job.is_success:
                    logger.info("Job completed: %s", job.edge_job)
                    jobs_set_state(job.edge_job.key, TaskInstanceState.SUCCESS)
                else:
                    logger.error("Job failed: %s", job.edge_job)
                    jobs_set_state(job.edge_job.key, TaskInstanceState.FAILED)
            else:
                used_concurrency += job.edge_job.concurrency_slots

            if job.logfile.exists() and job.logfile.stat().st_size > job.logsize:
                with job.logfile.open("rb") as logfile:
                    push_log_chunk_size = conf.getint("edge", "push_log_chunk_size")
                    logfile.seek(job.logsize, os.SEEK_SET)
                    read_data = logfile.read()
                    job.logsize += len(read_data)
                    # backslashreplace to keep not decoded characters and not raising exception
                    # replace null with question mark to fix issue during DB push
                    log_data = read_data.decode(errors="backslashreplace").replace("\x00", "\ufffd")
                    while True:
                        chunk_data = log_data[:push_log_chunk_size]
                        log_data = log_data[push_log_chunk_size:]
                        if not chunk_data:
                            break

                        logs_push(
                            task=job.edge_job.key,
                            log_chunk_time=timezone.utcnow(),
                            log_chunk_data=chunk_data,
                        )

        self.free_concurrency = self.concurrency - used_concurrency

    def heartbeat(self) -> None:
        """Report liveness state of worker to central site with stats."""
        state = (
            (EdgeWorkerState.TERMINATING if _EdgeWorkerCli.drain else EdgeWorkerState.RUNNING)
            if self.jobs
            else EdgeWorkerState.IDLE
        )
        sysinfo = self._get_sysinfo()
        try:
            self.queues = worker_set_state(self.hostname, state, len(self.jobs), self.queues, sysinfo)
        except EdgeWorkerVersionException:
            logger.info("Version mismatch of Edge worker and Core. Shutting down worker.")
            _EdgeWorkerCli.drain = True

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
