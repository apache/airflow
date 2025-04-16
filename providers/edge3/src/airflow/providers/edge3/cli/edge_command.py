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

import json
import logging
import os
import signal
import sys
from dataclasses import asdict
from datetime import datetime
from http import HTTPStatus
from multiprocessing import Process
from pathlib import Path
from subprocess import Popen
from time import sleep, time
from typing import TYPE_CHECKING

import psutil
from lockfile.pidlockfile import read_pid_from_pidfile, remove_existing_pidfile, write_pid_to_pidfile
from requests import HTTPError

from airflow import __version__ as airflow_version, settings
from airflow.cli.cli_config import ARG_PID, ARG_VERBOSE, ActionCommand, Arg
from airflow.configuration import conf
from airflow.providers.edge3 import __version__ as edge_provider_version
from airflow.providers.edge3.cli.api_client import (
    jobs_fetch,
    jobs_set_state,
    logs_logfile_path,
    logs_push,
    worker_register,
    worker_set_state,
)
from airflow.providers.edge3.cli.dataclasses import Job, MaintenanceMarker, WorkerStatus
from airflow.providers.edge3.models.edge_worker import EdgeWorkerState, EdgeWorkerVersionException
from airflow.providers.edge3.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils import cli as cli_utils, timezone
from airflow.utils.net import getfqdn
from airflow.utils.platform import IS_WINDOWS
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.providers.edge3.worker_api.datamodels import EdgeJobFetched

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


def _status_signal() -> signal.Signals:
    if IS_WINDOWS:
        return signal.SIGBREAK  # type: ignore[attr-defined]
    return signal.SIGUSR2


SIG_STATUS = _status_signal()


def _pid_file_path(pid_file: str | None) -> str:
    return cli_utils.setup_locations(process=EDGE_WORKER_PROCESS_NAME, pid=pid_file)[0]


def _get_pid(pid_file: str | None) -> int:
    pid = read_pid_from_pidfile(_pid_file_path(pid_file))
    if not pid:
        logger.warning("Could not find PID of worker.")
        sys.exit(1)
    return pid


def _status_file_path(pid_file: str | None) -> str:
    return cli_utils.setup_locations(process=EDGE_WORKER_PROCESS_NAME, pid=pid_file)[1]


def _maintenance_marker_file_path(pid_file: str | None) -> str:
    return cli_utils.setup_locations(process=EDGE_WORKER_PROCESS_NAME, pid=pid_file)[1][:-4] + ".in"


def _write_pid_to_pidfile(pid_file_path: str):
    """Write PIDs for Edge Workers to disk, handling existing PID files."""
    if Path(pid_file_path).exists():
        # Handle existing PID files on disk
        logger.info("An existing PID file has been found: %s.", pid_file_path)
        pid_stored_in_pid_file = read_pid_from_pidfile(pid_file_path)
        if os.getpid() == pid_stored_in_pid_file:
            raise SystemExit("A PID file has already been written")
        # PID file was written by dead or already running instance
        if psutil.pid_exists(pid_stored_in_pid_file):
            # case 1: another instance uses the same path for its PID file
            raise SystemExit(
                f"The PID file {pid_file_path} contains the PID of another running process. "
                "Configuration issue: edge worker instance must use different PID file paths!"
            )
        # case 2: previous instance crashed without cleaning up its PID file
        logger.warning("PID file is orphaned. Cleaning up.")
        remove_existing_pidfile(pid_file_path)
    logger.debug("PID file written to %s.", pid_file_path)
    write_pid_to_pidfile(pid_file_path)


def _edge_hostname() -> str:
    """Get the hostname of the edge worker that should be reported by tasks."""
    return os.environ.get("HOSTNAME", getfqdn())


class _EdgeWorkerCli:
    """Runner instance which executes the Edge Worker."""

    jobs: list[Job] = []
    """List of jobs that the worker is running currently."""
    last_hb: datetime | None = None
    """Timestamp of last heart beat sent to server."""
    drain: bool = False
    """Flag if job processing should be completed and no new jobs fetched for a graceful stop/shutdown."""
    maintenance_mode: bool = False
    """Flag if job processing should be completed and no new jobs fetched for maintenance mode. """
    maintenance_comments: str | None = None
    """Comments for maintenance mode."""

    edge_instance: _EdgeWorkerCli | None = None
    """Singleton instance of the worker."""

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

        _EdgeWorkerCli.edge_instance = self

    @staticmethod
    def signal_handler(sig: signal.Signals, frame):
        if sig == SIG_STATUS:
            marker_path = Path(_maintenance_marker_file_path(None))
            if marker_path.exists():
                request = MaintenanceMarker.from_json(marker_path.read_text())
                logger.info("Requested to set maintenance mode to %s", request.maintenance)
                _EdgeWorkerCli.maintenance_mode = request.maintenance == "on"
                if _EdgeWorkerCli.maintenance_mode and request.comments:
                    logger.info("Comments: %s", request.comments)
                    _EdgeWorkerCli.maintenance_comments = request.comments
                marker_path.unlink()
                # send heartbeat immediately to update state
                if _EdgeWorkerCli.edge_instance:
                    _EdgeWorkerCli.edge_instance.heartbeat(_EdgeWorkerCli.maintenance_comments)
            else:
                logger.info("Request to get status of Edge Worker received.")
            status_path = Path(_status_file_path(None))
            status_path.write_text(
                WorkerStatus(
                    job_count=len(_EdgeWorkerCli.jobs),
                    jobs=[job.edge_job.key for job in _EdgeWorkerCli.jobs],
                    state=_EdgeWorkerCli._get_state(),
                    maintenance=_EdgeWorkerCli.maintenance_mode,
                    maintenance_comments=_EdgeWorkerCli.maintenance_comments,
                    drain=_EdgeWorkerCli.drain,
                ).json
            )
        else:
            logger.info("Request to shut down Edge Worker received, waiting for jobs to complete.")
            _EdgeWorkerCli.drain = True

    def shutdown_handler(self, sig, frame):
        logger.info("SIGTERM received. Terminating all jobs and quit")
        for job in _EdgeWorkerCli.jobs:
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

    @staticmethod
    def _get_state() -> EdgeWorkerState:
        """State of the Edge Worker."""
        if _EdgeWorkerCli.jobs:
            if _EdgeWorkerCli.drain:
                return EdgeWorkerState.TERMINATING
            if _EdgeWorkerCli.maintenance_mode:
                return EdgeWorkerState.MAINTENANCE_PENDING
            return EdgeWorkerState.RUNNING

        if _EdgeWorkerCli.drain:
            if _EdgeWorkerCli.maintenance_mode:
                return EdgeWorkerState.OFFLINE_MAINTENANCE
            return EdgeWorkerState.OFFLINE

        if _EdgeWorkerCli.maintenance_mode:
            return EdgeWorkerState.MAINTENANCE_MODE
        return EdgeWorkerState.IDLE

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
                    server=conf.get("core", "execution_api_server_url"),
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
        _EdgeWorkerCli.jobs.append(Job(edge_job, process, logfile, 0))

    def start(self):
        """Start the execution in a loop until terminated."""
        try:
            self.last_hb = worker_register(
                self.hostname, EdgeWorkerState.STARTING, self.queues, self._get_sysinfo()
            ).last_update
        except EdgeWorkerVersionException as e:
            logger.info("Version mismatch of Edge worker and Core. Shutting down worker.")
            raise SystemExit(str(e))
        except HTTPError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                raise SystemExit("Error: API endpoint is not ready, please set [edge] api_enabled=True.")
            raise SystemExit(str(e))
        _write_pid_to_pidfile(self.pid_file_path)
        signal.signal(signal.SIGINT, _EdgeWorkerCli.signal_handler)
        signal.signal(SIG_STATUS, _EdgeWorkerCli.signal_handler)
        signal.signal(signal.SIGTERM, self.shutdown_handler)
        os.environ["HOSTNAME"] = self.hostname
        os.environ["AIRFLOW__CORE__HOSTNAME_CALLABLE"] = f"{_edge_hostname.__module__}._edge_hostname"
        try:
            self.worker_state_changed = self.heartbeat()
            self.last_hb = datetime.now()
            while not _EdgeWorkerCli.drain or _EdgeWorkerCli.jobs:
                self.loop()

            logger.info("Quitting worker, signal being offline.")
            try:
                worker_set_state(
                    self.hostname,
                    EdgeWorkerState.OFFLINE_MAINTENANCE
                    if _EdgeWorkerCli.maintenance_mode
                    else EdgeWorkerState.OFFLINE,
                    0,
                    self.queues,
                    self._get_sysinfo(),
                )
            except EdgeWorkerVersionException:
                logger.info("Version mismatch of Edge worker and Core. Quitting worker anyway.")
        finally:
            remove_existing_pidfile(self.pid_file_path)

    def loop(self):
        """Run a loop of scheduling and monitoring tasks."""
        new_job = False
        previous_jobs = _EdgeWorkerCli.jobs
        if not any((_EdgeWorkerCli.drain, _EdgeWorkerCli.maintenance_mode)) and self.free_concurrency > 0:
            new_job = self.fetch_job()
        self.check_running_jobs()

        if (
            _EdgeWorkerCli.drain
            or datetime.now().timestamp() - self.last_hb.timestamp() > self.hb_interval
            or self.worker_state_changed  # send heartbeat immediately if the state is different in db
            or bool(previous_jobs) != bool(_EdgeWorkerCli.jobs)  # when number of jobs changes from/to 0
        ):
            self.worker_state_changed = self.heartbeat()
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

        logger.info(
            "No new job to process%s",
            f", {len(_EdgeWorkerCli.jobs)} still running" if _EdgeWorkerCli.jobs else "",
        )
        return False

    def check_running_jobs(self) -> None:
        """Check which of the running tasks/jobs are completed and report back."""
        used_concurrency = 0
        for i in range(len(_EdgeWorkerCli.jobs) - 1, -1, -1):
            job = _EdgeWorkerCli.jobs[i]
            if not job.is_running:
                _EdgeWorkerCli.jobs.remove(job)
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

    def heartbeat(self, new_maintenance_comments: str | None = None) -> bool:
        """Report liveness state of worker to central site with stats."""
        state = _EdgeWorkerCli._get_state()
        sysinfo = self._get_sysinfo()
        worker_state_changed: bool = False
        try:
            worker_info = worker_set_state(
                self.hostname,
                state,
                len(_EdgeWorkerCli.jobs),
                self.queues,
                sysinfo,
                new_maintenance_comments,
            )
            self.queues = worker_info.queues
            if worker_info.state == EdgeWorkerState.MAINTENANCE_REQUEST:
                logger.info("Maintenance mode requested!")
                _EdgeWorkerCli.maintenance_mode = True
            elif (
                worker_info.state in [EdgeWorkerState.IDLE, EdgeWorkerState.RUNNING]
                and _EdgeWorkerCli.maintenance_mode
            ):
                logger.info("Exit Maintenance mode requested!")
                _EdgeWorkerCli.maintenance_mode = False
            if _EdgeWorkerCli.maintenance_mode:
                _EdgeWorkerCli.maintenance_comments = worker_info.maintenance_comments
            else:
                _EdgeWorkerCli.maintenance_comments = None

            worker_state_changed = worker_info.state != state
        except EdgeWorkerVersionException:
            logger.info("Version mismatch of Edge worker and Core. Shutting down worker.")
            _EdgeWorkerCli.drain = True
        return worker_state_changed

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
        hostname=args.edge_hostname or getfqdn(),
        queues=args.queues.split(",") if args.queues else None,
        concurrency=args.concurrency,
        job_poll_interval=conf.getint("edge", "job_poll_interval"),
        heartbeat_interval=conf.getint("edge", "heartbeat_interval"),
    )
    edge_worker.start()


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def status(args):
    """Check for Airflow Edge Worker status."""
    pid = _get_pid(args.pid)

    # Send Signal as notification to drop status JSON
    logger.debug("Sending SIGUSR2 to worker pid %i.", pid)
    status_min_date = time() - 1
    status_path = Path(_status_file_path(args.pid))
    worker_process = psutil.Process(pid)
    worker_process.send_signal(SIG_STATUS)
    while psutil.pid_exists(pid) and (
        not status_path.exists() or status_path.stat().st_mtime < status_min_date
    ):
        sleep(0.1)
    if not psutil.pid_exists(pid):
        logger.warning("PID of worker dis-appeared while checking for status.")
        sys.exit(2)
    if not status_path.exists() or status_path.stat().st_mtime < status_min_date:
        logger.warning("Could not read status of worker.")
        sys.exit(3)
    status = WorkerStatus.from_json(status_path.read_text())
    print(json.dumps(asdict(status), indent=4))


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def maintenance(args):
    """Set or Unset maintenance mode of worker."""
    if args.maintenance == "on" and not args.comments:
        logger.error("Comments are required when setting maintenance mode.")
        sys.exit(4)

    pid = _get_pid(args.pid)

    # Write marker JSON file
    from getpass import getuser

    marker_path = Path(_maintenance_marker_file_path(args.pid))
    logger.debug("Writing maintenance marker file to %s.", marker_path)
    marker_path.write_text(
        MaintenanceMarker(
            maintenance=args.maintenance,
            comments=f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {getuser()} put "
            f"node into maintenance mode via cli\nComment: {args.comments}"
            if args.maintenance == "on"
            else None,
        ).json
    )

    # Send Signal as notification to fetch maintenance marker
    logger.debug("Sending SIGUSR2 to worker pid %i.", pid)
    status_min_date = time() - 1
    status_path = Path(_status_file_path(args.pid))
    worker_process = psutil.Process(pid)
    worker_process.send_signal(SIG_STATUS)
    while psutil.pid_exists(pid) and (
        not status_path.exists() or status_path.stat().st_mtime < status_min_date
    ):
        sleep(0.1)
    if not psutil.pid_exists(pid):
        logger.warning("PID of worker dis-appeared while checking for status.")
        sys.exit(2)
    if not status_path.exists() or status_path.stat().st_mtime < status_min_date:
        logger.warning("Could not read status of worker.")
        sys.exit(3)
    status = WorkerStatus.from_json(status_path.read_text())

    if args.wait:
        if args.maintenance == "on" and status.state != EdgeWorkerState.MAINTENANCE_MODE:
            logger.info("Waiting for worker to be drained...")
            while True:
                sleep(4.5)
                worker_process.send_signal(SIG_STATUS)
                sleep(0.5)
                status = WorkerStatus.from_json(status_path.read_text())
                if status.state == EdgeWorkerState.MAINTENANCE_MODE:
                    logger.info("Worker was drained successfully!")
                    break
                if status.state not in [
                    EdgeWorkerState.MAINTENANCE_REQUEST,
                    EdgeWorkerState.MAINTENANCE_PENDING,
                ]:
                    logger.info("Worker maintenance was exited by someone else!")
                    break
        if args.maintenance == "off" and status.state == EdgeWorkerState.MAINTENANCE_MODE:
            logger.info("Waiting for worker to exit maintenance...")
            while status.state in [EdgeWorkerState.MAINTENANCE_MODE, EdgeWorkerState.MAINTENANCE_EXIT]:
                sleep(4.5)
                worker_process.send_signal(SIG_STATUS)
                sleep(0.5)
                status = WorkerStatus.from_json(status_path.read_text())

    print(json.dumps(asdict(status), indent=4))


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def stop(args):
    """Stop a running Airflow Edge Worker."""
    pid = _get_pid(args.pid)
    # Send SIGINT
    logger.info("Sending SIGINT to worker pid %i.", pid)
    worker_process = psutil.Process(pid)
    worker_process.send_signal(signal.SIGINT)

    if args.wait:
        logger.info("Waiting for worker to stop...")
        while psutil.pid_exists(pid):
            sleep(0.1)
        logger.info("Worker has been shut down.")


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
ARG_MAINTENANCE = Arg(("maintenance",), help="Desired maintenance state", choices=("on", "off"))
ARG_MAINTENANCE_COMMENT = Arg(
    ("-c", "--comments"),
    help="Maintenance comments to report reason. Required if maintenance is turned on.",
)
ARG_WAIT_MAINT = Arg(
    ("-w", "--wait"),
    default=False,
    help="Wait until edge worker has reached desired state.",
    action="store_true",
)
ARG_WAIT_STOP = Arg(
    ("-w", "--wait"),
    default=False,
    help="Wait until edge worker is shut down.",
    action="store_true",
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
        name=status.__name__,
        help=status.__doc__,
        func=status,
        args=(
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name=maintenance.__name__,
        help=maintenance.__doc__,
        func=maintenance,
        args=(
            ARG_MAINTENANCE,
            ARG_MAINTENANCE_COMMENT,
            ARG_WAIT_MAINT,
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name=stop.__name__,
        help=stop.__doc__,
        func=stop,
        args=(
            ARG_WAIT_STOP,
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
]
