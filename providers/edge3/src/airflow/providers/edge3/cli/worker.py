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
import signal
import sys
from datetime import datetime
from functools import cache
from http import HTTPStatus
from multiprocessing import Process
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING

from lockfile.pidlockfile import remove_existing_pidfile
from requests import HTTPError

from airflow import __version__ as airflow_version
from airflow.configuration import conf
from airflow.providers.common.compat.sdk import timezone
from airflow.providers.edge3 import __version__ as edge_provider_version
from airflow.providers.edge3.cli.api_client import (
    jobs_fetch,
    jobs_set_state,
    logs_push,
    worker_register,
    worker_set_state,
)
from airflow.providers.edge3.cli.dataclasses import Job, MaintenanceMarker, WorkerStatus
from airflow.providers.edge3.cli.signalling import (
    SIG_STATUS,
    maintenance_marker_file_path,
    status_file_path,
    write_pid_to_pidfile,
)
from airflow.providers.edge3.models.edge_worker import (
    EdgeWorkerDuplicateException,
    EdgeWorkerState,
    EdgeWorkerVersionException,
)
from airflow.utils.net import getfqdn
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.providers.edge3.worker_api.datamodels import EdgeJobFetched

logger = logging.getLogger(__name__)

if sys.platform == "darwin":
    setproctitle = lambda title: logger.debug("Mac OS detected, skipping setproctitle")
else:
    from setproctitle import setproctitle


def _edge_hostname() -> str:
    """Get the hostname of the edge worker that should be reported by tasks."""
    return os.environ.get("HOSTNAME", getfqdn())


class EdgeWorker:
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
    edge_instance: EdgeWorker | None = None
    """Singleton instance of the worker."""

    def __init__(
        self,
        pid_file_path: str,
        hostname: str,
        queues: list[str] | None,
        concurrency: int,
        job_poll_interval: int,
        heartbeat_interval: int,
        daemon: bool = False,
    ):
        self.pid_file_path = pid_file_path
        self.job_poll_interval = job_poll_interval
        self.hb_interval = heartbeat_interval
        self.hostname = hostname
        self.queues = queues
        self.concurrency = concurrency
        self.free_concurrency = concurrency
        self.daemon = daemon

        EdgeWorker.edge_instance = self

    @staticmethod
    def signal_handler(sig: signal.Signals, frame):
        if sig == SIG_STATUS:
            marker_path = Path(maintenance_marker_file_path(None))
            if marker_path.exists():
                request = MaintenanceMarker.from_json(marker_path.read_text())
                logger.info("Requested to set maintenance mode to %s", request.maintenance)
                EdgeWorker.maintenance_mode = request.maintenance == "on"
                if EdgeWorker.maintenance_mode and request.comments:
                    logger.info("Comments: %s", request.comments)
                    EdgeWorker.maintenance_comments = request.comments
                marker_path.unlink()
                # send heartbeat immediately to update state
                if EdgeWorker.edge_instance:
                    EdgeWorker.edge_instance.heartbeat(EdgeWorker.maintenance_comments)
            else:
                logger.info("Request to get status of Edge Worker received.")
            status_path = Path(status_file_path(None))
            status_path.write_text(
                WorkerStatus(
                    job_count=len(EdgeWorker.jobs),
                    jobs=[job.edge_job.key for job in EdgeWorker.jobs],
                    state=EdgeWorker._get_state(),
                    maintenance=EdgeWorker.maintenance_mode,
                    maintenance_comments=EdgeWorker.maintenance_comments,
                    drain=EdgeWorker.drain,
                ).json
            )
        else:
            logger.info("Request to shut down Edge Worker received, waiting for jobs to complete.")
            EdgeWorker.drain = True

    def shutdown_handler(self, sig, frame):
        logger.info("SIGTERM received. Terminating all jobs and quit")
        for job in EdgeWorker.jobs:
            os.killpg(job.process.pid, signal.SIGTERM)
        EdgeWorker.drain = True

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
        if EdgeWorker.jobs:
            if EdgeWorker.drain:
                return EdgeWorkerState.TERMINATING
            if EdgeWorker.maintenance_mode:
                return EdgeWorkerState.MAINTENANCE_PENDING
            return EdgeWorkerState.RUNNING

        if EdgeWorker.drain:
            if EdgeWorker.maintenance_mode:
                return EdgeWorkerState.OFFLINE_MAINTENANCE
            return EdgeWorkerState.OFFLINE

        if EdgeWorker.maintenance_mode:
            return EdgeWorkerState.MAINTENANCE_MODE
        return EdgeWorkerState.IDLE

    @staticmethod
    @cache
    def _execution_api_server_url() -> str:
        """Get the execution api server url from config or environment."""
        api_url = conf.get("edge", "api_url")
        execution_api_server_url = conf.get("core", "execution_api_server_url", fallback="")
        if not execution_api_server_url and api_url:
            # Derive execution api url from edge api url as fallback
            execution_api_server_url = api_url.replace("edge_worker/v1/rpcapi", "execution")
        logger.info("Using execution api server url: %s", execution_api_server_url)
        return execution_api_server_url

    @staticmethod
    def _run_job_via_supervisor(workload, execution_api_server_url) -> int:
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
                server=execution_api_server_url,
                log_path=workload.log_path,
            )
            return 0
        except Exception as e:
            logger.exception("Task execution failed: %s", e)
            return 1

    @staticmethod
    def _launch_job(edge_job: EdgeJobFetched):
        if TYPE_CHECKING:
            from airflow.executors.workloads import ExecuteTask

        workload: ExecuteTask = edge_job.command
        process = Process(
            target=EdgeWorker._run_job_via_supervisor,
            kwargs={"workload": workload, "execution_api_server_url": EdgeWorker._execution_api_server_url()},
        )
        process.start()
        base_log_folder = conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
        if TYPE_CHECKING:
            assert workload.log_path  # We need to assume this is defined in here
        logfile = Path(base_log_folder, workload.log_path)
        EdgeWorker.jobs.append(Job(edge_job, process, logfile, 0))

    def start(self):
        """Start the execution in a loop until terminated."""
        try:
            self.last_hb = worker_register(
                self.hostname, EdgeWorkerState.STARTING, self.queues, self._get_sysinfo()
            ).last_update
        except EdgeWorkerVersionException as e:
            logger.info("Version mismatch of Edge worker and Core. Shutting down worker.")
            raise SystemExit(str(e))
        except EdgeWorkerDuplicateException as e:
            logger.error(str(e))
            raise SystemExit(str(e))
        except HTTPError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                raise SystemExit("Error: API endpoint is not ready, please set [edge] api_enabled=True.")
            raise SystemExit(str(e))
        if not self.daemon:
            write_pid_to_pidfile(self.pid_file_path)
        signal.signal(signal.SIGINT, EdgeWorker.signal_handler)
        signal.signal(SIG_STATUS, EdgeWorker.signal_handler)
        signal.signal(signal.SIGTERM, self.shutdown_handler)
        os.environ["HOSTNAME"] = self.hostname
        os.environ["AIRFLOW__CORE__HOSTNAME_CALLABLE"] = f"{_edge_hostname.__module__}._edge_hostname"
        try:
            self.worker_state_changed = self.heartbeat()
            self.last_hb = datetime.now()
            while not EdgeWorker.drain or EdgeWorker.jobs:
                self.loop()

            logger.info("Quitting worker, signal being offline.")
            try:
                worker_set_state(
                    self.hostname,
                    EdgeWorkerState.OFFLINE_MAINTENANCE
                    if EdgeWorker.maintenance_mode
                    else EdgeWorkerState.OFFLINE,
                    0,
                    self.queues,
                    self._get_sysinfo(),
                )
            except EdgeWorkerVersionException:
                logger.info("Version mismatch of Edge worker and Core. Quitting worker anyway.")
        finally:
            if not self.daemon:
                remove_existing_pidfile(self.pid_file_path)

    def loop(self):
        """Run a loop of scheduling and monitoring tasks."""
        new_job = False
        previous_jobs = EdgeWorker.jobs
        if not any((EdgeWorker.drain, EdgeWorker.maintenance_mode)) and self.free_concurrency > 0:
            new_job = self.fetch_job()
        self.check_running_jobs()

        if (
            EdgeWorker.drain
            or datetime.now().timestamp() - self.last_hb.timestamp() > self.hb_interval
            or self.worker_state_changed  # send heartbeat immediately if the state is different in db
            or bool(previous_jobs) != bool(EdgeWorker.jobs)  # when number of jobs changes from/to 0
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
            EdgeWorker._launch_job(edge_job)
            jobs_set_state(edge_job.key, TaskInstanceState.RUNNING)
            return True

        logger.info(
            "No new job to process%s",
            f", {len(EdgeWorker.jobs)} still running" if EdgeWorker.jobs else "",
        )
        return False

    def check_running_jobs(self) -> None:
        """Check which of the running tasks/jobs are completed and report back."""
        used_concurrency = 0
        for i in range(len(EdgeWorker.jobs) - 1, -1, -1):
            job = EdgeWorker.jobs[i]
            if not job.is_running:
                EdgeWorker.jobs.remove(job)
                if job.is_success:
                    logger.info("Job completed: %s", job.edge_job)
                    jobs_set_state(job.edge_job.key, TaskInstanceState.SUCCESS)
                else:
                    logger.error("Job failed: %s", job.edge_job)
                    jobs_set_state(job.edge_job.key, TaskInstanceState.FAILED)
            else:
                used_concurrency += job.edge_job.concurrency_slots

            if (
                conf.getboolean("edge", "push_logs")
                and job.logfile.exists()
                and job.logfile.stat().st_size > job.logsize
            ):
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
        state = EdgeWorker._get_state()
        sysinfo = self._get_sysinfo()
        worker_state_changed: bool = False
        try:
            worker_info = worker_set_state(
                self.hostname,
                state,
                len(EdgeWorker.jobs),
                self.queues,
                sysinfo,
                new_maintenance_comments,
            )
            self.queues = worker_info.queues
            if worker_info.state == EdgeWorkerState.MAINTENANCE_REQUEST:
                logger.info("Maintenance mode requested!")
                EdgeWorker.maintenance_mode = True
            elif (
                worker_info.state in [EdgeWorkerState.IDLE, EdgeWorkerState.RUNNING]
                and EdgeWorker.maintenance_mode
            ):
                logger.info("Exit Maintenance mode requested!")
                EdgeWorker.maintenance_mode = False
            if EdgeWorker.maintenance_mode:
                EdgeWorker.maintenance_comments = worker_info.maintenance_comments
            else:
                EdgeWorker.maintenance_comments = None
            if worker_info.state == EdgeWorkerState.SHUTDOWN_REQUEST:
                logger.info("Shutdown requested!")
                EdgeWorker.drain = True

            worker_state_changed = worker_info.state != state
        except EdgeWorkerVersionException:
            logger.info("Version mismatch of Edge worker and Core. Shutting down worker.")
            EdgeWorker.drain = True
        return worker_state_changed

    def interruptible_sleep(self):
        """Sleeps but stops sleeping if drain is made."""
        drain_before_sleep = EdgeWorker.drain
        for _ in range(0, self.job_poll_interval * 10):
            sleep(0.1)
            if drain_before_sleep != EdgeWorker.drain:
                return
