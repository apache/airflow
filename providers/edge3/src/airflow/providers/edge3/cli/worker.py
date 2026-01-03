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
import traceback
from asyncio import Task, create_task, get_running_loop, run, sleep
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import cache
from http import HTTPStatus
from multiprocessing import Process, Queue
from pathlib import Path
from typing import TYPE_CHECKING

from aiofiles import open as aio_open
from aiohttp import ClientResponseError
from lockfile.pidlockfile import remove_existing_pidfile

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
    from airflow.executors.workloads import ExecuteTask

logger = logging.getLogger(__name__)
base_log_folder = conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
push_logs = conf.getboolean("edge", "push_logs")
push_log_chunk_size = conf.getint("edge", "push_log_chunk_size")

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
    drain: bool = False
    """Flag if job processing should be completed and no new jobs fetched for a graceful stop/shutdown."""
    maintenance_mode: bool = False
    """Flag if job processing should be completed and no new jobs fetched for maintenance mode. """
    maintenance_comments: str | None = None
    """Comments for maintenance mode."""
    background_tasks: set[Task] = set()

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
        self.thread_pool = ThreadPoolExecutor(max_workers=concurrency)
        self.daemon = daemon

    @property
    def free_concurrency(self) -> int:
        """Calculate the free concurrency of the worker."""
        used_concurrency = sum(job.edge_job.concurrency_slots for job in self.jobs)
        return self.concurrency - used_concurrency

    def signal_status(self):
        marker_path = Path(maintenance_marker_file_path(None))
        if marker_path.exists():
            request = MaintenanceMarker.from_json(marker_path.read_text())
            logger.info("Requested to set maintenance mode to %s", request.maintenance)
            self.maintenance_mode = request.maintenance == "on"
            if self.maintenance_mode and request.comments:
                logger.info("Comments: %s", request.comments)
                self.maintenance_comments = request.comments
            marker_path.unlink()
            run(self.heartbeat(self.maintenance_comments))  # send heartbeat immediately to update state
        else:
            logger.info("Request to get status of Edge Worker received.")
        status_path = Path(status_file_path(None))
        status_path.write_text(
            WorkerStatus(
                job_count=len(self.jobs),
                jobs=[job.edge_job.key for job in self.jobs],
                state=self._get_state(),
                maintenance=self.maintenance_mode,
                maintenance_comments=self.maintenance_comments,
                drain=self.drain,
            ).json
        )

    def signal_drain(self):
        self.drain = True
        logger.info("Request to shut down Edge Worker received, waiting for jobs to complete.")

    def shutdown_handler(self):
        self.drain = True
        msg = "SIGTERM received. Terminating all jobs and quit"
        logger.info(msg)
        for job in self.jobs:
            if job.process.pid:
                os.killpg(job.process.pid, signal.SIGTERM)

    def _get_sysinfo(self) -> dict:
        """Produce the sysinfo from worker to post to central site."""
        return {
            "airflow_version": airflow_version,
            "edge_provider_version": edge_provider_version,
            "concurrency": self.concurrency,
            "free_concurrency": self.free_concurrency,
        }

    def _get_state(self) -> EdgeWorkerState:
        """State of the Edge Worker."""
        if self.jobs:
            if self.drain:
                return EdgeWorkerState.TERMINATING
            if self.maintenance_mode:
                return EdgeWorkerState.MAINTENANCE_PENDING
            return EdgeWorkerState.RUNNING

        if self.drain:
            if self.maintenance_mode:
                return EdgeWorkerState.OFFLINE_MAINTENANCE
            return EdgeWorkerState.OFFLINE

        if self.maintenance_mode:
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
    def _run_job_via_supervisor(workload, execution_api_server_url, results_queue: Queue) -> int:
        from airflow.sdk.execution_time.supervisor import supervise

        # Ignore ctrl-c in this process -- we don't want to kill _this_ one. we let tasks run to completion
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        # TODO Check also:
        # /opt/airflow/task-sdk/src/airflow/sdk/execution_time/supervisor.py:480 DeprecationWarning: This process (pid=372) is multi-threaded, use of fork() may lead to deadlocks in the child.

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
            logger.exception("Task execution failed")
            results_queue.put(e)
            return 1

    def _launch_job(self, workload: ExecuteTask) -> tuple[Process, Queue[Exception]]:
        # Improvement: Use frozen GC to prevent child process from copying unnecessary memory
        # See _spawn_workers_with_gc_freeze() in airflow-core/src/airflow/executors/local_executor.py
        results_queue: Queue[Exception] = Queue()
        process = Process(
            target=EdgeWorker._run_job_via_supervisor,
            kwargs={
                "workload": workload,
                "execution_api_server_url": EdgeWorker._execution_api_server_url(),
                "results_queue": results_queue,
            },
        )
        process.start()
        return process, results_queue

    async def _push_logs_in_chunks(self, job: Job):
        if push_logs and job.logfile.exists() and job.logfile.stat().st_size > job.logsize:
            async with aio_open(job.logfile, mode="rb") as logf:
                await logf.seek(job.logsize, os.SEEK_SET)
                read_data = await logf.read()
                job.logsize += len(read_data)
                # backslashreplace to keep not decoded characters and not raising exception
                # replace null with question mark to fix issue during DB push
                log_data = read_data.decode(errors="backslashreplace").replace("\x00", "\ufffd")
                while True:
                    chunk_data = log_data[:push_log_chunk_size]
                    log_data = log_data[push_log_chunk_size:]
                    if not chunk_data:
                        break

                    await logs_push(
                        task=job.edge_job.key,
                        log_chunk_time=timezone.utcnow(),
                        log_chunk_data=chunk_data,
                    )

    async def start(self):
        """Start the execution in a loop until terminated."""
        try:
            await worker_register(self.hostname, EdgeWorkerState.STARTING, self.queues, self._get_sysinfo())
        except EdgeWorkerVersionException as e:
            logger.info("Version mismatch of Edge worker and Core. Shutting down worker.")
            raise SystemExit(str(e))
        except EdgeWorkerDuplicateException as e:
            logger.error(str(e))
            raise SystemExit(str(e))
        except ClientResponseError as e:
            if e.status == HTTPStatus.NOT_FOUND:
                raise SystemExit(
                    "Error: API endpoint is not ready, please set [edge] api_enabled=True. Or check if the URL is correct to your deployment."
                )
            raise SystemExit(str(e))
        if not self.daemon:
            write_pid_to_pidfile(self.pid_file_path)
        loop = get_running_loop()
        loop.add_signal_handler(signal.SIGINT, self.signal_drain)
        loop.add_signal_handler(SIG_STATUS, self.signal_status)
        loop.add_signal_handler(signal.SIGTERM, self.shutdown_handler)
        os.environ["HOSTNAME"] = self.hostname
        os.environ["AIRFLOW__CORE__HOSTNAME_CALLABLE"] = f"{_edge_hostname.__module__}._edge_hostname"
        try:
            await self.loop()

            logger.info("Quitting worker, signal being offline.")
            try:
                await worker_set_state(
                    self.hostname,
                    EdgeWorkerState.OFFLINE_MAINTENANCE if self.maintenance_mode else EdgeWorkerState.OFFLINE,
                    0,
                    self.queues,
                    self._get_sysinfo(),
                )
            except EdgeWorkerVersionException:
                logger.info("Version mismatch of Edge worker and Core. Quitting worker anyway.")
        finally:
            if not self.daemon:
                remove_existing_pidfile(self.pid_file_path)

    async def loop(self):
        """Run a loop of scheduling and monitoring tasks."""
        last_hb = datetime.now()
        worker_state_changed = True  # force heartbeat at start
        previous_jobs = 0
        while not self.drain or self.jobs:
            if (
                self.drain
                or datetime.now().timestamp() - last_hb.timestamp() > self.hb_interval
                or worker_state_changed  # send heartbeat immediately if the state is different in db
                or previous_jobs != len(self.jobs)  # when number of jobs changes
            ):
                worker_state_changed = await self.heartbeat()
                last_hb = datetime.now()
                previous_jobs = len(self.jobs)

            if self.maintenance_mode:
                logger.info("in maintenance mode%s", f", {len(self.jobs)} draining jobs" if self.jobs else "")
            elif not self.drain and self.free_concurrency > 0:
                task = create_task(self.fetch_and_run_job())
                self.background_tasks.add(task)
                task.add_done_callback(self.background_tasks.discard)
            else:
                logger.info("%i %s running", len(self.jobs), "job is" if len(self.jobs) == 1 else "jobs are")

            await self.interruptible_sleep()

    async def fetch_and_run_job(self) -> None:
        """Fetch, start and monitor a new job."""
        logger.debug("Attempting to fetch a new job...")
        edge_job = await jobs_fetch(self.hostname, self.queues, self.free_concurrency)
        if not edge_job:
            logger.info(
                "No new job to process%s",
                f", {len(self.jobs)} still running" if self.jobs else "",
            )
            return

        logger.info("Received job: %s", edge_job.identifier)

        workload: ExecuteTask = edge_job.command
        process, results_queue = self._launch_job(workload)
        if TYPE_CHECKING:
            assert workload.log_path  # We need to assume this is defined in here
        logfile = Path(base_log_folder, workload.log_path)
        job = Job(edge_job, process, logfile)
        self.jobs.append(job)
        await jobs_set_state(edge_job.key, TaskInstanceState.RUNNING)

        # As we got one job, directly fetch another one if possible
        if self.free_concurrency > 0:
            task = create_task(self.fetch_and_run_job())
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)

        while job.is_running:
            await self._push_logs_in_chunks(job)
            for _ in range(0, self.job_poll_interval * 10):
                await sleep(0.1)
                if not job.is_running:
                    break
        await self._push_logs_in_chunks(job)

        self.jobs.remove(job)
        if job.is_success:
            logger.info("Job completed: %s", job.edge_job.identifier)
            await jobs_set_state(job.edge_job.key, TaskInstanceState.SUCCESS)
        else:
            if results_queue.empty():
                ex_txt = "(Unknown error, no exception details available)"
            else:
                ex = results_queue.get()
                ex_txt = "\n".join(traceback.format_exception(ex))
            logger.error("Job failed: %s with:\n%s", job.edge_job.identifier, ex_txt)
            # Push it upwards to logs for better diagnostic as well
            await logs_push(
                task=job.edge_job.key,
                log_chunk_time=timezone.utcnow(),
                log_chunk_data=f"Error starting job:\n{ex_txt}",
            )
            await jobs_set_state(job.edge_job.key, TaskInstanceState.FAILED)

    async def heartbeat(self, new_maintenance_comments: str | None = None) -> bool:
        """Report liveness state of worker to central site with stats."""
        state = self._get_state()
        sysinfo = self._get_sysinfo()
        worker_state_changed: bool = False
        try:
            worker_info = await worker_set_state(
                self.hostname,
                state,
                len(self.jobs),
                self.queues,
                sysinfo,
                new_maintenance_comments,
            )
            self.queues = worker_info.queues
            if worker_info.state == EdgeWorkerState.MAINTENANCE_REQUEST:
                logger.info("Maintenance mode requested!")
                self.maintenance_mode = True
            elif (
                worker_info.state in [EdgeWorkerState.IDLE, EdgeWorkerState.RUNNING] and self.maintenance_mode
            ):
                logger.info("Exit Maintenance mode requested!")
                self.maintenance_mode = False
            if self.maintenance_mode:
                self.maintenance_comments = worker_info.maintenance_comments
            else:
                self.maintenance_comments = None
            if worker_info.state == EdgeWorkerState.SHUTDOWN_REQUEST:
                logger.info("Shutdown requested!")
                self.drain = True

            worker_state_changed = worker_info.state != state
        except EdgeWorkerVersionException:
            logger.info("Version mismatch of Edge worker and Core. Shutting down worker.")
            self.drain = True
        return worker_state_changed

    async def interruptible_sleep(self):
        """Sleeps but stops sleeping if drain is made or some job completed."""
        drain_before_sleep = self.drain
        jobcount_before_sleep = len(self.jobs)
        for _ in range(0, self.job_poll_interval * 10):
            await sleep(0.1)
            if drain_before_sleep != self.drain or len(self.jobs) < jobcount_before_sleep:
                return
