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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from subprocess import Popen
from time import sleep

from requests.auth import HTTPBasicAuth

from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.cli.cli_config import ARG_VERBOSE, ActionCommand, Arg
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.remote.models.remote_job import RemoteJob
from airflow.providers.remote.models.remote_logs import RemoteLogs
from airflow.providers.remote.models.remote_worker import RemoteWorker, RemoteWorkerState
from airflow.utils import cli as cli_utils
from airflow.utils.state import TaskInstanceState

logger = logging.getLogger(__name__)


def _hostname() -> str:
    if platform.system() == "Windows":
        return platform.uname().node
    else:
        return os.uname()[1]


@dataclass
class Job:
    """Holds all information for a task/job to be executed as bundle."""

    remote_job: RemoteJob
    process: Popen
    logfile: Path
    logsize: int
    """Last size of log file, point of last chunk push."""


@dataclass
class WorkerState:
    """Holds all information about the state of the worker instance."""

    hostname: str
    queues: list[str] | None
    drain_worker: bool
    api_url: str
    user: str
    password: str


def _fetch_job(state: WorkerState, jobs: list[Job]) -> bool:
    """Fetch and start a new job from central site."""
    logger.debug("Attempting to fetch a new job...")
    remote_job = RemoteJob.reserve_task(state.hostname, state.queues)
    if remote_job:
        logger.info("Received job: %s", remote_job)
        env = os.environ.copy()
        env["AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION"] = "True"
        env["AIRFLOW__CORE__INTERNAL_API_URL"] = state.api_url
        env["AIRFLOW__CORE__INTERNAL_API_USER"] = state.user
        env["AIRFLOW__CORE__INTERNAL_API_PASSWORD"] = state.password
        process = Popen(remote_job.command, close_fds=True, env=env)
        logfile = RemoteLogs.logfile_path(
            remote_job.dag_id,
            remote_job.run_id,
            remote_job.task_id,
            remote_job.map_index,
            remote_job.try_number,
        )
        jobs.append(Job(remote_job, process, logfile, 0))
        RemoteJob.set_state(remote_job.key, TaskInstanceState.RUNNING)
        return True

    logger.info("No new job to process%s", f", {len(jobs)} still running" if jobs else "")
    return False


def _check_running_jobs(jobs: list[Job]) -> None:
    """Check which of the running tasks/jobs are completed and report back."""
    for i in range(len(jobs) - 1, -1, -1):
        job = jobs[i]
        job.process.poll()
        if job.process.returncode is not None:
            jobs.remove(job)
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
                    dag_id=job.remote_job.dag_id,
                    run_id=job.remote_job.run_id,
                    task_id=job.remote_job.task_id,
                    map_index=job.remote_job.map_index,
                    try_number=job.remote_job.try_number,
                    log_chunk_time=datetime.now(),
                    log_chunk_data=logdata,
                )
                job.logsize += len(logdata)


def _heartbeat(state: WorkerState, jobs: list[Job]) -> None:
    """Report liveness state of worker to central site with stats."""
    reported_state = (
        (RemoteWorkerState.TERMINATING if state.drain_worker else RemoteWorkerState.RUNNING)
        if jobs
        else RemoteWorkerState.IDLE
    )
    RemoteWorker.set_state(state.hostname, reported_state, len(jobs), {})


@cli_utils.action_cli(check_db=False)
def worker(args):
    """Start Airflow Remote worker."""
    worker_state = WorkerState(
        hostname=args.remote_hostname or _hostname(),
        queues=args.queues.split(",") if args.queues else None,
        drain_worker=False,
        api_url=conf.get("remote", "api_url"),
        user=args.user or conf.get("remote", "user"),
        password=args.password or conf.get("remote", "password"),
    )
    job_poll_interval = conf.getint("remote", "job_poll_interval")
    heartbeat_interval = conf.getint("remote", "heartbeat_interval")
    if not worker_state.api_url:
        raise SystemExit("Error: API URL is not configured, please correct configuration.")
    logger.info("Starting worker with API endpoint %s", worker_state.api_url)
    InternalApiConfig.force_api_access(
        worker_state.api_url, HTTPBasicAuth(worker_state.user, worker_state.password)
    )

    concurrency: int = args.concurrency
    jobs: list[Job] = []
    new_job = False
    try:
        last_heartbeat = RemoteWorker.register_worker(
            worker_state.hostname, RemoteWorkerState.STARTING, worker_state.queues
        ).last_update
    except AirflowException as e:
        if "403" in str(e):
            raise SystemExit(f"Error: API endpoint authentication failed, check your credentials: {e}")
        raise SystemExit("Error: API endpoint is not ready, please set [remote] api_enabled=True.")

    def signal_handler(sig, frame):
        logger.info("Request to show down remote worker received, waiting for jobs to complete.")
        worker_state.drain_worker = True

    signal.signal(signal.SIGINT, signal_handler)

    while not worker_state.drain_worker or jobs:
        if not worker_state.drain_worker and len(jobs) < concurrency:
            new_job = _fetch_job(worker_state, jobs)
        _check_running_jobs(jobs)

        if (
            worker_state.drain_worker
            or datetime.now().timestamp() - last_heartbeat.timestamp() > heartbeat_interval
        ):
            _heartbeat(worker_state, jobs)
            last_heartbeat = datetime.now()

        if not new_job:
            sleep(job_poll_interval)
            new_job = False

    logger.info("Quitting worker, signal being offline.")
    RemoteWorker.set_state(worker_state.hostname, RemoteWorkerState.OFFLINE, 0, {})


ARG_CONCURRENCY = Arg(
    ("-c", "--concurrency"),
    type=int,
    help="The number of worker processes",
    default=1,
)
ARG_QUEUES = Arg(
    ("-q", "--queues"),
    help="Comma delimited list of queues to serve, serve all queues if not provided.",
)
ARG_REMOTE_HOSTNAME = Arg(
    ("-H", "--remote-hostname"),
    help="Set the hostname of worker if you have multiple workers on a single machine",
)
ARG_REMOTE_USER = Arg(
    ("-u", "--user"),
    help="Username to authenticate against Remote Worker API endpoint",
)
ARG_REMOTE_PASSWORD = Arg(
    ("-p", "--password"),
    help="Password to authenticate against Remote Worker API endpoint",
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
            ARG_REMOTE_USER,
            ARG_REMOTE_PASSWORD,
            ARG_VERBOSE,
        ),
    ),
]
