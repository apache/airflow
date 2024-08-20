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

from airflow import __version__ as airflow_version
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.cli.cli_config import ARG_VERBOSE, ActionCommand, Arg
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.remote import __version__ as remote_provider_version
from airflow.providers.remote.models.remote_job import RemoteJob
from airflow.providers.remote.models.remote_logs import RemoteLogs
from airflow.providers.remote.models.remote_worker import RemoteWorker, RemoteWorkerState
from airflow.utils import cli as cli_utils
from airflow.utils.state import TaskInstanceState

logger = logging.getLogger(__name__)


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


@dataclass
class Job:
    """Holds all information for a task/job to be executed as bundle."""

    remote_job: RemoteJob
    process: Popen
    logfile: Path
    logsize: int
    """Last size of log file, point of last chunk push."""


def _get_sysinfo() -> dict:
    """Produce the sysinfo from worker to post to central site."""
    return {
        "airflow_version": airflow_version,
        "remote_provider_version": remote_provider_version,
    }


def _fetch_job(hostname: str, queues: list[str] | None, jobs: list[Job]) -> bool:
    """Fetch and start a new job from central site."""
    logger.debug("Attempting to fetch a new job...")
    remote_job = RemoteJob.reserve_task(hostname, queues)
    if remote_job:
        logger.info("Received job: %s", remote_job)
        env = os.environ.copy()
        env["AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION"] = "True"
        env["AIRFLOW__CORE__INTERNAL_API_URL"] = conf.get("remote", "api_url")
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


def _heartbeat(hostname: str, jobs: list[Job], drain_worker: bool) -> None:
    """Report liveness state of worker to central site with stats."""
    state = (
        (RemoteWorkerState.TERMINATING if drain_worker else RemoteWorkerState.RUNNING)
        if jobs
        else RemoteWorkerState.IDLE
    )
    sysinfo = _get_sysinfo()
    RemoteWorker.set_state(hostname, state, len(jobs), sysinfo)


@cli_utils.action_cli(check_db=False)
def worker(args):
    """Start Airflow Remote worker."""
    job_poll_interval = conf.getint("remote", "job_poll_interval")
    heartbeat_interval = conf.getint("remote", "heartbeat_interval")

    hostname: str = args.remote_hostname or _hostname()
    queues: list[str] | None = args.queues.split(",") if args.queues else None
    concurrency: int = args.concurrency
    jobs: list[Job] = []
    new_job = False
    try:
        last_heartbeat = RemoteWorker.register_worker(
            hostname, RemoteWorkerState.STARTING, queues, _get_sysinfo()
        ).last_update
    except AirflowException as e:
        if "404:NOT FOUND" in str(e):
            raise SystemExit("Error: API endpoint is not ready, please set [remote] api_enabled=True.")
        raise SystemExit(str(e))

    drain_worker = [False]

    def signal_handler(sig, frame):
        logger.info("Request to show down remote worker received, waiting for jobs to complete.")
        drain_worker[0] = True

    signal.signal(signal.SIGINT, signal_handler)

    while not drain_worker[0] or jobs:
        if not drain_worker[0] and len(jobs) < concurrency:
            new_job = _fetch_job(hostname, queues, jobs)
        _check_running_jobs(jobs)

        if drain_worker[0] or datetime.now().timestamp() - last_heartbeat.timestamp() > heartbeat_interval:
            _heartbeat(hostname, jobs, drain_worker[0])
            last_heartbeat = datetime.now()

        if not new_job:
            sleep(job_poll_interval)
            new_job = False

    logger.info("Quitting worker, signal being offline.")
    RemoteWorker.set_state(hostname, RemoteWorkerState.OFFLINE, 0, _get_sysinfo())


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
REMOTE_COMMANDS: list[ActionCommand] = [
    ActionCommand(
        name=worker.__name__,
        help=worker.__doc__,
        func=worker,
        args=(
            ARG_CONCURRENCY,
            ARG_QUEUES,
            ARG_REMOTE_HOSTNAME,
            ARG_VERBOSE,
        ),
    ),
]
