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
from datetime import datetime
from subprocess import Popen
from time import sleep

from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.cli.cli_config import ARG_VERBOSE, ActionCommand, Arg
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.remote.models.remote_job import RemoteJob
from airflow.providers.remote.models.remote_worker import RemoteWorker, RemoteWorkerState
from airflow.utils import cli as cli_utils
from airflow.utils.state import TaskInstanceState

logger = logging.getLogger(__name__)


def _hostname() -> str:
    if platform.system() == "Windows":
        return platform.uname().node
    else:
        return os.uname()[1]


@cli_utils.action_cli(check_db=False)
def worker(args):
    """Start Airflow Remote worker."""
    api_url = conf.get("remote", "api_url")
    if not api_url:
        raise SystemExit("Error: API URL is not configured, please correct configuration.")
    logger.info("Starting worker with API endpoint %s", api_url)
    InternalApiConfig.force_api_access(api_url)

    hostname: str = args.remote_hostname or _hostname()
    queues = args.queues.split(",") if args.queues else None
    concurrency: int = args.concurrency
    jobs: list[RemoteJob] = []
    processes: list[Popen] = []
    some_activity = False
    try:
        last_heartbeat = RemoteWorker.register_worker(
            hostname, RemoteWorkerState.STARTING, queues
        ).last_update
    except AirflowException:
        raise SystemExit("Error: API endpoint is not ready, please set [remote] api_enabled=True.")

    drain_worker = [False]

    def signal_handler(sig, frame):
        logger.info("Request to show down remote worker received, waiting for jobs to complete.")
        drain_worker[0] = True

    signal.signal(signal.SIGINT, signal_handler)

    while not drain_worker[0] or jobs:
        if not drain_worker[0] and len(jobs) < concurrency:
            logger.debug("Attempting to fetch a new job...")
            job = RemoteJob.reserve_task(hostname, queues)
            if job:
                some_activity = True
                logger.info("Received job: %s", job)
                process = Popen(job.command, close_fds=True)
                jobs.append(job)
                processes.append(process)
                RemoteJob.set_state(job.key, TaskInstanceState.RUNNING)
            else:
                some_activity = False
                logger.info("No new job to process%s", f", {len(jobs)} still running" if jobs else "")
        for i in range(len(jobs) - 1, -1, -1):
            process = processes[i]
            job = jobs[i]
            process.poll()
            if process.returncode is not None:
                processes.remove(process)
                jobs.remove(job)
                if process.returncode == 0:
                    logger.info("Job completed: %s", job)
                    RemoteJob.set_state(job.key, TaskInstanceState.SUCCESS)
                else:
                    logger.error("Job failed: %s", job)
                    RemoteJob.set_state(job.key, TaskInstanceState.FAILED)

        if drain_worker[0] or datetime.now().timestamp() - last_heartbeat.timestamp() > 10:
            state = (
                (RemoteWorkerState.TERMINATING if drain_worker[0] else RemoteWorkerState.RUNNING)
                if jobs
                else RemoteWorkerState.IDLE
            )
            RemoteWorker.set_state(hostname, state, len(jobs), {})
            last_heartbeat = datetime.now()

        if not some_activity:
            sleep(5)
            some_activity = False

    logger.info("Quitting worker, signal being offline.")
    RemoteWorker.set_state(hostname, RemoteWorkerState.OFFLINE, 0, {})


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
