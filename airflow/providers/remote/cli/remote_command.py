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
import subprocess
from time import sleep

from airflow.cli.cli_config import ActionCommand
from airflow.providers.remote.models.remote_job import RemoteJob
from airflow.utils import cli as cli_utils
from airflow.utils.state import TaskInstanceState

logger = logging.getLogger(__name__)


def _hostname() -> str:
    if platform.system() == "Windows":
        return platform.uname().node
    else:
        return os.uname()[1]


@cli_utils.action_cli
def worker(args):
    """Start Airflow Remote worker."""
    while True:  # TODO implement handler for SIGINT
        logger.debug("Attempting to fetch a new job...")
        job = RemoteJob.reserve_task(_hostname())
        if not job:
            logger.info("No job to process")
            sleep(5)  # TODO make sleep flexible if something is to be done
            continue

        logger.info("Received job: %s", job)
        RemoteJob.set_state(job.key, TaskInstanceState.RUNNING)
        try:
            subprocess.check_call(job.command, close_fds=True)
            RemoteJob.set_state(job.key, TaskInstanceState.SUCCESS)
        except subprocess.CalledProcessError as e:
            logger.error("Failed to execute task %s.", e)
            RemoteJob.set_state(job.key, TaskInstanceState.FAILED)


REMOTE_COMMANDS: list[ActionCommand] = [
    ActionCommand(
        name=worker.__name__,
        help=worker.__doc__,
        func=worker,
        args=(),
    ),
]
