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

from typing import List

import rich_click as click

from airflow.cli import airflow_cmd
from airflow.jobs.base_job import BaseJob
from airflow.utils.session import provide_session
from airflow.utils.state import State


@airflow_cmd.group("jobs")
def jobs():
    """Manage jobs"""


@jobs.command("check")
@click.option(
    "--job-type",
    type=click.Choice({"BackfillJob", "LocalTaskJob", "SchedulerJob", "TriggererJob"}),
    help="The type of job(s) that will be checked. By default all job types are checked.",
)
@click.option(
    "--hostname", metavar="HOSTNAME", default=None, help="The hostname of job(s) that will be checked."
)
@click.option(
    "--limit",
    default=1,
    type=click.IntRange(min=0, max=None),
    help="The number of recent jobs that will be checked. To disable limit, set 0.",
)
@click.option(
    "--allow-multiple",
    is_flag=True,
    default=False,
    help="If passed, this command will be successful even if multiple matching alive jobs are found.",
)
@provide_session
def check(job_type: str, hostname: str, limit: int, allow_multiple: bool, session=None):
    """Checks if job(s) are still alive

    \b
    examples:
    To check if the local scheduler is still working properly, run:
    \b
        $ airflow jobs check --job-type SchedulerJob --hostname "$(hostname)"
    \b
    To check if any scheduler is running when you are using high availability, run:
    \b
        $ airflow jobs check --job-type SchedulerJob --allow-multiple --limit 100
    """
    if allow_multiple and not limit > 1:
        raise SystemExit("To use option --allow-multiple, you must set the limit to a value greater than 1.")
    query = (
        session.query(BaseJob)
        .filter(BaseJob.state == State.RUNNING)
        .order_by(BaseJob.latest_heartbeat.desc())
    )
    if job_type:
        query = query.filter(BaseJob.job_type == job_type)
    if hostname:
        query = query.filter(BaseJob.hostname == hostname)
    if limit > 0:
        query = query.limit(limit)

    jobs: List[BaseJob] = query.all()
    alive_jobs = [job for job in jobs if job.is_alive()]

    count_alive_jobs = len(alive_jobs)
    if count_alive_jobs == 0:
        raise SystemExit("No alive jobs found.")
    if count_alive_jobs > 1 and not allow_multiple:
        raise SystemExit(f"Found {count_alive_jobs} alive jobs. Expected only one.")
    if count_alive_jobs == 1:
        print("Found one alive job.")
    else:
        print(f"Found {count_alive_jobs} alive jobs.")
