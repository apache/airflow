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

from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.jobs.job import Job
from airflow.utils.net import get_hostname
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import JobState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@providers_configuration_loaded
@provide_session
def check(args, session: Session = NEW_SESSION) -> None:
    """Check if job(s) are still alive."""
    if args.allow_multiple and args.limit <= 1:
        raise SystemExit("To use option --allow-multiple, you must set the limit to a value greater than 1.")
    if args.hostname and args.local:
        raise SystemExit("You can't use --hostname and --local at the same time")

    query = select(Job).where(Job.state == JobState.RUNNING).order_by(Job.latest_heartbeat.desc())
    if args.job_type:
        query = query.where(Job.job_type == args.job_type)
    if args.hostname:
        query = query.where(Job.hostname == args.hostname)
    if args.local:
        query = query.where(Job.hostname == get_hostname())
    if args.limit > 0:
        query = query.limit(args.limit)

    alive_jobs: list[Job] = [job for job in session.scalars(query) if job.is_alive()]

    count_alive_jobs = len(alive_jobs)
    if count_alive_jobs == 0:
        raise SystemExit("No alive jobs found.")
    if count_alive_jobs > 1 and not args.allow_multiple:
        raise SystemExit(f"Found {count_alive_jobs} alive jobs. Expected only one.")
    if count_alive_jobs == 1:
        print("Found one alive job.")
    else:
        print(f"Found {count_alive_jobs} alive jobs.")
