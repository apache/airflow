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
"""Health command"""
import sys

from airflow.jobs.scheduler_job import SchedulerJob
from airflow.utils.net import get_hostname


def scheduler_health(args):
    """Check Airflow scheduler health at the command line"""
    exit_code = 1
    last_scheduler_heartbeat = None
    last_scheduler_host = None
    scheduler_job = SchedulerJob.most_recent_job()
    if scheduler_job:
        last_scheduler_heartbeat = scheduler_job.latest_heartbeat.isoformat()
        last_scheduler_host = scheduler_job.hostname
        if scheduler_job.is_alive() and args.validate_host and last_scheduler_host == get_hostname():
            exit_code = 0
        elif scheduler_job.is_alive() and not args.validate_host:
            exit_code = 0
    if exit_code == 0:
        if not args.silent:
            print('Scheduler healthy. Last heartbeat: {}'.format(last_scheduler_heartbeat))
    else:
        print(
            "Scheduler not healthy. Last reported scheduler host : {} , "
            "Last heartbeat : {} , "
            "Current Hostname : {}"
            .format(
                last_scheduler_host,
                last_scheduler_heartbeat,
                get_hostname()
            )
        )
    sys.exit(exit_code)
