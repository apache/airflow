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

"""Triggerer command"""
import signal

import daemon
from daemon.pidfile import TimeoutPIDLockFile

from airflow import settings
from airflow.jobs.triggerer_job import TriggererJob
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging, sigquit_handler


@cli_utils.action_cli
def triggerer(args):
    """Starts Airflow Triggerer"""
    settings.MASK_SECRETS_IN_LOGS = True
    print(settings.HEADER)
    job = TriggererJob(capacity=args.capacity)

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations(
            "triggerer", args.pid, args.stdout, args.stderr, args.log_file
        )
        handle = setup_logging(log_file)
        with open(stdout, 'w+') as stdout_handle, open(stderr, 'w+') as stderr_handle:
            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pid, -1),
                files_preserve=[handle],
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
            with ctx:
                job.run()

    else:
        # There is a bug in CPython (fixed in March 2022 but not yet released) that
        # makes async.io handle SIGTERM improperly by using async unsafe
        # functions and hanging the triggerer receive SIGPIPE while handling
        # SIGTERN/SIGINT and deadlocking itself. Until the bug is handled
        # we should rather rely on standard handling of the signals rather than
        # adding our own signal handlers. Seems that even if our signal handler
        # just run exit(0) - it caused a race condition that led to the hanging.
        #
        # More details:
        #   * https://bugs.python.org/issue39622
        #   * https://github.com/python/cpython/issues/83803
        #
        # signal.signal(signal.SIGINT, sigint_handler)
        # signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        job.run()
