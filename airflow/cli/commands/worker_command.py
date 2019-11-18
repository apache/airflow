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
"""Worker command"""
import os
import signal
import subprocess
import sys

import daemon
from daemon.pidfile import TimeoutPIDLockFile

from airflow import settings
from airflow.configuration import conf
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging, sigint_handler


@cli_utils.action_logging
def worker(args):
    """Starts Airflow Celery worker"""
    env = os.environ.copy()
    env['AIRFLOW_HOME'] = settings.AIRFLOW_HOME

    if not settings.validate_session():
        print("Worker exiting... database connection precheck failed! ")
        sys.exit(1)

    # Celery worker
    from airflow.executors.celery_executor import app as celery_app
    from celery.bin import worker  # pylint: disable=redefined-outer-name

    autoscale = args.autoscale
    if autoscale is None and conf.has_option("celery", "worker_autoscale"):
        autoscale = conf.get("celery", "worker_autoscale")
    worker = worker.worker(app=celery_app)   # pylint: disable=redefined-outer-name
    options = {
        'optimization': 'fair',
        'O': 'fair',
        'queues': args.queues,
        'concurrency': args.concurrency,
        'autoscale': autoscale,
        'hostname': args.celery_hostname,
        'loglevel': conf.get('core', 'LOGGING_LEVEL'),
    }

    if conf.has_option("celery", "pool"):
        options["pool"] = conf.get("celery", "pool")

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("worker",
                                                        args.pid,
                                                        args.stdout,
                                                        args.stderr,
                                                        args.log_file)
        handle = setup_logging(log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            files_preserve=[handle],
            stdout=stdout,
            stderr=stderr,
        )
        with ctx:
            sub_proc = subprocess.Popen(['airflow', 'serve_logs'], env=env, close_fds=True)
            worker.run(**options)
            sub_proc.kill()

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        sub_proc = subprocess.Popen(['airflow', 'serve_logs'], env=env, close_fds=True)

        worker.run(**options)
        sub_proc.kill()
