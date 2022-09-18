#
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
"""Celery command"""
from __future__ import annotations

from multiprocessing import Process

import daemon
import psutil
import sqlalchemy.exc
from celery import maybe_patch_concurrency  # type: ignore[attr-defined]
from daemon.pidfile import TimeoutPIDLockFile
from lockfile.pidlockfile import read_pid_from_pidfile, remove_existing_pidfile

from airflow import settings
from airflow.configuration import conf
from airflow.executors.celery_executor import app as celery_app
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging
from airflow.utils.serve_logs import serve_logs

WORKER_PROCESS_NAME = "worker"


@cli_utils.action_cli
def flower(args):
    """Starts Flower, Celery monitoring tool"""
    options = [
        "flower",
        conf.get('celery', 'BROKER_URL'),
        f"--address={args.hostname}",
        f"--port={args.port}",
    ]

    if args.broker_api:
        options.append(f"--broker-api={args.broker_api}")

    if args.url_prefix:
        options.append(f"--url-prefix={args.url_prefix}")

    if args.basic_auth:
        options.append(f"--basic-auth={args.basic_auth}")

    if args.flower_conf:
        options.append(f"--conf={args.flower_conf}")

    if args.daemon:
        pidfile, stdout, stderr, _ = setup_locations(
            process="flower",
            pid=args.pid,
            stdout=args.stdout,
            stderr=args.stderr,
            log=args.log_file,
        )
        with open(stdout, "a") as stdout, open(stderr, "a") as stderr:
            stdout.truncate(0)
            stderr.truncate(0)

            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pidfile, -1),
                stdout=stdout,
                stderr=stderr,
                umask=int(settings.DAEMON_UMASK, 8),
            )
            with ctx:
                celery_app.start(options)
    else:
        celery_app.start(options)


def _serve_logs(skip_serve_logs: bool = False) -> Process | None:
    """Starts serve_logs sub-process"""
    if skip_serve_logs is False:
        sub_proc = Process(target=serve_logs)
        sub_proc.start()
        return sub_proc
    return None


def _run_worker(options, skip_serve_logs):
    sub_proc = _serve_logs(skip_serve_logs)
    try:
        celery_app.worker_main(options)
    finally:
        if sub_proc:
            sub_proc.terminate()


@cli_utils.action_cli
def worker(args):
    """Starts Airflow Celery worker"""
    # Disable connection pool so that celery worker does not hold an unnecessary db connection
    settings.reconfigure_orm(disable_connection_pool=True)
    if not settings.validate_session():
        raise SystemExit("Worker exiting, database connection precheck failed.")

    autoscale = args.autoscale
    skip_serve_logs = args.skip_serve_logs

    if autoscale is None and conf.has_option("celery", "worker_autoscale"):
        autoscale = conf.get("celery", "worker_autoscale")

    # Setup locations
    pid_file_path, stdout, stderr, log_file = setup_locations(
        process=WORKER_PROCESS_NAME,
        pid=args.pid,
        stdout=args.stdout,
        stderr=args.stderr,
        log=args.log_file,
    )

    if hasattr(celery_app.backend, 'ResultSession'):
        # Pre-create the database tables now, otherwise SQLA via Celery has a
        # race condition where one of the subprocesses can die with "Table
        # already exists" error, because SQLA checks for which tables exist,
        # then issues a CREATE TABLE, rather than doing CREATE TABLE IF NOT
        # EXISTS
        try:
            session = celery_app.backend.ResultSession()
            session.close()
        except sqlalchemy.exc.IntegrityError:
            # At least on postgres, trying to create a table that already exist
            # gives a unique constraint violation or the
            # "pg_type_typname_nsp_index" table. If this happens we can ignore
            # it, we raced to create the tables and lost.
            pass

    # backwards-compatible: https://github.com/apache/airflow/pull/21506#pullrequestreview-879893763
    celery_log_level = conf.get('logging', 'CELERY_LOGGING_LEVEL')
    if not celery_log_level:
        celery_log_level = conf.get('logging', 'LOGGING_LEVEL')
    # Setup Celery worker
    options = [
        'worker',
        '-O',
        'fair',
        '--queues',
        args.queues,
        '--concurrency',
        args.concurrency,
        '--hostname',
        args.celery_hostname,
        '--loglevel',
        celery_log_level,
        '--pidfile',
        pid_file_path,
    ]
    if autoscale:
        options.extend(['--autoscale', autoscale])
    if args.without_mingle:
        options.append('--without-mingle')
    if args.without_gossip:
        options.append('--without-gossip')

    if conf.has_option("celery", "pool"):
        pool = conf.get("celery", "pool")
        options.extend(["--pool", pool])
        # Celery pools of type eventlet and gevent use greenlets, which
        # requires monkey patching the app:
        # https://eventlet.net/doc/patching.html#monkey-patch
        # Otherwise task instances hang on the workers and are never
        # executed.
        maybe_patch_concurrency(['-P', pool])

    if args.daemon:
        # Run Celery worker as daemon
        handle = setup_logging(log_file)

        with open(stdout, 'a') as stdout_handle, open(stderr, 'a') as stderr_handle:
            if args.umask:
                umask = args.umask
            else:
                umask = conf.get('celery', 'worker_umask', fallback=settings.DAEMON_UMASK)

            stdout_handle.truncate(0)
            stderr_handle.truncate(0)

            ctx = daemon.DaemonContext(
                files_preserve=[handle],
                umask=int(umask, 8),
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
            with ctx:
                _run_worker(options=options, skip_serve_logs=skip_serve_logs)
    else:
        # Run Celery worker in the same process
        _run_worker(options=options, skip_serve_logs=skip_serve_logs)


@cli_utils.action_cli
def stop_worker(args):
    """Sends SIGTERM to Celery worker"""
    # Read PID from file
    if args.pid:
        pid_file_path = args.pid
    else:
        pid_file_path, _, _, _ = setup_locations(process=WORKER_PROCESS_NAME)
    pid = read_pid_from_pidfile(pid_file_path)

    # Send SIGTERM
    if pid:
        worker_process = psutil.Process(pid)
        worker_process.terminate()

    # Remove pid file
    remove_existing_pidfile(pid_file_path)
