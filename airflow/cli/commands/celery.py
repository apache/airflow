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
from multiprocessing import Process
from typing import Optional

import daemon
import psutil
import rich_click as click
import sqlalchemy.exc
from celery import maybe_patch_concurrency
from daemon.pidfile import TimeoutPIDLockFile
from lockfile.pidlockfile import read_pid_from_pidfile, remove_existing_pidfile

from airflow import settings
from airflow.cli import airflow_cmd, click_daemon, click_log_file, click_pid, click_stderr, click_stdout
from airflow.configuration import conf
from airflow.executors.celery_executor import app as celery_app
from airflow.utils.cli import setup_locations, setup_logging
from airflow.utils.serve_logs import serve_logs

WORKER_PROCESS_NAME = "worker"


click_flower_host = click.option(
    "-H",
    "--hostname",
    default=conf.get("celery", "FLOWER_HOST"),
    help="Set the hostname on which to run the server",
)
click_flower_port = click.option(
    "-p",
    "--port",
    default=conf.get("celery", "FLOWER_PORT"),
    type=int,
    help="The port on which to run the server",
)
click_flower_broker_api = click.option("-a", "--broker-api", help="Broker API")
click_flower_url_prefix = click.option(
    "-u", "--url-prefix", default=conf.get("celery", "FLOWER_URL_PREFIX"), help="URL prefix for Flower"
)
click_flower_basic_auth = click.option(
    "-A",
    "--basic-auth",
    default=conf.get("celery", "FLOWER_BASIC_AUTH"),
    help=(
        "Securing Flower with Basic Authentication. "
        "Accepts user:password pairs separated by a comma. "
        "Example: flower_basic_auth = user1:password1,user2:password2"
    ),
)
click_flower_conf = click.option("-c", "--flower-conf", help="Configuration file for flower")
click_worker_autoscale = click.option(
    "-a", "--autoscale", help="Minimum and Maximum number of worker to autoscale"
)
click_worker_skip_serve_logs = click.option(
    "-s",
    "--skip-serve-logs",
    is_flag=True,
    default=False,
    help="Don't start the serve logs process along with the workers",
)
click_worker_queues = click.option(
    "-q",
    "--queues",
    default=conf.get("operators", "DEFAULT_QUEUE"),
    help="Comma delimited list of queues to serve",
)
click_worker_concurrency = click.option(
    "-c",
    "--concurrency",
    type=int,
    default=conf.get("celery", "worker_concurrency"),
    help="The number of worker processes",
)
click_worker_hostname = click.option(
    "-H",
    "--celery-hostname",
    help="Set the hostname of celery worker if you have multiple workers on a single machine",
)
click_worker_umask = click.option(
    "-u",
    "--umask",
    default=conf.get("celery", "worker_umask"),
    help="Set the umask of celery worker in daemon mode",
)
click_worker_without_mingle = click.option(
    "--without-mingle", is_flag=True, default=False, help="Don't synchronize with other workers at start-up"
)
click_worker_without_gossip = click.option(
    "--without-gossip", is_flag=True, default=False, help="Don't subscribe to other workers events"
)


@airflow_cmd.group()
def celery():
    """Celery components"""
    pass


@celery.command()
@click_flower_host
@click_flower_port
@click_flower_broker_api
@click_flower_url_prefix
@click_flower_basic_auth
@click_flower_conf
@click_stdout
@click_stderr
@click_pid
@click_daemon
@click_log_file
def flower(
    hostname, port, broker_api, url_prefix, basic_auth, flower_conf, stdout, stderr, pid, daemon_, log_file
):
    """Starts Flower, Celery monitoring tool"""
    options = [
        "flower",
        conf.get("celery", "BROKER_URL"),
        f"--address={hostname}",
        f"--port={port}",
    ]

    if broker_api:
        options.append(f"--broker-api={broker_api}")

    if url_prefix:
        options.append(f"--url-prefix={url_prefix}")

    if basic_auth:
        options.append(f"--basic-auth={basic_auth}")

    if flower_conf:
        options.append(f"--conf={flower_conf}")

    if daemon_:
        pidfile, stdout, stderr, _ = setup_locations(
            process="flower",
            pid=pid,
            stdout=stdout,
            stderr=stderr,
            log=log_file,
        )
        with open(stdout, "w+") as stdout, open(stderr, "w+") as stderr:
            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pidfile, -1),
                stdout=stdout,
                stderr=stderr,
            )
            with ctx:
                celery_app.start(options)
    else:
        celery_app.start(options)


def _serve_logs(skip_serve_logs: bool = False) -> Optional[Process]:
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


@celery.command()
@click_pid
@click_daemon
@click_stdout
@click_stderr
@click_log_file
@click_worker_autoscale
@click_worker_skip_serve_logs
@click_worker_queues
@click_worker_concurrency
@click_worker_hostname
@click_worker_umask
@click_worker_without_mingle
@click_worker_without_gossip
def worker(
    pid,
    daemon_,
    stdout,
    stderr,
    log_file,
    autoscale,
    skip_serve_logs,
    queues,
    concurrency,
    celery_hostname,
    umask,
    without_mingle,
    without_gossip,
):
    """Starts Airflow Celery worker"""
    # Disable connection pool so that celery worker does not hold an unnecessary db connection
    settings.reconfigure_orm(disable_connection_pool=True)
    if not settings.validate_session():
        raise SystemExit("Worker exiting, database connection precheck failed.")

    if autoscale is None and conf.has_option("celery", "worker_autoscale"):
        autoscale = conf.get("celery", "worker_autoscale")

    # Setup locations
    pid_file_path, stdout, stderr, log_file = setup_locations(
        process=WORKER_PROCESS_NAME,
        pid=pid,
        stdout=stdout,
        stderr=stderr,
        log=log_file,
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
        queues,
        '--concurrency',
        concurrency,
        '--hostname',
        celery_hostname,
        '--loglevel',
        celery_log_level,
        '--pidfile',
        pid_file_path,
    ]
    if autoscale:
        options.extend(['--autoscale', autoscale])
    if without_mingle:
        options.append('--without-mingle')
    if without_gossip:
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

    if daemon_:
        # Run Celery worker as daemon
        handle = setup_logging(log_file)

        with open(stdout, 'w+') as stdout_handle, open(stderr, 'w+') as stderr_handle:
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


@celery.command("stop")
@click_pid
def stop_worker(pid):
    """Stop the Celery worker gracefully by sending SIGTERM to worker"""
    # Read PID from file
    if pid:
        pid_file_path = pid
    else:
        pid_file_path, _, _, _ = setup_locations(process=WORKER_PROCESS_NAME)
    pid = read_pid_from_pidfile(pid_file_path)

    # Send SIGTERM
    if pid:
        worker_process = psutil.Process(pid)
        worker_process.terminate()

    # Remove pid file
    remove_existing_pidfile(pid_file_path)
