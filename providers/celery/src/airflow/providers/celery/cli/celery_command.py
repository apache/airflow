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
"""Celery command."""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager, suppress
from multiprocessing import Process

import psutil
import sqlalchemy.exc
from celery import maybe_patch_concurrency
from celery.app.defaults import DEFAULT_TASK_LOG_FMT
from celery.signals import after_setup_logger
from lockfile.pidlockfile import read_pid_from_pidfile, remove_existing_pidfile

from airflow import settings
from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.providers.celery.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations

WORKER_PROCESS_NAME = "worker"

log = logging.getLogger(__name__)


def _run_command_with_daemon_option(*args, **kwargs):
    try:
        from airflow.cli.commands.daemon_utils import run_command_with_daemon_option

        run_command_with_daemon_option(*args, **kwargs)
    except ImportError:
        from airflow.exceptions import AirflowOptionalProviderFeatureException

        raise AirflowOptionalProviderFeatureException(
            "Failed to import run_command_with_daemon_option. This feature is only available in Airflow versions >= 2.8.0"
        )


def _providers_configuration_loaded(func):
    def wrapper(*args, **kwargs):
        try:
            from airflow.utils.providers_configuration_loader import providers_configuration_loaded

            providers_configuration_loaded(func)(*args, **kwargs)
        except ImportError as e:
            from airflow.exceptions import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "Failed to import providers_configuration_loaded. This feature is only available in Airflow versions >= 2.8.0"
            ) from e

    return wrapper


@cli_utils.action_cli(check_db=False)
@_providers_configuration_loaded
def flower(args):
    """Start Flower, Celery monitoring tool."""
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    options = [
        "flower",
        conf.get("celery", "BROKER_URL"),
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

    _run_command_with_daemon_option(
        args=args, process_name="flower", callback=lambda: celery_app.start(options)
    )


@contextmanager
def _serve_logs(skip_serve_logs: bool = False):
    """Start serve_logs sub-process."""
    from airflow.utils.serve_logs import serve_logs

    sub_proc = None
    if skip_serve_logs is False:
        sub_proc = Process(target=serve_logs)
        sub_proc.start()
    try:
        yield
    finally:
        if sub_proc:
            sub_proc.terminate()


@contextmanager
def _run_stale_bundle_cleanup():
    """Start stale bundle cleanup sub-process."""
    check_interval = None
    with suppress(AirflowConfigException):  # remove when min airflow version >= 3.0
        check_interval = conf.getint(
            section="dag_processor",
            key="stale_bundle_cleanup_interval",
        )
    if not check_interval or check_interval <= 0 or not AIRFLOW_V_3_0_PLUS:
        # do not start bundle cleanup process
        try:
            yield
        finally:
            return
    from airflow.dag_processing.bundles.base import BundleUsageTrackingManager

    log.info("starting stale bundle cleanup process")
    sub_proc = None

    def bundle_cleanup_main():
        mgr = BundleUsageTrackingManager()
        while True:
            time.sleep(check_interval)
            mgr.remove_stale_bundle_versions()

    try:
        sub_proc = Process(target=bundle_cleanup_main)
        sub_proc.start()
        yield
    finally:
        if sub_proc:
            sub_proc.terminate()


@after_setup_logger.connect()
@_providers_configuration_loaded
def logger_setup_handler(logger, **kwargs):
    """
    Reconfigure the logger.

    * remove any previously configured handlers
    * logs of severity error, and above goes to stderr,
    * logs of severity lower than error goes to stdout.
    """
    if conf.getboolean("logging", "celery_stdout_stderr_separation", fallback=False):
        celery_formatter = logging.Formatter(DEFAULT_TASK_LOG_FMT)

        class NoErrorOrAboveFilter(logging.Filter):
            """Allow only logs with level *lower* than ERROR to be reported."""

            def filter(self, record):
                return record.levelno < logging.ERROR

        below_error_handler = logging.StreamHandler(sys.stdout)
        below_error_handler.addFilter(NoErrorOrAboveFilter())
        below_error_handler.setFormatter(celery_formatter)

        from_error_handler = logging.StreamHandler(sys.stderr)
        from_error_handler.setLevel(logging.ERROR)
        from_error_handler.setFormatter(celery_formatter)

        logger.handlers[:] = [below_error_handler, from_error_handler]


@cli_utils.action_cli(check_db=not AIRFLOW_V_3_0_PLUS)
@_providers_configuration_loaded
def worker(args):
    """Start Airflow Celery worker."""
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    # Check if a worker with the same hostname already exists
    if args.celery_hostname:
        inspect = celery_app.control.inspect()
        active_workers = inspect.active_queues()
        if active_workers:
            active_worker_names = list(active_workers.keys())
            # Check if any worker ends with @hostname
            if any(name.endswith(f"@{args.celery_hostname}") for name in active_worker_names):
                raise SystemExit(
                    f"Error: A worker with hostname '{args.celery_hostname}' is already running. "
                    "Please use a different hostname or stop the existing worker first."
                )

    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk.log import configure_logging

        configure_logging(output=sys.stdout.buffer)
    else:
        # Disable connection pool so that celery worker does not hold an unnecessary db connection
        settings.reconfigure_orm(disable_connection_pool=True)
        if not settings.validate_session():
            raise SystemExit("Worker exiting, database connection precheck failed.")

    autoscale = args.autoscale
    skip_serve_logs = args.skip_serve_logs

    if autoscale is None and conf.has_option("celery", "worker_autoscale"):
        autoscale = conf.get("celery", "worker_autoscale")

    if hasattr(celery_app.backend, "ResultSession"):
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
    celery_log_level = conf.get("logging", "CELERY_LOGGING_LEVEL")
    if not celery_log_level:
        celery_log_level = conf.get("logging", "LOGGING_LEVEL")

    # Setup Celery worker
    options = [
        "worker",
        "-O",
        "fair",
        "--queues",
        args.queues,
        "--concurrency",
        args.concurrency,
        "--loglevel",
        celery_log_level,
    ]
    if args.celery_hostname:
        options.extend(["--hostname", args.celery_hostname])
    if autoscale:
        options.extend(["--autoscale", autoscale])
    if args.without_mingle:
        options.append("--without-mingle")
    if args.without_gossip:
        options.append("--without-gossip")

    if conf.has_option("celery", "pool"):
        pool = conf.get("celery", "pool")
        options.extend(["--pool", pool])
        # Celery pools of type eventlet and gevent use greenlets, which
        # requires monkey patching the app:
        # https://eventlet.readthedocs.io/en/latest/patching.html#monkeypatching-the-standard-library
        # Otherwise task instances hang on the workers and are never
        # executed.
        maybe_patch_concurrency(["-P", pool])

    worker_pid_file_path, stdout, stderr, log_file = setup_locations(
        process=WORKER_PROCESS_NAME,
        stdout=args.stdout,
        stderr=args.stderr,
        log=args.log_file,
        pid=args.pid,
    )

    def run_celery_worker():
        with _serve_logs(skip_serve_logs), _run_stale_bundle_cleanup():
            celery_app.worker_main(options)

    if args.umask:
        umask = args.umask
    else:
        umask = conf.get("celery", "worker_umask", fallback=settings.DAEMON_UMASK)

    _run_command_with_daemon_option(
        args=args,
        process_name=WORKER_PROCESS_NAME,
        callback=run_celery_worker,
        should_setup_logging=True,
        umask=umask,
        pid_file=worker_pid_file_path,
    )


@cli_utils.action_cli(check_db=False)
@_providers_configuration_loaded
def stop_worker(args):
    """Send SIGTERM to Celery worker."""
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


@_providers_configuration_loaded
def _check_if_active_celery_worker(hostname: str):
    """Check if celery worker is active before executing dependent cli commands."""
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()
    if not active_workers:
        raise SystemExit("Error: No active Celery workers found!")
    if hostname not in active_workers:
        raise SystemExit(f"Error: {hostname} is unknown!")


@cli_utils.action_cli(check_db=False)
@_providers_configuration_loaded
def list_workers(args):
    """List all active celery workers."""
    workers = []
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()
    if active_workers:
        workers = [
            {
                "worker_name": worker,
                "queues": [queue["name"] for queue in active_workers[worker] if "name" in queue],
            }
            for worker in active_workers
        ]
    AirflowConsole().print_as(data=workers, output=args.output)


@cli_utils.action_cli(check_db=False)
@_providers_configuration_loaded
def shutdown_worker(args):
    """Request graceful shutdown of a celery worker."""
    _check_if_active_celery_worker(hostname=args.celery_hostname)
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    celery_app.control.shutdown(destination=[args.celery_hostname])


@cli_utils.action_cli(check_db=False)
@_providers_configuration_loaded
def shutdown_all_workers(args):
    """Request graceful shutdown all celery workers."""
    if not (
        args.yes
        or input(
            "This will shutdown all active celery workers connected to the celery broker, this cannot be undone! Proceed? (y/n)"
        ).upper()
        == "Y"
    ):
        raise SystemExit("Cancelled")
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    celery_app.control.broadcast("shutdown")


@cli_utils.action_cli(check_db=False)
@_providers_configuration_loaded
def add_queue(args):
    """Subscribe a Celery worker to specified queues."""
    _check_if_active_celery_worker(hostname=args.celery_hostname)
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    queues = args.queues.split(",")
    for queue in queues:
        celery_app.control.add_consumer(queue, destination=[args.celery_hostname])


@cli_utils.action_cli(check_db=False)
@_providers_configuration_loaded
def remove_queue(args):
    """Unsubscribe a Celery worker from specified queues."""
    _check_if_active_celery_worker(hostname=args.celery_hostname)
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    queues = args.queues.split(",")
    for queue in queues:
        celery_app.control.cancel_consumer(queue, destination=[args.celery_hostname])


@cli_utils.action_cli(check_db=False)
@_providers_configuration_loaded
def remove_all_queues(args):
    """Unsubscribe a Celery worker from all its active queues."""
    _check_if_active_celery_worker(hostname=args.celery_hostname)
    # This needs to be imported locally to not trigger Providers Manager initialization
    from airflow.providers.celery.executors.celery_executor import app as celery_app

    inspect = celery_app.control.inspect()
    active_workers = inspect.active_queues()

    if not active_workers or args.celery_hostname not in active_workers:
        print(f"No active queues found for worker: {args.celery_hostname}")
        return

    worker_queues = active_workers[args.celery_hostname]
    queue_names = [queue["name"] for queue in worker_queues if "name" in queue]

    if not queue_names:
        print(f"No queues to remove for worker: {args.celery_hostname}")
        return

    print(
        f"Removing {len(queue_names)} queue(s) from worker {args.celery_hostname}: {', '.join(queue_names)}"
    )

    for queue_name in queue_names:
        celery_app.control.cancel_consumer(queue_name, destination=[args.celery_hostname])

    print(f"Successfully removed all queues from worker: {args.celery_hostname}")
