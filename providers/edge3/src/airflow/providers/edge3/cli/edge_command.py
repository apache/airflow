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

import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import asdict
from datetime import datetime
from getpass import getuser
from pathlib import Path
from time import sleep, time

import psutil

from airflow import settings
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import conf
from airflow.providers.edge3.cli.dataclasses import MaintenanceMarker, WorkerStatus
from airflow.providers.edge3.cli.signalling import (
    EDGE_WORKER_PROCESS_NAME,
    get_pid,
    maintenance_marker_file_path,
    pid_file_path,
    status_file_path,
)
from airflow.providers.edge3.models.edge_worker import EdgeWorkerState
from airflow.utils import cli as cli_utils
from airflow.utils.net import getfqdn
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

logger = logging.getLogger(__name__)
EDGE_WORKER_HEADER = "\n".join(
    [
        r"   ____   __           _      __         __",
        r"  / __/__/ /__ ____   | | /| / /__  ____/ /_____ ____",
        r" / _// _  / _ `/ -_)  | |/ |/ / _ \/ __/  '_/ -_) __/",
        r"/___/\_,_/\_, /\__/   |__/|__/\___/_/ /_/\_\\__/_/",
        r"         /___/",
        r"",
    ]
)


@providers_configuration_loaded
def force_use_internal_api_on_edge_worker():
    """
    Ensure the environment is configured for the internal API without explicit declaration.

    This is only required for an Edge worker and must to be done before the Click CLI wrapper is initiated.
    That is because the CLI wrapper will attempt to establish a DB connection, which will fail before the
    function call can take effect. In an Edge worker, we need to "patch" the environment before starting.
    """
    # export Edge API to be used for internal API
    os.environ["_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK"] = "1"
    os.environ["AIRFLOW_ENABLE_AIP_44"] = "True"
    if "airflow" in sys.argv[0] and sys.argv[1:3] == ["edge", "worker"]:
        api_url = conf.get("edge", "api_url")
        if not api_url:
            raise SystemExit("Error: API URL is not configured, please correct configuration.")
        logger.info("Starting worker with API endpoint %s", api_url)
        os.environ["AIRFLOW__CORE__INTERNAL_API_URL"] = api_url


force_use_internal_api_on_edge_worker()


@providers_configuration_loaded
def _launch_worker(args):
    print(settings.HEADER)
    print(EDGE_WORKER_HEADER)

    from airflow.providers.edge3.cli.worker import EdgeWorker

    edge_worker = EdgeWorker(
        pid_file_path=pid_file_path(args.pid),
        hostname=args.edge_hostname or getfqdn(),
        queues=args.queues.split(",") if args.queues else None,
        concurrency=args.concurrency,
        job_poll_interval=conf.getint("edge", "job_poll_interval"),
        heartbeat_interval=conf.getint("edge", "heartbeat_interval"),
        daemon=args.daemon,
    )
    asyncio.run(edge_worker.start())


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def worker(args):
    """Start Airflow Edge Worker."""
    umask = args.umask or conf.get("edge", "worker_umask", fallback=settings.DAEMON_UMASK)

    run_command_with_daemon_option(
        args=args,
        process_name=EDGE_WORKER_PROCESS_NAME,
        callback=lambda: _launch_worker(args),
        should_setup_logging=True,
        pid_file=pid_file_path(args.pid),
        umask=umask,
    )


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def status(args):
    """Check for Airflow Local Edge Worker status."""
    from airflow.providers.edge3.cli.worker import SIG_STATUS

    pid = get_pid(args.pid)

    # Send Signal as notification to drop status JSON
    logger.debug("Sending SIGUSR2 to worker pid %i.", pid)
    status_min_date = time() - 1
    status_path = Path(status_file_path(args.pid))
    worker_process = psutil.Process(pid)
    worker_process.send_signal(SIG_STATUS)
    while psutil.pid_exists(pid) and (
        not status_path.exists() or status_path.stat().st_mtime < status_min_date
    ):
        sleep(0.1)
    if not psutil.pid_exists(pid):
        logger.warning("PID of worker dis-appeared while checking for status.")
        sys.exit(2)
    if not status_path.exists() or status_path.stat().st_mtime < status_min_date:
        logger.warning("Could not read status of worker.")
        sys.exit(3)
    status = WorkerStatus.from_json(status_path.read_text())
    print(json.dumps(asdict(status), indent=4))


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def maintenance(args):
    """Set or Unset maintenance mode of local edge worker."""
    from airflow.providers.edge3.cli.worker import SIG_STATUS

    if args.maintenance == "on" and not args.comments:
        logger.error("Comments are required when setting maintenance mode.")
        sys.exit(4)

    pid = get_pid(args.pid)

    # Write marker JSON file
    from getpass import getuser

    marker_path = Path(maintenance_marker_file_path(args.pid))
    logger.debug("Writing maintenance marker file to %s.", marker_path)
    marker_path.write_text(
        MaintenanceMarker(
            maintenance=args.maintenance,
            comments=f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {getuser()} put "
            f"node into maintenance mode via cli\nComment: {args.comments}"
            if args.maintenance == "on"
            else None,
        ).json
    )

    # Send Signal as notification to fetch maintenance marker
    logger.debug("Sending SIGUSR2 to worker pid %i.", pid)
    status_min_date = time() - 1
    status_path = Path(status_file_path(args.pid))
    worker_process = psutil.Process(pid)
    worker_process.send_signal(SIG_STATUS)
    while psutil.pid_exists(pid) and (
        not status_path.exists() or status_path.stat().st_mtime < status_min_date
    ):
        sleep(0.1)
    if not psutil.pid_exists(pid):
        logger.warning("PID of worker dis-appeared while checking for status.")
        sys.exit(2)
    if not status_path.exists() or status_path.stat().st_mtime < status_min_date:
        logger.warning("Could not read status of worker.")
        sys.exit(3)
    status = WorkerStatus.from_json(status_path.read_text())

    if args.wait:
        if args.maintenance == "on" and status.state != EdgeWorkerState.MAINTENANCE_MODE:
            logger.info("Waiting for worker to be drained...")
            while True:
                sleep(4.5)
                worker_process.send_signal(SIG_STATUS)
                sleep(0.5)
                status = WorkerStatus.from_json(status_path.read_text())
                if status.state == EdgeWorkerState.MAINTENANCE_MODE:
                    logger.info("Worker was drained successfully!")
                    break
                if status.state not in [
                    EdgeWorkerState.MAINTENANCE_REQUEST,
                    EdgeWorkerState.MAINTENANCE_PENDING,
                ]:
                    logger.info("Worker maintenance was exited by someone else!")
                    break
        if args.maintenance == "off" and status.state == EdgeWorkerState.MAINTENANCE_MODE:
            logger.info("Waiting for worker to exit maintenance...")
            while status.state in [EdgeWorkerState.MAINTENANCE_MODE, EdgeWorkerState.MAINTENANCE_EXIT]:
                sleep(4.5)
                worker_process.send_signal(SIG_STATUS)
                sleep(0.5)
                status = WorkerStatus.from_json(status_path.read_text())

    print(json.dumps(asdict(status), indent=4))


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def stop(args):
    """Stop a running local Airflow Edge Worker."""
    pid = get_pid(args.pid)
    # Send SIGINT
    logger.info("Sending SIGINT to worker pid %i.", pid)
    worker_process = psutil.Process(pid)
    worker_process.send_signal(signal.SIGINT)

    if args.wait:
        logger.info("Waiting for worker to stop...")
        while psutil.pid_exists(pid):
            sleep(0.1)
        logger.info("Worker has been shut down.")


def _check_valid_db_connection():
    """Check for a valid db connection before executing db dependent cli commands."""
    db_conn = conf.get("database", "sql_alchemy_conn")
    db_default = conf.get_default_value("database", "sql_alchemy_conn")
    if db_conn == db_default:
        raise SystemExit(
            "Error: The database connection is not set. Please set the connection in the configuration file."
        )


def _check_if_registered_edge_host(hostname: str):
    """Check if edge worker is registered with the db before executing dependent cli commands."""
    from airflow.providers.edge3.models.edge_worker import _fetch_edge_hosts_from_db

    if not _fetch_edge_hosts_from_db(hostname=hostname):
        raise SystemExit(f"Error: Edge Worker {hostname} is unknown!")


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def list_edge_workers(args) -> None:
    """Query the db to list all registered edge workers."""
    _check_valid_db_connection()
    from airflow.providers.edge3.models.edge_worker import get_registered_edge_hosts

    all_hosts_iter = get_registered_edge_hosts(states=args.state)
    # Format and print worker info on the screen
    fields = [
        "worker_name",
        "state",
        "queues",
        "jobs_active",
        "concurrency",
        "free_concurrency",
        "maintenance_comment",
    ]

    all_hosts = []
    for host in all_hosts_iter:
        host_data = {
            f: getattr(host, f, None) for f in fields if f not in ("concurrency", "free_concurrency")
        }
        try:
            sysinfo = json.loads(host.sysinfo or "{}")
            host_data["concurrency"] = sysinfo.get("concurrency")
            host_data["free_concurrency"] = sysinfo.get("free_concurrency")
        except (json.JSONDecodeError, TypeError):
            host_data["concurrency"] = None
            host_data["free_concurrency"] = None
        all_hosts.append(host_data)

    AirflowConsole().print_as(data=all_hosts, output=args.output)


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def put_remote_worker_on_maintenance(args) -> None:
    """Put remote edge worker on maintenance."""
    _check_valid_db_connection()
    _check_if_registered_edge_host(hostname=args.edge_hostname)
    from airflow.providers.edge3.models.edge_worker import request_maintenance

    request_maintenance(args.edge_hostname, args.comments)
    logger.info("%s has been put on maintenance by %s.", args.edge_hostname, getuser())


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def remove_remote_worker_from_maintenance(args) -> None:
    """Remove remote edge worker from maintenance."""
    _check_valid_db_connection()
    _check_if_registered_edge_host(hostname=args.edge_hostname)
    from airflow.providers.edge3.models.edge_worker import exit_maintenance

    exit_maintenance(args.edge_hostname)
    logger.info("%s has been removed from maintenance by %s.", args.edge_hostname, getuser())


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def remote_worker_update_maintenance_comment(args) -> None:
    """Update maintenance comments of the remote edge worker."""
    _check_valid_db_connection()
    _check_if_registered_edge_host(hostname=args.edge_hostname)
    from airflow.providers.edge3.models.edge_worker import change_maintenance_comment

    try:
        change_maintenance_comment(args.edge_hostname, args.comments)
        logger.info("Maintenance comments updated for %s by %s.", args.edge_hostname, getuser())
    except TypeError:
        raise SystemExit


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def remove_remote_worker(args) -> None:
    """Remove remote edge worker entry from db."""
    _check_valid_db_connection()
    _check_if_registered_edge_host(hostname=args.edge_hostname)
    from airflow.providers.edge3.models.edge_worker import remove_worker

    try:
        remove_worker(args.edge_hostname)
        logger.info("Edge Worker host %s removed by %s.", args.edge_hostname, getuser())
    except TypeError:
        raise SystemExit


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def remote_worker_request_shutdown(args) -> None:
    """Initiate the shutdown of the remote edge worker."""
    _check_valid_db_connection()
    _check_if_registered_edge_host(hostname=args.edge_hostname)
    from airflow.providers.edge3.models.edge_worker import request_shutdown

    request_shutdown(args.edge_hostname)
    logger.info("Requested shutdown of Edge Worker host %s by %s.", args.edge_hostname, getuser())


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def shutdown_all_workers(args) -> None:
    """Request graceful shutdown of all edge workers."""
    _check_valid_db_connection()
    if not (
        args.yes
        or input("This will shutdown all active edge workers, this cannot be undone! Proceed? (y/n)").upper()
        == "Y"
    ):
        raise SystemExit("Cancelled")

    from airflow.providers.edge3.models.edge_worker import get_registered_edge_hosts, request_shutdown

    all_hosts = list(get_registered_edge_hosts())
    if not all_hosts:
        logger.info("No edge workers found to shutdown.")
        return

    shutdown_count = 0
    for host in all_hosts:
        try:
            request_shutdown(host.worker_name)
            logger.info("Requested shutdown of Edge Worker host %s", host.worker_name)
            shutdown_count += 1
        except Exception as e:
            logger.error("Failed to shutdown Edge Worker host %s: %s", host.worker_name, e)

    logger.info("Requested shutdown of %d edge workers by %s.", shutdown_count, getuser())


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def add_worker_queues(args) -> None:
    """Add queues to an edge worker."""
    _check_valid_db_connection()
    _check_if_registered_edge_host(hostname=args.edge_hostname)
    from airflow.providers.edge3.models.edge_worker import add_worker_queues

    queues = args.queues.split(",") if args.queues else []
    if not queues:
        raise SystemExit("Error: No queues specified to add.")

    try:
        add_worker_queues(args.edge_hostname, queues)
        logger.info("Added queues %s to Edge Worker host %s by %s.", queues, args.edge_hostname, getuser())
    except TypeError as e:
        logger.error(str(e))
        raise SystemExit


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def remove_worker_queues(args) -> None:
    """Remove queues from an edge worker."""
    _check_valid_db_connection()
    _check_if_registered_edge_host(hostname=args.edge_hostname)
    from airflow.providers.edge3.models.edge_worker import remove_worker_queues

    queues = args.queues.split(",") if args.queues else []
    if not queues:
        raise SystemExit("Error: No queues specified to remove.")

    try:
        remove_worker_queues(args.edge_hostname, queues)
        logger.info(
            "Removed queues %s from Edge Worker host %s by %s.", queues, args.edge_hostname, getuser()
        )
    except TypeError as e:
        logger.error(str(e))
        raise SystemExit
