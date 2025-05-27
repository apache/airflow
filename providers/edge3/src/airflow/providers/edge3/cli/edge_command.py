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
from airflow.cli.cli_config import ARG_PID, ARG_VERBOSE, ActionCommand, Arg
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
from airflow.providers.edge3.cli.worker import SIG_STATUS, EdgeWorker
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
    Ensure that the environment is configured for the internal API without needing to declare it outside.

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

    edge_worker = EdgeWorker(
        pid_file_path=pid_file_path(args.pid),
        hostname=args.edge_hostname or getfqdn(),
        queues=args.queues.split(",") if args.queues else None,
        concurrency=args.concurrency,
        job_poll_interval=conf.getint("edge", "job_poll_interval"),
        heartbeat_interval=conf.getint("edge", "heartbeat_interval"),
        daemon=args.daemon,
    )
    edge_worker.start()


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
        "maintenance_comment",
    ]
    all_hosts = [{f: host.__getattribute__(f) for f in fields} for host in all_hosts_iter]
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


ARG_CONCURRENCY = Arg(
    ("-c", "--concurrency"),
    type=int,
    help="The number of worker processes",
    default=conf.getint("edge", "worker_concurrency", fallback=8),
)
ARG_QUEUES = Arg(
    ("-q", "--queues"),
    help="Comma delimited list of queues to serve, serve all queues if not provided.",
)
ARG_EDGE_HOSTNAME = Arg(
    ("-H", "--edge-hostname"),
    help="Set the hostname of worker if you have multiple workers on a single machine",
)
ARG_REQUIRED_EDGE_HOSTNAME = Arg(
    ("-H", "--edge-hostname"),
    help="Set the hostname of worker if you have multiple workers on a single machine",
    required=True,
)
ARG_MAINTENANCE = Arg(("maintenance",), help="Desired maintenance state", choices=("on", "off"))
ARG_MAINTENANCE_COMMENT = Arg(
    ("-c", "--comments"),
    help="Maintenance comments to report reason. Required if maintenance is turned on.",
)
ARG_REQUIRED_MAINTENANCE_COMMENT = Arg(
    ("-c", "--comments"),
    help="Maintenance comments to report reason. Required if enabling maintenance",
    required=True,
)
ARG_WAIT_MAINT = Arg(
    ("-w", "--wait"),
    default=False,
    help="Wait until edge worker has reached desired state.",
    action="store_true",
)
ARG_WAIT_STOP = Arg(
    ("-w", "--wait"),
    default=False,
    help="Wait until edge worker is shut down.",
    action="store_true",
)
ARG_OUTPUT = Arg(
    (
        "-o",
        "--output",
    ),
    help="Output format. Allowed values: json, yaml, plain, table (default: table)",
    metavar="(table, json, yaml, plain)",
    choices=("table", "json", "yaml", "plain"),
    default="table",
)
ARG_STATE = Arg(
    (
        "-s",
        "--state",
    ),
    nargs="+",
    help="State of the edge worker",
)

ARG_DAEMON = Arg(
    ("-D", "--daemon"), help="Daemonize instead of running in the foreground", action="store_true"
)
ARG_UMASK = Arg(
    ("-u", "--umask"),
    help="Set the umask of edge worker in daemon mode",
)
ARG_STDERR = Arg(("--stderr",), help="Redirect stderr to this file if run in daemon mode")
ARG_STDOUT = Arg(("--stdout",), help="Redirect stdout to this file if run in daemon mode")
ARG_LOG_FILE = Arg(("-l", "--log-file"), help="Location of the log file if run in daemon mode")

EDGE_COMMANDS: list[ActionCommand] = [
    ActionCommand(
        name=worker.__name__,
        help=worker.__doc__,
        func=worker,
        args=(
            ARG_CONCURRENCY,
            ARG_QUEUES,
            ARG_EDGE_HOSTNAME,
            ARG_PID,
            ARG_VERBOSE,
            ARG_DAEMON,
            ARG_STDOUT,
            ARG_STDERR,
            ARG_LOG_FILE,
            ARG_UMASK,
        ),
    ),
    ActionCommand(
        name=status.__name__,
        help=status.__doc__,
        func=status,
        args=(
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name=maintenance.__name__,
        help=maintenance.__doc__,
        func=maintenance,
        args=(
            ARG_MAINTENANCE,
            ARG_MAINTENANCE_COMMENT,
            ARG_WAIT_MAINT,
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name=stop.__name__,
        help=stop.__doc__,
        func=stop,
        args=(
            ARG_WAIT_STOP,
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="list-workers",
        help=list_edge_workers.__doc__,
        func=list_edge_workers,
        args=(
            ARG_OUTPUT,
            ARG_STATE,
        ),
    ),
    ActionCommand(
        name="remote-edge-worker-request-maintenance",
        help=put_remote_worker_on_maintenance.__doc__,
        func=put_remote_worker_on_maintenance,
        args=(
            ARG_REQUIRED_EDGE_HOSTNAME,
            ARG_REQUIRED_MAINTENANCE_COMMENT,
        ),
    ),
    ActionCommand(
        name="remote-edge-worker-exit-maintenance",
        help=remove_remote_worker_from_maintenance.__doc__,
        func=remove_remote_worker_from_maintenance,
        args=(ARG_REQUIRED_EDGE_HOSTNAME,),
    ),
    ActionCommand(
        name="remote-edge-worker-update-maintenance-comment",
        help=remote_worker_update_maintenance_comment.__doc__,
        func=remote_worker_update_maintenance_comment,
        args=(
            ARG_REQUIRED_EDGE_HOSTNAME,
            ARG_REQUIRED_MAINTENANCE_COMMENT,
        ),
    ),
    ActionCommand(
        name="remove-remote-edge-worker",
        help=remove_remote_worker.__doc__,
        func=remove_remote_worker,
        args=(ARG_REQUIRED_EDGE_HOSTNAME,),
    ),
    ActionCommand(
        name="shutdown-remote-edge-worker",
        help=remote_worker_request_shutdown.__doc__,
        func=remote_worker_request_shutdown,
        args=(ARG_REQUIRED_EDGE_HOSTNAME,),
    ),
]
