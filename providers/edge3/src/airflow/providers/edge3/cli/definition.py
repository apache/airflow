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

from airflow.cli.cli_config import ARG_PID, ARG_VERBOSE, ActionCommand, Arg, GroupCommand, lazy_load_command
from airflow.configuration import conf

if TYPE_CHECKING:
    import argparse


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
ARG_QUEUES_MANAGE = Arg(
    ("-q", "--queues"),
    help="Comma delimited list of queues to add or remove.",
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
ARG_YES = Arg(
    ("-y", "--yes"),
    help="Skip confirmation prompt and proceed with shutdown",
    action="store_true",
    default=False,
)

EDGE_COMMANDS: list[ActionCommand] = [
    ActionCommand(
        name="worker",
        help="Start Airflow Edge Worker.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.worker"),
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
        name="status",
        help="Check for Airflow Local Edge Worker status.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.status"),
        args=(
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="maintenance",
        help="Set or Unset maintenance mode of local edge worker.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.maintenance"),
        args=(
            ARG_MAINTENANCE,
            ARG_MAINTENANCE_COMMENT,
            ARG_WAIT_MAINT,
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="stop",
        help="Stop a running local Airflow Edge Worker.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.stop"),
        args=(
            ARG_WAIT_STOP,
            ARG_PID,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="list-workers",
        help="Query the db to list all registered edge workers.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.list_edge_workers"),
        args=(
            ARG_OUTPUT,
            ARG_STATE,
        ),
    ),
    ActionCommand(
        name="remote-edge-worker-request-maintenance",
        help="Put remote edge worker on maintenance.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.put_remote_worker_on_maintenance"),
        args=(
            ARG_REQUIRED_EDGE_HOSTNAME,
            ARG_REQUIRED_MAINTENANCE_COMMENT,
        ),
    ),
    ActionCommand(
        name="remote-edge-worker-exit-maintenance",
        help="Remove remote edge worker from maintenance.",
        func=lazy_load_command(
            "airflow.providers.edge3.cli.edge_command.remove_remote_worker_from_maintenance"
        ),
        args=(ARG_REQUIRED_EDGE_HOSTNAME,),
    ),
    ActionCommand(
        name="remote-edge-worker-update-maintenance-comment",
        help="Update maintenance comments of the remote edge worker.",
        func=lazy_load_command(
            "airflow.providers.edge3.cli.edge_command.remote_worker_update_maintenance_comment"
        ),
        args=(
            ARG_REQUIRED_EDGE_HOSTNAME,
            ARG_REQUIRED_MAINTENANCE_COMMENT,
        ),
    ),
    ActionCommand(
        name="remove-remote-edge-worker",
        help="Remove remote edge worker entry from db.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.remove_remote_worker"),
        args=(ARG_REQUIRED_EDGE_HOSTNAME,),
    ),
    ActionCommand(
        name="shutdown-remote-edge-worker",
        help="Initiate the shutdown of the remote edge worker.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.remote_worker_request_shutdown"),
        args=(ARG_REQUIRED_EDGE_HOSTNAME,),
    ),
    ActionCommand(
        name="add-worker-queues",
        help="Add queues to an edge worker.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.add_worker_queues"),
        args=(
            ARG_REQUIRED_EDGE_HOSTNAME,
            ARG_QUEUES_MANAGE,
        ),
    ),
    ActionCommand(
        name="remove-worker-queues",
        help="Remove queues from an edge worker.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.remove_worker_queues"),
        args=(
            ARG_REQUIRED_EDGE_HOSTNAME,
            ARG_QUEUES_MANAGE,
        ),
    ),
    ActionCommand(
        name="shutdown-all-workers",
        help="Request graceful shutdown of all edge workers.",
        func=lazy_load_command("airflow.providers.edge3.cli.edge_command.shutdown_all_workers"),
        args=(ARG_YES,),
    ),
]


def get_edge_cli_commands() -> list[GroupCommand]:
    return [
        GroupCommand(
            name="edge",
            help="Edge Worker components",
            description=(
                "Start and manage Edge Worker. Works only when using EdgeExecutor. For more information, "
                "see https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/edge_executor.html"
            ),
            subcommands=EDGE_COMMANDS,
        ),
    ]


def get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    from airflow.cli.cli_parser import AirflowHelpFormatter, DefaultHelpParser, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in get_edge_cli_commands():
        _add_command(subparsers, group_command)
    return parser
