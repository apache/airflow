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
"""CLI commands for Celery executor."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.cli.cli_config import (
    ARG_DAEMON,
    ARG_LOG_FILE,
    ARG_PID,
    ARG_SKIP_SERVE_LOGS,
    ARG_STDERR,
    ARG_STDOUT,
    ARG_VERBOSE,
    ActionCommand,
    Arg,
    GroupCommand,
    lazy_load_command,
)
from airflow.configuration import conf

if TYPE_CHECKING:
    import argparse

# flower cli args
ARG_BROKER_API = Arg(("-a", "--broker-api"), help="Broker API")
ARG_FLOWER_HOSTNAME = Arg(
    ("-H", "--hostname"),
    default=conf.get("celery", "FLOWER_HOST"),
    help="Set the hostname on which to run the server",
)
ARG_FLOWER_PORT = Arg(
    ("-p", "--port"),
    default=conf.getint("celery", "FLOWER_PORT"),
    type=int,
    help="The port on which to run the server",
)
ARG_FLOWER_CONF = Arg(("-c", "--flower-conf"), help="Configuration file for flower")
ARG_FLOWER_URL_PREFIX = Arg(
    ("-u", "--url-prefix"),
    default=conf.get("celery", "FLOWER_URL_PREFIX"),
    help="URL prefix for Flower",
)
ARG_FLOWER_BASIC_AUTH = Arg(
    ("-A", "--basic-auth"),
    default=conf.get("celery", "FLOWER_BASIC_AUTH"),
    help=(
        "Securing Flower with Basic Authentication. "
        "Accepts user:password pairs separated by a comma. "
        "Example: flower_basic_auth = user1:password1,user2:password2"
    ),
)

# worker cli args
ARG_AUTOSCALE = Arg(("-a", "--autoscale"), help="Minimum and Maximum number of worker to autoscale")
ARG_QUEUES = Arg(
    ("-q", "--queues"),
    help="Comma delimited list of queues to serve",
    default=conf.get("operators", "DEFAULT_QUEUE"),
)
ARG_CONCURRENCY = Arg(
    ("-c", "--concurrency"),
    type=int,
    help="The number of worker processes",
    default=conf.getint("celery", "worker_concurrency"),
)
ARG_CELERY_HOSTNAME = Arg(
    ("-H", "--celery-hostname"),
    help="Set the hostname of celery worker if you have multiple workers on a single machine",
)
ARG_UMASK = Arg(
    ("-u", "--umask"),
    help="Set the umask of celery worker in daemon mode",
)

ARG_WITHOUT_MINGLE = Arg(
    ("--without-mingle",),
    default=False,
    help="Don't synchronize with other workers at start-up",
    action="store_true",
)
ARG_WITHOUT_GOSSIP = Arg(
    ("--without-gossip",),
    default=False,
    help="Don't subscribe to other workers events",
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
ARG_FULL_CELERY_HOSTNAME = Arg(
    ("-H", "--celery-hostname"),
    required=True,
    help="Specify the full celery hostname. example: celery@hostname",
)
ARG_REQUIRED_QUEUES = Arg(
    ("-q", "--queues"),
    help="Comma delimited list of queues to serve",
    required=True,
)
ARG_YES = Arg(
    ("-y", "--yes"),
    help="Do not prompt to confirm. Use with care!",
    action="store_true",
    default=False,
)

CELERY_CLI_COMMAND_PATH = "airflow.providers.celery.cli.celery_command"

CELERY_COMMANDS = (
    ActionCommand(
        name="worker",
        help="Start a Celery worker node",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.worker"),
        args=(
            ARG_QUEUES,
            ARG_CONCURRENCY,
            ARG_CELERY_HOSTNAME,
            ARG_PID,
            ARG_DAEMON,
            ARG_UMASK,
            ARG_STDOUT,
            ARG_STDERR,
            ARG_LOG_FILE,
            ARG_AUTOSCALE,
            ARG_SKIP_SERVE_LOGS,
            ARG_WITHOUT_MINGLE,
            ARG_WITHOUT_GOSSIP,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="flower",
        help="Start a Celery Flower",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.flower"),
        args=(
            ARG_FLOWER_HOSTNAME,
            ARG_FLOWER_PORT,
            ARG_FLOWER_CONF,
            ARG_FLOWER_URL_PREFIX,
            ARG_FLOWER_BASIC_AUTH,
            ARG_BROKER_API,
            ARG_PID,
            ARG_DAEMON,
            ARG_STDOUT,
            ARG_STDERR,
            ARG_LOG_FILE,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="stop",
        help="Stop the Celery worker gracefully",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.stop_worker"),
        args=(ARG_PID, ARG_VERBOSE),
    ),
    ActionCommand(
        name="list-workers",
        help="List active celery workers",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.list_workers"),
        args=(ARG_OUTPUT,),
    ),
    ActionCommand(
        name="shutdown-worker",
        help="Request graceful shutdown of celery workers",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.shutdown_worker"),
        args=(ARG_FULL_CELERY_HOSTNAME,),
    ),
    ActionCommand(
        name="shutdown-all-workers",
        help="Request graceful shutdown of all active celery workers",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.shutdown_all_workers"),
        args=(ARG_YES,),
    ),
    ActionCommand(
        name="add-queue",
        help="Subscribe Celery worker to specified queues",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.add_queue"),
        args=(
            ARG_REQUIRED_QUEUES,
            ARG_FULL_CELERY_HOSTNAME,
        ),
    ),
    ActionCommand(
        name="remove-queue",
        help="Unsubscribe Celery worker from specified queues",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.remove_queue"),
        args=(
            ARG_REQUIRED_QUEUES,
            ARG_FULL_CELERY_HOSTNAME,
        ),
    ),
    ActionCommand(
        name="remove-all-queues",
        help="Unsubscribe Celery worker from all its active queues",
        func=lazy_load_command(f"{CELERY_CLI_COMMAND_PATH}.remove_all_queues"),
        args=(ARG_FULL_CELERY_HOSTNAME,),
    ),
)

CELERY_CLI_COMMANDS = [
    GroupCommand(
        name="celery",
        help="Celery components",
        description=(
            "Start celery components. Works only when using CeleryExecutor. For more information, "
            "see https://airflow.apache.org/docs/apache-airflow/stable/executor/celery.html"
        ),
        subcommands=CELERY_COMMANDS,
    ),
]


def get_celery_cli_commands():
    """Return CLI commands for Celery executor."""
    return CELERY_CLI_COMMANDS


def get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx.

    :meta private:
    """
    from airflow.cli.cli_parser import AirflowHelpFormatter, DefaultHelpParser, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in get_celery_cli_commands():
        _add_command(subparsers, group_command)
    return parser
