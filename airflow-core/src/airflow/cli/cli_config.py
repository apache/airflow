#!/usr/bin/env python
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
"""Explicit configuration and definition of Airflow CLI commands."""

from __future__ import annotations

import argparse
import json
import os
import textwrap
from collections.abc import Callable, Iterable
from typing import NamedTuple

import lazy_object_proxy

from airflow._shared.module_loading import import_string
from airflow._shared.timezones.timezone import parse as parsedate
from airflow.cli.commands.legacy_commands import check_legacy_command
from airflow.configuration import conf
from airflow.jobs.job import JobState
from airflow.utils.cli import ColorMode
from airflow.utils.state import DagRunState

BUILD_DOCS = "BUILDING_AIRFLOW_DOCS" in os.environ


def lazy_load_command(import_path: str) -> Callable:
    """Create a lazy loader for command."""
    _, _, name = import_path.rpartition(".")

    def command(*args, **kwargs):
        func = import_string(import_path)
        return func(*args, **kwargs)

    command.__name__ = name

    return command


class DefaultHelpParser(argparse.ArgumentParser):
    """CustomParser to display help message."""

    def _check_value(self, action, value):
        """Override _check_value and check conditionally added command."""
        if action.choices is not None and value not in action.choices:
            check_legacy_command(action, value)

        super()._check_value(action, value)

    def error(self, message):
        """Override error and use print_help instead of print_usage."""
        self.print_help()
        self.exit(2, f"\n{self.prog} command error: {message}, see help above.\n")


# Used in Arg to enable `None' as a distinct value from "not passed"
_UNSET = object()


class Arg:
    """Class to keep information about command line argument."""

    def __init__(
        self,
        flags=_UNSET,
        help=_UNSET,
        action=_UNSET,
        default=_UNSET,
        nargs=_UNSET,
        type=_UNSET,
        choices=_UNSET,
        required=_UNSET,
        metavar=_UNSET,
        dest=_UNSET,
    ):
        self.flags = flags
        self.kwargs = {}
        for k, v in locals().items():
            if k not in ("self", "flags") and v is not _UNSET:
                self.kwargs[k] = v

    def add_to_parser(self, parser: argparse.ArgumentParser):
        """Add this argument to an ArgumentParser."""
        if "metavar" in self.kwargs and "type" not in self.kwargs:
            if self.kwargs["metavar"] == "DIRPATH":

                def type(x):
                    return self._is_valid_directory(parser, x)

                self.kwargs["type"] = type
        parser.add_argument(*self.flags, **self.kwargs)

    def _is_valid_directory(self, parser, arg):
        if not os.path.isdir(arg):
            parser.error(f"The directory '{arg}' does not exist!")
        return arg


def positive_int(*, allow_zero):
    """Define a positive int type for an argument."""

    def _check(value):
        try:
            value = int(value)
            if allow_zero and value == 0:
                return value
            if value > 0:
                return value
        except ValueError:
            pass
        raise argparse.ArgumentTypeError(f"invalid positive int value: '{value}'")

    return _check


def string_list_type(val):
    """Parse comma-separated list and returns list of string (strips whitespace)."""
    return [x.strip() for x in val.split(",")]


def string_lower_type(val):
    """Lower arg."""
    if not val:
        return
    return val.strip().lower()


# Shared
ARG_DAG_ID = Arg(("dag_id",), help="The id of the dag")
ARG_TASK_ID = Arg(("task_id",), help="The id of the task")
ARG_LOGICAL_DATE = Arg(("-l", "--logical-date"), help="The logical date of the DAG", type=parsedate)
ARG_LOGICAL_DATE_OPTIONAL = Arg(
    ("logical_date",), nargs="?", help="The logical date of the DAG (optional)", type=parsedate
)
ARG_LOGICAL_DATE_OR_RUN_ID = Arg(
    ("logical_date_or_run_id",), help="The logical date of the DAG or run_id of the DAGRun"
)
ARG_LOGICAL_DATE_OR_RUN_ID_OPTIONAL = Arg(
    ("logical_date_or_run_id",),
    nargs="?",
    help="The logical date of the DAG or run_id of the DAGRun (optional)",
)
ARG_TASK_REGEX = Arg(("-t", "--task-regex"), help="The regex to filter specific task_ids (optional)")
ARG_BUNDLE_NAME = Arg(
    (
        "-B",
        "--bundle-name",
    ),
    help=("The name of the DAG bundle to use; may be provided more than once"),
    type=str,
    default=None,
    action="append",
)
ARG_START_DATE = Arg(
    ("-s", "--start-date"),
    help=(
        "Override start_date. Accepts multiple datetime formats including: "
        "YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SS±HH:MM (ISO 8601), "
        "and other formats supported by pendulum.parse()"
    ),
    type=parsedate,
)
ARG_END_DATE = Arg(
    ("-e", "--end-date"),
    help=(
        "Override end_date. Accepts multiple datetime formats including: "
        "YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SS±HH:MM (ISO 8601), "
        "and other formats supported by pendulum.parse()"
    ),
    type=parsedate,
)
ARG_OUTPUT_PATH = Arg(
    (
        "-o",
        "--output-path",
    ),
    help="The output for generated yaml files",
    type=str,
    default="[CWD]" if BUILD_DOCS else os.getcwd(),
)
ARG_PID = Arg(("--pid",), help="PID file location", nargs="?")
ARG_DAEMON = Arg(
    ("-D", "--daemon"), help="Daemonize instead of running in the foreground", action="store_true"
)
ARG_STDERR = Arg(("--stderr",), help="Redirect stderr to this file")
ARG_STDOUT = Arg(("--stdout",), help="Redirect stdout to this file")
ARG_LOG_FILE = Arg(("-l", "--log-file"), help="Location of the log file")
ARG_YES = Arg(
    ("-y", "--yes"),
    help="Do not prompt to confirm. Use with care!",
    action="store_true",
    default=False,
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
ARG_COLOR = Arg(
    ("--color",),
    help="Do emit colored output (default: auto)",
    choices={ColorMode.ON, ColorMode.OFF, ColorMode.AUTO},
    default=ColorMode.AUTO,
)

# DB args
ARG_VERSION_RANGE = Arg(
    ("-r", "--range"),
    help="Version range(start:end) for offline sql generation. Example: '2.0.2:2.2.3'",
    default=None,
)
ARG_REVISION_RANGE = Arg(
    ("--revision-range",),
    help=(
        "Migration revision range(start:end) to use for offline sql generation. "
        "Example: ``a13f7613ad25:7b2661a43ba3``"
    ),
    default=None,
)
ARG_SKIP_SERVE_LOGS = Arg(
    ("-s", "--skip-serve-logs"),
    default=False,
    help="Don't start the serve logs process along with the workers",
    action="store_true",
)

# list_dags
ARG_LIST_LOCAL = Arg(
    ("-l", "--local"),
    action="store_true",
    help="Shows local parsed DAGs and their import errors, ignores content serialized in DB",
)

# list_dag_runs
ARG_NO_BACKFILL = Arg(
    ("--no-backfill",), help="filter all the backfill dagruns given the dag id", action="store_true"
)
dagrun_states = tuple(state.value for state in DagRunState)
ARG_DR_STATE = Arg(
    ("--state",),
    help="Only list the DAG runs corresponding to the state",
    metavar=", ".join(dagrun_states),
    choices=dagrun_states,
)

# list_jobs
ARG_DAG_ID_OPT = Arg(("-d", "--dag-id"), help="The id of the dag")
ARG_LIMIT = Arg(("--limit",), help="Return a limited number of records")
job_states = tuple(state.value for state in JobState)
ARG_JOB_STATE = Arg(
    ("--state",),
    help="Only list the jobs corresponding to the state",
    metavar=", ".join(job_states),
    choices=job_states,
)

# next_execution
ARG_NUM_EXECUTIONS = Arg(
    ("-n", "--num-executions"),
    default=1,
    type=positive_int(allow_zero=False),
    help="The number of next logical date times to show",
)

# misc
ARG_MARK_SUCCESS = Arg(
    ("-m", "--mark-success"), help="Mark jobs as succeeded without running them", action="store_true"
)
ARG_INCLUDE_DESCRIPTIONS = Arg(
    ("-d", "--include-descriptions"),
    help="Show descriptions for the configuration variables",
    action="store_true",
)
ARG_INCLUDE_EXAMPLES = Arg(
    ("-e", "--include-examples"), help="Show examples for the configuration variables", action="store_true"
)
ARG_INCLUDE_SOURCES = Arg(
    ("-s", "--include-sources"), help="Show source of the configuration variable", action="store_true"
)
ARG_INCLUDE_ENV_VARS = Arg(
    ("-V", "--include-env-vars"), help="Show environment variable for each option", action="store_true"
)
ARG_COMMENT_OUT_EVERYTHING = Arg(
    ("-c", "--comment-out-everything"),
    help="Comment out all configuration options. Useful as starting point for new installation",
    action="store_true",
)
ARG_EXCLUDE_PROVIDERS = Arg(
    ("-p", "--exclude-providers"),
    help="Exclude provider configuration (they are included by default)",
    action="store_true",
)
ARG_DEFAULTS = Arg(
    ("-a", "--defaults"),
    help="Show only defaults - do not include local configuration, sources,"
    " includes descriptions, examples, variables. Comment out everything.",
    action="store_true",
)
ARG_VERBOSE = Arg(("-v", "--verbose"), help="Make logging output more verbose", action="store_true")
ARG_LOCAL = Arg(("-l", "--local"), help="Run the task using the LocalExecutor", action="store_true")
ARG_POOL = Arg(("--pool",), "Resource pool to use")

# teams
ARG_TEAM_NAME = Arg(("name",), help="Team name")

# backfill
ARG_BACKFILL_DAG = Arg(flags=("--dag-id",), help="The dag to backfill.", required=True)
ARG_BACKFILL_FROM_DATE = Arg(
    ("--from-date",), help="Earliest logical date to backfill.", type=parsedate, required=True
)
ARG_BACKFILL_TO_DATE = Arg(
    ("--to-date",), help="Latest logical date to backfill", type=parsedate, required=True
)
ARG_DAG_RUN_CONF = Arg(flags=("--dag-run-conf",), help="JSON dag run configuration.")
ARG_RUN_BACKWARDS = Arg(
    flags=("--run-backwards",),
    help=(
        "If set, the backfill will run tasks from the most recent logical date first. "
        "Not supported if there are tasks that depend_on_past."
    ),
    action="store_true",
)
ARG_MAX_ACTIVE_RUNS = Arg(
    ("--max-active-runs",),
    type=positive_int(allow_zero=False),
    help="Max active runs for this backfill.",
)
ARG_BACKFILL_DRY_RUN = Arg(
    ("--dry-run",),
    help="Perform a dry run",
    action="store_true",
)
ARG_BACKFILL_REPROCESS_BEHAVIOR = Arg(
    ("--reprocess-behavior",),
    help=(
        "When a run exists for the logical date, controls whether new runs will be "
        "created for the date. Default is none."
    ),
    choices=("none", "completed", "failed"),
)
ARG_BACKFILL_RUN_ON_LATEST_VERSION = Arg(
    ("--run-on-latest-version",),
    help=(
        "(Experimental) The backfill will run tasks using the latest bundle version instead of "
        "the version that was active when the original Dag run was created. Defaults to True."
    ),
    action="store_true",
    default=True,
)


# misc
ARG_TREAT_DAG_ID_AS_REGEX = Arg(
    ("--treat-dag-id-as-regex",),
    help=("if set, dag_id will be treated as regex instead of an exact string"),
    action="store_true",
)

# test_dag
ARG_DAGFILE_PATH = Arg(
    (
        "-f",
        "--dagfile-path",
    ),
    help="Path to the dag file. Can be absolute or relative to current directory",
)
ARG_SHOW_DAGRUN = Arg(
    ("--show-dagrun",),
    help=(
        "After completing the backfill, shows the diagram for current DAG Run.\n"
        "\n"
        "The diagram is in DOT language\n"
    ),
    action="store_true",
)
ARG_IMGCAT_DAGRUN = Arg(
    ("--imgcat-dagrun",),
    help=(
        "After completing the dag run, prints a diagram on the screen for the "
        "current DAG Run using the imgcat tool.\n"
    ),
    action="store_true",
)
ARG_SAVE_DAGRUN = Arg(
    ("--save-dagrun",),
    help="After completing the backfill, saves the diagram for current DAG Run to the indicated file.\n\n",
)
ARG_USE_EXECUTOR = Arg(
    ("--use-executor",),
    help="Use an executor to test the DAG. By default it runs the DAG without an executor. "
    "If set, it uses the executor configured in the environment.",
    action="store_true",
)
ARG_MARK_SUCCESS_PATTERN = Arg(
    ("--mark-success-pattern",),
    help=(
        "Don't run task_ids matching the regex <MARK_SUCCESS_PATTERN>, mark them as successful instead.\n"
        "Can be used to skip e.g. dependency check sensors or cleanup steps in local testing.\n"
    ),
)

# tasks_run
# This is a hidden option -- not meant for users to set or know about
ARG_SHUT_DOWN_LOGGING = Arg(
    ("--no-shut-down-logging",),
    help=argparse.SUPPRESS,
    dest="shut_down_logging",
    action="store_false",
    default=True,
)

# clear
ARG_UPSTREAM = Arg(("-u", "--upstream"), help="Include upstream tasks", action="store_true")
ARG_ONLY_FAILED = Arg(("-f", "--only-failed"), help="Only failed jobs", action="store_true")
ARG_ONLY_RUNNING = Arg(("-r", "--only-running"), help="Only running jobs", action="store_true")
ARG_DOWNSTREAM = Arg(("-d", "--downstream"), help="Include downstream tasks", action="store_true")
ARG_DAG_REGEX = Arg(
    ("-R", "--dag-regex"), help="Search dag_id as regex instead of exact string", action="store_true"
)

# show_dag
ARG_SAVE = Arg(("-s", "--save"), help="Saves the result to the indicated file.")

ARG_IMGCAT = Arg(("--imgcat",), help="Displays graph using the imgcat tool.", action="store_true")

# trigger_dag
ARG_RUN_ID = Arg(("-r", "--run-id"), help="Helps to identify this run")
ARG_CONF = Arg(("-c", "--conf"), help="JSON string that gets pickled into the DagRun's conf attribute")
ARG_REPLACE_MICRO = Arg(
    ("--no-replace-microseconds",),
    help="whether microseconds should be zeroed",
    dest="replace_microseconds",
    action="store_false",
    default=True,
)

# db
ARG_DB_TABLES = Arg(
    ("-t", "--tables"),
    help=lazy_object_proxy.Proxy(
        lambda: f"Table names to perform maintenance on (use comma-separated list).\n"
        f"Options: {import_string('airflow.cli.commands.db_command.all_tables')}"
    ),
    type=string_list_type,
)
ARG_DB_CLEANUP_TIMESTAMP = Arg(
    ("--clean-before-timestamp",),
    help="The date or timestamp before which data should be purged.\n"
    "If no timezone info is supplied then dates are assumed to be in airflow default timezone.\n"
    "Example: '2022-01-01 00:00:00+01:00'",
    type=parsedate,
    required=True,
)
ARG_DB_DRY_RUN = Arg(
    ("--dry-run",),
    help="Perform a dry run",
    action="store_true",
)
ARG_DB_SKIP_ARCHIVE = Arg(
    ("--skip-archive",),
    help="Don't preserve purged records in an archive table.",
    action="store_true",
)
ARG_DB_EXPORT_FORMAT = Arg(
    ("--export-format",),
    help="The file format to export the cleaned data",
    choices=("csv",),
    default="csv",
)
ARG_DB_OUTPUT_PATH = Arg(
    ("--output-path",),
    metavar="DIRPATH",
    help="The path to the output directory to export the cleaned data. This directory must exist.",
    required=True,
)
ARG_DB_DROP_ARCHIVES = Arg(
    ("--drop-archives",),
    help="Drop the archive tables after exporting. Use with caution.",
    action="store_true",
)
ARG_DB_RETRY = Arg(
    ("--retry",),
    default=0,
    type=positive_int(allow_zero=True),
    help="Retry database check upon failure",
)
ARG_DB_RETRY_DELAY = Arg(
    ("--retry-delay",),
    default=1,
    type=positive_int(allow_zero=False),
    help="Wait time between retries in seconds",
)
ARG_DB_BATCH_SIZE = Arg(
    ("--batch-size",),
    default=None,
    type=positive_int(allow_zero=False),
    help=(
        "Maximum number of rows to delete or archive in a single transaction.\n"
        "Lower values reduce long-running locks but increase the number of batches."
    ),
)

# pool
ARG_POOL_NAME = Arg(("pool",), metavar="NAME", help="Pool name")
ARG_POOL_SLOTS = Arg(("slots",), type=int, help="Pool slots")
ARG_POOL_DESCRIPTION = Arg(("description",), help="Pool description")
ARG_POOL_INCLUDE_DEFERRED = Arg(
    ("--include-deferred",), help="Include deferred tasks in calculations for Pool", action="store_true"
)
ARG_POOL_IMPORT = Arg(
    ("file",),
    metavar="FILEPATH",
    help="Import pools from JSON file. Example format::\n"
    + textwrap.indent(
        textwrap.dedent(
            """
            {
                "pool_1": {"slots": 5, "description": "", "include_deferred": true},
                "pool_2": {"slots": 10, "description": "test", "include_deferred": false}
            }"""
        ),
        " " * 4,
    ),
)

ARG_POOL_EXPORT = Arg(("file",), metavar="FILEPATH", help="Export all pools to JSON file")

# variables
ARG_VAR = Arg(("key",), help="Variable key")
ARG_VAR_VALUE = Arg(("value",), metavar="VALUE", help="Variable value")
ARG_DEFAULT = Arg(
    ("-d", "--default"), metavar="VAL", default=None, help="Default value returned if variable does not exist"
)
ARG_VAR_DESCRIPTION = Arg(
    ("--description",),
    default=None,
    required=False,
    help="Variable description, optional when setting a variable",
)
ARG_DESERIALIZE_JSON = Arg(("-j", "--json"), help="Deserialize JSON variable", action="store_true")
ARG_SERIALIZE_JSON = Arg(("-j", "--json"), help="Serialize JSON variable", action="store_true")
ARG_VAR_IMPORT = Arg(("file",), help="Import variables from .env, .json, .yaml or .yml file")
ARG_VAR_EXPORT = Arg(
    ("file",),
    help="Export all variables to JSON file",
    type=argparse.FileType("w", encoding="UTF-8"),
)
ARG_VAR_ACTION_ON_EXISTING_KEY = Arg(
    ("-a", "--action-on-existing-key"),
    help="Action to take if we encounter a variable key that already exists.",
    default="overwrite",
    choices=("overwrite", "fail", "skip"),
)

# kerberos
ARG_PRINCIPAL = Arg(("principal",), help="kerberos principal", nargs="?")
ARG_KEYTAB = Arg(("-k", "--keytab"), help="keytab", nargs="?", default=conf.get("kerberos", "keytab"))
ARG_KERBEROS_ONE_TIME_MODE = Arg(
    ("-o", "--one-time"), help="Run airflow kerberos one time instead of forever", action="store_true"
)
# tasks
ARG_MAP_INDEX = Arg(("--map-index",), type=int, default=-1, help="Mapped task index")


# database
ARG_MIGRATION_TIMEOUT = Arg(
    ("-t", "--migration-wait-timeout"),
    help="timeout to wait for db to migrate ",
    type=int,
    default=60,
)
ARG_DB_VERSION__UPGRADE = Arg(
    ("-n", "--to-version"),
    help=(
        "(Optional) The airflow version to upgrade to. Note: must provide either "
        "`--to-revision` or `--to-version`."
    ),
)
ARG_DB_REVISION__UPGRADE = Arg(
    ("-r", "--to-revision"),
    help="(Optional) If provided, only run migrations up to and including this Alembic revision.",
)
ARG_DB_VERSION__DOWNGRADE = Arg(
    ("-n", "--to-version"),
    help="(Optional) If provided, only run migrations up to this version.",
)
ARG_DB_FROM_VERSION = Arg(
    ("--from-version",),
    help="(Optional) If generating sql, may supply a *from* version",
)
ARG_DB_REVISION__DOWNGRADE = Arg(
    ("-r", "--to-revision"),
    help="The Alembic revision to downgrade to. Note: must provide either `--to-revision` or `--to-version`.",
)
ARG_DB_FROM_REVISION = Arg(
    ("--from-revision",),
    help="(Optional) If generating sql, may supply a *from* Alembic revision",
)
ARG_DB_SQL_ONLY = Arg(
    ("-s", "--show-sql-only"),
    help="Don't actually run migrations; just print out sql scripts for offline migration. "
    "Required if using either `--from-revision` or `--from-version`.",
    action="store_true",
    default=False,
)
ARG_DB_SKIP_INIT = Arg(
    ("-s", "--skip-init"),
    help="Only remove tables; do not perform db init.",
    action="store_true",
    default=False,
)

ARG_DB_MANAGER_PATH = Arg(
    ("import_path",),
    help="The import path of the database manager to use. ",
    default=None,
)

# api-server
ARG_API_SERVER_PORT = Arg(
    ("-p", "--port"),
    default=conf.get("api", "port"),
    type=int,
    help="The port on which to run the API server",
)
ARG_API_SERVER_WORKERS = Arg(
    ("-w", "--workers"),
    default=conf.get("api", "workers"),
    type=int,
    help="Number of workers to run on the API server",
)
ARG_API_SERVER_WORKER_TIMEOUT = Arg(
    ("-t", "--worker-timeout"),
    default=120,
    type=int,
    help="The timeout for waiting on API server workers",
)
ARG_API_SERVER_HOSTNAME = Arg(
    ("-H", "--host"),
    default=conf.get("api", "host"),
    help="Set the host on which to run the API server",
)
ARG_API_SERVER_LOG_CONFIG = Arg(
    ("--log-config",),
    default=conf.get("api", "log_config", fallback=None),
    help="(Optional) Path to the logging configuration file for the uvicorn server. If not set, the default uvicorn logging configuration will be used.",
)
ARG_API_SERVER_APPS = Arg(
    ("--apps",),
    help="Applications to run (comma-separated). Default is all. Options: core, execution, all",
    default="all",
)
ARG_API_SERVER_ALLOW_PROXY_FORWARDING = Arg(
    flags=("--proxy-headers",),
    help="Enable X-Forwarded-Proto, X-Forwarded-For, X-Forwarded-Port to populate remote address info.",
    action="store_true",
)
ARG_SSL_CERT = Arg(
    ("--ssl-cert",),
    default=conf.get("api", "ssl_cert"),
    help="Path to the SSL certificate for the webserver",
)
ARG_SSL_KEY = Arg(
    ("--ssl-key",),
    default=conf.get("api", "ssl_key"),
    help="Path to the key to use with the SSL certificate",
)
ARG_DEV = Arg(("-d", "--dev"), help="Start in development mode with hot-reload enabled", action="store_true")

# scheduler
ARG_NUM_RUNS = Arg(
    ("-n", "--num-runs"),
    default=conf.getint("scheduler", "num_runs"),
    type=int,
    help="Set the number of runs to execute before exiting",
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

ARG_TASK_PARAMS = Arg(("-t", "--task-params"), help="Sends a JSON params dict to the task")
ARG_POST_MORTEM = Arg(
    ("-m", "--post-mortem"), action="store_true", help="Open debugger on uncaught exception"
)
ARG_ENV_VARS = Arg(
    ("--env-vars",),
    help="Set env var in both parsing time and runtime for each of entry supplied in a JSON dict",
    type=json.loads,
)

# connections
ARG_CONN_ID = Arg(("conn_id",), help="Connection id, required to get/add/delete/test a connection", type=str)
ARG_CONN_URI = Arg(
    ("--conn-uri",), help="Connection URI, required to add a connection without conn_type", type=str
)
ARG_CONN_JSON = Arg(
    ("--conn-json",), help="Connection JSON, required to add a connection using JSON representation", type=str
)
ARG_CONN_TYPE = Arg(
    ("--conn-type",), help="Connection type, required to add a connection without conn_uri", type=str
)
ARG_CONN_DESCRIPTION = Arg(
    ("--conn-description",), help="Connection description, optional when adding a connection", type=str
)
ARG_CONN_HOST = Arg(("--conn-host",), help="Connection host, optional when adding a connection", type=str)
ARG_CONN_LOGIN = Arg(("--conn-login",), help="Connection login, optional when adding a connection", type=str)
ARG_CONN_PASSWORD = Arg(
    ("--conn-password",), help="Connection password, optional when adding a connection", type=str
)
ARG_CONN_SCHEMA = Arg(
    ("--conn-schema",), help="Connection schema, optional when adding a connection", type=str
)
ARG_CONN_PORT = Arg(("--conn-port",), help="Connection port, optional when adding a connection", type=str)
ARG_CONN_EXTRA = Arg(
    ("--conn-extra",), help="Connection `Extra` field, optional when adding a connection", type=str
)
ARG_CONN_EXPORT = Arg(
    ("file",),
    help="Output file path for exporting the connections",
    type=argparse.FileType("w", encoding="UTF-8"),
)
ARG_CONN_EXPORT_FORMAT = Arg(
    ("--format",),
    help="Deprecated -- use `--file-format` instead. File format to use for the export.",
    type=str,
    choices=["json", "yaml", "env"],
)
ARG_CONN_EXPORT_FILE_FORMAT = Arg(
    ("--file-format",), help="File format for the export", type=str, choices=["json", "yaml", "env"]
)
ARG_CONN_SERIALIZATION_FORMAT = Arg(
    ("--serialization-format",),
    help="When exporting as `.env` format, defines how connections should be serialized. Default is `uri`.",
    type=string_lower_type,
    choices=["json", "uri"],
)
ARG_CONN_IMPORT = Arg(("file",), help="Import connections from a file")
ARG_CONN_OVERWRITE = Arg(
    ("--overwrite",),
    help="Overwrite existing entries if a conflict occurs",
    required=False,
    action="store_true",
)

# providers
ARG_PROVIDER_NAME = Arg(
    ("provider_name",), help="Provider name, required to get provider information", type=str
)
ARG_FULL = Arg(
    ("-f", "--full"),
    help="Full information about the provider, including documentation information.",
    required=False,
    action="store_true",
)

# info
ARG_ANONYMIZE = Arg(
    ("--anonymize",),
    help="Minimize any personal identifiable information. Use it when sharing output with others.",
    action="store_true",
)
ARG_FILE_IO = Arg(
    ("--file-io",), help="Send output to file.io service and returns link.", action="store_true"
)

# config
ARG_SECTION = Arg(
    ("section",),
    help="The section name",
)
ARG_OPTION = Arg(
    ("option",),
    help="The option name",
)

# lint
ARG_LINT_CONFIG_SECTION = Arg(
    ("--section",),
    help="The section name(s) to lint in the airflow config.",
    type=string_list_type,
)
ARG_LINT_CONFIG_OPTION = Arg(
    ("--option",),
    help="The option name(s) to lint in the airflow config.",
    type=string_list_type,
)
ARG_LINT_CONFIG_IGNORE_SECTION = Arg(
    ("--ignore-section",),
    help="The section name(s) to ignore to lint in the airflow config.",
    type=string_list_type,
)
ARG_LINT_CONFIG_IGNORE_OPTION = Arg(
    ("--ignore-option",),
    help="The option name(s) to ignore to lint in the airflow config.",
    type=string_list_type,
)
ARG_OPTIONAL_SECTION = Arg(
    ("--section",),
    help="The section name",
)

# config update
ARG_UPDATE_CONFIG_SECTION = Arg(
    ("--section",),
    help="The section name(s) to update in the airflow config.",
    type=string_list_type,
)
ARG_UPDATE_CONFIG_OPTION = Arg(
    ("--option",),
    help="The option name(s) to update in the airflow config.",
    type=string_list_type,
)
ARG_UPDATE_CONFIG_IGNORE_SECTION = Arg(
    ("--ignore-section",),
    help="The section name(s) to ignore to update in the airflow config.",
    type=string_list_type,
)
ARG_UPDATE_CONFIG_IGNORE_OPTION = Arg(
    ("--ignore-option",),
    help="The option name(s) to ignore to update in the airflow config.",
    type=string_list_type,
)
ARG_UPDATE_CONFIG_FIX = Arg(
    ("--fix",),
    help="Automatically apply the configuration changes instead of performing a dry run. (Default: dry-run mode)",
    action="store_true",
)

ARG_UPDATE_ALL_RECOMMENDATIONS = Arg(
    ("--all-recommendations",),
    help="Include non-breaking (recommended) changes along with breaking ones. (Also use with --fix)",
    action="store_true",
)


# jobs check
ARG_JOB_TYPE_FILTER = Arg(
    ("--job-type",),
    choices=("SchedulerJob", "TriggererJob", "DagProcessorJob"),
    action="store",
    help="The type of job(s) that will be checked.",
)

ARG_JOB_HOSTNAME_FILTER = Arg(
    ("--hostname",),
    default=None,
    type=str,
    help="The hostname of job(s) that will be checked.",
)

ARG_JOB_HOSTNAME_CALLABLE_FILTER = Arg(
    ("--local",),
    action="store_true",
    help="If passed, this command will only show jobs from the local host "
    "(those with a hostname matching what `hostname_callable` returns).",
)

ARG_JOB_LIMIT = Arg(
    ("--limit",),
    default=1,
    type=positive_int(allow_zero=True),
    help="The number of recent jobs that will be checked. To disable limit, set 0. ",
)

ARG_ALLOW_MULTIPLE = Arg(
    ("--allow-multiple",),
    action="store_true",
    help="If passed, this command will be successful even if multiple matching alive jobs are found.",
)

# triggerer
ARG_CAPACITY = Arg(
    ("--capacity",),
    type=positive_int(allow_zero=False),
    help="The maximum number of triggers that a Triggerer will run at one time.",
)
ARG_QUEUES = Arg(
    ("--queues",),
    type=string_list_type,
    help="Optional comma-separated list of task queues which the triggerer should consume from.",
)

ARG_DAG_LIST_COLUMNS = Arg(
    ("--columns",),
    type=string_list_type,
    help="List of columns to render. (default: ['dag_id', 'fileloc', 'owner', 'is_paused'])",
    default=("dag_id", "fileloc", "owners", "is_paused", "bundle_name", "bundle_version"),
)

ARG_ASSET_LIST_COLUMNS = Arg(
    ("--columns",),
    type=string_list_type,
    help="List of columns to render. (default: ['name', 'uri', 'group', 'extra'])",
    default=("name", "uri", "group", "extra"),
)

ARG_ASSET_NAME = Arg(("--name",), default="", help="Asset name")
ARG_ASSET_URI = Arg(("--uri",), default="", help="Asset URI")
ARG_ASSET_ALIAS = Arg(("--alias",), default=False, action="store_true", help="Show asset alias")

ALTERNATIVE_CONN_SPECS_ARGS = [
    ARG_CONN_TYPE,
    ARG_CONN_DESCRIPTION,
    ARG_CONN_HOST,
    ARG_CONN_LOGIN,
    ARG_CONN_PASSWORD,
    ARG_CONN_SCHEMA,
    ARG_CONN_PORT,
]


class ActionCommand(NamedTuple):
    """Single CLI command."""

    name: str
    help: str
    func: Callable
    args: Iterable[Arg]
    description: str | None = None
    epilog: str | None = None
    hide: bool = False


class GroupCommand(NamedTuple):
    """ClI command with subcommands."""

    name: str
    help: str
    subcommands: Iterable
    description: str | None = None
    epilog: str | None = None


CLICommand = ActionCommand | GroupCommand

ASSETS_COMMANDS = (
    ActionCommand(
        name="list",
        help="List assets",
        func=lazy_load_command("airflow.cli.commands.asset_command.asset_list"),
        args=(ARG_ASSET_ALIAS, ARG_OUTPUT, ARG_VERBOSE, ARG_ASSET_LIST_COLUMNS),
    ),
    ActionCommand(
        name="details",
        help="Show asset details",
        func=lazy_load_command("airflow.cli.commands.asset_command.asset_details"),
        args=(ARG_ASSET_ALIAS, ARG_ASSET_NAME, ARG_ASSET_URI, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="materialize",
        help="Materialize an asset",
        func=lazy_load_command("airflow.cli.commands.asset_command.asset_materialize"),
        args=(ARG_ASSET_NAME, ARG_ASSET_URI, ARG_OUTPUT, ARG_VERBOSE),
    ),
)
BACKFILL_COMMANDS = (
    ActionCommand(
        name="create",
        help="Create a backfill for a dag.",
        description="Run subsections of a DAG for a specified date range.",
        func=lazy_load_command("airflow.cli.commands.backfill_command.create_backfill"),
        args=(
            ARG_BACKFILL_DAG,
            ARG_BACKFILL_FROM_DATE,
            ARG_BACKFILL_TO_DATE,
            ARG_DAG_RUN_CONF,
            ARG_RUN_BACKWARDS,
            ARG_MAX_ACTIVE_RUNS,
            ARG_BACKFILL_REPROCESS_BEHAVIOR,
            ARG_BACKFILL_RUN_ON_LATEST_VERSION,
            ARG_BACKFILL_DRY_RUN,
        ),
    ),
)
DAGS_COMMANDS = (
    ActionCommand(
        name="details",
        help="Get DAG details given a DAG id",
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_details"),
        args=(ARG_DAG_ID, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="list",
        help="List all the DAGs",
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_list_dags"),
        args=(ARG_OUTPUT, ARG_VERBOSE, ARG_DAG_LIST_COLUMNS, ARG_BUNDLE_NAME, ARG_LIST_LOCAL),
    ),
    ActionCommand(
        name="list-import-errors",
        help="List all the DAGs that have import errors",
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_list_import_errors"),
        args=(ARG_BUNDLE_NAME, ARG_OUTPUT, ARG_VERBOSE, ARG_LIST_LOCAL),
    ),
    ActionCommand(
        name="report",
        help="Show DagBag loading report",
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_report"),
        args=(ARG_BUNDLE_NAME, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="list-runs",
        help="List DAG runs given a DAG id",
        description=(
            "List DAG runs given a DAG id. If state option is given, it will only search for all the "
            "dagruns with the given state. If no_backfill option is given, it will filter out all "
            "backfill dagruns for given dag id. If start_date is given, it will filter out all the "
            "dagruns that were executed before this date. If end_date is given, it will filter out "
            "all the dagruns that were executed after this date. "
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_list_dag_runs"),
        args=(
            ARG_DAG_ID,
            ARG_NO_BACKFILL,
            ARG_DR_STATE,
            ARG_OUTPUT,
            ARG_VERBOSE,
            ARG_START_DATE,
            ARG_END_DATE,
        ),
    ),
    ActionCommand(
        name="list-jobs",
        help="List the jobs",
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_list_jobs"),
        args=(ARG_DAG_ID_OPT, ARG_JOB_STATE, ARG_LIMIT, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="state",
        help="Get the status of a dag run",
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_state"),
        args=(ARG_DAG_ID, ARG_LOGICAL_DATE_OR_RUN_ID, ARG_VERBOSE),
    ),
    ActionCommand(
        name="next-execution",
        help="Get the next logical datetimes of a DAG",
        description=(
            "Get the next logical datetimes of a DAG. It returns one execution unless the "
            "num-executions option is given"
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_next_execution"),
        args=(ARG_DAG_ID, ARG_NUM_EXECUTIONS, ARG_VERBOSE),
    ),
    ActionCommand(
        name="pause",
        help="Pause DAG(s)",
        description=(
            "Pause one or more DAGs. This command allows to halt the execution of specified DAGs, "
            "disabling further task scheduling. Use `--treat-dag-id-as-regex` to target multiple DAGs by "
            "treating the `--dag-id` as a regex pattern."
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_pause"),
        args=(ARG_DAG_ID, ARG_TREAT_DAG_ID_AS_REGEX, ARG_YES, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="unpause",
        help="Resume paused DAG(s)",
        description=(
            "Resume one or more DAGs. This command allows to restore the execution of specified "
            "DAGs, enabling further task scheduling. Use `--treat-dag-id-as-regex` to target multiple DAGs "
            "treating the `--dag-id` as a regex pattern."
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_unpause"),
        args=(ARG_DAG_ID, ARG_TREAT_DAG_ID_AS_REGEX, ARG_YES, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="trigger",
        help=(
            "Trigger a new DAG run. If DAG is paused then dagrun state will remain queued, "
            "and the task won't run."
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_trigger"),
        args=(
            ARG_DAG_ID,
            ARG_RUN_ID,
            ARG_CONF,
            ARG_LOGICAL_DATE,
            ARG_VERBOSE,
            ARG_REPLACE_MICRO,
            ARG_OUTPUT,
        ),
    ),
    ActionCommand(
        name="delete",
        help="Delete all DB records related to the specified DAG",
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_delete"),
        args=(ARG_DAG_ID, ARG_YES, ARG_VERBOSE),
    ),
    ActionCommand(
        name="show",
        help="Displays DAG's tasks with their dependencies",
        description=(
            "The --imgcat option only works in iTerm.\n"
            "\n"
            "For more information, see: https://www.iterm2.com/documentation-images.html\n"
            "\n"
            "The --save option saves the result to the indicated file.\n"
            "\n"
            "The file format is determined by the file extension. "
            "For more information about supported "
            "format, see: https://www.graphviz.org/doc/info/output.html\n"
            "\n"
            "If you want to create a PNG file then you should execute the following command:\n"
            "airflow dags show <DAG_ID> --save output.png\n"
            "\n"
            "If you want to create a DOT file then you should execute the following command:\n"
            "airflow dags show <DAG_ID> --save output.dot\n"
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_show"),
        args=(ARG_DAG_ID, ARG_SAVE, ARG_IMGCAT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="show-dependencies",
        help="Displays DAGs with their dependencies",
        description=(
            "The --imgcat option only works in iTerm.\n"
            "\n"
            "For more information, see: https://www.iterm2.com/documentation-images.html\n"
            "\n"
            "The --save option saves the result to the indicated file.\n"
            "\n"
            "The file format is determined by the file extension. "
            "For more information about supported "
            "format, see: https://www.graphviz.org/doc/info/output.html\n"
            "\n"
            "If you want to create a PNG file then you should execute the following command:\n"
            "airflow dags show-dependencies --save output.png\n"
            "\n"
            "If you want to create a DOT file then you should execute the following command:\n"
            "airflow dags show-dependencies --save output.dot\n"
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_dependencies_show"),
        args=(
            ARG_SAVE,
            ARG_IMGCAT,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="test",
        help="Execute one single DagRun",
        description=(
            "Execute one single DagRun for a given DAG and logical date.\n"
            "\n"
            "You can test a DAG in three ways:\n"
            "1. Using default bundle:\n"
            "   airflow dags test <DAG_ID>\n"
            "\n"
            "2. Using a specific bundle if multiple DAG bundles are configured:\n"
            "   airflow dags test <DAG_ID> --bundle-name <BUNDLE_NAME> (or -B <BUNDLE_NAME>)\n"
            "\n"
            "3. Using a specific DAG file:\n"
            "   airflow dags test <DAG_ID> --dagfile-path <PATH> (or -f <PATH>)\n"
            "\n"
            "The --imgcat-dagrun option only works in iTerm.\n"
            "\n"
            "For more information, see: https://www.iterm2.com/documentation-images.html\n"
            "\n"
            "If --save-dagrun is used, then, after completing the backfill, saves the diagram "
            "for current DAG Run to the indicated file.\n"
            "The file format is determined by the file extension. "
            "For more information about supported format, "
            "see: https://www.graphviz.org/doc/info/output.html\n"
            "\n"
            "If you want to create a PNG file then you should execute the following command:\n"
            "airflow dags test <DAG_ID> <LOGICAL_DATE> --save-dagrun output.png\n"
            "\n"
            "If you want to create a DOT file then you should execute the following command:\n"
            "airflow dags test <DAG_ID> <LOGICAL_DATE> --save-dagrun output.dot\n"
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_test"),
        args=(
            ARG_DAG_ID,
            ARG_LOGICAL_DATE_OPTIONAL,
            ARG_BUNDLE_NAME,
            ARG_DAGFILE_PATH,
            ARG_CONF,
            ARG_SHOW_DAGRUN,
            ARG_IMGCAT_DAGRUN,
            ARG_SAVE_DAGRUN,
            ARG_USE_EXECUTOR,
            ARG_VERBOSE,
            ARG_MARK_SUCCESS_PATTERN,
        ),
    ),
    ActionCommand(
        name="reserialize",
        help="Reserialize DAGs by parsing the DagBag files",
        description=(
            "Reserialize DAGs in the metadata DB. This can be "
            "particularly useful if your serialized DAGs become out of sync with the Airflow "
            "version you are using."
        ),
        func=lazy_load_command("airflow.cli.commands.dag_command.dag_reserialize"),
        args=(
            ARG_BUNDLE_NAME,
            ARG_VERBOSE,
        ),
    ),
)
TASKS_COMMANDS = (
    ActionCommand(
        name="list",
        help="List the tasks within a DAG",
        func=lazy_load_command("airflow.cli.commands.task_command.task_list"),
        args=(ARG_DAG_ID, ARG_BUNDLE_NAME, ARG_VERBOSE),
    ),
    ActionCommand(
        name="clear",
        help="Clear a set of task instance, as if they never ran",
        func=lazy_load_command("airflow.cli.commands.task_command.task_clear"),
        args=(
            ARG_DAG_ID,
            ARG_TASK_REGEX,
            ARG_START_DATE,
            ARG_END_DATE,
            ARG_BUNDLE_NAME,
            ARG_UPSTREAM,
            ARG_DOWNSTREAM,
            ARG_YES,
            ARG_ONLY_FAILED,
            ARG_ONLY_RUNNING,
            ARG_DAG_REGEX,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="state",
        help="Get the status of a task instance",
        func=lazy_load_command("airflow.cli.commands.task_command.task_state"),
        args=(
            ARG_DAG_ID,
            ARG_TASK_ID,
            ARG_LOGICAL_DATE_OR_RUN_ID,
            ARG_BUNDLE_NAME,
            ARG_VERBOSE,
            ARG_MAP_INDEX,
        ),
    ),
    ActionCommand(
        name="failed-deps",
        help="Returns the unmet dependencies for a task instance",
        description=(
            "Returns the unmet dependencies for a task instance from the perspective of the scheduler. "
            "In other words, why a task instance doesn't get scheduled and then queued by the scheduler, "
            "and then run by an executor."
        ),
        func=lazy_load_command("airflow.cli.commands.task_command.task_failed_deps"),
        args=(
            ARG_DAG_ID,
            ARG_TASK_ID,
            ARG_LOGICAL_DATE_OR_RUN_ID,
            ARG_BUNDLE_NAME,
            ARG_MAP_INDEX,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="render",
        help="Render a task instance's template(s)",
        func=lazy_load_command("airflow.cli.commands.task_command.task_render"),
        args=(
            ARG_DAG_ID,
            ARG_TASK_ID,
            ARG_LOGICAL_DATE_OR_RUN_ID,
            ARG_BUNDLE_NAME,
            ARG_VERBOSE,
            ARG_MAP_INDEX,
        ),
    ),
    ActionCommand(
        name="test",
        help="Test a task instance",
        description=(
            "Test a task instance. This will run a task without checking for dependencies or recording "
            "its state in the database"
        ),
        func=lazy_load_command("airflow.cli.commands.task_command.task_test"),
        args=(
            ARG_DAG_ID,
            ARG_TASK_ID,
            ARG_LOGICAL_DATE_OR_RUN_ID_OPTIONAL,
            ARG_BUNDLE_NAME,
            ARG_TASK_PARAMS,
            ARG_POST_MORTEM,
            ARG_ENV_VARS,
            ARG_MAP_INDEX,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="states-for-dag-run",
        help="Get the status of all task instances in a dag run",
        func=lazy_load_command("airflow.cli.commands.task_command.task_states_for_dag_run"),
        args=(ARG_DAG_ID, ARG_LOGICAL_DATE_OR_RUN_ID, ARG_OUTPUT, ARG_VERBOSE),
    ),
)
POOLS_COMMANDS = (
    ActionCommand(
        name="list",
        help="List pools",
        func=lazy_load_command("airflow.cli.commands.pool_command.pool_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="get",
        help="Get pool size",
        func=lazy_load_command("airflow.cli.commands.pool_command.pool_get"),
        args=(ARG_POOL_NAME, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="set",
        help="Configure pool",
        func=lazy_load_command("airflow.cli.commands.pool_command.pool_set"),
        args=(
            ARG_POOL_NAME,
            ARG_POOL_SLOTS,
            ARG_POOL_DESCRIPTION,
            ARG_POOL_INCLUDE_DEFERRED,
            ARG_OUTPUT,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="delete",
        help="Delete pool",
        func=lazy_load_command("airflow.cli.commands.pool_command.pool_delete"),
        args=(ARG_POOL_NAME, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="import",
        help="Import pools",
        func=lazy_load_command("airflow.cli.commands.pool_command.pool_import"),
        args=(ARG_POOL_IMPORT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="export",
        help="Export all pools",
        func=lazy_load_command("airflow.cli.commands.pool_command.pool_export"),
        args=(ARG_POOL_EXPORT, ARG_VERBOSE),
    ),
)
VARIABLES_COMMANDS = (
    ActionCommand(
        name="list",
        help="List variables",
        func=lazy_load_command("airflow.cli.commands.variable_command.variables_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="get",
        help="Get variable",
        func=lazy_load_command("airflow.cli.commands.variable_command.variables_get"),
        args=(ARG_VAR, ARG_DESERIALIZE_JSON, ARG_DEFAULT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="set",
        help="Set variable",
        func=lazy_load_command("airflow.cli.commands.variable_command.variables_set"),
        args=(ARG_VAR, ARG_VAR_VALUE, ARG_VAR_DESCRIPTION, ARG_SERIALIZE_JSON, ARG_VERBOSE),
    ),
    ActionCommand(
        name="delete",
        help="Delete variable",
        func=lazy_load_command("airflow.cli.commands.variable_command.variables_delete"),
        args=(ARG_VAR, ARG_VERBOSE),
    ),
    ActionCommand(
        name="import",
        help="Import variables",
        func=lazy_load_command("airflow.cli.commands.variable_command.variables_import"),
        args=(ARG_VAR_IMPORT, ARG_VAR_ACTION_ON_EXISTING_KEY, ARG_VERBOSE),
    ),
    ActionCommand(
        name="export",
        help="Export all variables",
        description=(
            "All variables can be exported in STDOUT using the following command:\n"
            "airflow variables export -\n"
        ),
        func=lazy_load_command("airflow.cli.commands.variable_command.variables_export"),
        args=(ARG_VAR_EXPORT, ARG_VERBOSE),
    ),
)
TEAMS_COMMANDS = (
    ActionCommand(
        name="create",
        help="Create a team",
        func=lazy_load_command("airflow.cli.commands.team_command.team_create"),
        args=(ARG_TEAM_NAME, ARG_VERBOSE),
    ),
    ActionCommand(
        name="delete",
        help="Delete a team",
        func=lazy_load_command("airflow.cli.commands.team_command.team_delete"),
        args=(ARG_TEAM_NAME, ARG_YES, ARG_VERBOSE),
    ),
    ActionCommand(
        name="list",
        help="List teams",
        func=lazy_load_command("airflow.cli.commands.team_command.team_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
)
DB_COMMANDS = (
    ActionCommand(
        name="check-migrations",
        help="Check if migration have finished",
        description="Check if migration have finished (or continually check until timeout)",
        func=lazy_load_command("airflow.cli.commands.db_command.check_migrations"),
        args=(ARG_MIGRATION_TIMEOUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="reset",
        help="Burn down and rebuild the metadata database",
        func=lazy_load_command("airflow.cli.commands.db_command.resetdb"),
        args=(ARG_YES, ARG_DB_SKIP_INIT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="migrate",
        help="Migrates the metadata database to the latest version",
        description=(
            "Migrate the schema of the metadata database. "
            "Create the database if it does not exist "
            "To print but not execute commands, use option ``--show-sql-only``. "
            "If using options ``--from-revision`` or ``--from-version``, you must also use "
            "``--show-sql-only``, because if actually *running* migrations, we should only "
            "migrate from the *current* Alembic revision."
        ),
        func=lazy_load_command("airflow.cli.commands.db_command.migratedb"),
        args=(
            ARG_DB_REVISION__UPGRADE,
            ARG_DB_VERSION__UPGRADE,
            ARG_DB_SQL_ONLY,
            ARG_DB_FROM_REVISION,
            ARG_DB_FROM_VERSION,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="downgrade",
        help="Downgrade the schema of the metadata database.",
        description=(
            "Downgrade the schema of the metadata database. "
            "You must provide either `--to-revision` or `--to-version`. "
            "To print but not execute commands, use option `--show-sql-only`. "
            "If using options `--from-revision` or `--from-version`, you must also use `--show-sql-only`, "
            "because if actually *running* migrations, we should only migrate from the *current* Alembic "
            "revision."
        ),
        func=lazy_load_command("airflow.cli.commands.db_command.downgrade"),
        args=(
            ARG_DB_REVISION__DOWNGRADE,
            ARG_DB_VERSION__DOWNGRADE,
            ARG_DB_SQL_ONLY,
            ARG_YES,
            ARG_DB_FROM_REVISION,
            ARG_DB_FROM_VERSION,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="shell",
        help="Runs a shell to access the database",
        func=lazy_load_command("airflow.cli.commands.db_command.shell"),
        args=(ARG_VERBOSE,),
    ),
    ActionCommand(
        name="check",
        help="Check if the database can be reached",
        func=lazy_load_command("airflow.cli.commands.db_command.check"),
        args=(ARG_VERBOSE, ARG_DB_RETRY, ARG_DB_RETRY_DELAY),
    ),
    ActionCommand(
        name="clean",
        help="Purge old records in metastore tables",
        func=lazy_load_command("airflow.cli.commands.db_command.cleanup_tables"),
        args=(
            ARG_DB_TABLES,
            ARG_DB_DRY_RUN,
            ARG_DB_CLEANUP_TIMESTAMP,
            ARG_VERBOSE,
            ARG_YES,
            ARG_DB_SKIP_ARCHIVE,
            ARG_DB_BATCH_SIZE,
        ),
    ),
    ActionCommand(
        name="export-archived",
        help="Export archived data from the archive tables",
        func=lazy_load_command("airflow.cli.commands.db_command.export_archived"),
        args=(
            ARG_DB_EXPORT_FORMAT,
            ARG_DB_OUTPUT_PATH,
            ARG_DB_DROP_ARCHIVES,
            ARG_DB_TABLES,
            ARG_YES,
        ),
    ),
    ActionCommand(
        name="drop-archived",
        help="Drop archived tables created through the db clean command",
        func=lazy_load_command("airflow.cli.commands.db_command.drop_archived"),
        args=(ARG_DB_TABLES, ARG_YES),
    ),
)
CONNECTIONS_COMMANDS = (
    ActionCommand(
        name="get",
        help="Get a connection",
        func=lazy_load_command("airflow.cli.commands.connection_command.connections_get"),
        args=(ARG_CONN_ID, ARG_COLOR, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="list",
        help="List connections",
        func=lazy_load_command("airflow.cli.commands.connection_command.connections_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="add",
        help="Add a connection",
        func=lazy_load_command("airflow.cli.commands.connection_command.connections_add"),
        args=(ARG_CONN_ID, ARG_CONN_URI, ARG_CONN_JSON, ARG_CONN_EXTRA, *ALTERNATIVE_CONN_SPECS_ARGS),
    ),
    ActionCommand(
        name="delete",
        help="Delete a connection",
        func=lazy_load_command("airflow.cli.commands.connection_command.connections_delete"),
        args=(ARG_CONN_ID, ARG_COLOR, ARG_VERBOSE),
    ),
    ActionCommand(
        name="export",
        help="Export all connections",
        description=(
            "All connections can be exported in STDOUT using the following command:\n"
            "airflow connections export -\n"
            "The file format can be determined by the provided file extension. E.g., The following "
            "command will export the connections in JSON format:\n"
            "airflow connections export /tmp/connections.json\n"
            "The --file-format parameter can be used to control the file format. E.g., "
            "the default format is JSON in STDOUT mode, which can be overridden using: \n"
            "airflow connections export - --file-format yaml\n"
            "The --file-format parameter can also be used for the files, for example:\n"
            "airflow connections export /tmp/connections --file-format json.\n"
            "When exporting in `env` file format, you control whether URI format or JSON format "
            "is used to serialize the connection by passing `uri` or `json` with option "
            "`--serialization-format`.\n"
        ),
        func=lazy_load_command("airflow.cli.commands.connection_command.connections_export"),
        args=(
            ARG_CONN_EXPORT,
            ARG_CONN_EXPORT_FORMAT,
            ARG_CONN_EXPORT_FILE_FORMAT,
            ARG_CONN_SERIALIZATION_FORMAT,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="import",
        help="Import connections from a file",
        description=(
            "Connections can be imported from the output of the export command.\n"
            "The filetype must by json, yaml or env and will be automatically inferred."
        ),
        func=lazy_load_command("airflow.cli.commands.connection_command.connections_import"),
        args=(
            ARG_CONN_IMPORT,
            ARG_CONN_OVERWRITE,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="test",
        help="Test a connection",
        func=lazy_load_command("airflow.cli.commands.connection_command.connections_test"),
        args=(ARG_CONN_ID, ARG_VERBOSE),
    ),
    ActionCommand(
        name="create-default-connections",
        help="Creates all the default connections from all the providers",
        func=lazy_load_command("airflow.cli.commands.connection_command.create_default_connections"),
        args=(ARG_VERBOSE,),
    ),
)
PROVIDERS_COMMANDS = (
    ActionCommand(
        name="list",
        help="List installed providers",
        func=lazy_load_command("airflow.cli.commands.provider_command.providers_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="get",
        help="Get detailed information about a provider",
        func=lazy_load_command("airflow.cli.commands.provider_command.provider_get"),
        args=(ARG_OUTPUT, ARG_VERBOSE, ARG_FULL, ARG_COLOR, ARG_PROVIDER_NAME),
    ),
    ActionCommand(
        name="links",
        help="List extra links registered by the providers",
        func=lazy_load_command("airflow.cli.commands.provider_command.extra_links_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="widgets",
        help="Get information about registered connection form widgets",
        func=lazy_load_command("airflow.cli.commands.provider_command.connection_form_widget_list"),
        args=(
            ARG_OUTPUT,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="hooks",
        help="List registered provider hooks",
        func=lazy_load_command("airflow.cli.commands.provider_command.hooks_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="triggers",
        help="List registered provider triggers",
        func=lazy_load_command("airflow.cli.commands.provider_command.triggers_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="behaviours",
        help="Get information about registered connection types with custom behaviours",
        func=lazy_load_command("airflow.cli.commands.provider_command.connection_field_behaviours"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="logging",
        help="Get information about task logging handlers provided",
        func=lazy_load_command("airflow.cli.commands.provider_command.logging_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="secrets",
        help="Get information about secrets backends provided",
        func=lazy_load_command("airflow.cli.commands.provider_command.secrets_backends_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="executors",
        help="Get information about executors provided",
        func=lazy_load_command("airflow.cli.commands.provider_command.executors_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="queues",
        help="Get information about queues provided",
        func=lazy_load_command("airflow.cli.commands.provider_command.queues_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="notifications",
        help="Get information about notifications provided",
        func=lazy_load_command("airflow.cli.commands.provider_command.notifications_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="configs",
        help="Get information about provider configuration",
        func=lazy_load_command("airflow.cli.commands.provider_command.config_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="lazy-loaded",
        help="Checks that provider configuration is lazy loaded",
        func=lazy_load_command("airflow.cli.commands.provider_command.lazy_loaded"),
        args=(ARG_VERBOSE,),
    ),
    ActionCommand(
        name="auth-managers",
        help="Get information about auth managers provided",
        func=lazy_load_command("airflow.cli.commands.provider_command.auth_managers_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
)


CONFIG_COMMANDS = (
    ActionCommand(
        name="get-value",
        help="Print the value of the configuration",
        func=lazy_load_command("airflow.cli.commands.config_command.get_value"),
        args=(
            ARG_SECTION,
            ARG_OPTION,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="list",
        help="List options for the configuration",
        func=lazy_load_command("airflow.cli.commands.config_command.show_config"),
        args=(
            ARG_OPTIONAL_SECTION,
            ARG_COLOR,
            ARG_INCLUDE_DESCRIPTIONS,
            ARG_INCLUDE_EXAMPLES,
            ARG_INCLUDE_SOURCES,
            ARG_INCLUDE_ENV_VARS,
            ARG_COMMENT_OUT_EVERYTHING,
            ARG_EXCLUDE_PROVIDERS,
            ARG_DEFAULTS,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="lint",
        help="lint options for the configuration changes while migrating from Airflow 2.x to Airflow 3.0",
        func=lazy_load_command("airflow.cli.commands.config_command.lint_config"),
        args=(
            ARG_LINT_CONFIG_SECTION,
            ARG_LINT_CONFIG_OPTION,
            ARG_LINT_CONFIG_IGNORE_SECTION,
            ARG_LINT_CONFIG_IGNORE_OPTION,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="update",
        help="update options for the configuration changes while migrating from Airflow 2.x to Airflow 3.0",
        func=lazy_load_command("airflow.cli.commands.config_command.update_config"),
        args=(
            ARG_UPDATE_CONFIG_SECTION,
            ARG_UPDATE_CONFIG_OPTION,
            ARG_UPDATE_CONFIG_IGNORE_SECTION,
            ARG_UPDATE_CONFIG_IGNORE_OPTION,
            ARG_VERBOSE,
            ARG_UPDATE_CONFIG_FIX,
            ARG_UPDATE_ALL_RECOMMENDATIONS,
        ),
    ),
)

JOBS_COMMANDS = (
    ActionCommand(
        name="check",
        help="Checks if job(s) are still alive",
        func=lazy_load_command("airflow.cli.commands.jobs_command.check"),
        args=(
            ARG_JOB_TYPE_FILTER,
            ARG_JOB_HOSTNAME_FILTER,
            ARG_JOB_HOSTNAME_CALLABLE_FILTER,
            ARG_JOB_LIMIT,
            ARG_ALLOW_MULTIPLE,
            ARG_VERBOSE,
        ),
        epilog=(
            "examples:\n"
            "To check if the local scheduler is still working properly, run:\n"
            "\n"
            '    $ airflow jobs check --job-type SchedulerJob --local"\n'
            "\n"
            "To check if any scheduler is running when you are using high availability, run:\n"
            "\n"
            "    $ airflow jobs check --job-type SchedulerJob --allow-multiple --limit 100"
        ),
    ),
)

DB_MANAGERS_COMMANDS = (
    ActionCommand(
        name="reset",
        help="Burn down and rebuild the specified external database",
        func=lazy_load_command("airflow.cli.commands.db_manager_command.resetdb"),
        args=(ARG_DB_MANAGER_PATH, ARG_YES, ARG_DB_SKIP_INIT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="migrate",
        help="Migrates the specified external database to the latest version",
        description=(
            "Migrate the schema of the metadata database. "
            "Create the database if it does not exist "
            "To print but not execute commands, use option ``--show-sql-only``. "
            "If using options ``--from-revision`` or ``--from-version``, you must also use "
            "``--show-sql-only``, because if actually *running* migrations, we should only "
            "migrate from the *current* Alembic revision."
        ),
        func=lazy_load_command("airflow.cli.commands.db_manager_command.migratedb"),
        args=(
            ARG_DB_MANAGER_PATH,
            ARG_DB_REVISION__UPGRADE,
            ARG_DB_VERSION__UPGRADE,
            ARG_DB_SQL_ONLY,
            ARG_DB_FROM_REVISION,
            ARG_DB_FROM_VERSION,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="downgrade",
        help="Downgrade the schema of the external metadata database.",
        description=(
            "Downgrade the schema of the metadata database. "
            "You must provide either `--to-revision` or `--to-version`. "
            "To print but not execute commands, use option `--show-sql-only`. "
            "If using options `--from-revision` or `--from-version`, you must also use `--show-sql-only`, "
            "because if actually *running* migrations, we should only migrate from the *current* Alembic "
            "revision."
        ),
        func=lazy_load_command("airflow.cli.commands.db_manager_command.downgrade"),
        args=(
            ARG_DB_MANAGER_PATH,
            ARG_DB_REVISION__DOWNGRADE,
            ARG_DB_VERSION__DOWNGRADE,
            ARG_DB_SQL_ONLY,
            ARG_YES,
            ARG_DB_FROM_REVISION,
            ARG_DB_FROM_VERSION,
            ARG_VERBOSE,
        ),
    ),
)
core_commands: list[CLICommand] = [
    GroupCommand(
        name="dags",
        help="Manage DAGs",
        subcommands=DAGS_COMMANDS,
    ),
    GroupCommand(
        name="backfill",
        help="Manage backfills",
        subcommands=BACKFILL_COMMANDS,
    ),
    GroupCommand(
        name="tasks",
        help="Manage tasks",
        subcommands=TASKS_COMMANDS,
    ),
    GroupCommand(
        name="assets",
        help="Manage assets",
        subcommands=ASSETS_COMMANDS,
    ),
    GroupCommand(
        name="pools",
        help="Manage pools",
        subcommands=POOLS_COMMANDS,
    ),
    GroupCommand(
        name="variables",
        help="Manage variables",
        subcommands=VARIABLES_COMMANDS,
    ),
    GroupCommand(
        name="teams",
        help="Manage teams",
        subcommands=TEAMS_COMMANDS,
    ),
    GroupCommand(
        name="jobs",
        help="Manage jobs",
        subcommands=JOBS_COMMANDS,
    ),
    GroupCommand(
        name="db",
        help="Database operations",
        subcommands=DB_COMMANDS,
    ),
    ActionCommand(
        name="kerberos",
        help="Start a kerberos ticket renewer",
        func=lazy_load_command("airflow.cli.commands.kerberos_command.kerberos"),
        args=(
            ARG_PRINCIPAL,
            ARG_KEYTAB,
            ARG_PID,
            ARG_DAEMON,
            ARG_KERBEROS_ONE_TIME_MODE,
            ARG_STDOUT,
            ARG_STDERR,
            ARG_LOG_FILE,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="api-server",
        help="Start an Airflow API server instance",
        func=lazy_load_command("airflow.cli.commands.api_server_command.api_server"),
        args=(
            ARG_API_SERVER_PORT,
            ARG_API_SERVER_WORKERS,
            ARG_API_SERVER_WORKER_TIMEOUT,
            ARG_API_SERVER_HOSTNAME,
            ARG_PID,
            ARG_DAEMON,
            ARG_STDOUT,
            ARG_STDERR,
            ARG_API_SERVER_LOG_CONFIG,
            ARG_API_SERVER_APPS,
            ARG_LOG_FILE,
            ARG_SSL_CERT,
            ARG_SSL_KEY,
            ARG_DEV,
            ARG_API_SERVER_ALLOW_PROXY_FORWARDING,
        ),
    ),
    ActionCommand(
        name="scheduler",
        help="Start a scheduler instance",
        func=lazy_load_command("airflow.cli.commands.scheduler_command.scheduler"),
        args=(
            ARG_NUM_RUNS,
            ARG_PID,
            ARG_DAEMON,
            ARG_STDOUT,
            ARG_STDERR,
            ARG_LOG_FILE,
            ARG_SKIP_SERVE_LOGS,
            ARG_VERBOSE,
            ARG_DEV,
        ),
        epilog=(
            "Signals:\n"
            "\n"
            "  - SIGUSR2: Dump a snapshot of task state being tracked by the executor.\n"
            "\n"
            "    Example:\n"
            '        pkill -f -USR2 "airflow scheduler"'
        ),
    ),
    ActionCommand(
        name="triggerer",
        help="Start a triggerer instance",
        func=lazy_load_command("airflow.cli.commands.triggerer_command.triggerer"),
        args=(
            ARG_PID,
            ARG_DAEMON,
            ARG_STDOUT,
            ARG_STDERR,
            ARG_LOG_FILE,
            ARG_CAPACITY,
            ARG_VERBOSE,
            ARG_SKIP_SERVE_LOGS,
            ARG_DEV,
            ARG_QUEUES,
        ),
    ),
    ActionCommand(
        name="dag-processor",
        help="Start a dag processor instance",
        func=lazy_load_command("airflow.cli.commands.dag_processor_command.dag_processor"),
        args=(
            ARG_PID,
            ARG_DAEMON,
            ARG_BUNDLE_NAME,
            ARG_NUM_RUNS,
            ARG_STDOUT,
            ARG_STDERR,
            ARG_LOG_FILE,
            ARG_VERBOSE,
            ARG_DEV,
        ),
    ),
    ActionCommand(
        name="version",
        help="Show the version",
        func=lazy_load_command("airflow.cli.commands.version_command.version"),
        args=(),
    ),
    ActionCommand(
        name="cheat-sheet",
        help="Display cheat sheet",
        func=lazy_load_command("airflow.cli.commands.cheat_sheet_command.cheat_sheet"),
        args=(ARG_VERBOSE,),
    ),
    GroupCommand(
        name="connections",
        help="Manage connections",
        subcommands=CONNECTIONS_COMMANDS,
    ),
    GroupCommand(
        name="providers",
        help="Display providers",
        subcommands=PROVIDERS_COMMANDS,
    ),
    ActionCommand(
        name="rotate-fernet-key",
        func=lazy_load_command("airflow.cli.commands.rotate_fernet_key_command.rotate_fernet_key"),
        help="Rotate encrypted connection credentials and variables",
        description=(
            "Rotate all encrypted connection credentials and variables; see "
            "https://airflow.apache.org/docs/apache-airflow/stable/howto/secure-connections.html"
            "#rotating-encryption-keys"
        ),
        args=(),
    ),
    GroupCommand(name="config", help="View configuration", subcommands=CONFIG_COMMANDS),
    ActionCommand(
        name="info",
        help="Show information about current Airflow and environment",
        func=lazy_load_command("airflow.cli.commands.info_command.show_info"),
        args=(
            ARG_ANONYMIZE,
            ARG_FILE_IO,
            ARG_VERBOSE,
            ARG_OUTPUT,
        ),
    ),
    ActionCommand(
        name="plugins",
        help="Dump information about loaded plugins",
        func=lazy_load_command("airflow.cli.commands.plugins_command.dump_plugins"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="standalone",
        help="Run an all-in-one copy of Airflow",
        func=lazy_load_command("airflow.cli.commands.standalone_command.standalone"),
        args=(),
    ),
    GroupCommand(
        name="db-manager",
        help="Manage externally connected database managers",
        subcommands=DB_MANAGERS_COMMANDS,
    ),
]


def _remove_dag_id_opt(command: ActionCommand):
    cmd = command._asdict()
    cmd["args"] = (arg for arg in command.args if arg is not ARG_DAG_ID)
    return ActionCommand(**cmd)


dag_cli_commands: list[CLICommand] = [
    GroupCommand(
        name="dags",
        help="Manage DAGs",
        subcommands=[
            _remove_dag_id_opt(sp)
            for sp in DAGS_COMMANDS
            if sp.name in ["backfill", "list-runs", "pause", "unpause", "test"]
        ],
    ),
    GroupCommand(
        name="tasks",
        help="Manage tasks",
        subcommands=[_remove_dag_id_opt(sp) for sp in TASKS_COMMANDS if sp.name in ["list", "test", "run"]],
    ),
]
DAG_CLI_DICT: dict[str, CLICommand] = {sp.name: sp for sp in dag_cli_commands}
