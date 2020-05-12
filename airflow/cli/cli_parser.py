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
"""Command-line interface"""

import argparse
import json
import os
import textwrap
from argparse import Action, ArgumentError, RawTextHelpFormatter
from typing import Callable, Dict, Iterable, List, NamedTuple, Set, Union

from tabulate import tabulate_formats

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.utils.cli import ColorMode
from airflow.utils.helpers import partition
from airflow.utils.module_loading import import_string
from airflow.utils.timezone import parse as parsedate

DAGS_FOLDER = settings.DAGS_FOLDER

BUILD_DOCS = "BUILDING_AIRFLOW_DOCS" in os.environ
if BUILD_DOCS:
    DAGS_FOLDER = '[AIRFLOW_HOME]/dags'


def lazy_load_command(import_path: str) -> Callable:
    """Create a lazy loader for command"""
    _, _, name = import_path.rpartition('.')

    def command(*args, **kwargs):
        func = import_string(import_path)
        return func(*args, **kwargs)

    command.__name__ = name  # type: ignore

    return command


class DefaultHelpParser(argparse.ArgumentParser):
    """CustomParser to display help message"""

    def _check_value(self, action, value):
        """Override _check_value and check conditionally added command"""
        executor = conf.get('core', 'EXECUTOR')
        if value == 'celery' and executor != ExecutorLoader.CELERY_EXECUTOR:
            message = f'celery subcommand works only with CeleryExecutor, your current executor: {executor}'
            raise ArgumentError(action, message)

        super()._check_value(action, value)

    def error(self, message):
        """Override error and use print_instead of print_usage"""
        self.print_help()
        self.exit(2, f'\n{self.prog} command error: {message}, see help above.\n')


# Used in Arg to enable `None' as a distinct value from "not passed"
_UNSET = object()


class Arg:
    """Class to keep information about command line argument"""
    # pylint: disable=redefined-builtin,unused-argument
    def __init__(self, flags=_UNSET, help=_UNSET, action=_UNSET, default=_UNSET, nargs=_UNSET, type=_UNSET,
                 choices=_UNSET, required=_UNSET, metavar=_UNSET):
        self.flags = flags
        self.kwargs = {}
        for k, v in locals().items():
            if v is _UNSET:
                continue
            if k in ("self", "flags"):
                continue

            self.kwargs[k] = v
    # pylint: enable=redefined-builtin,unused-argument

    def add_to_parser(self, parser: argparse.ArgumentParser):
        """Add this argument to an ArgumentParser"""
        parser.add_argument(*self.flags, **self.kwargs)


# Shared
ARG_DAG_ID = Arg(
    ("dag_id",),
    help="The id of the dag")
ARG_TASK_ID = Arg(
    ("task_id",),
    help="The id of the task")
ARG_EXECUTION_DATE = Arg(
    ("execution_date",),
    help="The execution date of the DAG",
    type=parsedate)
ARG_TASK_REGEX = Arg(
    ("-t", "--task-regex"),
    help="The regex to filter specific task_ids to backfill (optional)")
ARG_SUBDIR = Arg(
    ("-S", "--subdir"),
    help=(
        "File location or directory from which to look for the dag. "
        "Defaults to '[AIRFLOW_HOME]/dags' where [AIRFLOW_HOME] is the "
        "value you set for 'AIRFLOW_HOME' config you set in 'airflow.cfg' "),
    default=DAGS_FOLDER)
ARG_START_DATE = Arg(
    ("-s", "--start-date"),
    help="Override start_date YYYY-MM-DD",
    type=parsedate)
ARG_END_DATE = Arg(
    ("-e", "--end-date"),
    help="Override end_date YYYY-MM-DD",
    type=parsedate)
ARG_DRY_RUN = Arg(
    ("-n", "--dry-run"),
    help="Perform a dry run",
    action="store_true")
ARG_PID = Arg(
    ("--pid",),
    help="PID file location",
    nargs='?')
ARG_DAEMON = Arg(
    ("-D", "--daemon"),
    help="Daemonize instead of running in the foreground",
    action="store_true")
ARG_STDERR = Arg(
    ("--stderr",),
    help="Redirect stderr to this file")
ARG_STDOUT = Arg(
    ("--stdout",),
    help="Redirect stdout to this file")
ARG_LOG_FILE = Arg(
    ("-l", "--log-file"),
    help="Location of the log file")
ARG_YES = Arg(
    ("-y", "--yes"),
    help="Do not prompt to confirm reset. Use with care!",
    action="store_true",
    default=False)
ARG_OUTPUT = Arg(
    ("--output",),
    help=(
        "Output table format. The specified value is passed to "
        "the tabulate module (https://pypi.org/project/tabulate/). "
    ),
    choices=tabulate_formats,
    default="plain")
ARG_COLOR = Arg(
    ('--color',),
    help="Do emit colored output (default: auto)",
    choices={ColorMode.ON, ColorMode.OFF, ColorMode.AUTO},
    default=ColorMode.AUTO)

# list_dag_runs
ARG_NO_BACKFILL = Arg(
    ("--no-backfill",),
    help="filter all the backfill dagruns given the dag id",
    action="store_true")
ARG_STATE = Arg(
    ("--state",),
    help="Only list the dag runs corresponding to the state")

# list_jobs
ARG_LIMIT = Arg(
    ("--limit",),
    help="Return a limited number of records")

# backfill
ARG_MARK_SUCCESS = Arg(
    ("-m", "--mark-success"),
    help="Mark jobs as succeeded without running them",
    action="store_true")
ARG_VERBOSE = Arg(
    ("-v", "--verbose"),
    help="Make logging output more verbose",
    action="store_true")
ARG_LOCAL = Arg(
    ("-l", "--local"),
    help="Run the task using the LocalExecutor",
    action="store_true")
ARG_DONOT_PICKLE = Arg(
    ("-x", "--donot-pickle"),
    help=(
        "Do not attempt to pickle the DAG object to send over "
        "to the workers, just tell the workers to run their version "
        "of the code"),
    action="store_true")
ARG_BF_IGNORE_DEPENDENCIES = Arg(
    ("-i", "--ignore-dependencies"),
    help=(
        "Skip upstream tasks, run only the tasks "
        "matching the regexp. Only works in conjunction "
        "with task_regex"),
    action="store_true")
ARG_BF_IGNORE_FIRST_DEPENDS_ON_PAST = Arg(
    ("-I", "--ignore-first-depends-on-past"),
    help=(
        "Ignores depends_on_past dependencies for the first "
        "set of tasks only (subsequent executions in the backfill "
        "DO respect depends_on_past)"),
    action="store_true")
ARG_POOL = Arg(("--pool",), "Resource pool to use")
ARG_DELAY_ON_LIMIT = Arg(
    ("--delay-on-limit",),
    help=("Amount of time in seconds to wait when the limit "
          "on maximum active dag runs (max_active_runs) has "
          "been reached before trying to execute a dag run "
          "again"),
    type=float,
    default=1.0)
ARG_RESET_DAG_RUN = Arg(
    ("--reset-dagruns",),
    help=(
        "if set, the backfill will delete existing "
        "backfill-related DAG runs and start "
        "anew with fresh, running DAG runs"),
    action="store_true")
ARG_RERUN_FAILED_TASKS = Arg(
    ("--rerun-failed-tasks",),
    help=(
        "if set, the backfill will auto-rerun "
        "all the failed tasks for the backfill date range "
        "instead of throwing exceptions"),
    action="store_true")
ARG_RUN_BACKWARDS = Arg(
    ("-B", "--run-backwards",),
    help=(
        "if set, the backfill will run tasks from the most "
        "recent day first.  if there are tasks that depend_on_past "
        "this option will throw an exception"),
    action="store_true")
# test_dag
ARG_SHOW_DAGRUN = Arg(
    ("--show-dagrun", ),
    help=(
        "After completing the backfill, shows the diagram for current DAG Run.\n"
        "\n"
        "The diagram is in DOT language\n"),
    action='store_true')
ARG_IMGCAT_DAGRUN = Arg(
    ("--imgcat-dagrun", ),
    help=(
        "After completing the dag run, prints a diagram on the screen for the "
        "current DAG Run using the imgcat tool.\n"
        "\n"
        "For more information, see: https://www.iterm2.com/documentation-images.html"
    ),
    action='store_true')
ARG_SAVE_DAGRUN = Arg(
    ("--save-dagrun", ),
    help=(
        "After completing the backfill, saves the diagram for current DAG Run to the indicated file.\n"
        "\n"
        "The file format is determined by the file extension. For more information about supported "
        "format, see: https://www.graphviz.org/doc/info/output.html\n"
        "\n"
        "If you want to create a PNG file then you should execute the following command:\n"
        "airflow dags test <DAG_ID> <EXECUTION_DATE> --save-dagrun output.png\n"
        "\n"
        "If you want to create a DOT file then you should execute the following command:\n"
        "airflow dags test <DAG_ID> <EXECUTION_DATE> --save-dagrun output.dot\n"))

# list_tasks
ARG_TREE = Arg(
    ("-t", "--tree"),
    help="Tree view",
    action="store_true")

# clear
ARG_UPSTREAM = Arg(
    ("-u", "--upstream"),
    help="Include upstream tasks",
    action="store_true")
ARG_ONLY_FAILED = Arg(
    ("-f", "--only-failed"),
    help="Only failed jobs",
    action="store_true")
ARG_ONLY_RUNNING = Arg(
    ("-r", "--only-running"),
    help="Only running jobs",
    action="store_true")
ARG_DOWNSTREAM = Arg(
    ("-d", "--downstream"),
    help="Include downstream tasks",
    action="store_true")
ARG_EXCLUDE_SUBDAGS = Arg(
    ("-x", "--exclude-subdags"),
    help="Exclude subdags",
    action="store_true")
ARG_EXCLUDE_PARENTDAG = Arg(
    ("-X", "--exclude-parentdag"),
    help="Exclude ParentDAGS if the task cleared is a part of a SubDAG",
    action="store_true")
ARG_DAG_REGEX = Arg(
    ("-R", "--dag-regex"),
    help="Search dag_id as regex instead of exact string",
    action="store_true")

# show_dag
ARG_SAVE = Arg(
    ("-s", "--save"),
    help=(
        "Saves the result to the indicated file.\n"
        "\n"
        "The file format is determined by the file extension. For more information about supported "
        "format, see: https://www.graphviz.org/doc/info/output.html\n"
        "\n"
        "If you want to create a PNG file then you should execute the following command:\n"
        "airflow dags show <DAG_ID> --save output.png\n"
        "\n"
        "If you want to create a DOT file then you should execute the following command:\n"
        "airflow dags show <DAG_ID> --save output.dot\n"))
ARG_IMGCAT = Arg(
    ("--imgcat", ),
    help=(
        "Displays graph using the imgcat tool. \n"
        "\n"
        "For more information, see: https://www.iterm2.com/documentation-images.html"),
    action='store_true')

# trigger_dag
ARG_RUN_ID = Arg(
    ("-r", "--run-id"),
    help="Helps to identify this run")
ARG_CONF = Arg(
    ('-c', '--conf'),
    help="JSON string that gets pickled into the DagRun's conf attribute")
ARG_EXEC_DATE = Arg(
    ("-e", "--exec-date"),
    help="The execution date of the DAG",
    type=parsedate)

# pool
ARG_POOL_NAME = Arg(
    ("pool",),
    metavar='NAME',
    help="Pool name")
ARG_POOL_SLOTS = Arg(
    ("slots",),
    type=int,
    help="Pool slots")
ARG_POOL_DESCRIPTION = Arg(
    ("description",),
    help="Pool description")
ARG_POOL_IMPORT = Arg(
    ("file",),
    metavar="FILEPATH",
    help="Import pools from JSON file")
ARG_POOL_EXPORT = Arg(
    ("file",),
    metavar="FILEPATH",
    help="Export all pools to JSON file")

# variables
ARG_VAR = Arg(
    ("key",),
    help="Variable key")
ARG_VAR_VALUE = Arg(
    ("value",),
    metavar='VALUE',
    help="Variable value")
ARG_DEFAULT = Arg(
    ("-d", "--default"),
    metavar="VAL",
    default=None,
    help="Default value returned if variable does not exist")
ARG_JSON = Arg(
    ("-j", "--json"),
    help="Deserialize JSON variable",
    action="store_true")
ARG_VAR_IMPORT = Arg(
    ("file",),
    help="Import variables from JSON file")
ARG_VAR_EXPORT = Arg(
    ("file",),
    help="Export all variables to JSON file")

# kerberos
ARG_PRINCIPAL = Arg(
    ("principal",),
    help="kerberos principal",
    nargs='?')
ARG_KEYTAB = Arg(
    ("-k", "--keytab"),
    help="keytab",
    nargs='?',
    default=conf.get('kerberos', 'keytab'))
# run
# TODO(aoen): "force" is a poor choice of name here since it implies it overrides
# all dependencies (not just past success), e.g. the ignore_depends_on_past
# dependency. This flag should be deprecated and renamed to 'ignore_ti_state' and
# the "ignore_all_dependencies" command should be called the"force" command
# instead.
ARG_INTERACTIVE = Arg(
    ('-N', '--interactive'),
    help='Do not capture standard output and error streams '
         '(useful for interactive debugging)',
    action='store_true')
ARG_FORCE = Arg(
    ("-f", "--force"),
    help="Ignore previous task instance state, rerun regardless if task already succeeded/failed",
    action="store_true")
ARG_RAW = Arg(("-r", "--raw"), argparse.SUPPRESS, "store_true")
ARG_IGNORE_ALL_DEPENDENCIES = Arg(
    ("-A", "--ignore-all-dependencies"),
    help="Ignores all non-critical dependencies, including ignore_ti_state and ignore_task_deps",
    action="store_true")
# TODO(aoen): ignore_dependencies is a poor choice of name here because it is too
# vague (e.g. a task being in the appropriate state to be run is also a dependency
# but is not ignored by this flag), the name 'ignore_task_dependencies' is
# slightly better (as it ignores all dependencies that are specific to the task),
# so deprecate the old command name and use this instead.
ARG_IGNORE_DEPENDENCIES = Arg(
    ("-i", "--ignore-dependencies"),
    help="Ignore task-specific dependencies, e.g. upstream, depends_on_past, and retry delay dependencies",
    action="store_true")
ARG_IGNORE_DEPENDS_ON_PAST = Arg(
    ("-I", "--ignore-depends-on-past"),
    help="Ignore depends_on_past dependencies (but respect upstream dependencies)",
    action="store_true")
ARG_SHIP_DAG = Arg(
    ("--ship-dag",),
    help="Pickles (serializes) the DAG and ships it to the worker",
    action="store_true")
ARG_PICKLE = Arg(
    ("-p", "--pickle"),
    help="Serialized pickle object of the entire dag (used internally)")
ARG_JOB_ID = Arg(
    ("-j", "--job-id"),
    help=argparse.SUPPRESS)
ARG_CFG_PATH = Arg(
    ("--cfg-path",),
    help="Path to config file to use instead of airflow.cfg")
ARG_MIGRATION_TIMEOUT = Arg(
    ("-t", "--migration-wait-timeout"),
    help="timeout to wait for db to migrate ",
    type=int,
    default=0,
)

# webserver
ARG_PORT = Arg(
    ("-p", "--port"),
    default=conf.get('webserver', 'WEB_SERVER_PORT'),
    type=int,
    help="The port on which to run the server")
ARG_SSL_CERT = Arg(
    ("--ssl-cert",),
    default=conf.get('webserver', 'WEB_SERVER_SSL_CERT'),
    help="Path to the SSL certificate for the webserver")
ARG_SSL_KEY = Arg(
    ("--ssl-key",),
    default=conf.get('webserver', 'WEB_SERVER_SSL_KEY'),
    help="Path to the key to use with the SSL certificate")
ARG_WORKERS = Arg(
    ("-w", "--workers"),
    default=conf.get('webserver', 'WORKERS'),
    type=int,
    help="Number of workers to run the webserver on")
ARG_WORKERCLASS = Arg(
    ("-k", "--workerclass"),
    default=conf.get('webserver', 'WORKER_CLASS'),
    choices=['sync', 'eventlet', 'gevent', 'tornado'],
    help="The worker class to use for Gunicorn")
ARG_WORKER_TIMEOUT = Arg(
    ("-t", "--worker-timeout"),
    default=conf.get('webserver', 'WEB_SERVER_WORKER_TIMEOUT'),
    type=int,
    help="The timeout for waiting on webserver workers")
ARG_HOSTNAME = Arg(
    ("-H", "--hostname"),
    default=conf.get('webserver', 'WEB_SERVER_HOST'),
    help="Set the hostname on which to run the web server")
ARG_DEBUG = Arg(
    ("-d", "--debug"),
    help="Use the server that ships with Flask in debug mode",
    action="store_true")
ARG_ACCESS_LOGFILE = Arg(
    ("-A", "--access-logfile"),
    default=conf.get('webserver', 'ACCESS_LOGFILE'),
    help="The logfile to store the webserver access log. Use '-' to print to "
         "stderr")
ARG_ERROR_LOGFILE = Arg(
    ("-E", "--error-logfile"),
    default=conf.get('webserver', 'ERROR_LOGFILE'),
    help="The logfile to store the webserver error log. Use '-' to print to "
         "stderr")

# scheduler
ARG_DAG_ID_OPT = Arg(
    ("-d", "--dag-id"),
    help="The id of the dag to run"
)
ARG_NUM_RUNS = Arg(
    ("-n", "--num-runs"),
    default=conf.getint('scheduler', 'num_runs'),
    type=int,
    help="Set the number of runs to execute before exiting")

# worker
ARG_DO_PICKLE = Arg(
    ("-p", "--do-pickle"),
    default=False,
    help=(
        "Attempt to pickle the DAG object to send over "
        "to the workers, instead of letting workers run their version "
        "of the code"),
    action="store_true")
ARG_QUEUES = Arg(
    ("-q", "--queues"),
    help="Comma delimited list of queues to serve",
    default=conf.get('celery', 'DEFAULT_QUEUE'))
ARG_CONCURRENCY = Arg(
    ("-c", "--concurrency"),
    type=int,
    help="The number of worker processes",
    default=conf.get('celery', 'worker_concurrency'))
ARG_CELERY_HOSTNAME = Arg(
    ("-H", "--celery-hostname"),
    help=("Set the hostname of celery worker "
          "if you have multiple workers on a single machine"))
ARG_UMASK = Arg(
    ("-u", "--umask"),
    help="Set the umask of celery worker in daemon mode",
    default=conf.get('celery', 'worker_umask'))

# flower
ARG_BROKER_API = Arg(("-a", "--broker-api"), help="Broker API")
ARG_FLOWER_HOSTNAME = Arg(
    ("-H", "--hostname"),
    default=conf.get('celery', 'FLOWER_HOST'),
    help="Set the hostname on which to run the server")
ARG_FLOWER_PORT = Arg(
    ("-p", "--port"),
    default=conf.get('celery', 'FLOWER_PORT'),
    type=int,
    help="The port on which to run the server")
ARG_FLOWER_CONF = Arg(
    ("-c", "--flower-conf"),
    help="Configuration file for flower")
ARG_FLOWER_URL_PREFIX = Arg(
    ("-u", "--url-prefix"),
    default=conf.get('celery', 'FLOWER_URL_PREFIX'),
    help="URL prefix for Flower")
ARG_FLOWER_BASIC_AUTH = Arg(
    ("-A", "--basic-auth"),
    default=conf.get('celery', 'FLOWER_BASIC_AUTH'),
    help=("Securing Flower with Basic Authentication. "
          "Accepts user:password pairs separated by a comma. "
          "Example: flower_basic_auth = user1:password1,user2:password2"))
ARG_TASK_PARAMS = Arg(
    ("-t", "--task-params"),
    help="Sends a JSON params dict to the task")
ARG_POST_MORTEM = Arg(
    ("-m", "--post-mortem"),
    action="store_true",
    help="Open debugger on uncaught exception")
ARG_ENV_VARS = Arg(
    ("--env-vars", ),
    help="Set env var in both parsing time and runtime for each of entry supplied in a JSON dict",
    type=json.loads)

# connections
ARG_CONN_ID = Arg(
    ('conn_id',),
    help='Connection id, required to add/delete a connection',
    type=str)
ARG_CONN_ID_FILTER = Arg(
    ('--conn-id',),
    help='If passed, only items with the specified connection ID will be displayed',
    type=str)
ARG_CONN_URI = Arg(
    ('--conn-uri',),
    help='Connection URI, required to add a connection without conn_type',
    type=str)
ARG_CONN_TYPE = Arg(
    ('--conn-type',),
    help='Connection type, required to add a connection without conn_uri',
    type=str)
ARG_CONN_HOST = Arg(
    ('--conn-host',),
    help='Connection host, optional when adding a connection',
    type=str)
ARG_CONN_LOGIN = Arg(
    ('--conn-login',),
    help='Connection login, optional when adding a connection',
    type=str)
ARG_CONN_PASSWORD = Arg(
    ('--conn-password',),
    help='Connection password, optional when adding a connection',
    type=str)
ARG_CONN_SCHEMA = Arg(
    ('--conn-schema',),
    help='Connection schema, optional when adding a connection',
    type=str)
ARG_CONN_PORT = Arg(
    ('--conn-port',),
    help='Connection port, optional when adding a connection',
    type=str)
ARG_CONN_EXTRA = Arg(
    ('--conn-extra',),
    help='Connection `Extra` field, optional when adding a connection',
    type=str)
ARG_INCLUDE_SECRETS = Arg(
    ('--include-secrets',),
    help=(
        "If passed, the connection in the secret backend will also be displayed."
        "To use this option you must pass `--conn_id` option."
        ""
    ),
    action="store_true",
    default=False)
# users
ARG_USERNAME = Arg(
    ('-u', '--username'),
    help='Username of the user',
    required=True,
    type=str)
ARG_USERNAME_OPTIONAL = Arg(
    ('-u', '--username'),
    help='Username of the user',
    type=str)
ARG_FIRSTNAME = Arg(
    ('-f', '--firstname'),
    help='First name of the user',
    required=True,
    type=str)
ARG_LASTNAME = Arg(
    ('-l', '--lastname'),
    help='Last name of the user',
    required=True,
    type=str)
ARG_ROLE = Arg(
    ('-r', '--role'),
    help='Role of the user. Existing roles include Admin, '
         'User, Op, Viewer, and Public',
    required=True,
    type=str,)
ARG_EMAIL = Arg(
    ('-e', '--email'),
    help='Email of the user',
    required=True,
    type=str)
ARG_EMAIL_OPTIONAL = Arg(
    ('-e', '--email'),
    help='Email of the user',
    type=str)
ARG_PASSWORD = Arg(
    ('-p', '--password'),
    help='Password of the user, required to create a user '
         'without --use-random-password',
    type=str)
ARG_USE_RANDOM_PASSWORD = Arg(
    ('--use-random-password',),
    help='Do not prompt for password. Use random string instead.'
         ' Required to create a user without --password ',
    default=False,
    action='store_true')
ARG_USER_IMPORT = Arg(
    ("import",),
    metavar="FILEPATH",
    help="Import users from JSON file. Example format::\n" +
         textwrap.indent(textwrap.dedent('''
            [
                {
                    "email": "foo@bar.org",
                    "firstname": "Jon",
                    "lastname": "Doe",
                    "roles": ["Public"],
                    "username": "jondoe"
                }
            ]'''), " " * 4))
ARG_USER_EXPORT = Arg(
    ("export",),
    metavar="FILEPATH",
    help="Export all users to JSON file")

# roles
ARG_CREATE_ROLE = Arg(
    ('-c', '--create'),
    help='Create a new role',
    action='store_true')
ARG_LIST_ROLES = Arg(
    ('-l', '--list'),
    help='List roles',
    action='store_true')
ARG_ROLES = Arg(
    ('role',),
    help='The name of a role',
    nargs='*')
ARG_AUTOSCALE = Arg(
    ('-a', '--autoscale'),
    help="Minimum and Maximum number of worker to autoscale")
ARG_SKIP_SERVE_LOGS = Arg(
    ("-s", "--skip-serve-logs"),
    default=False,
    help="Don't start the serve logs process along with the workers",
    action="store_true")

# info
ARG_ANONYMIZE = Arg(
    ('--anonymize',),
    help=(
        'Minimize any personal identifiable information. '
        'Use it when sharing output with others.'
    ),
    action='store_true'
)
ARG_FILE_IO = Arg(
    ('--file-io',),
    help=(
        'Send output to file.io service and returns link.'
    ),
    action='store_true'
)

ALTERNATIVE_CONN_SPECS_ARGS = [
    ARG_CONN_TYPE, ARG_CONN_HOST, ARG_CONN_LOGIN, ARG_CONN_PASSWORD, ARG_CONN_SCHEMA, ARG_CONN_PORT
]


class ActionCommand(NamedTuple):
    """Single CLI command"""
    name: str
    help: str
    func: Callable
    args: Iterable[Arg]


class GroupCommand(NamedTuple):
    """ClI command with subcommands"""
    name: str
    help: str
    subcommands: Iterable


CLICommand = Union[ActionCommand, GroupCommand]


DAGS_COMMANDS = (
    ActionCommand(
        name='list',
        help="List all the DAGs",
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_list_dags'),
        args=(ARG_SUBDIR, ARG_OUTPUT),
    ),
    ActionCommand(
        name='report',
        help='Show DagBag loading report',
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_report'),
        args=(ARG_SUBDIR, ARG_OUTPUT),
    ),
    ActionCommand(
        name='list_runs',
        help=(
            "List dag runs given a DAG id. If state option is given, it will only search for all the "
            "dagruns with the given state. If no_backfill option is given, it will filter out all "
            "backfill dagruns for given dag id. If start_date is given, it will filter out all the "
            "dagruns that were executed before this date. If end_date is given, it will filter out "
            "all the dagruns that were executed after this date. "
        ),
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_list_dag_runs'),
        args=(ARG_DAG_ID_OPT, ARG_NO_BACKFILL, ARG_STATE, ARG_OUTPUT, ARG_START_DATE, ARG_END_DATE),
    ),
    ActionCommand(
        name='list_jobs',
        help="List the jobs",
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_list_jobs'),
        args=(ARG_DAG_ID_OPT, ARG_STATE, ARG_LIMIT, ARG_OUTPUT,),
    ),
    ActionCommand(
        name='state',
        help="Get the status of a dag run",
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_state'),
        args=(ARG_DAG_ID, ARG_EXECUTION_DATE, ARG_SUBDIR),
    ),
    ActionCommand(
        name='next_execution',
        help="Get the next execution datetime of a DAG",
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_next_execution'),
        args=(ARG_DAG_ID, ARG_SUBDIR),
    ),
    ActionCommand(
        name='pause',
        help='Pause a DAG',
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_pause'),
        args=(ARG_DAG_ID, ARG_SUBDIR),
    ),
    ActionCommand(
        name='unpause',
        help='Resume a paused DAG',
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_unpause'),
        args=(ARG_DAG_ID, ARG_SUBDIR),
    ),
    ActionCommand(
        name='trigger',
        help='Trigger a DAG run',
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_trigger'),
        args=(ARG_DAG_ID, ARG_SUBDIR, ARG_RUN_ID, ARG_CONF, ARG_EXEC_DATE),
    ),
    ActionCommand(
        name='delete',
        help="Delete all DB records related to the specified DAG",
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_delete'),
        args=(ARG_DAG_ID, ARG_YES),
    ),
    ActionCommand(
        name='show',
        help="Displays DAG's tasks with their dependencies",
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_show'),
        args=(ARG_DAG_ID, ARG_SUBDIR, ARG_SAVE, ARG_IMGCAT,),
    ),
    ActionCommand(
        name='backfill',
        help=(
            "Run subsections of a DAG for a specified date range. If reset_dag_run option is used, "
            "backfill will first prompt users whether airflow should clear all the previous dag_run and "
            "task_instances within the backfill date range. If rerun_failed_tasks is used, backfill "
            "will auto re-run the previous failed task instances  within the backfill date range"
        ),
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_backfill'),
        args=(
            ARG_DAG_ID, ARG_TASK_REGEX, ARG_START_DATE, ARG_END_DATE, ARG_MARK_SUCCESS, ARG_LOCAL,
            ARG_DONOT_PICKLE, ARG_YES, ARG_BF_IGNORE_DEPENDENCIES, ARG_BF_IGNORE_FIRST_DEPENDS_ON_PAST,
            ARG_SUBDIR, ARG_POOL, ARG_DELAY_ON_LIMIT, ARG_DRY_RUN, ARG_VERBOSE, ARG_CONF,
            ARG_RESET_DAG_RUN, ARG_RERUN_FAILED_TASKS, ARG_RUN_BACKWARDS
        ),
    ),
    ActionCommand(
        name='test',
        help="Execute one run of a DAG",
        func=lazy_load_command('airflow.cli.commands.dag_command.dag_test'),
        args=(
            ARG_DAG_ID, ARG_EXECUTION_DATE, ARG_SUBDIR, ARG_SHOW_DAGRUN, ARG_IMGCAT_DAGRUN, ARG_SAVE_DAGRUN
        ),
    ),
)
TASKS_COMMANDS = (
    ActionCommand(
        name='list',
        help="List the tasks within a DAG",
        func=lazy_load_command('airflow.cli.commands.task_command.task_list'),
        args=(ARG_DAG_ID, ARG_TREE, ARG_SUBDIR),
    ),
    ActionCommand(
        name='clear',
        help="Clear a set of task instance, as if they never ran",
        func=lazy_load_command('airflow.cli.commands.task_command.task_clear'),
        args=(
            ARG_DAG_ID, ARG_TASK_REGEX, ARG_START_DATE, ARG_END_DATE, ARG_SUBDIR, ARG_UPSTREAM,
            ARG_DOWNSTREAM, ARG_YES, ARG_ONLY_FAILED, ARG_ONLY_RUNNING, ARG_EXCLUDE_SUBDAGS,
            ARG_EXCLUDE_PARENTDAG, ARG_DAG_REGEX
        ),
    ),
    ActionCommand(
        name='state',
        help="Get the status of a task instance",
        func=lazy_load_command('airflow.cli.commands.task_command.task_state'),
        args=(ARG_DAG_ID, ARG_TASK_ID, ARG_EXECUTION_DATE, ARG_SUBDIR),
    ),
    ActionCommand(
        name='failed_deps',
        help=(
            "Returns the unmet dependencies for a task instance from the perspective of the scheduler. "
            "In other words, why a task instance doesn't get scheduled and then queued by the scheduler, "
            "and then run by an executor."
        ),
        func=lazy_load_command('airflow.cli.commands.task_command.task_failed_deps'),
        args=(ARG_DAG_ID, ARG_TASK_ID, ARG_EXECUTION_DATE, ARG_SUBDIR),
    ),
    ActionCommand(
        name='render',
        help="Render a task instance's template(s)",
        func=lazy_load_command('airflow.cli.commands.task_command.task_render'),
        args=(ARG_DAG_ID, ARG_TASK_ID, ARG_EXECUTION_DATE, ARG_SUBDIR),
    ),
    ActionCommand(
        name='run',
        help="Run a single task instance",
        func=lazy_load_command('airflow.cli.commands.task_command.task_run'),
        args=(
            ARG_DAG_ID, ARG_TASK_ID, ARG_EXECUTION_DATE, ARG_SUBDIR, ARG_MARK_SUCCESS, ARG_FORCE,
            ARG_POOL, ARG_CFG_PATH, ARG_LOCAL, ARG_RAW, ARG_IGNORE_ALL_DEPENDENCIES,
            ARG_IGNORE_DEPENDENCIES, ARG_IGNORE_DEPENDS_ON_PAST, ARG_SHIP_DAG, ARG_PICKLE, ARG_JOB_ID,
            ARG_INTERACTIVE,
        ),
    ),
    ActionCommand(
        name='test',
        help=(
            "Test a task instance. This will run a task without checking for dependencies or recording "
            "its state in the database"
        ),
        func=lazy_load_command('airflow.cli.commands.task_command.task_test'),
        args=(
            ARG_DAG_ID, ARG_TASK_ID, ARG_EXECUTION_DATE, ARG_SUBDIR, ARG_DRY_RUN,
            ARG_TASK_PARAMS, ARG_POST_MORTEM, ARG_ENV_VARS
        ),
    ),
    ActionCommand(
        name='states_for_dag_run',
        help="Get the status of all task instances in a dag run",
        func=lazy_load_command('airflow.cli.commands.task_command.task_states_for_dag_run'),
        args=(ARG_DAG_ID, ARG_EXECUTION_DATE, ARG_OUTPUT),
    ),
)
POOLS_COMMANDS = (
    ActionCommand(
        name='list',
        help='List pools',
        func=lazy_load_command('airflow.cli.commands.pool_command.pool_list'),
        args=(ARG_OUTPUT,),
    ),
    ActionCommand(
        name='get',
        help='Get pool size',
        func=lazy_load_command('airflow.cli.commands.pool_command.pool_get'),
        args=(ARG_POOL_NAME, ARG_OUTPUT,),
    ),
    ActionCommand(
        name='set',
        help='Configure pool',
        func=lazy_load_command('airflow.cli.commands.pool_command.pool_set'),
        args=(ARG_POOL_NAME, ARG_POOL_SLOTS, ARG_POOL_DESCRIPTION, ARG_OUTPUT,),
    ),
    ActionCommand(
        name='delete',
        help='Delete pool',
        func=lazy_load_command('airflow.cli.commands.pool_command.pool_delete'),
        args=(ARG_POOL_NAME, ARG_OUTPUT,),
    ),
    ActionCommand(
        name='import',
        help='Import pools',
        func=lazy_load_command('airflow.cli.commands.pool_command.pool_import'),
        args=(ARG_POOL_IMPORT, ARG_OUTPUT,),
    ),
    ActionCommand(
        name='export',
        help='Export all pools',
        func=lazy_load_command('airflow.cli.commands.pool_command.pool_export'),
        args=(ARG_POOL_EXPORT, ARG_OUTPUT,),
    ),
)
VARIABLES_COMMANDS = (
    ActionCommand(
        name='list',
        help='List variables',
        func=lazy_load_command('airflow.cli.commands.variable_command.variables_list'),
        args=(),
    ),
    ActionCommand(
        name='get',
        help='Get variable',
        func=lazy_load_command('airflow.cli.commands.variable_command.variables_get'),
        args=(ARG_VAR, ARG_JSON, ARG_DEFAULT),
    ),
    ActionCommand(
        name='set',
        help='Set variable',
        func=lazy_load_command('airflow.cli.commands.variable_command.variables_set'),
        args=(ARG_VAR, ARG_VAR_VALUE, ARG_JSON),
    ),
    ActionCommand(
        name='delete',
        help='Delete variable',
        func=lazy_load_command('airflow.cli.commands.variable_command.variables_delete'),
        args=(ARG_VAR,),
    ),
    ActionCommand(
        name='import',
        help='Import variables',
        func=lazy_load_command('airflow.cli.commands.variable_command.variables_import'),
        args=(ARG_VAR_IMPORT,),
    ),
    ActionCommand(
        name='export',
        help='Export all variables',
        func=lazy_load_command('airflow.cli.commands.variable_command.variables_export'),
        args=(ARG_VAR_EXPORT,),
    ),
)
DB_COMMANDS = (
    ActionCommand(
        name='init',
        help="Initialize the metadata database",
        func=lazy_load_command('airflow.cli.commands.db_command.initdb'),
        args=(),
    ),
    ActionCommand(
        name="check-migrations",
        help="Check if migration have finished (or continually check until timeout)",
        func=lazy_load_command('airflow.cli.commands.db_command.check_migrations'),
        args=(ARG_MIGRATION_TIMEOUT,),
    ),
    ActionCommand(
        name='reset',
        help="Burn down and rebuild the metadata database",
        func=lazy_load_command('airflow.cli.commands.db_command.resetdb'),
        args=(ARG_YES,),
    ),
    ActionCommand(
        name='upgrade',
        help="Upgrade the metadata database to latest version",
        func=lazy_load_command('airflow.cli.commands.db_command.upgradedb'),
        args=(),
    ),
    ActionCommand(
        name='shell',
        help="Runs a shell to access the database",
        func=lazy_load_command('airflow.cli.commands.db_command.shell'),
        args=(),
    ),
    ActionCommand(
        name='check',
        help="Check if the database can be reached.",
        func=lazy_load_command('airflow.cli.commands.db_command.check'),
        args=(),
    ),
)
CONNECTIONS_COMMANDS = (
    ActionCommand(
        name='list',
        help='List connections',
        func=lazy_load_command('airflow.cli.commands.connection_command.connections_list'),
        args=(ARG_OUTPUT, ARG_CONN_ID_FILTER, ARG_INCLUDE_SECRETS),
    ),
    ActionCommand(
        name='add',
        help='Add a connection',
        func=lazy_load_command('airflow.cli.commands.connection_command.connections_add'),
        args=(ARG_CONN_ID, ARG_CONN_URI, ARG_CONN_EXTRA) + tuple(ALTERNATIVE_CONN_SPECS_ARGS),
    ),
    ActionCommand(
        name='delete',
        help='Delete a connection',
        func=lazy_load_command('airflow.cli.commands.connection_command.connections_delete'),
        args=(ARG_CONN_ID,),
    ),
)
USERS_COMMANDS = (
    ActionCommand(
        name='list',
        help='List users',
        func=lazy_load_command('airflow.cli.commands.user_command.users_list'),
        args=(ARG_OUTPUT,),
    ),
    ActionCommand(
        name='create',
        help='Create a user',
        func=lazy_load_command('airflow.cli.commands.user_command.users_create'),
        args=(
            ARG_ROLE, ARG_USERNAME, ARG_EMAIL, ARG_FIRSTNAME, ARG_LASTNAME, ARG_PASSWORD,
            ARG_USE_RANDOM_PASSWORD
        )
    ),
    ActionCommand(
        name='delete',
        help='Delete a user',
        func=lazy_load_command('airflow.cli.commands.user_command.users_delete'),
        args=(ARG_USERNAME,),
    ),
    ActionCommand(
        name='add_role',
        help='Add role to a user',
        func=lazy_load_command('airflow.cli.commands.user_command.add_role'),
        args=(ARG_USERNAME_OPTIONAL, ARG_EMAIL_OPTIONAL, ARG_ROLE),
    ),
    ActionCommand(
        name='remove_role',
        help='Remove role from a user',
        func=lazy_load_command('airflow.cli.commands.user_command.remove_role'),
        args=(ARG_USERNAME_OPTIONAL, ARG_EMAIL_OPTIONAL, ARG_ROLE),
    ),
    ActionCommand(
        name='import',
        help='Import users',
        func=lazy_load_command('airflow.cli.commands.user_command.users_import'),
        args=(ARG_USER_IMPORT,),
    ),
    ActionCommand(
        name='export',
        help='Export all users',
        func=lazy_load_command('airflow.cli.commands.user_command.users_export'),
        args=(ARG_USER_EXPORT,),
    ),
)
ROLES_COMMANDS = (
    ActionCommand(
        name='list',
        help='List roles',
        func=lazy_load_command('airflow.cli.commands.role_command.roles_list'),
        args=(ARG_OUTPUT,),
    ),
    ActionCommand(
        name='create',
        help='Create role',
        func=lazy_load_command('airflow.cli.commands.role_command.roles_create'),
        args=(ARG_ROLES,),
    ),
)

CELERY_COMMANDS = (
    ActionCommand(
        name='worker',
        help="Start a Celery worker node",
        func=lazy_load_command('airflow.cli.commands.celery_command.worker'),
        args=(
            ARG_DO_PICKLE, ARG_QUEUES, ARG_CONCURRENCY, ARG_CELERY_HOSTNAME, ARG_PID, ARG_DAEMON,
            ARG_UMASK, ARG_STDOUT, ARG_STDERR, ARG_LOG_FILE, ARG_AUTOSCALE, ARG_SKIP_SERVE_LOGS
        ),
    ),
    ActionCommand(
        name='flower',
        help="Start a Celery Flower",
        func=lazy_load_command('airflow.cli.commands.celery_command.flower'),
        args=(
            ARG_FLOWER_HOSTNAME, ARG_FLOWER_PORT, ARG_FLOWER_CONF, ARG_FLOWER_URL_PREFIX,
            ARG_FLOWER_BASIC_AUTH, ARG_BROKER_API, ARG_PID, ARG_DAEMON, ARG_STDOUT, ARG_STDERR,
            ARG_LOG_FILE
        ),
    ),
    ActionCommand(
        name='stop',
        help="Stop the Celery worker gracefully",
        func=lazy_load_command('airflow.cli.commands.celery_command.stop_worker'),
        args=(),
    )
)

airflow_commands: List[CLICommand] = [
    GroupCommand(
        name='dags',
        help='List and manage DAGs',
        subcommands=DAGS_COMMANDS,
    ),
    GroupCommand(
        name='tasks',
        help='List and manage tasks',
        subcommands=TASKS_COMMANDS,
    ),
    GroupCommand(
        name='pools',
        help="CRUD operations on pools",
        subcommands=POOLS_COMMANDS,
    ),
    GroupCommand(
        name='variables',
        help="CRUD operations on variables",
        subcommands=VARIABLES_COMMANDS,
    ),
    GroupCommand(
        name='db',
        help="Database operations",
        subcommands=DB_COMMANDS,
    ),
    ActionCommand(
        name='kerberos',
        help="Start a kerberos ticket renewer",
        func=lazy_load_command('airflow.cli.commands.kerberos_command.kerberos'),
        args=(ARG_PRINCIPAL, ARG_KEYTAB, ARG_PID, ARG_DAEMON, ARG_STDOUT, ARG_STDERR, ARG_LOG_FILE),
    ),
    ActionCommand(
        name='webserver',
        help="Start a Airflow webserver instance",
        func=lazy_load_command('airflow.cli.commands.webserver_command.webserver'),
        args=(
            ARG_PORT, ARG_WORKERS, ARG_WORKERCLASS, ARG_WORKER_TIMEOUT, ARG_HOSTNAME, ARG_PID,
            ARG_DAEMON, ARG_STDOUT, ARG_STDERR, ARG_ACCESS_LOGFILE, ARG_ERROR_LOGFILE, ARG_LOG_FILE,
            ARG_SSL_CERT, ARG_SSL_KEY, ARG_DEBUG
        ),
    ),
    ActionCommand(
        name='scheduler',
        help="Start a scheduler instance",
        func=lazy_load_command('airflow.cli.commands.scheduler_command.scheduler'),
        args=(
            ARG_DAG_ID_OPT, ARG_SUBDIR, ARG_NUM_RUNS, ARG_DO_PICKLE, ARG_PID, ARG_DAEMON, ARG_STDOUT,
            ARG_STDERR, ARG_LOG_FILE
        ),
    ),
    ActionCommand(
        name='version',
        help="Show the version",
        func=lazy_load_command('airflow.cli.commands.version_command.version'),
        args=(),
    ),
    GroupCommand(
        name='connections',
        help="List/Add/Delete connections",
        subcommands=CONNECTIONS_COMMANDS,
    ),
    GroupCommand(
        name='users',
        help="CRUD operations on users",
        subcommands=USERS_COMMANDS,
    ),
    GroupCommand(
        name='roles',
        help='Create/List roles',
        subcommands=ROLES_COMMANDS,
    ),
    ActionCommand(
        name='sync_perm',
        help="Update permissions for existing roles and DAGs",
        func=lazy_load_command('airflow.cli.commands.sync_perm_command.sync_perm'),
        args=(),
    ),
    ActionCommand(
        name='rotate_fernet_key',
        func=lazy_load_command('airflow.cli.commands.rotate_fernet_key_command.rotate_fernet_key'),
        help=(
            'Rotate all encrypted connection credentials and variables; see '
            'https://airflow.readthedocs.io/en/stable/howto/secure-connections.html'
            '#rotating-encryption-keys'
        ),
        args=(),
    ),
    ActionCommand(
        name='config',
        help='Show current application configuration',
        func=lazy_load_command('airflow.cli.commands.config_command.show_config'),
        args=(ARG_COLOR, ),
    ),
    ActionCommand(
        name='info',
        help='Show information about current Airflow and environment',
        func=lazy_load_command('airflow.cli.commands.info_command.show_info'),
        args=(ARG_ANONYMIZE, ARG_FILE_IO, ),
    ),
    GroupCommand(
        name="celery",
        help=(
            'Start celery components. Works only when using CeleryExecutor. For more information, see '
            'https://airflow.readthedocs.io/en/stable/executor/celery.html'
        ),
        subcommands=CELERY_COMMANDS
    )
]
ALL_COMMANDS_DICT: Dict[str, CLICommand] = {sp.name: sp for sp in airflow_commands}
DAG_CLI_COMMANDS: Set[str] = {
    'list_tasks', 'backfill', 'test', 'run', 'pause', 'unpause', 'list_dag_runs'
}


class AirflowHelpFormatter(argparse.HelpFormatter):
    """
    Custom help formatter to display help message.

    It displays simple commands and groups of commands in separate sections.
    """
    def _format_action(self, action: Action):
        if isinstance(action, argparse._SubParsersAction):  # pylint: disable=protected-access
            parts = []
            subactions = action._get_subactions()  # pylint: disable=protected-access
            action_subcommnads, group_subcommnands = partition(
                lambda d: isinstance(ALL_COMMANDS_DICT[d.dest], GroupCommand), subactions
            )
            parts.append("\n")
            parts.append('%*s%s:\n' % (self._current_indent, '', "Groups"))
            self._indent()
            for subaction in group_subcommnands:
                parts.append(self._format_action(subaction))
            self._dedent()

            parts.append("\n")
            parts.append('%*s%s:\n' % (self._current_indent, '', "Commands"))
            self._indent()

            for subaction in action_subcommnads:
                parts.append(self._format_action(subaction))
            self._dedent()

            # return a single string
            return self._join_parts(parts)

        return super()._format_action(action)


def get_parser(dag_parser: bool = False) -> argparse.ArgumentParser:
    """Creates and returns command line argument parser"""
    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest='subcommand')
    subparsers.required = True

    subparser_list = DAG_CLI_COMMANDS if dag_parser else ALL_COMMANDS_DICT.keys()
    sub_name: str
    for sub_name in sorted(subparser_list):
        sub: CLICommand = ALL_COMMANDS_DICT[sub_name]
        _add_command(subparsers, sub)
    return parser


def _sort_args(args: Iterable[Arg]) -> Iterable[Arg]:
    """
    Sort subcommand optional args, keep positional args
    """
    def get_long_option(arg: Arg):
        """
        Get long option from Arg.flags
        """
        return arg.flags[0] if len(arg.flags) == 1 else arg.flags[1]
    positional, optional = partition(lambda x: x.flags[0].startswith("-"), args)
    yield from positional
    yield from sorted(optional, key=lambda x: get_long_option(x).lower())


def _add_command(
    subparsers: argparse._SubParsersAction,  # pylint: disable=protected-access
    sub: CLICommand
) -> None:
    sub_proc = subparsers.add_parser(
        sub.name, help=sub.help
    )
    sub_proc.formatter_class = RawTextHelpFormatter

    if isinstance(sub, GroupCommand):
        _add_group_command(sub, sub_proc)
    elif isinstance(sub, ActionCommand):
        _add_action_command(sub, sub_proc)
    else:
        raise AirflowException("Invalid command definition.")


def _add_action_command(sub: ActionCommand, sub_proc: argparse.ArgumentParser) -> None:
    for arg in _sort_args(sub.args):
        arg.add_to_parser(sub_proc)
    sub_proc.set_defaults(func=sub.func)


def _add_group_command(sub: GroupCommand, sub_proc: argparse.ArgumentParser) -> None:
    subcommands = sub.subcommands
    sub_subparsers = sub_proc.add_subparsers(dest="subcommand")
    sub_subparsers.required = True

    for command in sorted(subcommands, key=lambda x: x.name):
        _add_command(sub_subparsers, command)
