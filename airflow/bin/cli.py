#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
import os
import textwrap
from argparse import RawTextHelpFormatter
from typing import Callable

from tabulate import tabulate_formats

from airflow import api, settings
from airflow.configuration import conf
from airflow.executors.executor_loader import ExecutorLoader
from airflow.utils.cli import alternative_conn_specs
from airflow.utils.module_loading import import_string
from airflow.utils.timezone import parse as parsedate

api.load_auth()

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


class Arg:
    """Class to keep information about command line argument"""
    # pylint: disable=redefined-builtin
    def __init__(self, flags=None, help=None, action=None, default=None, nargs=None,
                 type=None, choices=None, required=None, metavar=None):
        self.flags = flags
        self.help = help
        self.action = action
        self.default = default
        self.nargs = nargs
        self.type = type
        self.choices = choices
        self.required = required
        self.metavar = metavar
    # pylint: enable=redefined-builtin


class CLIFactory:
    """
    Factory class which generates command line argument parser and holds information
    about all available Airflow commands
    """
    args = {
        # Shared
        'dag_id': Arg(("dag_id",), "The id of the dag"),
        'task_id': Arg(("task_id",), "The id of the task"),
        'execution_date': Arg(
            ("execution_date",), help="The execution date of the DAG",
            type=parsedate),
        'task_regex': Arg(
            ("-t", "--task_regex"),
            "The regex to filter specific task_ids to backfill (optional)"),
        'subdir': Arg(
            ("-sd", "--subdir"),
            "File location or directory from which to look for the dag. "
            "Defaults to '[AIRFLOW_HOME]/dags' where [AIRFLOW_HOME] is the "
            "value you set for 'AIRFLOW_HOME' config you set in 'airflow.cfg' ",
            default=DAGS_FOLDER),
        'start_date': Arg(
            ("-s", "--start_date"), "Override start_date YYYY-MM-DD",
            type=parsedate),
        'end_date': Arg(
            ("-e", "--end_date"), "Override end_date YYYY-MM-DD",
            type=parsedate),
        'dry_run': Arg(
            ("-dr", "--dry_run"), "Perform a dry run", "store_true"),
        'pid': Arg(
            ("--pid",), "PID file location",
            nargs='?'),
        'daemon': Arg(
            ("-D", "--daemon"), "Daemonize instead of running "
                                "in the foreground",
            "store_true"),
        'stderr': Arg(
            ("--stderr",), "Redirect stderr to this file"),
        'stdout': Arg(
            ("--stdout",), "Redirect stdout to this file"),
        'log_file': Arg(
            ("-l", "--log-file"), "Location of the log file"),
        'yes': Arg(
            ("-y", "--yes"),
            "Do not prompt to confirm reset. Use with care!",
            "store_true",
            default=False),
        'output': Arg(
            ("--output",), (
                "Output table format. The specified value is passed to "
                "the tabulate module (https://pypi.org/project/tabulate/). "
                "Valid values are: ({})".format("|".join(tabulate_formats))
            ),
            choices=tabulate_formats,
            default="fancy_grid"),

        # list_dag_runs
        'no_backfill': Arg(
            ("--no_backfill",),
            "filter all the backfill dagruns given the dag id", "store_true"),
        'state': Arg(
            ("--state",),
            "Only list the dag runs corresponding to the state"
        ),

        # list_jobs
        'limit': Arg(
            ("--limit",),
            "Return a limited number of records"
        ),

        # backfill
        'mark_success': Arg(
            ("-m", "--mark_success"),
            "Mark jobs as succeeded without running them", "store_true"),
        'verbose': Arg(
            ("-v", "--verbose"),
            "Make logging output more verbose", "store_true"),
        'local': Arg(
            ("-l", "--local"),
            "Run the task using the LocalExecutor", "store_true"),
        'donot_pickle': Arg(
            ("-x", "--donot_pickle"), (
                "Do not attempt to pickle the DAG object to send over "
                "to the workers, just tell the workers to run their version "
                "of the code"),
            "store_true"),
        'bf_ignore_dependencies': Arg(
            ("-i", "--ignore_dependencies"),
            (
                "Skip upstream tasks, run only the tasks "
                "matching the regexp. Only works in conjunction "
                "with task_regex"),
            "store_true"),
        'bf_ignore_first_depends_on_past': Arg(
            ("-I", "--ignore_first_depends_on_past"),
            (
                "Ignores depends_on_past dependencies for the first "
                "set of tasks only (subsequent executions in the backfill "
                "DO respect depends_on_past)"),
            "store_true"),
        'pool': Arg(("--pool",), "Resource pool to use"),
        'delay_on_limit': Arg(
            ("--delay_on_limit",),
            help=("Amount of time in seconds to wait when the limit "
                  "on maximum active dag runs (max_active_runs) has "
                  "been reached before trying to execute a dag run "
                  "again"),
            type=float,
            default=1.0),
        'reset_dag_run': Arg(
            ("--reset_dagruns",),
            (
                "if set, the backfill will delete existing "
                "backfill-related DAG runs and start "
                "anew with fresh, running DAG runs"),
            "store_true"),
        'rerun_failed_tasks': Arg(
            ("--rerun_failed_tasks",),
            (
                "if set, the backfill will auto-rerun "
                "all the failed tasks for the backfill date range "
                "instead of throwing exceptions"),
            "store_true"),
        'run_backwards': Arg(
            ("-B", "--run_backwards",),
            (
                "if set, the backfill will run tasks from the most "
                "recent day first.  if there are tasks that depend_on_past "
                "this option will throw an exception"),
            "store_true"),

        # list_tasks
        'tree': Arg(("-t", "--tree"), "Tree view", "store_true"),
        # list_dags
        'report': Arg(
            ("-r", "--report"), "Show DagBag loading report", "store_true"),
        # clear
        'upstream': Arg(
            ("-u", "--upstream"), "Include upstream tasks", "store_true"),
        'only_failed': Arg(
            ("-f", "--only_failed"), "Only failed jobs", "store_true"),
        'only_running': Arg(
            ("-r", "--only_running"), "Only running jobs", "store_true"),
        'downstream': Arg(
            ("-d", "--downstream"), "Include downstream tasks", "store_true"),
        'exclude_subdags': Arg(
            ("-x", "--exclude_subdags"),
            "Exclude subdags", "store_true"),
        'exclude_parentdag': Arg(
            ("-xp", "--exclude_parentdag"),
            "Exclude ParentDAGS if the task cleared is a part of a SubDAG",
            "store_true"),
        'dag_regex': Arg(
            ("-dx", "--dag_regex"),
            "Search dag_id as regex instead of exact string", "store_true"),
        # show_dag
        'save': Arg(
            ("-s", "--save"),
            "Saves the result to the indicated file.\n"
            "\n"
            "The file format is determined by the file extension. For more information about supported "
            "format, see: https://www.graphviz.org/doc/info/output.html\n"
            "\n"
            "If you want to create a PNG file then you should execute the following command:\n"
            "airflow dags show <DAG_ID> --save output.png\n"
            "\n"
            "If you want to create a DOT file then you should execute the following command:\n"
            "airflow dags show <DAG_ID> --save output.dot\n"
        ),
        'imgcat': Arg(
            ("--imgcat", ),
            "Displays graph using the imgcat tool. \n"
            "\n"
            "For more information, see: https://www.iterm2.com/documentation-images.html",
            action='store_true'),
        # trigger_dag
        'run_id': Arg(("-r", "--run_id"), "Helps to identify this run"),
        'conf': Arg(
            ('-c', '--conf'),
            "JSON string that gets pickled into the DagRun's conf attribute"),
        'exec_date': Arg(
            ("-e", "--exec_date"), help="The execution date of the DAG",
            type=parsedate),
        # pool
        'pool_name': Arg(
            ("pool",),
            metavar='NAME',
            help="Pool name"),
        'pool_slots': Arg(
            ("slots",),
            type=int,
            help="Pool slots"),
        'pool_description': Arg(
            ("description",),
            help="Pool description"),
        'pool_import': Arg(
            ("file",),
            metavar="FILEPATH",
            help="Import pools from JSON file"),
        'pool_export': Arg(
            ("file",),
            metavar="FILEPATH",
            help="Export all pools to JSON file"),
        # variables
        'var': Arg(
            ("key",),
            help="Variable key"),
        'var_value': Arg(
            ("value",),
            metavar='VALUE',
            help="Variable value"),
        'default': Arg(
            ("-d", "--default"),
            metavar="VAL",
            default=None,
            help="Default value returned if variable does not exist"),
        'json': Arg(
            ("-j", "--json"),
            help="Deserialize JSON variable",
            action="store_true"),
        'var_import': Arg(
            ("file",),
            help="Import variables from JSON file"),
        'var_export': Arg(
            ("file",),
            help="Export all variables to JSON file"),
        # kerberos
        'principal': Arg(
            ("principal",), "kerberos principal", nargs='?'),
        'keytab': Arg(
            ("-kt", "--keytab"), "keytab",
            nargs='?', default=conf.get('kerberos', 'keytab')),
        # run
        # TODO(aoen): "force" is a poor choice of name here since it implies it overrides
        # all dependencies (not just past success), e.g. the ignore_depends_on_past
        # dependency. This flag should be deprecated and renamed to 'ignore_ti_state' and
        # the "ignore_all_dependencies" command should be called the"force" command
        # instead.
        'interactive': Arg(
            ('-int', '--interactive'),
            help='Do not capture standard output and error streams '
                 '(useful for interactive debugging)',
            action='store_true'),
        'force': Arg(
            ("-f", "--force"),
            "Ignore previous task instance state, rerun regardless if task already "
            "succeeded/failed",
            "store_true"),
        'raw': Arg(("-r", "--raw"), argparse.SUPPRESS, "store_true"),
        'ignore_all_dependencies': Arg(
            ("-A", "--ignore_all_dependencies"),
            "Ignores all non-critical dependencies, including ignore_ti_state and "
            "ignore_task_deps",
            "store_true"),
        # TODO(aoen): ignore_dependencies is a poor choice of name here because it is too
        # vague (e.g. a task being in the appropriate state to be run is also a dependency
        # but is not ignored by this flag), the name 'ignore_task_dependencies' is
        # slightly better (as it ignores all dependencies that are specific to the task),
        # so deprecate the old command name and use this instead.
        'ignore_dependencies': Arg(
            ("-i", "--ignore_dependencies"),
            "Ignore task-specific dependencies, e.g. upstream, depends_on_past, and "
            "retry delay dependencies",
            "store_true"),
        'ignore_depends_on_past': Arg(
            ("-I", "--ignore_depends_on_past"),
            "Ignore depends_on_past dependencies (but respect "
            "upstream dependencies)",
            "store_true"),
        'ship_dag': Arg(
            ("--ship_dag",),
            "Pickles (serializes) the DAG and ships it to the worker",
            "store_true"),
        'pickle': Arg(
            ("-p", "--pickle"),
            "Serialized pickle object of the entire dag (used internally)"),
        'job_id': Arg(("-j", "--job_id"), argparse.SUPPRESS),
        'cfg_path': Arg(
            ("--cfg_path",), "Path to config file to use instead of airflow.cfg"),
        # webserver
        'port': Arg(
            ("-p", "--port"),
            default=conf.get('webserver', 'WEB_SERVER_PORT'),
            type=int,
            help="The port on which to run the server"),
        'ssl_cert': Arg(
            ("--ssl_cert",),
            default=conf.get('webserver', 'WEB_SERVER_SSL_CERT'),
            help="Path to the SSL certificate for the webserver"),
        'ssl_key': Arg(
            ("--ssl_key",),
            default=conf.get('webserver', 'WEB_SERVER_SSL_KEY'),
            help="Path to the key to use with the SSL certificate"),
        'workers': Arg(
            ("-w", "--workers"),
            default=conf.get('webserver', 'WORKERS'),
            type=int,
            help="Number of workers to run the webserver on"),
        'workerclass': Arg(
            ("-k", "--workerclass"),
            default=conf.get('webserver', 'WORKER_CLASS'),
            choices=['sync', 'eventlet', 'gevent', 'tornado'],
            help="The worker class to use for Gunicorn"),
        'worker_timeout': Arg(
            ("-t", "--worker_timeout"),
            default=conf.get('webserver', 'WEB_SERVER_WORKER_TIMEOUT'),
            type=int,
            help="The timeout for waiting on webserver workers"),
        'hostname': Arg(
            ("-hn", "--hostname"),
            default=conf.get('webserver', 'WEB_SERVER_HOST'),
            help="Set the hostname on which to run the web server"),
        'debug': Arg(
            ("-d", "--debug"),
            "Use the server that ships with Flask in debug mode",
            "store_true"),
        'access_logfile': Arg(
            ("-A", "--access_logfile"),
            default=conf.get('webserver', 'ACCESS_LOGFILE'),
            help="The logfile to store the webserver access log. Use '-' to print to "
                 "stderr"),
        'error_logfile': Arg(
            ("-E", "--error_logfile"),
            default=conf.get('webserver', 'ERROR_LOGFILE'),
            help="The logfile to store the webserver error log. Use '-' to print to "
                 "stderr"),
        # scheduler
        'dag_id_opt': Arg(("-d", "--dag_id"), help="The id of the dag to run"),
        'num_runs': Arg(
            ("-n", "--num_runs"),
            default=conf.getint('scheduler', 'num_runs'), type=int,
            help="Set the number of runs to execute before exiting"),
        # worker
        'do_pickle': Arg(
            ("-p", "--do_pickle"),
            default=False,
            help=(
                "Attempt to pickle the DAG object to send over "
                "to the workers, instead of letting workers run their version "
                "of the code"),
            action="store_true"),
        'queues': Arg(
            ("-q", "--queues"),
            help="Comma delimited list of queues to serve",
            default=conf.get('celery', 'DEFAULT_QUEUE')),
        'concurrency': Arg(
            ("-c", "--concurrency"),
            type=int,
            help="The number of worker processes",
            default=conf.get('celery', 'worker_concurrency')),
        'celery_hostname': Arg(
            ("-cn", "--celery_hostname"),
            help=("Set the hostname of celery worker "
                  "if you have multiple workers on a single machine")),
        # flower
        'broker_api': Arg(("-a", "--broker_api"), help="Broker api"),
        'flower_hostname': Arg(
            ("-hn", "--hostname"),
            default=conf.get('celery', 'FLOWER_HOST'),
            help="Set the hostname on which to run the server"),
        'flower_port': Arg(
            ("-p", "--port"),
            default=conf.get('celery', 'FLOWER_PORT'),
            type=int,
            help="The port on which to run the server"),
        'flower_conf': Arg(
            ("-fc", "--flower_conf"),
            help="Configuration file for flower"),
        'flower_url_prefix': Arg(
            ("-u", "--url_prefix"),
            default=conf.get('celery', 'FLOWER_URL_PREFIX'),
            help="URL prefix for Flower"),
        'flower_basic_auth': Arg(
            ("-ba", "--basic_auth"),
            default=conf.get('celery', 'FLOWER_BASIC_AUTH'),
            help=("Securing Flower with Basic Authentication. "
                  "Accepts user:password pairs separated by a comma. "
                  "Example: flower_basic_auth = user1:password1,user2:password2")),
        'task_params': Arg(
            ("-tp", "--task_params"),
            help="Sends a JSON params dict to the task"),
        'post_mortem': Arg(
            ("-pm", "--post_mortem"),
            action="store_true",
            help="Open debugger on uncaught exception",
        ),
        # connections
        'conn_id': Arg(
            ('conn_id',),
            help='Connection id, required to add/delete a connection',
            type=str),
        'conn_uri': Arg(
            ('--conn_uri',),
            help='Connection URI, required to add a connection without conn_type',
            type=str),
        'conn_type': Arg(
            ('--conn_type',),
            help='Connection type, required to add a connection without conn_uri',
            type=str),
        'conn_host': Arg(
            ('--conn_host',),
            help='Connection host, optional when adding a connection',
            type=str),
        'conn_login': Arg(
            ('--conn_login',),
            help='Connection login, optional when adding a connection',
            type=str),
        'conn_password': Arg(
            ('--conn_password',),
            help='Connection password, optional when adding a connection',
            type=str),
        'conn_schema': Arg(
            ('--conn_schema',),
            help='Connection schema, optional when adding a connection',
            type=str),
        'conn_port': Arg(
            ('--conn_port',),
            help='Connection port, optional when adding a connection',
            type=str),
        'conn_extra': Arg(
            ('--conn_extra',),
            help='Connection `Extra` field, optional when adding a connection',
            type=str),
        # users
        'username': Arg(
            ('--username',),
            help='Username of the user',
            required=True,
            type=str),
        'username_optional': Arg(
            ('--username',),
            help='Username of the user',
            type=str),
        'firstname': Arg(
            ('--firstname',),
            help='First name of the user',
            required=True,
            type=str),
        'lastname': Arg(
            ('--lastname',),
            help='Last name of the user',
            required=True,
            type=str),
        'role': Arg(
            ('--role',),
            help='Role of the user. Existing roles include Admin, '
                 'User, Op, Viewer, and Public',
            required=True,
            type=str,
        ),
        'email': Arg(
            ('--email',),
            help='Email of the user',
            required=True,
            type=str),
        'email_optional': Arg(
            ('--email',),
            help='Email of the user',
            type=str),
        'password': Arg(
            ('--password',),
            help='Password of the user, required to create a user '
                 'without --use_random_password',
            type=str),
        'use_random_password': Arg(
            ('--use_random_password',),
            help='Do not prompt for password. Use random string instead.'
                 ' Required to create a user without --password ',
            default=False,
            action='store_true'),
        'user_import': Arg(
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
                    ]'''), " " * 4),
        ),
        'user_export': Arg(
            ("export",),
            metavar="FILEPATH",
            help="Export all users to JSON file"),
        # roles
        'create_role': Arg(
            ('-c', '--create'),
            help='Create a new role',
            action='store_true'),
        'list_roles': Arg(
            ('-l', '--list'),
            help='List roles',
            action='store_true'),
        'roles': Arg(
            ('role',),
            help='The name of a role',
            nargs='*'),
        'autoscale': Arg(
            ('-a', '--autoscale'),
            help="Minimum and Maximum number of worker to autoscale"),
        'skip_serve_logs': Arg(
            ("-s", "--skip_serve_logs"),
            default=False,
            help="Don't start the serve logs process along with the workers",
            action="store_true"),
    }
    DAGS_SUBCOMMANDS = (
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_list_dags'),
            'name': 'list',
            'help': "List all the DAGs",
            'args': ('subdir', 'report'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_list_dag_runs'),
            'name': 'list_runs',
            'help': "List dag runs given a DAG id. If state option is given, it will only "
                    "search for all the dagruns with the given state. "
                    "If no_backfill option is given, it will filter out "
                    "all backfill dagruns for given dag id",
            'args': ('dag_id', 'no_backfill', 'state', 'output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_list_jobs'),
            'name': 'list_jobs',
            'help': "List the jobs",
            'args': ('dag_id_opt', 'state', 'limit', 'output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_state'),
            'name': 'state',
            'help': "Get the status of a dag run",
            'args': ('dag_id', 'execution_date', 'subdir'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_next_execution'),
            'name': 'next_execution',
            'help': "Get the next execution datetime of a DAG",
            'args': ('dag_id', 'subdir'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_pause'),
            'name': 'pause',
            'help': 'Pause a DAG',
            'args': ('dag_id', 'subdir'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_unpause'),
            'name': 'unpause',
            'help': 'Resume a paused DAG',
            'args': ('dag_id', 'subdir'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_trigger'),
            'name': 'trigger',
            'help': 'Trigger a DAG run',
            'args': ('dag_id', 'subdir', 'run_id', 'conf', 'exec_date'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_delete'),
            'name': 'delete',
            'help': "Delete all DB records related to the specified DAG",
            'args': ('dag_id', 'yes'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_show'),
            'name': 'show',
            'help': "Displays DAG's tasks with their dependencies",
            'args': ('dag_id', 'subdir', 'save', 'imgcat',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.dag_command.dag_backfill'),
            'name': 'backfill',
            'help': "Run subsections of a DAG for a specified date range. "
                    "If reset_dag_run option is used,"
                    " backfill will first prompt users whether airflow "
                    "should clear all the previous dag_run and task_instances "
                    "within the backfill date range. "
                    "If rerun_failed_tasks is used, backfill "
                    "will auto re-run the previous failed task instances"
                    " within the backfill date range",
            'args': (
                'dag_id', 'task_regex', 'start_date', 'end_date',
                'mark_success', 'local', 'donot_pickle', 'yes',
                'bf_ignore_dependencies', 'bf_ignore_first_depends_on_past',
                'subdir', 'pool', 'delay_on_limit', 'dry_run', 'verbose', 'conf',
                'reset_dag_run', 'rerun_failed_tasks', 'run_backwards'
            ),
        },
    )
    TASKS_COMMANDS = (
        {
            'func': lazy_load_command('airflow.cli.commands.task_command.task_list'),
            'name': 'list',
            'help': "List the tasks within a DAG",
            'args': ('dag_id', 'tree', 'subdir'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.task_command.task_clear'),
            'name': 'clear',
            'help': "Clear a set of task instance, as if they never ran",
            'args': (
                'dag_id', 'task_regex', 'start_date', 'end_date', 'subdir',
                'upstream', 'downstream', 'yes', 'only_failed',
                'only_running', 'exclude_subdags', 'exclude_parentdag', 'dag_regex'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.task_command.task_state'),
            'name': 'state',
            'help': "Get the status of a task instance",
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.task_command.task_failed_deps'),
            'name': 'failed_deps',
            'help': (
                "Returns the unmet dependencies for a task instance from the perspective "
                "of the scheduler. In other words, why a task instance doesn't get "
                "scheduled and then queued by the scheduler, and then run by an "
                "executor)"),
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.task_command.task_render'),
            'name': 'render',
            'help': "Render a task instance's template(s)",
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.task_command.task_run'),
            'name': 'run',
            'help': "Run a single task instance",
            'args': (
                'dag_id', 'task_id', 'execution_date', 'subdir',
                'mark_success', 'force', 'pool', 'cfg_path',
                'local', 'raw', 'ignore_all_dependencies', 'ignore_dependencies',
                'ignore_depends_on_past', 'ship_dag', 'pickle', 'job_id', 'interactive',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.task_command.task_test'),
            'name': 'test',
            'help': (
                "Test a task instance. This will run a task without checking for "
                "dependencies or recording its state in the database"),
            'args': (
                'dag_id', 'task_id', 'execution_date', 'subdir', 'dry_run',
                'task_params', 'post_mortem'),
        },
    )
    POOLS_COMMANDS = (
        {
            'func': lazy_load_command('airflow.cli.commands.pool_command.pool_list'),
            'name': 'list',
            'help': 'List pools',
            'args': ('output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.pool_command.pool_get'),
            'name': 'get',
            'help': 'Get pool size',
            'args': ('pool_name', 'output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.pool_command.pool_set'),
            'name': 'set',
            'help': 'Configure pool',
            'args': ('pool_name', 'pool_slots', 'pool_description', 'output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.pool_command.pool_delete'),
            'name': 'delete',
            'help': 'Delete pool',
            'args': ('pool_name', 'output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.pool_command.pool_import'),
            'name': 'import',
            'help': 'Import pools',
            'args': ('pool_import', 'output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.pool_command.pool_export'),
            'name': 'export',
            'help': 'Export all pools',
            'args': ('pool_export', 'output',),
        },
    )
    VARIABLES_COMMANDS = (
        {
            'func': lazy_load_command('airflow.cli.commands.variable_command.variables_list'),
            'name': 'list',
            'help': 'List variables',
            'args': (),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.variable_command.variables_get'),
            'name': 'get',
            'help': 'Get variable',
            'args': ('var', 'json', 'default'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.variable_command.variables_set'),
            'name': 'set',
            'help': 'Set variable',
            'args': ('var', 'var_value', 'json'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.variable_command.variables_delete'),
            'name': 'delete',
            'help': 'Delete variable',
            'args': ('var',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.variable_command.variables_import'),
            'name': 'import',
            'help': 'Import variables',
            'args': ('var_import',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.variable_command.variables_export'),
            'name': 'export',
            'help': 'Export all variables',
            'args': ('var_export',),
        },
    )
    DB_COMMANDS = (
        {
            'func': lazy_load_command('airflow.cli.commands.db_command.initdb'),
            'name': 'init',
            'help': "Initialize the metadata database",
            'args': (),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.db_command.resetdb'),
            'name': 'reset',
            'help': "Burn down and rebuild the metadata database",
            'args': ('yes',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.db_command.upgradedb'),
            'name': 'upgrade',
            'help': "Upgrade the metadata database to latest version",
            'args': tuple(),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.db_command.shell'),
            'name': 'shell',
            'help': "Runs a shell to access the database",
            'args': tuple(),
        },
    )
    CONNECTIONS_COMMANDS = (
        {
            'func': lazy_load_command('airflow.cli.commands.connection_command.connections_list'),
            'name': 'list',
            'help': 'List connections',
            'args': ('output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.connection_command.connections_add'),
            'name': 'add',
            'help': 'Add a connection',
            'args': ('conn_id', 'conn_uri', 'conn_extra') + tuple(alternative_conn_specs),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.connection_command.connections_delete'),
            'name': 'delete',
            'help': 'Delete a connection',
            'args': ('conn_id',),
        },
    )
    USERS_COMMANDSS = (
        {
            'func': lazy_load_command('airflow.cli.commands.user_command.users_list'),
            'name': 'list',
            'help': 'List users',
            'args': ('output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.user_command.users_create'),
            'name': 'create',
            'help': 'Create a user',
            'args': ('role', 'username', 'email', 'firstname', 'lastname', 'password',
                     'use_random_password')
        },
        {
            'func': lazy_load_command('airflow.cli.commands.user_command.users_delete'),
            'name': 'delete',
            'help': 'Delete a user',
            'args': ('username',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.user_command.add_role'),
            'name': 'add_role',
            'help': 'Add role to a user',
            'args': ('username_optional', 'email_optional', 'role'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.user_command.remove_role'),
            'name': 'remove_role',
            'help': 'Remove role from a user',
            'args': ('username_optional', 'email_optional', 'role'),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.user_command.users_import'),
            'name': 'import',
            'help': 'Import users',
            'args': ('user_import',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.user_command.users_export'),
            'name': 'export',
            'help': 'Export all users',
            'args': ('user_export',),
        },
    )
    ROLES_COMMANDS = (
        {
            'func': lazy_load_command('airflow.cli.commands.role_command.roles_list'),
            'name': 'list',
            'help': 'List roles',
            'args': ('output',),
        },
        {
            'func': lazy_load_command('airflow.cli.commands.role_command.roles_create'),
            'name': 'create',
            'help': 'Create role',
            'args': ('roles',),
        },
    )
    subparsers = [
        {
            'help': 'List and manage DAGs',
            'name': 'dags',
            'subcommands': DAGS_SUBCOMMANDS,
        }, {
            'help': 'List and manage tasks',
            'name': 'tasks',
            'subcommands': TASKS_COMMANDS,
        }, {
            'help': "CRUD operations on pools",
            'name': 'pools',
            'subcommands': POOLS_COMMANDS,
        }, {
            'help': "CRUD operations on variables",
            'name': 'variables',
            'subcommands': VARIABLES_COMMANDS,
            "args": ('set', 'get', 'json', 'default',
                     'var_import', 'var_export', 'var_delete'),
        }, {
            'help': "Database operations",
            'name': 'db',
            'subcommands': DB_COMMANDS,
        }, {
            'name': 'kerberos',
            'func': lazy_load_command('airflow.cli.commands.kerberos_command.kerberos'),
            'help': "Start a kerberos ticket renewer",
            'args': ('principal', 'keytab', 'pid',
                     'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'name': 'webserver',
            'func': lazy_load_command('airflow.cli.commands.webserver_command.webserver'),
            'help': "Start a Airflow webserver instance",
            'args': ('port', 'workers', 'workerclass', 'worker_timeout', 'hostname',
                     'pid', 'daemon', 'stdout', 'stderr', 'access_logfile',
                     'error_logfile', 'log_file', 'ssl_cert', 'ssl_key', 'debug'),
        }, {
            'name': 'scheduler',
            'func': lazy_load_command('airflow.cli.commands.scheduler_command.scheduler'),
            'help': "Start a scheduler instance",
            'args': ('dag_id_opt', 'subdir', 'num_runs',
                     'do_pickle', 'pid', 'daemon', 'stdout', 'stderr',
                     'log_file'),
        }, {
            'name': 'version',
            'func': lazy_load_command('airflow.cli.commands.version_command.version'),
            'help': "Show the version",
            'args': tuple(),
        }, {
            'help': "List/Add/Delete connections",
            'name': 'connections',
            'subcommands': CONNECTIONS_COMMANDS,
        }, {
            'help': "CRUD operations on users",
            'name': 'users',
            'subcommands': USERS_COMMANDSS,
        }, {
            'help': 'Create/List roles',
            'name': 'roles',
            'subcommands': ROLES_COMMANDS,
        }, {
            'name': 'sync_perm',
            'func': lazy_load_command('airflow.cli.commands.sync_perm_command.sync_perm'),
            'help': "Update permissions for existing roles and DAGs",
            'args': tuple(),
        },
        {
            'name': 'rotate_fernet_key',
            'func': lazy_load_command('airflow.cli.commands.rotate_fernet_key_command.rotate_fernet_key'),
            'help': 'Rotate all encrypted connection credentials and variables; see '
                    'https://airflow.readthedocs.io/en/stable/howto/secure-connections.html'
                    '#rotating-encryption-keys',
            'args': (),
        },
        {
            'name': 'config',
            'func': lazy_load_command('airflow.cli.commands.config_command.show_config'),
            'help': 'Show current application configuration',
            'args': (),
        },
    ]
    if conf.get("core", "EXECUTOR") == ExecutorLoader.CELERY_EXECUTOR or BUILD_DOCS:
        subparsers.append({
            "help": "Start celery components",
            "name": "celery",
            "subcommands": (
                {
                    'name': 'worker',
                    'func': lazy_load_command('airflow.cli.commands.celery_command.worker'),
                    'help': "Start a Celery worker node",
                    'args': ('do_pickle', 'queues', 'concurrency', 'celery_hostname',
                             'pid', 'daemon', 'stdout', 'stderr', 'log_file', 'autoscale', 'skip_serve_logs'),
                }, {
                    'name': 'flower',
                    'func': lazy_load_command('airflow.cli.commands.celery_command.flower'),
                    'help': "Start a Celery Flower",
                    'args': (
                        'flower_hostname', 'flower_port', 'flower_conf', 'flower_url_prefix',
                        'flower_basic_auth', 'broker_api', 'pid', 'daemon', 'stdout', 'stderr', 'log_file'),
                },
            )
        })
    subparsers_dict = {sp.get('name') or sp['func'].__name__: sp for sp in subparsers}  # type: ignore
    dag_subparsers = (
        'list_tasks', 'backfill', 'test', 'run', 'pause', 'unpause', 'list_dag_runs')

    @classmethod
    def get_parser(cls, dag_parser=False):
        """Creates and returns command line argument parser"""
        class DefaultHelpParser(argparse.ArgumentParser):
            """Override argparse.ArgumentParser.error and use print_help instead of print_usage"""
            def error(self, message):
                self.print_help()
                self.exit(2, '\n{} command error: {}, see help above.\n'.format(self.prog, message))
        parser = DefaultHelpParser()
        subparsers = parser.add_subparsers(
            help='sub-command help', dest='subcommand')
        subparsers.required = True

        subparser_list = cls.dag_subparsers if dag_parser else cls.subparsers_dict.keys()
        for sub in sorted(subparser_list):
            sub = cls.subparsers_dict[sub]
            cls._add_subcommand(subparsers, sub)
        return parser

    @classmethod
    def _add_subcommand(cls, subparsers, sub):
        dag_parser = False
        sub_proc = subparsers.add_parser(
            sub.get('name') or sub['func'].__name__, help=sub['help']  # type: ignore
        )
        sub_proc.formatter_class = RawTextHelpFormatter

        subcommands = sub.get('subcommands', [])
        if subcommands:
            sub_subparsers = sub_proc.add_subparsers(dest='subcommand')
            sub_subparsers.required = True
            for command in subcommands:
                cls._add_subcommand(sub_subparsers, command)
        else:
            for arg in sub['args']:
                if 'dag_id' in arg and dag_parser:
                    continue
                arg = cls.args[arg]
                kwargs = {
                    f: v
                    for f, v in vars(arg).items() if f != 'flags' and v}
                sub_proc.add_argument(*arg.flags, **kwargs)
            sub_proc.set_defaults(func=sub['func'])


def get_parser():
    """Calls static method inside factory which creates argument parser"""
    return CLIFactory.get_parser()
