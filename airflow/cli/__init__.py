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
import os

import rich_click as click

from airflow import settings
from airflow.utils.cli import ColorMode
from airflow.utils.timezone import parse as parsedate

BUILD_DOCS = "BUILDING_AIRFLOW_DOCS" in os.environ

click_color = click.option(
    '--color',
    type=click.Choice([ColorMode.ON, ColorMode.OFF, ColorMode.AUTO]),
    default=ColorMode.AUTO,
    help="Do emit colored output (default: auto)",
)
click_conf = click.option(
    '-c', '--conf', help="JSON string that gets pickled into the DagRun's conf attribute"
)
click_daemon = click.option(
    "-D", "--daemon", 'daemon_', is_flag=True, help="Daemonize instead of running in the foreground"
)
click_dag_id = click.argument("dag_id", help="The id of the dag")
click_dag_id_opt = click.option("-d", "--dag-id", help="The id of the dag")
click_debug = click.option(
    "-d", "--debug", is_flag=True, help="Use the server that ships with Flask in debug mode"
)
click_dry_run = click.option(
    '-n',
    '--dry-run',
    is_flag=True,
    default=False,
    help="Perform a dry run for each task. Only renders Template Fields for each task, nothing else",
)
click_end_date = click.option(
    "-e",
    "--end-date",
    type=parsedate,
    help="Override end_date YYYY-MM-DD",
)
click_execution_date = click.argument("execution_date", help="The execution date of the DAG", type=parsedate)
click_execution_date_or_run_id = click.argument(
    "execution_date_or_run_id", help="The execution_date of the DAG or run_id of the DAGRun"
)
click_log_file = click.option(
    "-l",
    "--log-file",
    metavar="LOG_FILE",
    type=click.Path(exists=False, dir_okay=False, writable=True),
    help="Location of the log file",
)
click_output = click.option(
    "-o",
    "--output",
    type=click.Choice(["table", "json", "yaml", "plain"]),
    default="table",
    help="Output format.",
)
click_pid = click.option("--pid", metavar="PID", type=click.Path(exists=False), help="PID file location")
click_start_date = click.option(
    "-s",
    "--start-date",
    type=parsedate,
    help="Override start_date YYYY-MM-DD",
)
click_stderr = click.option(
    "--stderr",
    metavar="STDERR",
    type=click.Path(exists=False, dir_okay=False, writable=True),
    help="Redirect stderr to this file",
)
click_stdout = click.option(
    "--stdout",
    metavar="STDOUT",
    type=click.Path(exists=False, dir_okay=False, writable=True),
    help="Redirect stdout to this file",
)
click_subdir = click.option(
    "-S",
    "--subdir",
    default='[AIRFLOW_HOME]/dags' if BUILD_DOCS else settings.DAGS_FOLDER,
    type=click.Path(),
    help=(
        "File location or directory from which to look for the dag. "
        "Defaults to '[AIRFLOW_HOME]/dags' where [AIRFLOW_HOME] is the "
        "value you set for 'AIRFLOW_HOME' config you set in 'airflow.cfg' "
    ),
)
click_task_id = click.argument("task_id", help="The id of the task")
click_task_regex = click.option(
    "-t", "--task-regex", help="The regex to filter specific task_ids to backfill (optional)"
)
click_verbose = click.option(
    '-v', '--verbose', is_flag=True, default=False, help="Make logging output more verbose"
)
click_yes = click.option(
    '-y', '--yes', is_flag=True, default=False, help="Do not prompt to confirm. Use with care!"
)


# https://click.palletsprojects.com/en/8.1.x/documentation/#help-parameter-customization
@click.group(context_settings={'help_option_names': ['-h', '--help']})
@click.pass_context
def airflow_cmd(ctx):
    pass
