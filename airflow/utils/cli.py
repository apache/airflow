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
"""Utilities module for cli"""
from __future__ import annotations

import functools
import logging
import os
import re
import socket
import sys
import threading
import traceback
import warnings
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.utils import cli_action_loggers
from airflow.utils.log.non_caching_file_handler import NonCachingFileHandler
from airflow.utils.platform import getuser, is_terminal_support_colors
from airflow.utils.session import provide_session

T = TypeVar("T", bound=Callable)

if TYPE_CHECKING:
    from airflow.models.dag import DAG

logger = logging.getLogger(__name__)


def _check_cli_args(args):
    if not args:
        raise ValueError("Args should be set")
    if not isinstance(args[0], Namespace):
        raise ValueError(
            f"1st positional argument should be argparse.Namespace instance, but is {type(args[0])}"
        )


def action_cli(func=None, check_db=True):
    def action_logging(f: T) -> T:
        """
        Decorates function to execute function at the same time submitting action_logging
        but in CLI context. It will call action logger callbacks twice,
        one for pre-execution and the other one for post-execution.

        Action logger will be called with below keyword parameters:
            sub_command : name of sub-command
            start_datetime : start datetime instance by utc
            end_datetime : end datetime instance by utc
            full_command : full command line arguments
            user : current user
            log : airflow.models.log.Log ORM instance
            dag_id : dag id (optional)
            task_id : task_id (optional)
            execution_date : execution date (optional)
            error : exception instance if there's an exception

        :param f: function instance
        :return: wrapped function
        """

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            """
            An wrapper for cli functions. It assumes to have Namespace instance
            at 1st positional argument

            :param args: Positional argument. It assumes to have Namespace instance
                at 1st positional argument
            :param kwargs: A passthrough keyword argument
            """
            _check_cli_args(args)
            metrics = _build_metrics(f.__name__, args[0])
            cli_action_loggers.on_pre_execution(**metrics)
            verbose = getattr(args[0], "verbose", False)
            root_logger = logging.getLogger()
            if verbose:
                root_logger.setLevel(logging.DEBUG)
                for handler in root_logger.handlers:
                    handler.setLevel(logging.DEBUG)
            try:
                # Check and run migrations if necessary
                if check_db:
                    from airflow.utils.db import check_and_run_migrations, synchronize_log_template

                    check_and_run_migrations()
                    synchronize_log_template()
                return f(*args, **kwargs)
            except Exception as e:
                metrics['error'] = e
                raise
            finally:
                metrics['end_datetime'] = datetime.utcnow()
                cli_action_loggers.on_post_execution(**metrics)

        return cast(T, wrapper)

    if func:
        return action_logging(func)
    return action_logging


def _build_metrics(func_name, namespace):
    """
    Builds metrics dict from function args
    It assumes that function arguments is from airflow.bin.cli module's function
    and has Namespace instance where it optionally contains "dag_id", "task_id",
    and "execution_date".

    :param func_name: name of function
    :param namespace: Namespace instance from argparse
    :return: dict with metrics
    """
    sub_commands_to_check = {'users', 'connections'}
    sensitive_fields = {'-p', '--password', '--conn-password'}
    full_command = list(sys.argv)
    sub_command = full_command[1] if len(full_command) > 1 else None
    if sub_command in sub_commands_to_check:
        for idx, command in enumerate(full_command):
            if command in sensitive_fields:
                # For cases when password is passed as "--password xyz" (with space between key and value)
                full_command[idx + 1] = "*" * 8
            else:
                # For cases when password is passed as "--password=xyz" (with '=' between key and value)
                for sensitive_field in sensitive_fields:
                    if command.startswith(f'{sensitive_field}='):
                        full_command[idx] = f'{sensitive_field}={"*" * 8}'

    metrics = {
        'sub_command': func_name,
        'start_datetime': datetime.utcnow(),
        'full_command': f'{full_command}',
        'user': getuser(),
    }

    if not isinstance(namespace, Namespace):
        raise ValueError(
            f"namespace argument should be argparse.Namespace instance, but is {type(namespace)}"
        )
    tmp_dic = vars(namespace)
    metrics['dag_id'] = tmp_dic.get('dag_id')
    metrics['task_id'] = tmp_dic.get('task_id')
    metrics['execution_date'] = tmp_dic.get('execution_date')
    metrics['host_name'] = socket.gethostname()

    return metrics


def process_subdir(subdir: str | None):
    """Expands path to absolute by replacing 'DAGS_FOLDER', '~', '.', etc."""
    if subdir:
        if not settings.DAGS_FOLDER:
            raise ValueError("DAGS_FOLDER variable in settings should be filled.")
        subdir = subdir.replace('DAGS_FOLDER', settings.DAGS_FOLDER)
        subdir = os.path.abspath(os.path.expanduser(subdir))
    return subdir


def get_dag_by_file_location(dag_id: str):
    """Returns DAG of a given dag_id by looking up file location"""
    from airflow.models import DagBag, DagModel

    # Benefit is that logging from other dags in dagbag will not appear
    dag_model = DagModel.get_current(dag_id)
    if dag_model is None:
        raise AirflowException(
            f"Dag {dag_id!r} could not be found; either it does not exist or it failed to parse."
        )
    dagbag = DagBag(dag_folder=dag_model.fileloc)
    return dagbag.dags[dag_id]


def _search_for_dag_file(val: str | None) -> str | None:
    """
    Search for the file referenced at fileloc.

    By the time we get to this function, we've already run this `val` through `process_subdir`
    and loaded the DagBag there and came up empty.  So here, if `val` is a file path, we make
    a last ditch effort to try and find a dag file with the same name in our dags folder. (This
    avoids the unnecessary dag parsing that would occur if we just parsed the dags folder).

    If `val` is a path to a file, this likely means that the serializing process had a dags_folder
    equal to only the dag file in question. This prevents us from determining the relative location.
    And if the paths are different between worker and dag processor / scheduler, then we won't find
    the dag at the given location.
    """
    if val and Path(val).suffix in ('.zip', '.py'):
        matches = list(Path(settings.DAGS_FOLDER).rglob(Path(val).name))
        if len(matches) == 1:
            return matches[0].as_posix()
    return None


def get_dag(
    subdir: str | None, dag_id: str, include_examples=conf.getboolean('core', 'LOAD_EXAMPLES')
) -> DAG:
    """
    Returns DAG of a given dag_id

    First it we'll try to use the given subdir.  If that doesn't work, we'll try to
    find the correct path (assuming it's a file) and failing that, use the configured
    dags folder.
    """
    from airflow.models import DagBag

    first_path = process_subdir(subdir)
    dagbag = DagBag(first_path, include_examples=include_examples)
    if dag_id not in dagbag.dags:
        fallback_path = _search_for_dag_file(subdir) or settings.DAGS_FOLDER
        logger.warning("Dag %r not found in path %s; trying path %s", dag_id, first_path, fallback_path)
        dagbag = DagBag(dag_folder=fallback_path, include_examples=include_examples)
        if dag_id not in dagbag.dags:
            raise AirflowException(
                f"Dag {dag_id!r} could not be found; either it does not exist or it failed to parse."
            )
    return dagbag.dags[dag_id]


def get_dags(subdir: str | None, dag_id: str, use_regex: bool = False):
    """Returns DAG(s) matching a given regex or dag_id"""
    from airflow.models import DagBag

    if not use_regex:
        return [get_dag(subdir, dag_id)]
    dagbag = DagBag(process_subdir(subdir))
    matched_dags = [dag for dag in dagbag.dags.values() if re.search(dag_id, dag.dag_id)]
    if not matched_dags:
        raise AirflowException(
            f'dag_id could not be found with regex: {dag_id}. Either the dag did not exist or '
            f'it failed to parse.'
        )
    return matched_dags


@provide_session
def get_dag_by_pickle(pickle_id, session=None):
    """Fetch DAG from the database using pickling"""
    from airflow.models import DagPickle

    dag_pickle = session.query(DagPickle).filter(DagPickle.id == pickle_id).first()
    if not dag_pickle:
        raise AirflowException(f"pickle_id could not be found in DagPickle.id list: {pickle_id}")
    pickle_dag = dag_pickle.pickle
    return pickle_dag


def setup_locations(process, pid=None, stdout=None, stderr=None, log=None):
    """Creates logging paths"""
    if not stderr:
        stderr = os.path.join(settings.AIRFLOW_HOME, f'airflow-{process}.err')
    if not stdout:
        stdout = os.path.join(settings.AIRFLOW_HOME, f'airflow-{process}.out')
    if not log:
        log = os.path.join(settings.AIRFLOW_HOME, f'airflow-{process}.log')

    if not pid:
        pid = os.path.join(settings.AIRFLOW_HOME, f'airflow-{process}.pid')
    else:
        pid = os.path.abspath(pid)

    return pid, stdout, stderr, log


def setup_logging(filename):
    """Creates log file handler for daemon process"""
    root = logging.getLogger()
    handler = NonCachingFileHandler(filename)
    formatter = logging.Formatter(settings.SIMPLE_LOG_FORMAT)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(settings.LOGGING_LEVEL)

    return handler.stream


def sigint_handler(sig, frame):
    """
    Returns without error on SIGINT or SIGTERM signals in interactive command mode
    e.g. CTRL+C or kill <PID>
    """
    sys.exit(0)


def sigquit_handler(sig, frame):
    """
    Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\
    """
    print(f"Dumping stack traces for all threads in PID {os.getpid()}")
    id_to_name = {th.ident: th.name for th in threading.enumerate()}
    code = []
    for thread_id, stack in sys._current_frames().items():
        code.append(f"\n# Thread: {id_to_name.get(thread_id, '')}({thread_id})")
        for filename, line_number, name, line in traceback.extract_stack(stack):
            code.append(f'File: "{filename}", line {line_number}, in {name}')
            if line:
                code.append(f"  {line.strip()}")
    print("\n".join(code))


class ColorMode:
    """Coloring modes. If `auto` is then automatically detected."""

    ON = "on"
    OFF = "off"
    AUTO = "auto"


def should_use_colors(args) -> bool:
    """Processes arguments and decides whether to enable color in output"""
    if args.color == ColorMode.ON:
        return True
    if args.color == ColorMode.OFF:
        return False
    return is_terminal_support_colors()


def suppress_logs_and_warning(f: T) -> T:
    """
    Decorator to suppress logging and warning messages
    in cli functions.
    """

    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        _check_cli_args(args)
        if args[0].verbose:
            f(*args, **kwargs)
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                logging.disable(logging.CRITICAL)
                try:
                    f(*args, **kwargs)
                finally:
                    # logging output again depends on the effective
                    # levels of individual loggers
                    logging.disable(logging.NOTSET)

    return cast(T, _wrapper)
