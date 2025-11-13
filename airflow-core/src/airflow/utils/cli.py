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
"""Utilities module for cli."""

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
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, cast

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.exceptions import AirflowException
from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
from airflow.utils import cli_action_loggers
from airflow.utils.log.non_caching_file_handler import NonCachingFileHandler
from airflow.utils.platform import getuser, is_terminal_support_colors

T = TypeVar("T", bound=Callable)

if TYPE_CHECKING:
    from airflow.sdk import DAG
    from airflow.serialization.serialized_objects import SerializedDAG

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
        Decorate function to execute function at the same time submitting action_logging but in CLI context.

        It will call action logger callbacks twice, one for
        pre-execution and the other one for post-execution.

        Action logger will be called with below keyword parameters:
            sub_command : name of sub-command
            start_datetime : start datetime instance by utc
            end_datetime : end datetime instance by utc
            full_command : full command line arguments
            user : current user
            log : airflow.models.log.Log ORM instance
            dag_id : dag id (optional)
            task_id : task_id (optional)
            logical_date : logical date (optional)
            error : exception instance if there's an exception

        :param f: function instance
        :return: wrapped function
        """

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            """
            Wrap cli functions; assume Namespace instance as first positional argument.

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
                    from airflow.configuration import conf
                    from airflow.utils.db import check_and_run_migrations, synchronize_log_template

                    if conf.getboolean("database", "check_migrations"):
                        check_and_run_migrations()
                    synchronize_log_template()
                return f(*args, **kwargs)
            except Exception as e:
                metrics["error"] = e
                raise
            finally:
                metrics["end_datetime"] = timezone.utcnow()
                cli_action_loggers.on_post_execution(**metrics)

        return cast("T", wrapper)

    if func:
        return action_logging(func)
    return action_logging


def _build_metrics(func_name, namespace):
    """
    Build metrics dict from function args.

    It assumes that function arguments is from airflow.bin.cli module's function
    and has Namespace instance where it optionally contains "dag_id", "task_id",
    and "logical_date".

    :param func_name: name of function
    :param namespace: Namespace instance from argparse
    :return: dict with metrics
    """
    from airflow._shared.secrets_masker import _secrets_masker

    sub_commands_to_check_for_sensitive_fields = {"users", "connections"}
    sub_commands_to_check_for_sensitive_key = {"variables"}
    sensitive_fields = {"-p", "--password", "--conn-password"}
    full_command = list(sys.argv)
    sub_command = full_command[1] if len(full_command) > 1 else None
    # For cases when value under sub_commands_to_check_for_sensitive_key have sensitive info
    if sub_command in sub_commands_to_check_for_sensitive_key:
        key = full_command[-2] if len(full_command) > 3 else None
        if key and _secrets_masker().should_hide_value_for_key(key):
            # Mask the sensitive value since key contain sensitive keyword
            full_command[-1] = "*" * 8
    elif sub_command in sub_commands_to_check_for_sensitive_fields:
        for idx, command in enumerate(full_command):
            if command in sensitive_fields:
                # For cases when password is passed as "--password xyz" (with space between key and value)
                full_command[idx + 1] = "*" * 8
            else:
                # For cases when password is passed as "--password=xyz" (with '=' between key and value)
                for sensitive_field in sensitive_fields:
                    if command.startswith(f"{sensitive_field}="):
                        full_command[idx] = f"{sensitive_field}={'*' * 8}"

    # handle conn-json and conn-uri separately as it requires different handling
    if "--conn-json" in full_command:
        import json

        json_index = full_command.index("--conn-json") + 1
        conn_json = json.loads(full_command[json_index])
        for k in conn_json:
            if k and _secrets_masker().should_hide_value_for_key(k):
                conn_json[k] = "*" * 8
        full_command[json_index] = json.dumps(conn_json)

    if "--conn-uri" in full_command:
        from urllib.parse import urlparse, urlunparse

        uri_index = full_command.index("--conn-uri") + 1
        conn_uri = full_command[uri_index]
        parsed_uri = urlparse(conn_uri)
        netloc = parsed_uri.netloc
        if parsed_uri.password:
            password = "*" * 8
            netloc = f"{parsed_uri.username}:{password}@{parsed_uri.hostname}"
            if parsed_uri.port:
                netloc += f":{parsed_uri.port}"

        full_command[uri_index] = urlunparse(
            (
                parsed_uri.scheme,
                netloc,
                parsed_uri.path,
                parsed_uri.params,
                parsed_uri.query,
                parsed_uri.fragment,
            )
        )

    metrics = {
        "sub_command": func_name,
        "start_datetime": timezone.utcnow(),
        "full_command": f"{full_command}",
        "user": getuser(),
    }

    if not isinstance(namespace, Namespace):
        raise ValueError(
            f"namespace argument should be argparse.Namespace instance, but is {type(namespace)}"
        )
    tmp_dic = vars(namespace)
    metrics["dag_id"] = tmp_dic.get("dag_id")
    metrics["task_id"] = tmp_dic.get("task_id")
    metrics["logical_date"] = tmp_dic.get("logical_date")
    metrics["host_name"] = socket.gethostname()

    return metrics


def process_subdir(subdir: str | None):
    """Expand path to absolute by replacing 'DAGS_FOLDER', '~', '.', etc."""
    if subdir:
        if not settings.DAGS_FOLDER:
            raise ValueError("DAGS_FOLDER variable in settings should be filled.")
        subdir = subdir.replace("DAGS_FOLDER", settings.DAGS_FOLDER)
        subdir = os.path.abspath(os.path.expanduser(subdir))
    return subdir


def get_dag_by_file_location(dag_id: str):
    """Return DAG of a given dag_id by looking up file location."""
    # TODO: AIP-66 - investigate more, can we use serdag?
    from airflow.dag_processing.dagbag import DagBag
    from airflow.models import DagModel

    # Benefit is that logging from other dags in dagbag will not appear
    dag_model = DagModel.get_current(dag_id)
    if dag_model is None:
        raise AirflowException(
            f"Dag {dag_id!r} could not be found; either it does not exist or it failed to parse."
        )
    # This method is called only when we explicitly do not have a bundle name
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
    if val and Path(val).suffix in (".zip", ".py"):
        matches = list(Path(settings.DAGS_FOLDER).rglob(Path(val).name))
        if len(matches) == 1:
            return matches[0].as_posix()
    return None


def get_bagged_dag(bundle_names: list | None, dag_id: str, dagfile_path: str | None = None) -> DAG:
    """
    Return DAG of a given dag_id.

    First we'll try to use the given subdir.  If that doesn't work, we'll try to
    find the correct path (assuming it's a file) and failing that, use the configured
    dags folder.
    """
    from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db

    manager = DagBundlesManager()
    for bundle_name in bundle_names or ():
        bundle = manager.get_bundle(bundle_name)
        with _airflow_parsing_context_manager(dag_id=dag_id):
            dagbag = DagBag(
                dag_folder=dagfile_path or bundle.path,
                bundle_path=bundle.path,
                bundle_name=bundle.name,
                include_examples=False,
            )
        if dag := dagbag.dags.get(dag_id):
            return dag

    manager.sync_bundles_to_db()
    for bundle in manager.get_all_dag_bundles():
        bundle.initialize()
        with _airflow_parsing_context_manager(dag_id=dag_id):
            dagbag = DagBag(
                dag_folder=dagfile_path or bundle.path,
                bundle_path=bundle.path,
                bundle_name=bundle.name,
                include_examples=False,
            )
            sync_bag_to_db(dagbag, bundle.name, bundle.version)
        if dag := dagbag.dags.get(dag_id):
            return dag
        if dag:
            break
    raise AirflowException(
        f"Dag {dag_id!r} could not be found; either it does not exist or it failed to parse."
    )


def get_db_dag(bundle_names: list | None, dag_id: str, dagfile_path: str | None = None) -> SerializedDAG:
    """
    Return DAG of a given dag_id.

    This gets a serialized dag from the database.
    """
    from airflow.models.serialized_dag import SerializedDagModel

    if dag := SerializedDagModel.get_dag(dag_id):
        return dag
    raise AirflowException(f"Dag {dag_id!r} could not be found in the database.")


def get_dags(bundle_names: list | None, dag_id: str, use_regex: bool = False, from_db: bool = False):
    """Return DAG(s) matching a given regex or dag_id."""
    from airflow.dag_processing.dagbag import DagBag

    bundle_names = bundle_names or []

    if not use_regex:
        if from_db:
            return [get_db_dag(bundle_names=bundle_names, dag_id=dag_id)]
        return [get_bagged_dag(bundle_names=bundle_names, dag_id=dag_id)]

    def _find_dag(bundle):
        dagbag = DagBag(dag_folder=bundle.path, bundle_path=bundle.path, bundle_name=bundle.name)
        matched_dags = [dag for dag in dagbag.dags.values() if re.search(dag_id, dag.dag_id)]
        return matched_dags

    manager = DagBundlesManager()
    matched_dags = []
    for bundle_name in bundle_names:
        bundle = manager.get_bundle(bundle_name)
        matched_dags = _find_dag(bundle)
        if matched_dags:
            break
    if not matched_dags:
        # Search in all bundles
        all_bundles = list(manager.get_all_dag_bundles())
        for bundle in all_bundles:
            matched_dags = _find_dag(bundle)
            if matched_dags:
                break
    if not matched_dags:
        raise AirflowException(
            f"dag_id could not be found with regex: {dag_id}. Either the dag did not exist or "
            f"it failed to parse."
        )
    return matched_dags


def setup_locations(process, pid=None, stdout=None, stderr=None, log=None):
    """Create logging paths."""
    if not stderr:
        stderr = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process}.err")
    if not stdout:
        stdout = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process}.out")
    if not log:
        log = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process}.log")

    if not pid:
        pid = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process}.pid")
    else:
        pid = os.path.abspath(pid)

    return pid, stdout, stderr, log


def setup_logging(filename):
    """Create log file handler for daemon process."""
    root = logging.getLogger()
    handler = NonCachingFileHandler(filename)
    formatter = logging.Formatter(settings.SIMPLE_LOG_FORMAT)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(settings.LOGGING_LEVEL)

    return handler.stream


def sigint_handler(sig, frame):
    """
    Return without error on SIGINT or SIGTERM signals in interactive command mode.

    e.g. CTRL+C or kill <PID>
    """
    sys.exit(0)


def sigquit_handler(sig, frame):
    """
    Help debug deadlocks by printing stacktraces when this gets a SIGQUIT.

    e.g. kill -s QUIT <PID> or CTRL+
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
    """Process arguments and decide whether to enable color in output."""
    if args.color == ColorMode.ON:
        return True
    if args.color == ColorMode.OFF:
        return False
    return is_terminal_support_colors()


def suppress_logs_and_warning(f: T) -> T:
    """Suppress logging and warning messages in cli functions."""

    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        _check_cli_args(args)
        if args[0].verbose:
            f(*args, **kwargs)
        else:
            from airflow._shared.logging.structlog import respect_stdlib_disable

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                logging.disable(logging.CRITICAL)

                def drop(*_, **__):
                    from structlog import DropEvent

                    raise DropEvent()

                old_fn = respect_stdlib_disable.__code__
                respect_stdlib_disable.__code__ = drop.__code__
                try:
                    f(*args, **kwargs)
                finally:
                    # logging output again depends on the effective
                    # levels of individual loggers
                    logging.disable(logging.NOTSET)
                    respect_stdlib_disable.__code__ = old_fn

    return cast("T", _wrapper)


def validate_dag_bundle_arg(bundle_names: list[str]) -> None:
    """Make sure only known bundles are passed as arguments."""
    known_bundles = {b.name for b in DagBundlesManager().get_all_dag_bundles()}

    unknown_bundles: set[str] = set(bundle_names) - known_bundles
    if unknown_bundles:
        raise SystemExit(f"Bundles not found: {', '.join(unknown_bundles)}")


def should_enable_hot_reload(args) -> bool:
    """Check whether hot-reload should be enabled based on --dev flag or DEV_MODE env var."""
    if getattr(args, "dev", False):
        return True
    return os.getenv("DEV_MODE", "false").lower() == "true"
