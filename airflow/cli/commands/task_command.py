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
"""Task sub-commands"""
from __future__ import annotations

import datetime
import importlib
import json
import logging
import os
import textwrap
from contextlib import contextmanager, redirect_stderr, redirect_stdout, suppress
from typing import Generator, Union

from pendulum.parsing.exceptions import ParserError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session

from airflow import settings
from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import conf
from airflow.exceptions import AirflowException, DagRunNotFound, TaskInstanceNotFound
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.local_task_job import LocalTaskJob
from airflow.models import DagPickle, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULER_QUEUED_DEPS
from airflow.typing_compat import Literal
from airflow.utils import cli as cli_utils
from airflow.utils.cli import (
    get_dag,
    get_dag_by_file_location,
    get_dag_by_pickle,
    get_dags,
    suppress_logs_and_warning,
)
from airflow.utils.dates import timezone
from airflow.utils.log.logging_mixin import StreamLogWriter
from airflow.utils.log.secrets_masker import RedactedIO
from airflow.utils.net import get_hostname
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.state import DagRunState

log = logging.getLogger(__name__)

CreateIfNecessary = Union[Literal[False], Literal["db"], Literal["memory"]]


def _generate_temporary_run_id() -> str:
    """Generate a ``run_id`` for a DAG run that will be created temporarily.

    This is used mostly by ``airflow task test`` to create a DAG run that will
    be deleted after the task is run.
    """
    return f"__airflow_temporary_run_{timezone.utcnow().isoformat()}__"


def _get_dag_run(
    *,
    dag: DAG,
    create_if_necessary: CreateIfNecessary,
    exec_date_or_run_id: str | None = None,
    session: Session,
) -> tuple[DagRun, bool]:
    """Try to retrieve a DAG run from a string representing either a run ID or logical date.

    This checks DAG runs like this:

    1. If the input ``exec_date_or_run_id`` matches a DAG run ID, return the run.
    2. Try to parse the input as a date. If that works, and the resulting
       date matches a DAG run's logical date, return the run.
    3. If ``create_if_necessary`` is *False* and the input works for neither of
       the above, raise ``DagRunNotFound``.
    4. Try to create a new DAG run. If the input looks like a date, use it as
       the logical date; otherwise use it as a run ID and set the logical date
       to the current time.
    """
    if not exec_date_or_run_id and not create_if_necessary:
        raise ValueError("Must provide `exec_date_or_run_id` if not `create_if_necessary`.")
    execution_date: datetime.datetime | None = None
    if exec_date_or_run_id:
        dag_run = dag.get_dagrun(run_id=exec_date_or_run_id, session=session)
        if dag_run:
            return dag_run, False
        with suppress(ParserError, TypeError):
            execution_date = timezone.parse(exec_date_or_run_id)
        try:
            dag_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag.dag_id, DagRun.execution_date == execution_date)
                .one()
            )
        except NoResultFound:
            if not create_if_necessary:
                raise DagRunNotFound(
                    f"DagRun for {dag.dag_id} with run_id or execution_date "
                    f"of {exec_date_or_run_id!r} not found"
                ) from None
        else:
            return dag_run, False

    if execution_date is not None:
        dag_run_execution_date = execution_date
    else:
        dag_run_execution_date = timezone.utcnow()

    if create_if_necessary == "memory":
        dag_run = DagRun(dag.dag_id, run_id=exec_date_or_run_id, execution_date=dag_run_execution_date)
        return dag_run, True
    elif create_if_necessary == "db":
        dag_run = dag.create_dagrun(
            state=DagRunState.QUEUED,
            execution_date=dag_run_execution_date,
            run_id=_generate_temporary_run_id(),
            session=session,
        )
        return dag_run, True
    raise ValueError(f"unknown create_if_necessary value: {create_if_necessary!r}")


@provide_session
def _get_ti(
    task: BaseOperator,
    map_index: int,
    *,
    exec_date_or_run_id: str | None = None,
    pool: str | None = None,
    create_if_necessary: CreateIfNecessary = False,
    session: Session = NEW_SESSION,
) -> tuple[TaskInstance, bool]:
    """Get the task instance through DagRun.run_id, if that fails, get the TI the old way"""
    if not exec_date_or_run_id and not create_if_necessary:
        raise ValueError("Must provide `exec_date_or_run_id` if not `create_if_necessary`.")
    if task.is_mapped:
        if map_index < 0:
            raise RuntimeError("No map_index passed to mapped task")
    elif map_index >= 0:
        raise RuntimeError("map_index passed to non-mapped task")
    dag_run, dr_created = _get_dag_run(
        dag=task.dag,
        exec_date_or_run_id=exec_date_or_run_id,
        create_if_necessary=create_if_necessary,
        session=session,
    )

    ti_or_none = dag_run.get_task_instance(task.task_id, map_index=map_index, session=session)
    if ti_or_none is None:
        if not create_if_necessary:
            raise TaskInstanceNotFound(
                f"TaskInstance for {task.dag.dag_id}, {task.task_id}, map={map_index} with "
                f"run_id or execution_date of {exec_date_or_run_id!r} not found"
            )
        # TODO: Validate map_index is in range?
        ti = TaskInstance(task, run_id=dag_run.run_id, map_index=map_index)
        ti.dag_run = dag_run
    else:
        ti = ti_or_none
    ti.refresh_from_task(task, pool_override=pool)
    return ti, dr_created


def _run_task_by_selected_method(args, dag: DAG, ti: TaskInstance) -> None:
    """
    Runs the task in one of 3 modes

    - using LocalTaskJob
    - as raw task
    - by executor
    """
    if args.local:
        _run_task_by_local_task_job(args, ti)
    elif args.raw:
        _run_raw_task(args, ti)
    else:
        _run_task_by_executor(args, dag, ti)


def _run_task_by_executor(args, dag, ti):
    """
    Sends the task to the executor for execution. This can result in the task being started by another host
    if the executor implementation does
    """
    pickle_id = None
    if args.ship_dag:
        try:
            # Running remotely, so pickling the DAG
            with create_session() as session:
                pickle = DagPickle(dag)
                session.add(pickle)
            pickle_id = pickle.id
            # TODO: This should be written to a log
            print(f"Pickled dag {dag} as pickle_id: {pickle_id}")
        except Exception as e:
            print("Could not pickle the DAG")
            print(e)
            raise e
    executor = ExecutorLoader.get_default_executor()
    executor.job_id = "manual"
    executor.start()
    print("Sending to executor.")
    executor.queue_task_instance(
        ti,
        mark_success=args.mark_success,
        pickle_id=pickle_id,
        ignore_all_deps=args.ignore_all_dependencies,
        ignore_depends_on_past=args.ignore_depends_on_past,
        ignore_task_deps=args.ignore_dependencies,
        ignore_ti_state=args.force,
        pool=args.pool,
    )
    executor.heartbeat()
    executor.end()


def _run_task_by_local_task_job(args, ti):
    """Run LocalTaskJob, which monitors the raw task execution process"""
    run_job = LocalTaskJob(
        task_instance=ti,
        mark_success=args.mark_success,
        pickle_id=args.pickle,
        ignore_all_deps=args.ignore_all_dependencies,
        ignore_depends_on_past=args.ignore_depends_on_past,
        ignore_task_deps=args.ignore_dependencies,
        ignore_ti_state=args.force,
        pool=args.pool,
        external_executor_id=_extract_external_executor_id(args),
    )
    try:
        run_job.run()

    finally:
        if args.shut_down_logging:
            logging.shutdown()


RAW_TASK_UNSUPPORTED_OPTION = [
    "ignore_all_dependencies",
    "ignore_depends_on_past",
    "ignore_dependencies",
    "force",
]


def _run_raw_task(args, ti: TaskInstance) -> None:
    """Runs the main task handling code"""
    ti._run_raw_task(
        mark_success=args.mark_success,
        job_id=args.job_id,
        pool=args.pool,
    )


def _extract_external_executor_id(args) -> str | None:
    if hasattr(args, "external_executor_id"):
        return getattr(args, "external_executor_id")
    return os.environ.get("external_executor_id", None)


@contextmanager
def _capture_task_logs(ti: TaskInstance) -> Generator[None, None, None]:
    """Manage logging context for a task run

    - Replace the root logger configuration with the airflow.task configuration
      so we can capture logs from any custom loggers used in the task.

    - Redirect stdout and stderr to the task instance log, as INFO and WARNING
      level messages, respectively.

    """
    modify = not settings.DONOT_MODIFY_HANDLERS

    if modify:
        root_logger, task_logger = logging.getLogger(), logging.getLogger("airflow.task")

        orig_level = root_logger.level
        root_logger.setLevel(task_logger.level)
        orig_handlers = root_logger.handlers.copy()
        root_logger.handlers[:] = task_logger.handlers

    try:
        info_writer = StreamLogWriter(ti.log, logging.INFO)
        warning_writer = StreamLogWriter(ti.log, logging.WARNING)

        with redirect_stdout(info_writer), redirect_stderr(warning_writer):
            yield

    finally:
        if modify:
            # Restore the root logger to its original state.
            root_logger.setLevel(orig_level)
            root_logger.handlers[:] = orig_handlers


@cli_utils.action_cli(check_db=False)
def task_run(args, dag=None):
    """Run a single task instance.

    Note that there must be at least one DagRun for this to start,
    i.e. it must have been scheduled and/or triggered previously.
    Alternatively, if you just need to run it for testing then use
    "airflow tasks test ..." command instead.
    """
    # Load custom airflow config

    if args.local and args.raw:
        raise AirflowException(
            "Option --raw and --local are mutually exclusive. "
            "Please remove one option to execute the command."
        )

    if args.raw:
        unsupported_options = [o for o in RAW_TASK_UNSUPPORTED_OPTION if getattr(args, o)]

        if unsupported_options:
            unsupported_raw_task_flags = ", ".join(f"--{o}" for o in RAW_TASK_UNSUPPORTED_OPTION)
            unsupported_flags = ", ".join(f"--{o}" for o in unsupported_options)
            raise AirflowException(
                "Option --raw does not work with some of the other options on this command. "
                "You can't use --raw option and the following options: "
                f"{unsupported_raw_task_flags}. "
                f"You provided the option {unsupported_flags}. "
                "Delete it to execute the command."
            )
    if dag and args.pickle:
        raise AirflowException("You cannot use the --pickle option when using DAG.cli() method.")
    if args.cfg_path:
        with open(args.cfg_path) as conf_file:
            conf_dict = json.load(conf_file)

        if os.path.exists(args.cfg_path):
            os.remove(args.cfg_path)

        conf.read_dict(conf_dict, source=args.cfg_path)
        settings.configure_vars()

    settings.MASK_SECRETS_IN_LOGS = True

    # IMPORTANT, have to re-configure ORM with the NullPool, otherwise, each "run" command may leave
    # behind multiple open sleeping connections while heartbeating, which could
    # easily exceed the database connection limit when
    # processing hundreds of simultaneous tasks.
    settings.reconfigure_orm(disable_connection_pool=True)

    if args.pickle:
        print(f"Loading pickle id: {args.pickle}")
        dag = get_dag_by_pickle(args.pickle)
    elif not dag:
        dag = get_dag(args.subdir, args.dag_id, include_examples=False)
    else:
        # Use DAG from parameter
        pass
    task = dag.get_task(task_id=args.task_id)
    ti, _ = _get_ti(task, args.map_index, exec_date_or_run_id=args.execution_date_or_run_id, pool=args.pool)
    ti.init_run_context(raw=args.raw)

    hostname = get_hostname()

    log.info("Running %s on host %s", ti, hostname)

    if args.interactive:
        _run_task_by_selected_method(args, dag, ti)
    else:
        with _capture_task_logs(ti):
            _run_task_by_selected_method(args, dag, ti)


@cli_utils.action_cli(check_db=False)
def task_failed_deps(args):
    """
    Returns the unmet dependencies for a task instance from the perspective of the
    scheduler (i.e. why a task instance doesn't get scheduled and then queued by the
    scheduler, and then run by an executor).
    >>> airflow tasks failed-deps tutorial sleep 2015-01-01
    Task instance dependencies not met:
    Dagrun Running: Task instance's dagrun did not exist: Unknown reason
    Trigger Rule: Task's trigger rule 'all_success' requires all upstream tasks
    to have succeeded, but found 1 non-success(es).
    """
    dag = get_dag(args.subdir, args.dag_id)
    task = dag.get_task(task_id=args.task_id)
    ti, _ = _get_ti(task, args.map_index, exec_date_or_run_id=args.execution_date_or_run_id)

    dep_context = DepContext(deps=SCHEDULER_QUEUED_DEPS)
    failed_deps = list(ti.get_failed_dep_statuses(dep_context=dep_context))
    # TODO, Do we want to print or log this
    if failed_deps:
        print("Task instance dependencies not met:")
        for dep in failed_deps:
            print(f"{dep.dep_name}: {dep.reason}")
    else:
        print("Task instance dependencies are all met.")


@cli_utils.action_cli(check_db=False)
@suppress_logs_and_warning
def task_state(args):
    """
    Returns the state of a TaskInstance at the command line.
    >>> airflow tasks state tutorial sleep 2015-01-01
    success
    """
    dag = get_dag(args.subdir, args.dag_id)
    task = dag.get_task(task_id=args.task_id)
    ti, _ = _get_ti(task, args.map_index, exec_date_or_run_id=args.execution_date_or_run_id)
    print(ti.current_state())


@cli_utils.action_cli(check_db=False)
@suppress_logs_and_warning
def task_list(args, dag=None):
    """Lists the tasks within a DAG at the command line"""
    dag = dag or get_dag(args.subdir, args.dag_id)
    if args.tree:
        dag.tree_view()
    else:
        tasks = sorted(t.task_id for t in dag.tasks)
        print("\n".join(tasks))


SUPPORTED_DEBUGGER_MODULES: list[str] = [
    "pudb",
    "web_pdb",
    "ipdb",
    "pdb",
]


def _guess_debugger():
    """
    Trying to guess the debugger used by the user. When it doesn't find any user-installed debugger,
    returns ``pdb``.

    List of supported debuggers:

    * `pudb <https://github.com/inducer/pudb>`__
    * `web_pdb <https://github.com/romanvm/python-web-pdb>`__
    * `ipdb <https://github.com/gotcha/ipdb>`__
    * `pdb <https://docs.python.org/3/library/pdb.html>`__
    """
    for mod in SUPPORTED_DEBUGGER_MODULES:
        try:
            return importlib.import_module(mod)
        except ImportError:
            continue
    return importlib.import_module("pdb")


@cli_utils.action_cli(check_db=False)
@suppress_logs_and_warning
@provide_session
def task_states_for_dag_run(args, session=None):
    """Get the status of all task instances in a DagRun"""
    dag_run = (
        session.query(DagRun)
        .filter(DagRun.run_id == args.execution_date_or_run_id, DagRun.dag_id == args.dag_id)
        .one_or_none()
    )
    if not dag_run:
        try:
            execution_date = timezone.parse(args.execution_date_or_run_id)
            dag_run = (
                session.query(DagRun)
                .filter(DagRun.execution_date == execution_date, DagRun.dag_id == args.dag_id)
                .one_or_none()
            )
        except (ParserError, TypeError) as err:
            raise AirflowException(f"Error parsing the supplied execution_date. Error: {str(err)}")

    if dag_run is None:
        raise DagRunNotFound(
            f"DagRun for {args.dag_id} with run_id or execution_date of {args.execution_date_or_run_id!r} "
            "not found"
        )

    has_mapped_instances = any(ti.map_index >= 0 for ti in dag_run.task_instances)

    def format_task_instance(ti: TaskInstance) -> dict[str, str]:
        data = {
            "dag_id": ti.dag_id,
            "execution_date": dag_run.execution_date.isoformat(),
            "task_id": ti.task_id,
            "state": ti.state,
            "start_date": ti.start_date.isoformat() if ti.start_date else "",
            "end_date": ti.end_date.isoformat() if ti.end_date else "",
        }
        if has_mapped_instances:
            data["map_index"] = str(ti.map_index) if ti.map_index >= 0 else ""
        return data

    AirflowConsole().print_as(data=dag_run.task_instances, output=args.output, mapper=format_task_instance)


@cli_utils.action_cli(check_db=False)
def task_test(args, dag=None):
    """Tests task for a given dag_id"""
    # We want to log output from operators etc to show up here. Normally
    # airflow.task would redirect to a file, but here we want it to propagate
    # up to the normal airflow handler.

    settings.MASK_SECRETS_IN_LOGS = True

    handlers = logging.getLogger("airflow.task").handlers
    already_has_stream_handler = False
    for handler in handlers:
        already_has_stream_handler = isinstance(handler, logging.StreamHandler)
        if already_has_stream_handler:
            break
    if not already_has_stream_handler:
        logging.getLogger("airflow.task").propagate = True

    env_vars = {"AIRFLOW_TEST_MODE": "True"}
    if args.env_vars:
        env_vars.update(args.env_vars)
        os.environ.update(env_vars)

    dag = dag or get_dag(args.subdir, args.dag_id)

    task = dag.get_task(task_id=args.task_id)
    # Add CLI provided task_params to task.params
    if args.task_params:
        passed_in_params = json.loads(args.task_params)
        task.params.update(passed_in_params)

    if task.params:
        task.params.validate()

    ti, dr_created = _get_ti(
        task, args.map_index, exec_date_or_run_id=args.execution_date_or_run_id, create_if_necessary="db"
    )

    try:
        with redirect_stdout(RedactedIO()):
            if args.dry_run:
                ti.dry_run()
            else:
                ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)
    except Exception:
        if args.post_mortem:
            debugger = _guess_debugger()
            debugger.post_mortem()
        else:
            raise
    finally:
        if not already_has_stream_handler:
            # Make sure to reset back to normal. When run for CLI this doesn't
            # matter, but it does for test suite
            logging.getLogger("airflow.task").propagate = False
        if dr_created:
            with create_session() as session:
                session.delete(ti.dag_run)


@cli_utils.action_cli(check_db=False)
@suppress_logs_and_warning
def task_render(args):
    """Renders and displays templated fields for a given task"""
    dag = get_dag(args.subdir, args.dag_id)
    task = dag.get_task(task_id=args.task_id)
    ti, _ = _get_ti(
        task, args.map_index, exec_date_or_run_id=args.execution_date_or_run_id, create_if_necessary="memory"
    )
    ti.render_templates()
    for attr in task.__class__.template_fields:
        print(
            textwrap.dedent(
                f"""        # ----------------------------------------------------------
        # property: {attr}
        # ----------------------------------------------------------
        {getattr(task, attr)}
        """
            )
        )


@cli_utils.action_cli(check_db=False)
def task_clear(args):
    """Clears all task instances or only those matched by regex for a DAG(s)"""
    logging.basicConfig(level=settings.LOGGING_LEVEL, format=settings.SIMPLE_LOG_FORMAT)

    if args.dag_id and not args.subdir and not args.dag_regex and not args.task_regex:
        dags = [get_dag_by_file_location(args.dag_id)]
    else:
        # todo clear command only accepts a single dag_id. no reason for get_dags with 's' except regex?
        dags = get_dags(args.subdir, args.dag_id, use_regex=args.dag_regex)

        if args.task_regex:
            for idx, dag in enumerate(dags):
                dags[idx] = dag.partial_subset(
                    task_ids_or_regex=args.task_regex,
                    include_downstream=args.downstream,
                    include_upstream=args.upstream,
                )

    DAG.clear_dags(
        dags,
        start_date=args.start_date,
        end_date=args.end_date,
        only_failed=args.only_failed,
        only_running=args.only_running,
        confirm_prompt=not args.yes,
        include_subdags=not args.exclude_subdags,
        include_parentdag=not args.exclude_parentdag,
    )
