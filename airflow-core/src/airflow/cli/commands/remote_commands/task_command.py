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
"""Task sub-commands."""

from __future__ import annotations

import functools
import importlib
import json
import logging
import os
import textwrap
from contextlib import redirect_stdout
from typing import TYPE_CHECKING, Protocol, cast

from airflow import settings
from airflow.cli.simple_table import AirflowConsole
from airflow.cli.utils import fetch_dag_run_from_run_id_or_logical_date_string
from airflow.exceptions import DagRunNotFound, TaskDeferred, TaskInstanceNotFound
from airflow.models import TaskInstance
from airflow.models.dag import DAG, _run_inline_trigger
from airflow.models.dagrun import DagRun
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.execution_time.secrets_masker import RedactedIO
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULER_QUEUED_DEPS
from airflow.utils import cli as cli_utils, timezone
from airflow.utils.cli import (
    get_dag,
    get_dag_by_file_location,
    get_dags,
    suppress_logs_and_warning,
)
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.state import DagRunState
from airflow.utils.task_instance_session import set_current_task_instance_session
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from typing import Literal

    from sqlalchemy.orm.session import Session

    from airflow.models.operator import Operator

    CreateIfNecessary = Literal[False, "db", "memory"]

log = logging.getLogger(__name__)


def _generate_temporary_run_id() -> str:
    """
    Generate a ``run_id`` for a DAG run that will be created temporarily.

    This is used mostly by ``airflow task test`` to create a DAG run that will
    be deleted after the task is run.
    """
    return f"__airflow_temporary_run_{timezone.utcnow().isoformat()}__"


def _get_dag_run(
    *,
    dag: DAG,
    create_if_necessary: CreateIfNecessary,
    logical_date_or_run_id: str | None = None,
    session: Session | None = None,
) -> tuple[DagRun, bool]:
    """
    Try to retrieve a DAG run from a string representing either a run ID or logical date.

    This checks DAG runs like this:

    1. If the input ``logical_date_or_run_id`` matches a DAG run ID, return the run.
    2. Try to parse the input as a date. If that works, and the resulting
       date matches a DAG run's logical date, return the run.
    3. If ``create_if_necessary`` is *False* and the input works for neither of
       the above, raise ``DagRunNotFound``.
    4. Try to create a new DAG run. If the input looks like a date, use it as
       the logical date; otherwise use it as a run ID and set the logical date
       to the current time.
    """
    if not logical_date_or_run_id and not create_if_necessary:
        raise ValueError("Must provide `logical_date_or_run_id` if not `create_if_necessary`.")

    logical_date = None
    if logical_date_or_run_id:
        dag_run, logical_date = fetch_dag_run_from_run_id_or_logical_date_string(
            dag_id=dag.dag_id,
            value=logical_date_or_run_id,
            session=session,
        )
        if dag_run is not None:
            return dag_run, False
        elif not create_if_necessary:
            raise DagRunNotFound(
                f"DagRun for {dag.dag_id} with run_id or logical_date of {logical_date_or_run_id!r} not found"
            )

    dag_run_logical_date = timezone.coerce_datetime(logical_date)
    data_interval = (
        dag.timetable.infer_manual_data_interval(run_after=dag_run_logical_date)
        if dag_run_logical_date
        else None
    )
    run_after = data_interval.end if data_interval else timezone.utcnow()
    if create_if_necessary == "memory":
        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_id=logical_date_or_run_id,
            run_type=DagRunType.MANUAL,
            logical_date=dag_run_logical_date,
            data_interval=data_interval,
            run_after=run_after,
            triggered_by=DagRunTriggeredByType.CLI,
            state=DagRunState.RUNNING,
        )
        return dag_run, True
    elif create_if_necessary == "db":
        dag_run = dag.create_dagrun(
            run_id=_generate_temporary_run_id(),
            logical_date=dag_run_logical_date,
            data_interval=data_interval,
            run_after=run_after,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.CLI,
            dag_version=None,
            state=DagRunState.RUNNING,
            session=session,
        )
        return dag_run, True
    raise ValueError(f"unknown create_if_necessary value: {create_if_necessary!r}")


@provide_session
def _get_ti(
    task: Operator,
    map_index: int,
    *,
    logical_date_or_run_id: str | None = None,
    pool: str | None = None,
    create_if_necessary: CreateIfNecessary = False,
    session: Session = NEW_SESSION,
):
    dag = task.dag
    if dag is None:
        raise ValueError("Cannot get task instance for a task not assigned to a DAG")
    if not isinstance(dag, DAG):
        # TODO: Task-SDK: Shouldn't really happen, and this command will go away before 3.0
        raise ValueError(
            f"We need a {DAG.__module__}.DAG, but we got {type(dag).__module__}.{type(dag).__name__}!"
        )

    # this check is imperfect because diff dags could have tasks with same name
    # but in a task, dag_id is a property that accesses its dag, and we don't
    # currently include the dag when serializing an operator
    if task.task_id not in dag.task_dict:
        raise ValueError(f"Provided task {task.task_id} is not in dag '{dag.dag_id}.")

    if not logical_date_or_run_id and not create_if_necessary:
        raise ValueError("Must provide `logical_date_or_run_id` if not `create_if_necessary`.")
    if task.get_needs_expansion():
        if map_index < 0:
            raise RuntimeError("No map_index passed to mapped task")
    elif map_index >= 0:
        raise RuntimeError("map_index passed to non-mapped task")
    dag_run, dr_created = _get_dag_run(
        dag=dag,
        logical_date_or_run_id=logical_date_or_run_id,
        create_if_necessary=create_if_necessary,
        session=session,
    )

    ti_or_none = dag_run.get_task_instance(task.task_id, map_index=map_index, session=session)
    ti: TaskInstance
    if ti_or_none is None:
        if not create_if_necessary:
            raise TaskInstanceNotFound(
                f"TaskInstance for {dag.dag_id}, {task.task_id}, map={map_index} with "
                f"run_id or logical_date of {logical_date_or_run_id!r} not found"
            )
        # TODO: Validate map_index is in range?
        ti = TaskInstance(task, run_id=dag_run.run_id, map_index=map_index)
        if dag_run in session:
            session.add(ti)
        ti.dag_run = dag_run
    else:
        ti = ti_or_none
    ti.refresh_from_task(task, pool_override=pool)

    # we do refresh_from_task so that if TI has come back via RPC, we ensure that ti.task
    # is the original task object and not the result of the round trip
    ti.refresh_from_task(task, pool_override=pool)

    ti.dag_model  # we must ensure dag model is loaded eagerly for bundle info

    return ti, dr_created


class TaskCommandMarker:
    """Marker for listener hooks, to properly detect from which component they are called."""


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def task_failed_deps(args) -> None:
    """
    Get task instance dependencies that were not met.

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
    ti, _ = _get_ti(task, args.map_index, logical_date_or_run_id=args.logical_date_or_run_id)
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
@providers_configuration_loaded
def task_state(args) -> None:
    """
    Return the state of a TaskInstance at the command line.

    >>> airflow tasks state tutorial sleep 2015-01-01
    success
    """
    dag = get_dag(args.subdir, args.dag_id)
    task = dag.get_task(task_id=args.task_id)
    ti, _ = _get_ti(task, args.map_index, logical_date_or_run_id=args.logical_date_or_run_id)
    print(ti.current_state())


@cli_utils.action_cli(check_db=False)
@suppress_logs_and_warning
@providers_configuration_loaded
def task_list(args, dag: DAG | None = None) -> None:
    """List the tasks within a DAG at the command line."""
    dag = dag or get_dag(args.subdir, args.dag_id)
    tasks = sorted(t.task_id for t in dag.tasks)
    print("\n".join(tasks))


class _SupportedDebugger(Protocol):
    def post_mortem(self) -> None: ...


SUPPORTED_DEBUGGER_MODULES = [
    "pudb",
    "web_pdb",
    "ipdb",
    "pdb",
]


def _guess_debugger() -> _SupportedDebugger:
    """
    Try to guess the debugger used by the user.

    When it doesn't find any user-installed debugger, returns ``pdb``.

    List of supported debuggers:

    * `pudb <https://github.com/inducer/pudb>`__
    * `web_pdb <https://github.com/romanvm/python-web-pdb>`__
    * `ipdb <https://github.com/gotcha/ipdb>`__
    * `pdb <https://docs.python.org/3/library/pdb.html>`__
    """
    exc: Exception
    for mod_name in SUPPORTED_DEBUGGER_MODULES:
        try:
            return cast(_SupportedDebugger, importlib.import_module(mod_name))
        except ImportError as e:
            exc = e
    raise exc


@cli_utils.action_cli(check_db=False)
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_session
def task_states_for_dag_run(args, session: Session = NEW_SESSION) -> None:
    """Get the status of all task instances in a DagRun."""
    dag_run, _ = fetch_dag_run_from_run_id_or_logical_date_string(
        dag_id=args.dag_id,
        value=args.logical_date_or_run_id,
        session=session,
    )

    if dag_run is None:
        raise DagRunNotFound(
            f"DagRun for {args.dag_id} with run_id or logical_date of {args.logical_date_or_run_id!r} "
            "not found"
        )

    has_mapped_instances = any(ti.map_index >= 0 for ti in dag_run.task_instances)

    def format_task_instance(ti: TaskInstance) -> dict[str, str]:
        data = {
            "dag_id": ti.dag_id,
            "logical_date": dag_run.logical_date.isoformat() if dag_run.logical_date else "",
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
@provide_session
def task_test(args, dag: DAG | None = None, session: Session = NEW_SESSION) -> None:
    """Test task for a given dag_id."""
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

    if task.params and isinstance(task.params, ParamsDict):
        task.params.validate()

    ti, dr_created = _get_ti(
        task, args.map_index, logical_date_or_run_id=args.logical_date_or_run_id, create_if_necessary="db"
    )
    try:
        with redirect_stdout(RedactedIO()):
            if args.dry_run:
                ti.dry_run()
            else:
                ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, raise_on_defer=True)
    except TaskDeferred as defer:
        ti.defer_task(exception=defer, session=session)
        log.info("[TASK TEST] running trigger in line")

        event = _run_inline_trigger(defer.trigger)
        ti.next_method = defer.method_name
        ti.next_kwargs = {"event": event.payload} if event else defer.kwargs

        execute_callable = getattr(task, ti.next_method)
        if ti.next_kwargs:
            execute_callable = functools.partial(execute_callable, **ti.next_kwargs)
        context = ti.get_template_context(ignore_param_exceptions=False)
        execute_callable(context)

        log.info("[TASK TEST] Trigger completed")
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
@providers_configuration_loaded
def task_render(args, dag: DAG | None = None) -> None:
    """Render and displays templated fields for a given task."""
    if not dag:
        dag = get_dag(args.subdir, args.dag_id)
    task = dag.get_task(task_id=args.task_id)
    ti, _ = _get_ti(
        task, args.map_index, logical_date_or_run_id=args.logical_date_or_run_id, create_if_necessary="memory"
    )
    with create_session() as session, set_current_task_instance_session(session=session):
        ti.render_templates()
    for attr in task.template_fields:
        print(
            textwrap.dedent(
                f"""        # ----------------------------------------------------------
        # property: {attr}
        # ----------------------------------------------------------
        {getattr(ti.task, attr)}
        """
            )
        )


@cli_utils.action_cli(check_db=False)
@providers_configuration_loaded
def task_clear(args) -> None:
    """Clear all task instances or only those matched by regex for a DAG(s)."""
    logging.basicConfig(level=settings.LOGGING_LEVEL, format=settings.SIMPLE_LOG_FORMAT)

    if args.dag_id and not args.subdir and not args.dag_regex and not args.task_regex:
        dags = [get_dag_by_file_location(args.dag_id)]
    else:
        # todo clear command only accepts a single dag_id. no reason for get_dags with 's' except regex?
        dags = get_dags(args.subdir, args.dag_id, use_regex=args.dag_regex)

        if args.task_regex:
            for idx, dag in enumerate(dags):
                dags[idx] = dag.partial_subset(
                    task_ids=args.task_regex,
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
    )


class LoggerMutationHelper:
    """
    Helper for moving and resetting handlers and other logger attrs.

    :meta private:
    """

    def __init__(self, logger: logging.Logger) -> None:
        self.handlers = logger.handlers[:]
        self.level = logger.level
        self.propagate = logger.propagate
        self.source_logger = logger

    def apply(self, logger: logging.Logger, replace: bool = True) -> None:
        """
        Set ``logger`` with attrs stored on instance.

        If ``logger`` is root logger, don't change propagate.
        """
        if replace:
            logger.handlers[:] = self.handlers
        else:
            for h in self.handlers:
                if h not in logger.handlers:
                    logger.addHandler(h)
        logger.level = self.level
        if logger is not logging.getLogger():
            logger.propagate = self.propagate

    def move(self, logger: logging.Logger, replace: bool = True) -> None:
        """
        Replace ``logger`` attrs with those from source.

        :param logger: target logger
        :param replace: if True, remove all handlers from target first; otherwise add if not present.
        """
        self.apply(logger, replace=replace)
        self.source_logger.propagate = True
        self.source_logger.handlers[:] = []

    def reset(self) -> None:
        self.apply(self.source_logger)

    def __enter__(self) -> LoggerMutationHelper:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.reset()
