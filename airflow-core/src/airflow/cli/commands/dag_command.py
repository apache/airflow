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

"""Dag sub-commands."""

from __future__ import annotations

import ast
import errno
import json
import logging
import operator
import re
import subprocess
import sys
from typing import TYPE_CHECKING, cast

from sqlalchemy import func, select

from airflow._shared.timezones import timezone
from airflow.api.client import get_current_api_client
from airflow.api_fastapi.core_api.datamodels.dags import DAGResponse
from airflow.cli.simple_table import AirflowConsole
from airflow.cli.utils import fetch_dag_run_from_run_id_or_logical_date_string
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.jobs.job import Job
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.models.dag import get_next_data_interval
from airflow.models.errors import ParseImportError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import cli as cli_utils
from airflow.utils.cli import get_bagged_dag, suppress_logs_and_warning, validate_dag_bundle_arg
from airflow.utils.dot_renderer import render_dag, render_dag_dependencies
from airflow.utils.helpers import ask_yesno
from airflow.utils.platform import getuser
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from collections.abc import Iterable

    from graphviz.dot import Dot
    from sqlalchemy.orm import Session

    from airflow import DAG
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.timetables.base import DataInterval

DAG_DETAIL_FIELDS = {*DAGResponse.model_fields, *DAGResponse.model_computed_fields}

log = logging.getLogger(__name__)


@cli_utils.action_cli
@providers_configuration_loaded
def dag_trigger(args) -> None:
    """Create a dag run for the specified dag."""
    api_client = get_current_api_client()
    try:
        user = getuser()
    except AirflowConfigException as e:
        log.warning("Failed to get user name from os: %s, not setting the triggering user", e)
        user = None
    try:
        message = api_client.trigger_dag(
            dag_id=args.dag_id,
            run_id=args.run_id,
            conf=args.conf,
            logical_date=args.logical_date,
            triggering_user_name=user,
            replace_microseconds=args.replace_microseconds,
        )
        AirflowConsole().print_as(
            data=[message] if message is not None else [],
            output=args.output,
        )
    except OSError as err:
        raise AirflowException(err)


@cli_utils.action_cli
@providers_configuration_loaded
def dag_delete(args) -> None:
    """Delete all DB records related to the specified dag."""
    api_client = get_current_api_client()
    if (
        args.yes
        or input("This will drop all existing records related to the specified DAG. Proceed? (y/n)").upper()
        == "Y"
    ):
        try:
            message = api_client.delete_dag(dag_id=args.dag_id)
            print(message)
        except OSError as err:
            raise AirflowException(err)
    else:
        print("Cancelled")


@cli_utils.action_cli
@providers_configuration_loaded
def dag_pause(args) -> None:
    """Pauses a DAG."""
    set_is_paused(True, args)


@cli_utils.action_cli
@providers_configuration_loaded
def dag_unpause(args) -> None:
    """Unpauses a DAG."""
    set_is_paused(False, args)


@providers_configuration_loaded
@provide_session
def set_is_paused(is_paused: bool, args, *, session: Session = NEW_SESSION) -> None:
    """Set is_paused for DAG by a given dag_id."""
    query = select(DagModel)
    if args.treat_dag_id_as_regex:
        query = query.where(DagModel.dag_id.regexp_match(args.dag_id))
    else:
        query = query.where(DagModel.dag_id == args.dag_id)

    query = query.where(DagModel.is_paused != is_paused)

    matched_dags = list(session.scalars(query).all())
    if not matched_dags:
        print(f"No {'un' if is_paused else ''}paused DAGs were found")
        return

    if not args.yes and args.treat_dag_id_as_regex:
        dags_ids = [dag.dag_id for dag in matched_dags]
        question = (
            f"You are about to {'un' if not is_paused else ''}pause {len(dags_ids)} DAGs:\n"
            f"{','.join(dags_ids)}"
            f"\n\nAre you sure? [y/n]"
        )
        if not ask_yesno(question):
            print("Operation cancelled by user")
            return

    def _update_is_paused(dag_model: DagModel) -> bool:
        old_is_paused = dag_model.is_paused
        dag_model.is_paused = is_paused
        return old_is_paused

    old_values = [
        {"dag_id": dag_model.dag_id, "is_paused": _update_is_paused(dag_model)} for dag_model in matched_dags
    ]
    session.commit()
    AirflowConsole().print_as(data=old_values, output=args.output)


@providers_configuration_loaded
def dag_dependencies_show(args) -> None:
    """Display DAG dependencies, save to file or show as imgcat image."""
    deduplicated_dag_dependencies = {
        dag_id: list(set(dag_dependencies))
        for dag_id, dag_dependencies in SerializedDagModel.get_dag_dependencies().items()
    }
    dot = render_dag_dependencies(deduplicated_dag_dependencies)
    filename = args.save
    imgcat = args.imgcat

    if filename and imgcat:
        raise SystemExit(
            "Option --save and --imgcat are mutually exclusive. "
            "Please remove one option to execute the command.",
        )
    if filename:
        _save_dot_to_file(dot, filename)
    elif imgcat:
        _display_dot_via_imgcat(dot)
    else:
        print(dot.source)


@providers_configuration_loaded
def dag_show(args) -> None:
    """Display DAG or saves its graphic representation to the file."""
    from airflow.models.serialized_dag import SerializedDagModel

    if not (dag := SerializedDagModel.get_dag(dag_id=args.dag_id)):
        raise SystemExit(f"Can not find dag {args.dag_id!r} in database")

    dot = render_dag(dag)
    filename = args.save
    imgcat = args.imgcat

    if filename and imgcat:
        raise SystemExit(
            "Option --save and --imgcat are mutually exclusive. "
            "Please remove one option to execute the command.",
        )
    if filename:
        _save_dot_to_file(dot, filename)
    elif imgcat:
        _display_dot_via_imgcat(dot)
    else:
        print(dot.source)


def _display_dot_via_imgcat(dot: Dot) -> None:
    data = dot.pipe(format="png")
    try:
        with subprocess.Popen("imgcat", stdout=subprocess.PIPE, stdin=subprocess.PIPE) as proc:
            out, err = proc.communicate(data)
            if out:
                print(out.decode("utf-8"))
            if err:
                print(err.decode("utf-8"))
    except OSError as e:
        if e.errno == errno.ENOENT:
            raise SystemExit("Failed to execute. Make sure the imgcat executables are on your systems 'PATH'")
        raise


def _save_dot_to_file(dot: Dot, filename: str) -> None:
    filename_without_ext, _, ext = filename.rpartition(".")
    dot.render(filename=filename_without_ext, format=ext, cleanup=True)
    print(f"File {filename} saved")


def _get_dagbag_dag_details(dag: DAG) -> dict:
    """Return a dagbag dag details dict."""
    from airflow.serialization.encoders import coerce_to_core_timetable

    core_timetable = coerce_to_core_timetable(dag.timetable)
    return {
        "dag_id": dag.dag_id,
        "dag_display_name": dag.dag_display_name,
        "bundle_name": None,
        "bundle_version": None,
        "is_paused": None,
        "is_stale": None,
        "last_parsed_time": None,
        "last_parse_duration": None,
        "last_expired": None,
        "relative_fileloc": dag.relative_fileloc,
        "fileloc": dag.fileloc,
        "file_token": None,
        "owners": dag.owner,
        "description": dag.description,
        "timetable_summary": core_timetable.summary,
        "timetable_description": core_timetable.description,
        "tags": dag.tags,
        "max_active_tasks": dag.max_active_tasks,
        "max_active_runs": dag.max_active_runs,
        "max_consecutive_failed_dag_runs": dag.max_consecutive_failed_dag_runs,
        "has_task_concurrency_limits": any(
            t.max_active_tis_per_dag is not None or t.max_active_tis_per_dagrun is not None for t in dag.tasks
        ),
        "has_import_errors": False,
        "next_dagrun_data_interval_start": None,
        "next_dagrun_data_interval_end": None,
        "next_dagrun_logical_date": None,
        "next_dagrun_run_after": None,
    }


@cli_utils.action_cli
@providers_configuration_loaded
@provide_session
def dag_state(args, session: Session = NEW_SESSION) -> None:
    """
    Return the state (and conf if exists) of a DagRun at the command line.

    >>> airflow dags state tutorial 2015-01-01T00:00:00.000000
    running
    >>> airflow dags state a_dag_with_conf_passed 2015-01-01T00:00:00.000000
    failed, {"name": "bob", "age": "42"}
    """
    dag = DagModel.get_dagmodel(args.dag_id, session=session)

    if not dag:
        raise SystemExit(f"DAG: {args.dag_id} does not exist in 'dag' table")
    dr, _ = fetch_dag_run_from_run_id_or_logical_date_string(
        dag_id=dag.dag_id,
        value=args.logical_date_or_run_id,
        session=session,
    )
    if not dr:
        print(None)
    elif dr.conf:
        print(f"{dr.state}, {json.dumps(dr.conf)}")
    else:
        print(dr.state)


@cli_utils.action_cli
@providers_configuration_loaded
def dag_next_execution(args) -> None:
    """
    Return the next logical datetime of a DAG at the command line.

    >>> airflow dags next-execution tutorial
    2018-08-31 10:38:00
    """
    from airflow.models.serialized_dag import SerializedDagModel

    with create_session() as session:
        dag = SerializedDagModel.get_dag(args.dag_id, session=session)
        last_parsed_dag: DagModel | None = session.scalars(
            select(DagModel).where(DagModel.dag_id == args.dag_id)
        ).one_or_none()

    if not dag or not last_parsed_dag:
        raise SystemExit(f"DAG: {args.dag_id} does not exist in the database")

    if last_parsed_dag.is_paused:
        print("[INFO] Please be reminded this DAG is PAUSED now.", file=sys.stderr)

    def print_execution_interval(interval: DataInterval | None):
        if interval is None:
            print(
                "[WARN] No following schedule can be found. "
                "This DAG may have schedule interval '@once' or `None`.",
                file=sys.stderr,
            )
            print(None)
            return
        print(interval.start.isoformat())

    next_interval = get_next_data_interval(dag.timetable, last_parsed_dag)
    print_execution_interval(next_interval)

    for _ in range(1, args.num_executions):
        next_info = dag.next_dagrun_info(next_interval, restricted=False)
        next_interval = None if next_info is None else next_info.data_interval
        print_execution_interval(next_interval)


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_session
def dag_list_dags(args, session: Session = NEW_SESSION) -> None:
    """Display dags with or without stats at the command line."""
    cols = args.columns if args.columns else []

    if invalid_cols := [c for c in cols if c not in DAG_DETAIL_FIELDS]:
        from rich import print as rich_print

        rich_print(
            f"[red][bold]Error:[/bold] Ignoring the following invalid columns: {invalid_cols}.  "
            f"List of valid columns: {sorted(DAG_DETAIL_FIELDS)}",
            file=sys.stderr,
        )

    dagbag_import_errors = 0
    dags_list = []
    if args.local:
        from airflow.dag_processing.dagbag import DagBag

        # Get import errors from the local area
        if args.bundle_name:
            manager = DagBundlesManager()
            validate_dag_bundle_arg(args.bundle_name)
            all_bundles = list(manager.get_all_dag_bundles())
            bundles_to_search = set(args.bundle_name)

            for bundle in all_bundles:
                if bundle.name in bundles_to_search:
                    dagbag = DagBag(bundle.path, bundle_path=bundle.path, bundle_name=bundle.name)
                    dagbag.collect_dags()
                    dags_list.extend(list(dagbag.dags.values()))
                    dagbag_import_errors += len(dagbag.import_errors)
        else:
            dagbag = DagBag()
            dagbag.collect_dags()
            dags_list.extend(list(dagbag.dags.values()))
            dagbag_import_errors += len(dagbag.import_errors)
    else:
        dags_list.extend(cast("DAG", sm.dag) for sm in session.scalars(select(SerializedDagModel)))
        pie_stmt = select(func.count()).select_from(ParseImportError)
        if args.bundle_name:
            pie_stmt = pie_stmt.where(ParseImportError.bundle_name.in_(args.bundle_name))
        dagbag_import_errors = session.scalar(pie_stmt) or 0

    if dagbag_import_errors > 0:
        from rich import print as rich_print

        rich_print(
            "[red][bold]Error:[/bold] Failed to load all files. "
            "For details, run `airflow dags list-import-errors`",
            file=sys.stderr,
        )

    def get_dag_detail(dag: DAG) -> dict:
        if dag_model := DagModel.get_dagmodel(dag.dag_id, session=session):
            dag_detail = DAGResponse.model_validate(dag_model, from_attributes=True).model_dump()
        else:
            dag_detail = _get_dagbag_dag_details(dag)
        if not cols:
            return dag_detail
        return {col: dag_detail[col] for col in cols if col in DAG_DETAIL_FIELDS}

    def filter_dags_by_bundle(dags: Iterable[DAG], bundle_names: list[str] | None) -> Iterable[DAG]:
        """Filter DAGs based on the specified bundle name, if provided."""
        if not bundle_names:
            return dags

        validate_dag_bundle_arg(bundle_names)
        selected_dag_ids = set(
            session.scalars(select(DagModel.dag_id).where(DagModel.bundle_name.in_(bundle_names)))
        )
        return (dag for dag in dags if dag.dag_id in selected_dag_ids)

    AirflowConsole().print_as(
        data=sorted(
            filter_dags_by_bundle(dags_list, args.bundle_name if not args.local else None),
            key=operator.attrgetter("dag_id"),
        ),
        output=args.output,
        mapper=get_dag_detail,
    )


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_session
def dag_details(args, session: Session = NEW_SESSION):
    """Get DAG details given a DAG id."""
    dag = DagModel.get_dagmodel(args.dag_id, session=session)
    if not dag:
        raise SystemExit(f"DAG: {args.dag_id} does not exist in 'dag' table")
    dag_detail = DAGResponse.from_orm(dag).model_dump()

    if args.output in ["table", "plain"]:
        data = [{"property_name": key, "property_value": value} for key, value in dag_detail.items()]
    else:
        data = [dag_detail]

    AirflowConsole().print_as(
        data=data,
        output=args.output,
    )


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_session
def dag_list_import_errors(args, session: Session = NEW_SESSION) -> None:
    """Display dags with import errors on the command line."""
    data = []

    if args.local:
        # Get import errors from local areas

        if args.bundle_name:
            manager = DagBundlesManager()
            validate_dag_bundle_arg(args.bundle_name)
            all_bundles = list(manager.get_all_dag_bundles())
            bundles_to_search = set(args.bundle_name)

            for bundle in all_bundles:
                if bundle.name in bundles_to_search:
                    dagbag = DagBag(bundle.path, bundle_path=bundle.path, bundle_name=bundle.name)
                    for filename, errors in dagbag.import_errors.items():
                        data.append({"bundle_name": bundle.name, "filepath": filename, "error": errors})
        else:
            dagbag = DagBag()
            for filename, errors in dagbag.import_errors.items():
                data.append({"filepath": filename, "error": errors})

    else:
        # Get import errors from the DB
        query = select(ParseImportError)
        if args.bundle_name:
            validate_dag_bundle_arg(args.bundle_name)
            query = query.where(ParseImportError.bundle_name.in_(args.bundle_name))

        dagbag_import_errors = session.scalars(query).all()

        for import_error in dagbag_import_errors:
            data.append(
                {
                    "bundle_name": import_error.bundle_name or "",
                    "filepath": import_error.filename or "",
                    "error": import_error.stacktrace or "",
                }
            )
    AirflowConsole().print_as(
        data=data,
        output=args.output,
    )
    if data:
        sys.exit(1)


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
def dag_report(args) -> None:
    """Display dagbag stats at the command line."""
    manager = DagBundlesManager()

    all_bundles = list(manager.get_all_dag_bundles())
    if args.bundle_name:
        validate_dag_bundle_arg(args.bundle_name)
        bundles_to_reserialize = set(args.bundle_name)
    else:
        bundles_to_reserialize = {b.name for b in all_bundles}

    all_dagbag_stats = []
    for bundle in all_bundles:
        if bundle.name not in bundles_to_reserialize:
            continue
        bundle.initialize()
        dagbag = DagBag(bundle.path, bundle_path=bundle.path, bundle_name=bundle.name, include_examples=False)
        all_dagbag_stats.extend(dagbag.dagbag_stats)

    AirflowConsole().print_as(
        data=all_dagbag_stats,
        output=args.output,
        mapper=lambda x: {
            "file": x.file,
            "duration": x.duration,
            "dag_num": x.dag_num,
            "task_num": x.task_num,
            "dags": sorted(ast.literal_eval(x.dags)),
        },
    )


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_session
def dag_list_jobs(args, dag: DAG | None = None, session: Session = NEW_SESSION) -> None:
    """List latest n jobs."""
    queries = []
    if dag:
        args.dag_id = dag.dag_id
    if args.dag_id:
        dag = DagModel.get_dagmodel(args.dag_id, session=session)

        if not dag:
            raise SystemExit(f"DAG: {args.dag_id} does not exist in 'dag' table")
        queries.append(Job.dag_id == args.dag_id)

    if args.state:
        queries.append(Job.state == args.state)

    fields = ["dag_id", "state", "job_type", "start_date", "end_date"]
    all_jobs_iter = session.scalars(
        select(Job).where(*queries).order_by(Job.start_date.desc()).limit(args.limit)
    )
    all_jobs = [{f: str(job.__getattribute__(f)) for f in fields} for job in all_jobs_iter]

    AirflowConsole().print_as(
        data=all_jobs,
        output=args.output,
    )


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_session
def dag_list_dag_runs(args, dag: DAG | None = None, session: Session = NEW_SESSION) -> None:
    """List dag runs for a given DAG."""
    if dag:
        args.dag_id = dag.dag_id
    else:
        dag = DagModel.get_dagmodel(args.dag_id, session=session)

        if not dag:
            raise SystemExit(f"DAG: {args.dag_id} does not exist in 'dag' table")

    state = args.state.lower() if args.state else None
    dag_runs = DagRun.find(
        dag_id=args.dag_id,
        state=state,
        no_backfills=args.no_backfill,
        logical_start_date=args.start_date,
        logical_end_date=args.end_date,
        session=session,
    )
    dag_runs.sort(key=operator.attrgetter("run_after"), reverse=True)

    def _render_dagrun(dr: DagRun) -> dict[str, str]:
        return {
            "dag_id": dr.dag_id,
            "run_id": dr.run_id,
            "state": dr.state,
            "run_after": dr.run_after.isoformat(),
            "logical_date": dr.logical_date.isoformat() if dr.logical_date else "",
            "start_date": dr.start_date.isoformat() if dr.start_date else "",
            "end_date": dr.end_date.isoformat() if dr.end_date else "",
        }

    AirflowConsole().print_as(data=dag_runs, output=args.output, mapper=_render_dagrun)


@cli_utils.action_cli
@providers_configuration_loaded
@provide_session
def dag_test(args, dag: DAG | None = None, session: Session = NEW_SESSION) -> None:
    """Execute one single DagRun for a given DAG and logical date."""
    run_conf = None
    if args.conf:
        try:
            run_conf = json.loads(args.conf)
        except ValueError as e:
            raise SystemExit(f"Configuration {args.conf!r} is not valid JSON. Error: {e}")
    logical_date = args.logical_date or timezone.utcnow()
    use_executor = args.use_executor

    mark_success_pattern = (
        re.compile(args.mark_success_pattern) if args.mark_success_pattern is not None else None
    )

    dag = dag or get_bagged_dag(
        bundle_names=args.bundle_name,
        dag_id=args.dag_id,
        dagfile_path=args.dagfile_path,
    )
    if not dag:
        raise AirflowException(
            f"Dag {args.dag_id!r} could not be found; either it does not exist or it failed to parse."
        )

    dr: DagRun = dag.test(
        logical_date=logical_date,
        run_conf=run_conf,
        use_executor=use_executor,
        mark_success_pattern=mark_success_pattern,
    )
    show_dagrun = args.show_dagrun
    imgcat = args.imgcat_dagrun
    filename = args.save_dagrun
    if show_dagrun or imgcat or filename:
        tis = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == args.dag_id,
                TaskInstance.run_id == dr.run_id,
            )
        ).all()

        dot_graph = render_dag(cast("SerializedDAG", dag), tis=list(tis))
        print()
        if filename:
            _save_dot_to_file(dot_graph, filename)
        if imgcat:
            _display_dot_via_imgcat(dot_graph)
        if show_dagrun:
            print(dot_graph.source)

    if dr and dr.state == DagRunState.FAILED:
        raise SystemExit("DagRun failed")


@cli_utils.action_cli
@providers_configuration_loaded
@provide_session
def dag_reserialize(args, session: Session = NEW_SESSION) -> None:
    """Serialize a DAG instance."""
    manager = DagBundlesManager()
    manager.sync_bundles_to_db(session=session)
    session.commit()

    all_bundles = list(manager.get_all_dag_bundles())
    if args.bundle_name:
        validate_dag_bundle_arg(args.bundle_name)
        bundles_to_reserialize = set(args.bundle_name)
    else:
        bundles_to_reserialize = {b.name for b in all_bundles}

    for bundle in all_bundles:
        if bundle.name not in bundles_to_reserialize:
            continue
        bundle.initialize()
        dag_bag = DagBag(
            bundle.path, bundle_path=bundle.path, bundle_name=bundle.name, include_examples=False
        )
        sync_bag_to_db(dag_bag, bundle.name, bundle_version=bundle.get_current_version(), session=session)
