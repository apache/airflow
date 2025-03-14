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

import errno
import json
import logging
import re
import subprocess
from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
from airflow.utils import cli as cli_utils, timezone
from airflow.utils.cli import get_dag, validate_dag_bundle_arg
from airflow.utils.dot_renderer import render_dag, render_dag_dependencies
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from graphviz.dot import Dot
    from sqlalchemy.orm import Session

    from airflow.models.dag import DAG
log = logging.getLogger(__name__)


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
        else:
            raise


def _save_dot_to_file(dot: Dot, filename: str) -> None:
    filename_without_ext, _, ext = filename.rpartition(".")
    dot.render(filename=filename_without_ext, format=ext, cleanup=True)
    print(f"File {filename} saved")


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
    elif filename:
        _save_dot_to_file(dot, filename)
    elif imgcat:
        _display_dot_via_imgcat(dot)
    else:
        print(dot.source)


@providers_configuration_loaded
def dag_show(args) -> None:
    """Display DAG or saves its graphic representation to the file."""
    dag = get_dag(args.subdir, args.dag_id)
    dot = render_dag(dag)
    filename = args.save
    imgcat = args.imgcat

    if filename and imgcat:
        raise SystemExit(
            "Option --save and --imgcat are mutually exclusive. "
            "Please remove one option to execute the command.",
        )
    elif filename:
        _save_dot_to_file(dot, filename)
    elif imgcat:
        _display_dot_via_imgcat(dot)
    else:
        print(dot.source)


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

    with _airflow_parsing_context_manager(dag_id=args.dag_id):
        dag = dag or get_dag(subdir=args.subdir, dag_id=args.dag_id)
    dr: DagRun = dag.test(
        logical_date=logical_date,
        run_conf=run_conf,
        use_executor=use_executor,
        mark_success_pattern=mark_success_pattern,
        session=session,
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

        dot_graph = render_dag(dag, tis=tis)
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
        dag_bag = DagBag(bundle.path, bundle_path=bundle.path, include_examples=False)
        dag_bag.sync_to_db(bundle.name, bundle_version=bundle.get_current_version(), session=session)
