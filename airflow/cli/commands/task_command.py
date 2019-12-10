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
"""Task sub-commands"""
import importlib
import json
import logging
import os
import textwrap
from contextlib import redirect_stderr, redirect_stdout

from airflow import DAG, AirflowException, conf, jobs, settings
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models import DagPickle, TaskInstance
from airflow.ti_deps.dep_context import SCHEDULER_QUEUED_DEPS, DepContext
from airflow.utils import cli as cli_utils, db
from airflow.utils.cli import get_dag, get_dags
from airflow.utils.log.logging_mixin import StreamLogWriter
from airflow.utils.net import get_hostname


def _run(args, dag, ti):
    if args.local:
        run_job = jobs.LocalTaskJob(
            task_instance=ti,
            mark_success=args.mark_success,
            pickle_id=args.pickle,
            ignore_all_deps=args.ignore_all_dependencies,
            ignore_depends_on_past=args.ignore_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            ignore_ti_state=args.force,
            pool=args.pool)
        run_job.run()
    elif args.raw:
        ti._run_raw_task(  # pylint: disable=protected-access
            mark_success=args.mark_success,
            job_id=args.job_id,
            pool=args.pool,
        )
    else:
        pickle_id = None
        if args.ship_dag:
            try:
                # Running remotely, so pickling the DAG
                with db.create_session() as session:
                    pickle = DagPickle(dag)
                    session.add(pickle)
                    pickle_id = pickle.id
                    # TODO: This should be written to a log
                    print('Pickled dag {dag} as pickle_id: {pickle_id}'.format(
                        dag=dag, pickle_id=pickle_id))
            except Exception as e:
                print('Could not pickle the DAG')
                print(e)
                raise e

        executor = ExecutorLoader.get_default_executor()
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
            pool=args.pool)
        executor.heartbeat()
        executor.end()


@cli_utils.action_logging
def task_run(args, dag=None):
    """Runs a single task instance"""
    if dag:
        args.dag_id = dag.dag_id

    # Load custom airflow config
    if args.cfg_path:
        with open(args.cfg_path, 'r') as conf_file:
            conf_dict = json.load(conf_file)

        if os.path.exists(args.cfg_path):
            os.remove(args.cfg_path)

        conf.read_dict(conf_dict, source=args.cfg_path)
        settings.configure_vars()

    # IMPORTANT, have to use the NullPool, otherwise, each "run" command may leave
    # behind multiple open sleeping connections while heartbeating, which could
    # easily exceed the database connection limit when
    # processing hundreds of simultaneous tasks.
    settings.configure_orm(disable_connection_pool=True)

    if not args.pickle and not dag:
        dag = get_dag(args)
    elif not dag:
        with db.create_session() as session:
            print(f'Loading pickle id {args.pickle}')
            dag_pickle = session.query(DagPickle).filter(DagPickle.id == args.pickle).first()
            if not dag_pickle:
                raise AirflowException("Who hid the pickle!? [missing pickle]")
            dag = dag_pickle.pickle

    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    ti.refresh_from_db()

    ti.init_run_context(raw=args.raw)

    hostname = get_hostname()
    print(f"Running {ti} on host {hostname}")

    if args.interactive:
        _run(args, dag, ti)
    else:
        with redirect_stdout(StreamLogWriter(ti.log, logging.INFO)), \
                redirect_stderr(StreamLogWriter(ti.log, logging.WARN)):
            _run(args, dag, ti)
    logging.shutdown()


@cli_utils.action_logging
def task_failed_deps(args):
    """
    Returns the unmet dependencies for a task instance from the perspective of the
    scheduler (i.e. why a task instance doesn't get scheduled and then queued by the
    scheduler, and then run by an executor).
    >>> airflow tasks failed_deps tutorial sleep 2015-01-01
    Task instance dependencies not met:
    Dagrun Running: Task instance's dagrun did not exist: Unknown reason
    Trigger Rule: Task's trigger rule 'all_success' requires all upstream tasks
    to have succeeded, but found 1 non-success(es).
    """
    dag = get_dag(args)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)

    dep_context = DepContext(deps=SCHEDULER_QUEUED_DEPS)
    failed_deps = list(ti.get_failed_dep_statuses(dep_context=dep_context))
    # TODO, Do we want to print or log this
    if failed_deps:
        print("Task instance dependencies not met:")
        for dep in failed_deps:
            print("{}: {}".format(dep.dep_name, dep.reason))
    else:
        print("Task instance dependencies are all met.")


@cli_utils.action_logging
def task_state(args):
    """
    Returns the state of a TaskInstance at the command line.
    >>> airflow tasks state tutorial sleep 2015-01-01
    success
    """
    dag = get_dag(args)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    print(ti.current_state())


@cli_utils.action_logging
def task_list(args, dag=None):
    """Lists the tasks within a DAG at the command line"""
    dag = dag or get_dag(args)
    if args.tree:
        dag.tree_view()
    else:
        tasks = sorted([t.task_id for t in dag.tasks])
        print("\n".join(sorted(tasks)))


@cli_utils.action_logging
def task_test(args, dag=None):
    """Tests task for a given dag_id"""
    # We want log outout from operators etc to show up here. Normally
    # airflow.task would redirect to a file, but here we want it to propagate
    # up to the normal airflow handler.
    handlers = logging.getLogger('airflow.task').handlers
    already_has_stream_handler = False
    for handler in handlers:
        already_has_stream_handler = isinstance(handler, logging.StreamHandler)
        if already_has_stream_handler:
            break
    if not already_has_stream_handler:
        logging.getLogger('airflow.task').propagate = True

    dag = dag or get_dag(args)

    task = dag.get_task(task_id=args.task_id)
    # Add CLI provided task_params to task.params
    if args.task_params:
        passed_in_params = json.loads(args.task_params)
        task.params.update(passed_in_params)
    ti = TaskInstance(task, args.execution_date)

    try:
        if args.dry_run:
            ti.dry_run()
        else:
            ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)
    except Exception:  # pylint: disable=broad-except
        if args.post_mortem:
            try:
                debugger = importlib.import_module("ipdb")
            except ImportError:
                debugger = importlib.import_module("pdb")
            debugger.post_mortem()
        else:
            raise
    finally:
        if not already_has_stream_handler:
            # Make sure to reset back to normal. When run for CLI this doesn't
            # matter, but it does for test suite
            logging.getLogger('airflow.task').propagate = False


@cli_utils.action_logging
def task_render(args):
    """Renders and displays templated fields for a given task"""
    dag = get_dag(args)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    ti.render_templates()
    for attr in task.__class__.template_fields:
        print(textwrap.dedent("""\
        # ----------------------------------------------------------
        # property: {}
        # ----------------------------------------------------------
        {}
        """.format(attr, getattr(task, attr))))


@cli_utils.action_logging
def task_clear(args):
    """Clears all task instances or only those matched by regex for a DAG(s)"""
    logging.basicConfig(
        level=settings.LOGGING_LEVEL,
        format=settings.SIMPLE_LOG_FORMAT)
    dags = get_dags(args)

    if args.task_regex:
        for idx, dag in enumerate(dags):
            dags[idx] = dag.sub_dag(
                task_regex=args.task_regex,
                include_downstream=args.downstream,
                include_upstream=args.upstream)

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
