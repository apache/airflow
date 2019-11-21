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

"""Dag sub-commands"""
import errno
import json
import logging
import signal
import subprocess
import textwrap

from tabulate import tabulate

from airflow import DAG, AirflowException, LoggingMixin, conf, jobs, settings
from airflow.api.client import get_current_api_client
from airflow.models import DagBag, DagModel, DagRun, TaskInstance
from airflow.utils import cli as cli_utils, db
from airflow.utils.cli import get_dag, process_subdir, sigint_handler
from airflow.utils.dot_renderer import render_dag


@cli_utils.action_logging
def dag_backfill(args, dag=None):
    """Creates backfill job or dry run for a DAG"""
    logging.basicConfig(
        level=settings.LOGGING_LEVEL,
        format=settings.SIMPLE_LOG_FORMAT)

    signal.signal(signal.SIGTERM, sigint_handler)

    dag = dag or get_dag(args)

    if not args.start_date and not args.end_date:
        raise AirflowException("Provide a start_date and/or end_date")

    # If only one date is passed, using same as start and end
    args.end_date = args.end_date or args.start_date
    args.start_date = args.start_date or args.end_date

    if args.task_regex:
        dag = dag.sub_dag(
            task_regex=args.task_regex,
            include_upstream=not args.ignore_dependencies)

    run_conf = None
    if args.conf:
        run_conf = json.loads(args.conf)

    if args.dry_run:
        print("Dry run of DAG {0} on {1}".format(args.dag_id,
                                                 args.start_date))
        for task in dag.tasks:
            print("Task {0}".format(task.task_id))
            ti = TaskInstance(task, args.start_date)
            ti.dry_run()
    else:
        if args.reset_dagruns:
            DAG.clear_dags(
                [dag],
                start_date=args.start_date,
                end_date=args.end_date,
                confirm_prompt=not args.yes,
                include_subdags=True,
            )

        dag.run(
            start_date=args.start_date,
            end_date=args.end_date,
            mark_success=args.mark_success,
            local=args.local,
            donot_pickle=(args.donot_pickle or
                          conf.getboolean('core', 'donot_pickle')),
            ignore_first_depends_on_past=args.ignore_first_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            pool=args.pool,
            delay_on_limit_secs=args.delay_on_limit,
            verbose=args.verbose,
            conf=run_conf,
            rerun_failed_tasks=args.rerun_failed_tasks,
            run_backwards=args.run_backwards
        )


@cli_utils.action_logging
def dag_trigger(args):
    """
    Creates a dag run for the specified dag
    """
    api_client = get_current_api_client()
    log = LoggingMixin().log
    try:
        message = api_client.trigger_dag(dag_id=args.dag_id,
                                         run_id=args.run_id,
                                         conf=args.conf,
                                         execution_date=args.exec_date)
    except OSError as err:
        log.error(err)
        raise AirflowException(err)
    log.info(message)


@cli_utils.action_logging
def dag_delete(args):
    """
    Deletes all DB records related to the specified dag
    """
    api_client = get_current_api_client()
    log = LoggingMixin().log
    if args.yes or input(
            "This will drop all existing records related to the specified DAG. "
            "Proceed? (y/n)").upper() == "Y":
        try:
            message = api_client.delete_dag(dag_id=args.dag_id)
        except OSError as err:
            log.error(err)
            raise AirflowException(err)
        log.info(message)
    else:
        print("Bail.")


@cli_utils.action_logging
def dag_pause(args):
    """Pauses a DAG"""
    set_is_paused(True, args)


@cli_utils.action_logging
def dag_unpause(args):
    """Unpauses a DAG"""
    set_is_paused(False, args)


def set_is_paused(is_paused, args):
    """Sets is_paused for DAG by a given dag_id"""
    DagModel.get_dagmodel(args.dag_id).set_is_paused(
        is_paused=is_paused,
    )

    print("Dag: {}, paused: {}".format(args.dag_id, str(is_paused)))


def dag_show(args):
    """Displays DAG or saves it's graphic representation to the file"""
    dag = get_dag(args)
    dot = render_dag(dag)
    if args.save:
        filename, _, fileformat = args.save.rpartition('.')
        dot.render(filename=filename, format=fileformat, cleanup=True)
        print("File {} saved".format(args.save))
    elif args.imgcat:
        data = dot.pipe(format='png')
        try:
            proc = subprocess.Popen("imgcat", stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise AirflowException(
                    "Failed to execute. Make sure the imgcat executables are on your systems \'PATH\'"
                )
            else:
                raise
        out, err = proc.communicate(data)
        if out:
            print(out.decode('utf-8'))
        if err:
            print(err.decode('utf-8'))
    else:
        print(dot.source)


@cli_utils.action_logging
def dag_state(args):
    """
    Returns the state of a DagRun at the command line.
    >>> airflow dags state tutorial 2015-01-01T00:00:00.000000
    running
    """
    dag = get_dag(args)
    dr = DagRun.find(dag.dag_id, execution_date=args.execution_date)
    print(dr[0].state if len(dr) > 0 else None)  # pylint: disable=len-as-condition


@cli_utils.action_logging
def dag_next_execution(args):
    """
    Returns the next execution datetime of a DAG at the command line.
    >>> airflow dags next_execution tutorial
    2018-08-31 10:38:00
    """
    dag = get_dag(args)

    if dag.is_paused:
        print("[INFO] Please be reminded this DAG is PAUSED now.")

    if dag.latest_execution_date:
        next_execution_dttm = dag.following_schedule(dag.latest_execution_date)

        if next_execution_dttm is None:
            print("[WARN] No following schedule can be found. " +
                  "This DAG may have schedule interval '@once' or `None`.")

        print(next_execution_dttm)
    else:
        print("[WARN] Only applicable when there is execution record found for the DAG.")
        print(None)


@cli_utils.action_logging
def dag_list_dags(args):
    """Displays dags with or without stats at the command line"""
    dagbag = DagBag(process_subdir(args.subdir))
    list_template = textwrap.dedent("""\n
    -------------------------------------------------------------------
    DAGS
    -------------------------------------------------------------------
    {dag_list}
    """)
    dag_list = "\n".join(sorted(dagbag.dags))
    print(list_template.format(dag_list=dag_list))
    if args.report:
        print(dagbag.dagbag_report())


@cli_utils.action_logging
def dag_list_jobs(args, dag=None):
    """Lists latest n jobs"""
    queries = []
    if dag:
        args.dag_id = dag.dag_id
    if args.dag_id:
        dagbag = DagBag()

        if args.dag_id not in dagbag.dags:
            error_message = "Dag id {} not found".format(args.dag_id)
            raise AirflowException(error_message)
        queries.append(jobs.BaseJob.dag_id == args.dag_id)

    if args.state:
        queries.append(jobs.BaseJob.state == args.state)

    with db.create_session() as session:
        all_jobs = (session
                    .query(jobs.BaseJob)
                    .filter(*queries)
                    .order_by(jobs.BaseJob.start_date.desc())
                    .limit(args.limit)
                    .all())
        fields = ['dag_id', 'state', 'job_type', 'start_date', 'end_date']
        all_jobs = [[job.__getattribute__(field) for field in fields] for job in all_jobs]
        msg = tabulate(all_jobs,
                       [field.capitalize().replace('_', ' ') for field in fields],
                       tablefmt=args.output)
        print(msg)


@cli_utils.action_logging
def dag_list_dag_runs(args, dag=None):
    """Lists dag runs for a given DAG"""
    if dag:
        args.dag_id = dag.dag_id

    dagbag = DagBag()

    if args.dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(args.dag_id)
        raise AirflowException(error_message)

    dag_runs = []
    state = args.state.lower() if args.state else None
    for dag_run in DagRun.find(dag_id=args.dag_id,
                               state=state,
                               no_backfills=args.no_backfill):
        dag_runs.append({
            'id': dag_run.id,
            'run_id': dag_run.run_id,
            'state': dag_run.state,
            'dag_id': dag_run.dag_id,
            'execution_date': dag_run.execution_date.isoformat(),
            'start_date': ((dag_run.start_date or '') and
                           dag_run.start_date.isoformat()),
        })
    if not dag_runs:
        print('No dag runs for {dag_id}'.format(dag_id=args.dag_id))

    header_template = textwrap.dedent("""\n
    {line}
    DAG RUNS
    {line}
    {dag_run_header}
    """)

    dag_runs.sort(key=lambda x: x['execution_date'], reverse=True)
    dag_run_header = '%-3s | %-20s | %-10s | %-20s | %-20s |' % ('id',
                                                                 'run_id',
                                                                 'state',
                                                                 'execution_date',
                                                                 'start_date')
    print(header_template.format(dag_run_header=dag_run_header,
                                 line='-' * 120))
    for dag_run in dag_runs:
        record = '%-3s | %-20s | %-10s | %-20s | %-20s |' % (dag_run['id'],
                                                             dag_run['run_id'],
                                                             dag_run['state'],
                                                             dag_run['execution_date'],
                                                             dag_run['start_date'])
        print(record)
