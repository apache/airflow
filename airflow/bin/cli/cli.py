#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
import traceback
import warnings
from importlib import import_module

import daemon
import psutil
import reprlib
from builtins import input
from daemon.pidfile import TimeoutPIDLockFile
from sqlalchemy import func
from sqlalchemy.orm import exc
from tabulate import tabulate

import airflow
from airflow import api
from airflow import jobs, settings
from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.executors import GetDefaultExecutor
from airflow.models import (DagModel, DagBag, TaskInstance,
                            DagPickle, DagRun, Variable, DagStat,
                            Pool, Connection)
from airflow.ti_deps.dep_context import (DepContext, SCHEDULER_DEPS)
from airflow.utils import db as db_utils
from airflow.utils.log.logging_mixin import (LoggingMixin, redirect_stderr,
                                             redirect_stdout, set_context)
from airflow.www.app import cached_app

api.load_auth()
api_module = import_module(conf.get('cli', 'api_client'))
api_client = api_module.Client(api_base_url=conf.get('cli', 'endpoint_url'),
                               auth=api.api_auth.client_auth)

log = LoggingMixin().log


def sigint_handler(sig, frame):
    sys.exit(0)


def sigquit_handler(sig, frame):
    """Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\
    """
    print("Dumping stack traces for all threads in PID {}".format(os.getpid()))
    id_to_name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for thread_id, stack in sys._current_frames().items():
        code.append("\n# Thread: {}({})"
                    .format(id_to_name.get(thread_id, ""), thread_id))
        for filename, line_number, name, line in traceback.extract_stack(stack):
            code.append('File: "{}", line {}, in {}'
                        .format(filename, line_number, name))
            if line:
                code.append("  {}".format(line.strip()))
    print("\n".join(code))


def setup_logging(filename):
    root = logging.getLogger()
    handler = logging.FileHandler(filename)
    formatter = logging.Formatter(settings.SIMPLE_LOG_FORMAT)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(settings.LOGGING_LEVEL)

    return handler.stream


def setup_locations(process, pid=None, stdout=None, stderr=None, log=None):
    if not stderr:
        stderr = os.path.join(os.path.expanduser(settings.AIRFLOW_HOME), "airflow-{}.err".format(process))
    if not stdout:
        stdout = os.path.join(os.path.expanduser(settings.AIRFLOW_HOME), "airflow-{}.out".format(process))
    if not log:
        log = os.path.join(os.path.expanduser(settings.AIRFLOW_HOME), "airflow-{}.log".format(process))
    if not pid:
        pid = os.path.join(os.path.expanduser(settings.AIRFLOW_HOME), "airflow-{}.pid".format(process))

    return pid, stdout, stderr, log


def process_subdir(subdir):
    if subdir:
        subdir = subdir.replace('DAGS_FOLDER', settings.DAGS_FOLDER)
        subdir = os.path.abspath(os.path.expanduser(subdir))
        return subdir


def get_dag(args):
    dagbag = DagBag(process_subdir(args.subdir))
    if args.dag_id not in dagbag.dags:
        raise AirflowException(
            'dag_id could not be found: {}. Either the dag did not exist or it failed to '
            'parse.'.format(args.dag_id))
    return dagbag.dags[args.dag_id]


def get_dags(args):
    if not args.dag_regex:
        return [get_dag(args)]
    dagbag = DagBag(process_subdir(args.subdir))
    matched_dags = [dag for dag in dagbag.dags.values() if re.search(
        args.dag_id, dag.dag_id)]
    if not matched_dags:
        raise AirflowException(
            'dag_id could not be found with regex: {}. Either the dag did not exist '
            'or it failed to parse.'.format(args.dag_id))
    return matched_dags


def backfill(args, dag=None):
    logging.basicConfig(
        level=settings.LOGGING_LEVEL,
        format=settings.SIMPLE_LOG_FORMAT)

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

    if args.dry_run:
        print("Dry run of DAG {0} on {1}".format(args.dag_id,
                                                 args.start_date))
        for task in dag.tasks:
            print("Task {0}".format(task.task_id))
            ti = TaskInstance(task, args.start_date)
            ti.dry_run()
    else:
        dag.run(
            start_date=args.start_date,
            end_date=args.end_date,
            mark_success=args.mark_success,
            include_adhoc=args.include_adhoc,
            local=args.local,
            donot_pickle=(args.donot_pickle or
                          conf.getboolean('core', 'donot_pickle')),
            ignore_first_depends_on_past=args.ignore_first_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            pool=args.pool,
            delay_on_limit_secs=args.delay_on_limit)


def trigger_dag(args):
    """
    Creates a dag run for the specified dag
    :param args:
    :return:
    """
    log = LoggingMixin().log
    try:
        message = api_client.trigger_dag(dag_id=args.dag_id,
                                         run_id=args.run_id,
                                         conf=args.conf,
                                         execution_date=args.exec_date)
    except IOError as err:
        log.error(err)
        raise AirflowException(err)
    log.info(message)


def pool(args):
    session = settings.Session()
    if args.get or (args.set and args.set[0]) or args.delete:
        name = args.get or args.delete or args.set[0]
    pool = (
        session.query(Pool)
        .filter(Pool.pool == name)
        .first())
    if pool and args.get:
        print("{} ".format(pool))
        return
    elif not pool and (args.get or args.delete):
        print("No pool named {} found".format(name))
    elif not pool and args.set:
        pool = Pool(
            pool=name,
            slots=args.set[1],
            description=args.set[2])
        session.add(pool)
        session.commit()
        print("{} ".format(pool))
    elif pool and args.set:
        pool.slots = args.set[1]
        pool.description = args.set[2]
        session.commit()
        print("{} ".format(pool))
        return
    elif pool and args.delete:
        session.query(Pool).filter_by(pool=args.delete).delete()
        session.commit()
        print("Pool {} deleted".format(name))


def variables(args):

    if args.get:
        try:
            var = Variable.get(args.get,
                               deserialize_json=args.json,
                               default_var=args.default)
            print(var)
        except ValueError as e:
            print(e)
    if args.delete:
        session = settings.Session()
        session.query(Variable).filter_by(key=args.delete).delete()
        session.commit()
        session.close()
    if args.set:
        Variable.set(args.set[0], args.set[1])
    # Work around 'import' as a reserved keyword
    imp = getattr(args, 'import')
    if imp:
        if os.path.exists(imp):
            import_helper(imp)
        else:
            print("Missing variables file.")
    if args.export:
        export_helper(args.export)
    if not (args.set or args.get or imp or args.export or args.delete):
        # list all variables
        session = settings.Session()
        vars = session.query(Variable)
        msg = "\n".join(var.key for var in vars)
        print(msg)


def import_helper(filepath):
    with open(filepath, 'r') as varfile:
        var = varfile.read()

    try:
        d = json.loads(var)
    except Exception:
        print("Invalid variables file.")
    else:
        try:
            n = 0
            for k, v in d.items():
                if isinstance(v, dict):
                    Variable.set(k, v, serialize_json=True)
                else:
                    Variable.set(k, v)
                n += 1
        except Exception:
            pass
        finally:
            print("{} of {} variables successfully updated.".format(n, len(d)))


def export_helper(filepath):
    session = settings.Session()
    qry = session.query(Variable).all()
    session.close()

    var_dict = {}
    d = json.JSONDecoder()
    for var in qry:
        val = None
        try:
            val = d.decode(var.val)
        except Exception:
            val = var.val
        var_dict[var.key] = val

    with open(filepath, 'w') as varfile:
        varfile.write(json.dumps(var_dict, sort_keys=True, indent=4))
    print("{} variables successfully exported to {}".format(len(var_dict), filepath))


def pause(args, dag=None):
    set_is_paused(True, args, dag)


def unpause(args, dag=None):
    set_is_paused(False, args, dag)


def set_is_paused(is_paused, args, dag=None):
    dag = dag or get_dag(args)

    session = settings.Session()
    dm = session.query(DagModel).filter(
        DagModel.dag_id == dag.dag_id).first()
    dm.is_paused = is_paused
    session.commit()

    msg = "Dag: {}, paused: {}".format(dag, str(dag.is_paused))
    print(msg)


def run(args, dag=None):
    # Disable connection pooling to reduce the # of connections on the DB
    # while it's waiting for the task to finish.
    settings.configure_orm(disable_connection_pool=True)

    if dag:
        args.dag_id = dag.dag_id

    log = LoggingMixin().log

    # Load custom airflow config
    if args.cfg_path:
        with open(args.cfg_path, 'r') as conf_file:
            conf_dict = json.load(conf_file)

        if os.path.exists(args.cfg_path):
            os.remove(args.cfg_path)

        for section, config in conf_dict.items():
            for option, value in config.items():
                conf.set(section, option, value)
        settings.configure_vars()
        settings.configure_orm()

    logging.root.handlers = []
    if args.raw:
        # Output to STDOUT for the parent process to read and log
        logging.basicConfig(
            stream=sys.stdout,
            level=settings.LOGGING_LEVEL,
            format=settings.LOG_FORMAT)
    else:
        # Setting up logging to a file.

        # To handle log writing when tasks are impersonated, the log files need to
        # be writable by the user that runs the Airflow command and the user
        # that is impersonated. This is mainly to handle corner cases with the
        # SubDagOperator. When the SubDagOperator is run, all of the operators
        # run under the impersonated user and create appropriate log files
        # as the impersonated user. However, if the user manually runs tasks
        # of the SubDagOperator through the UI, then the log files are created
        # by the user that runs the Airflow command. For example, the Airflow
        # run command may be run by the `airflow_sudoable` user, but the Airflow
        # tasks may be run by the `airflow` user. If the log files are not
        # writable by both users, then it's possible that re-running a task
        # via the UI (or vice versa) results in a permission error as the task
        # tries to write to a log file created by the other user.
        log_base = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        directory = log_base + "/{args.dag_id}/{args.task_id}".format(args=args)
        # Create the log file and give it group writable permissions
        # TODO(aoen): Make log dirs and logs globally readable for now since the SubDag
        # operator is not compatible with impersonation (e.g. if a Celery executor is used
        # for a SubDag operator and the SubDag operator has a different owner than the
        # parent DAG)
        if not os.path.exists(directory):
            # Create the directory as globally writable using custom mkdirs
            # as os.makedirs doesn't set mode properly.
            mkdirs(directory, 0o775)
        iso = args.execution_date.isoformat()
        filename = "{directory}/{iso}".format(**locals())

        if not os.path.exists(filename):
            open(filename, "a").close()
            os.chmod(filename, 0o666)

        logging.basicConfig(
            filename=filename,
            level=settings.LOGGING_LEVEL,
            format=settings.LOG_FORMAT)

    if not args.pickle and not dag:
        dag = get_dag(args)
    elif not dag:
        session = settings.Session()
        log.info('Loading pickle id {args.pickle}'.format(args=args))
        dag_pickle = session.query(
            DagPickle).filter(DagPickle.id == args.pickle).first()
        if not dag_pickle:
            raise AirflowException("Who hid the pickle!? [missing pickle]")
        dag = dag_pickle.pickle

    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    ti.refresh_from_db()

    if not ti.hostname:
        ti.update_hostname(hostname=socket.getfqdn())
    if args.kubernetes_mode:
        print("Congratulations! You're in kubernetes mode!")
        executor = GetDefaultExecutor()
        executor.start()
        print("Sending to executor.")
        executor.queue_task_instance(
            ti,
            mark_success=args.mark_success,
            ignore_all_deps=args.ignore_all_dependencies,
            ignore_depends_on_past=args.ignore_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            ignore_ti_state=args.force,
            pool=args.pool)
        executor.heartbeat(km=True)
        executor.end()

    elif args.local:
        print("Logging into: " + filename)
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
        ti.run(
            mark_success=args.mark_success,
            ignore_all_deps=args.ignore_all_dependencies,
            ignore_depends_on_past=args.ignore_depends_on_past,
            ignore_task_deps=args.ignore_dependencies,
            ignore_ti_state=args.force,
            job_id=args.job_id,
            pool=args.pool,
        )
    elif args.pickle:
        pickle_id = None
        if args.ship_dag:
            try:
                # Running remotely, so pickling the DAG
                session = settings.Session()
                pickle = DagPickle(dag)
                session.add(pickle)
                session.commit()
                pickle_id = pickle.id
                print((
                    'Pickled dag {dag} '
                    'as pickle_id:{pickle_id}').format(**locals()))
            except Exception as e:
                print('Could not pickle the DAG')
                print(e)
                raise e

        executor = GetDefaultExecutor()
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

    # Child processes should not flush or upload to remote
    if args.raw:
        return

    hostname = socket.getfqdn()
    log.info("Running %s on host %s", ti, hostname)

    with redirect_stdout(ti.log, logging.INFO), redirect_stderr(ti.log, logging.WARN):
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
            ti._run_raw_task(
                mark_success=args.mark_success,
                job_id=args.job_id,
                pool=args.pool,
            )
        else:
            pickle_id = None
            if args.ship_dag:
                try:
                    # Running remotely, so pickling the DAG
                    session = settings.Session()
                    pickle = DagPickle(dag)
                    session.add(pickle)
                    session.commit()
                    pickle_id = pickle.id
                    # TODO: This should be written to a log
                    print((
                              'Pickled dag {dag} '
                              'as pickle_id:{pickle_id}').format(**locals()))
                except Exception as e:
                    print('Could not pickle the DAG')
                    print(e)
                    raise e

            executor = GetDefaultExecutor()
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

    logging.shutdown()


def task_failed_deps(args):
    """
    Returns the unmet dependencies for a task instance from the perspective of the
    scheduler (i.e. why a task instance doesn't get scheduled and then queued by the
    scheduler, and then run by an executor).

    >>> airflow task_failed_deps tutorial sleep 2015-01-01
    Task instance dependencies not met:
    Dagrun Running: Task instance's dagrun did not exist: Unknown reason
    Trigger Rule: Task's trigger rule 'all_success' requires all upstream tasks to have succeeded, but found 1 non-success(es).
    """
    dag = get_dag(args)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)

    dep_context = DepContext(deps=SCHEDULER_DEPS)
    failed_deps = list(ti.get_failed_dep_statuses(dep_context=dep_context))
    # TODO, Do we want to print or log this
    if failed_deps:
        print("Task instance dependencies not met:")
        for dep in failed_deps:
            print("{}: {}".format(dep.dep_name, dep.reason))
    else:
        print("Task instance dependencies are all met.")


def task_state(args):
    """
    Returns the state of a TaskInstance at the command line.

    >>> airflow task_state tutorial sleep 2015-01-01
    success
    """
    dag = get_dag(args)
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    print(ti.current_state())


def dag_state(args):
    """
    Returns the state of a DagRun at the command line.

    >>> airflow dag_state tutorial 2015-01-01T00:00:00.000000
    running
    """
    dag = get_dag(args)
    dr = DagRun.find(dag.dag_id, execution_date=args.execution_date)
    print(dr[0].state if len(dr) > 0 else None)


def list_dags(args):
    dagbag = DagBag(process_subdir(args.subdir))
    s = textwrap.dedent("""\n
    -------------------------------------------------------------------
    DAGS
    -------------------------------------------------------------------
    {dag_list}
    """)
    dag_list = "\n".join(sorted(dagbag.dags))
    print(s.format(dag_list=dag_list))
    if args.report:
        print(dagbag.dagbag_report())


def list_tasks(args, dag=None):
    dag = dag or get_dag(args)
    if args.tree:
        dag.tree_view()
    else:
        tasks = sorted([t.task_id for t in dag.tasks])
        print("\n".join(sorted(tasks)))


def test(args, dag=None):
    dag = dag or get_dag(args)

    task = dag.get_task(task_id=args.task_id)
    # Add CLI provided task_params to task.params
    if args.task_params:
        passed_in_params = json.loads(args.task_params)
        task.params.update(passed_in_params)
    ti = TaskInstance(task, args.execution_date)

    if args.dry_run:
        ti.dry_run()
    else:
        ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)


def render(args):
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


def clear(args):
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
        confirm_prompt=not args.no_confirm,
        include_subdags=not args.exclude_subdags)


def get_num_ready_workers_running(gunicorn_master_proc):
    workers = psutil.Process(gunicorn_master_proc.pid).children()

    def ready_prefix_on_cmdline(proc):
        try:
            cmdline = proc.cmdline()
            if len(cmdline) > 0:
                return settings.GUNICORN_WORKER_READY_PREFIX in cmdline[0]
        except psutil.NoSuchProcess:
            pass
        return False

    ready_workers = [proc for proc in workers if ready_prefix_on_cmdline(proc)]
    return len(ready_workers)


def restart_workers(gunicorn_master_proc, num_workers_expected):
    """
    Runs forever, monitoring the child processes of @gunicorn_master_proc and
    restarting workers occasionally.

    Each iteration of the loop traverses one edge of this state transition
    diagram, where each state (node) represents
    [ num_ready_workers_running / num_workers_running ]. We expect most time to
    be spent in [n / n]. `bs` is the setting webserver.worker_refresh_batch_size.

    The horizontal transition at ? happens after the new worker parses all the
    dags (so it could take a while!)

       V ────────────────────────────────────────────────────────────────────────┐
    [n / n] ──TTIN──> [ [n, n+bs) / n + bs ]  ────?───> [n + bs / n + bs] ──TTOU─┘
       ^                          ^───────────────┘
       │
       │      ┌────────────────v
       └──────┴────── [ [0, n) / n ] <─── start

    We change the number of workers by sending TTIN and TTOU to the gunicorn
    master process, which increases and decreases the number of child workers
    respectively. Gunicorn guarantees that on TTOU workers are terminated
    gracefully and that the oldest worker is terminated.
    """

    def wait_until_true(fn):
        """
        Sleeps until fn is true
        """
        while not fn():
            time.sleep(0.1)

    def get_num_workers_running(gunicorn_master_proc):
        workers = psutil.Process(gunicorn_master_proc.pid).children()
        return len(workers)

    def start_refresh(gunicorn_master_proc):
        batch_size = conf.getint('webserver', 'worker_refresh_batch_size')
        log.debug('%s doing a refresh of %s workers', state, batch_size)
        sys.stdout.flush()
        sys.stderr.flush()

        excess = 0
        for _ in range(batch_size):
            gunicorn_master_proc.send_signal(signal.SIGTTIN)
            excess += 1
            wait_until_true(lambda: num_workers_expected + excess ==
                                    get_num_workers_running(gunicorn_master_proc))

    wait_until_true(lambda: num_workers_expected ==
                            get_num_workers_running(gunicorn_master_proc))

    while True:
        num_workers_running = get_num_workers_running(gunicorn_master_proc)
        num_ready_workers_running = get_num_ready_workers_running(gunicorn_master_proc)

        state = '[{0} / {1}]'.format(num_ready_workers_running, num_workers_running)

        # Whenever some workers are not ready, wait until all workers are ready
        if num_ready_workers_running < num_workers_running:
            log.debug('%s some workers are starting up, waiting...', state)
            sys.stdout.flush()
            time.sleep(1)

        # Kill a worker gracefully by asking gunicorn to reduce number of workers
        elif num_workers_running > num_workers_expected:
            excess = num_workers_running - num_workers_expected
            log.debug('%s killing %s workers', state, excess)

            for _ in range(excess):
                gunicorn_master_proc.send_signal(signal.SIGTTOU)
                excess -= 1
                wait_until_true(lambda: num_workers_expected + excess ==
                                        get_num_workers_running(gunicorn_master_proc))

        # Start a new worker by asking gunicorn to increase number of workers
        elif num_workers_running == num_workers_expected:
            refresh_interval = conf.getint('webserver', 'worker_refresh_interval')
            log.debug(
                '%s sleeping for %ss starting doing a refresh...',
                state, refresh_interval
            )
            time.sleep(refresh_interval)
            start_refresh(gunicorn_master_proc)

        else:
            # num_ready_workers_running == num_workers_running < num_workers_expected
            log.error((
                "%s some workers seem to have died and gunicorn"
                "did not restart them as expected"
            ), state)
            time.sleep(10)
            if len(
                psutil.Process(gunicorn_master_proc.pid).children()
            ) < num_workers_expected:
                start_refresh(gunicorn_master_proc)


def webserver(args):
    print(settings.HEADER)

    app = cached_app(conf)
    access_logfile = args.access_logfile or conf.get('webserver', 'access_logfile')
    error_logfile = args.error_logfile or conf.get('webserver', 'error_logfile')
    num_workers = args.workers or conf.get('webserver', 'workers')
    worker_timeout = (args.worker_timeout or
                      conf.get('webserver', 'webserver_worker_timeout'))
    ssl_cert = args.ssl_cert or conf.get('webserver', 'web_server_ssl_cert')
    ssl_key = args.ssl_key or conf.get('webserver', 'web_server_ssl_key')
    if not ssl_cert and ssl_key:
        raise AirflowException(
            'An SSL certificate must also be provided for use with ' + ssl_key)
    if ssl_cert and not ssl_key:
        raise AirflowException(
            'An SSL key must also be provided for use with ' + ssl_cert)

    if args.debug:
        print(
            "Starting the web server on port {0} and host {1}.".format(
                args.port, args.hostname))
        app.run(debug=True, port=args.port, host=args.hostname,
                ssl_context=(ssl_cert, ssl_key) if ssl_cert and ssl_key else None)
    else:
        pid, stdout, stderr, log_file = setup_locations("webserver", args.pid, args.stdout, args.stderr, args.log_file)
        if args.daemon:
            handle = setup_logging(log_file)
            stdout = open(stdout, 'w+')
            stderr = open(stderr, 'w+')

        print(
            textwrap.dedent('''\
                Running the Gunicorn Server with:
                Workers: {num_workers} {args.workerclass}
                Host: {args.hostname}:{args.port}
                Timeout: {worker_timeout}
                Logfiles: {access_logfile} {error_logfile}
                =================================================================\
            '''.format(**locals())))

        run_args = [
            'gunicorn',
            '-w', str(num_workers),
            '-k', str(args.workerclass),
            '-t', str(worker_timeout),
            '-b', args.hostname + ':' + str(args.port),
            '-n', 'airflow-webserver',
            '-p', str(pid),
            '-c', 'python:airflow.www.gunicorn_config'
        ]

        if args.access_logfile:
            run_args += ['--access-logfile', str(args.access_logfile)]

        if args.error_logfile:
            run_args += ['--error-logfile', str(args.error_logfile)]

        if args.daemon:
            run_args += ['-D']

        if ssl_cert:
            run_args += ['--certfile', ssl_cert, '--keyfile', ssl_key]

        run_args += ["airflow.www.app:cached_app()"]

        gunicorn_master_proc = None

        def kill_proc(dummy_signum, dummy_frame):
            gunicorn_master_proc.terminate()
            gunicorn_master_proc.wait()
            sys.exit(0)

        def monitor_gunicorn(gunicorn_master_proc):
            # These run forever until SIG{INT, TERM, KILL, ...} signal is sent
            if conf.getint('webserver', 'worker_refresh_interval') > 0:
                restart_workers(gunicorn_master_proc, num_workers)
            else:
                while True:
                    time.sleep(1)

        if args.daemon:
            base, ext = os.path.splitext(pid)
            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(base + "-monitor" + ext, -1),
                files_preserve=[handle],
                stdout=stdout,
                stderr=stderr,
                signal_map={
                    signal.SIGINT: kill_proc,
                    signal.SIGTERM: kill_proc
                },
            )
            with ctx:
                subprocess.Popen(run_args, close_fds=True)

                # Reading pid file directly, since Popen#pid doesn't
                # seem to return the right value with DaemonContext.
                while True:
                    try:
                        with open(pid) as f:
                            gunicorn_master_proc_pid = int(f.read())
                            break
                    except IOError:
                        log.debug("Waiting for gunicorn's pid file to be created.")
                        time.sleep(0.1)

                gunicorn_master_proc = psutil.Process(gunicorn_master_proc_pid)
                monitor_gunicorn(gunicorn_master_proc)

            stdout.close()
            stderr.close()
        else:
            gunicorn_master_proc = subprocess.Popen(run_args, close_fds=True)

            signal.signal(signal.SIGINT, kill_proc)
            signal.signal(signal.SIGTERM, kill_proc)

            monitor_gunicorn(gunicorn_master_proc)


def scheduler(args):
    print(settings.HEADER)
    job = jobs.SchedulerJob(
        dag_id=args.dag_id,
        subdir=process_subdir(args.subdir),
        run_duration=args.run_duration,
        num_runs=args.num_runs,
        do_pickle=args.do_pickle)

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("scheduler", args.pid, args.stdout, args.stderr, args.log_file)
        handle = setup_logging(log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            files_preserve=[handle],
            stdout=stdout,
            stderr=stderr,
        )
        with ctx:
            job.run()

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        job.run()


def serve_logs(args):
    print("Starting flask")
    import flask
    flask_app = flask.Flask(__name__)

    @flask_app.route('/log/<path:filename>')
    def serve_logs(filename):  # noqa
        log = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        return flask.send_from_directory(
            log,
            filename,
            mimetype="application/json",
            as_attachment=False)

    WORKER_LOG_SERVER_PORT = \
        int(conf.get('celery', 'WORKER_LOG_SERVER_PORT'))
    flask_app.run(
        host='0.0.0.0', port=WORKER_LOG_SERVER_PORT)


def worker(args):
    env = os.environ.copy()
    env['AIRFLOW_HOME'] = settings.AIRFLOW_HOME

    # Celery worker
    from airflow.bin.airflow.executors.celery_executor import app as celery_app
    from celery.bin import worker

    worker = worker.worker(app=celery_app)
    options = {
        'optimization': 'fair',
        'O': 'fair',
        'queues': args.queues,
        'concurrency': args.concurrency,
        'hostname': args.celery_hostname,
    }

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("worker", args.pid, args.stdout, args.stderr, args.log_file)
        handle = setup_logging(log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            files_preserve=[handle],
            stdout=stdout,
            stderr=stderr,
        )
        with ctx:
            sp = subprocess.Popen(['airflow', 'serve_logs'], env=env, close_fds=True)
            worker.run(**options)
            sp.kill()

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        sp = subprocess.Popen(['airflow', 'serve_logs'], env=env, close_fds=True)

        worker.run(**options)
        sp.kill()


def initdb(args):  # noqa
    print("DB: " + repr(settings.engine.url))
    db_utils.initdb()
    print("Done.")


def resetdb(args):
    print("DB: " + repr(settings.engine.url))
    if args.yes or input(
        "This will drop existing tables if they exist. "
        "Proceed? (y/n)").upper() == "Y":
        db_utils.resetdb()
    else:
        print("Bail.")


def upgradedb(args):  # noqa
    print("DB: " + repr(settings.engine.url))
    db_utils.upgradedb()

    # Populate DagStats table
    session = settings.Session()
    ds_rows = session.query(DagStat).count()
    if not ds_rows:
        qry = (
            session.query(DagRun.dag_id, DagRun.state, func.count('*'))
                .group_by(DagRun.dag_id, DagRun.state)
        )
        for dag_id, state, count in qry:
            session.add(DagStat(dag_id=dag_id, state=state, count=count))
        session.commit()


def version(args):  # noqa
    print(settings.HEADER + "  v" + airflow.bin.airflow.__version__)


def connections(args):
    if args.list:
        # Check that no other flags were passed to the command
        invalid_args = list()
        for arg in ['conn_id', 'conn_uri', 'conn_extra'] + alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)
        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the ' +
                   '--list flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
            return

        session = settings.Session()
        conns = session.query(Connection.conn_id, Connection.conn_type,
                              Connection.host, Connection.port,
                              Connection.is_encrypted,
                              Connection.is_extra_encrypted,
                              Connection.extra).all()
        conns = [map(reprlib.repr, conn) for conn in conns]
        print(tabulate(conns, ['Conn Id', 'Conn Type', 'Host', 'Port',
                               'Is Encrypted', 'Is Extra Encrypted', 'Extra'],
                       tablefmt="fancy_grid"))
        return

    if args.delete:
        # Check that only the `conn_id` arg was passed to the command
        invalid_args = list()
        for arg in ['conn_uri', 'conn_extra'] + alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)
        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the ' +
                   '--delete flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
            return

        if args.conn_id is None:
            print('\n\tTo delete a connection, you Must provide a value for ' +
                  'the --conn_id flag.\n')
            return

        session = settings.Session()
        try:
            to_delete = (session
                         .query(Connection)
                         .filter(Connection.conn_id == args.conn_id)
                         .one())
        except exc.NoResultFound:
            msg = '\n\tDid not find a connection with `conn_id`={conn_id}\n'
            msg = msg.format(conn_id=args.conn_id)
            print(msg)
            return
        except exc.MultipleResultsFound:
            msg = ('\n\tFound more than one connection with ' +
                   '`conn_id`={conn_id}\n')
            msg = msg.format(conn_id=args.conn_id)
            print(msg)
            return
        else:
            deleted_conn_id = to_delete.conn_id
            session.delete(to_delete)
            session.commit()
            msg = '\n\tSuccessfully deleted `conn_id`={conn_id}\n'
            msg = msg.format(conn_id=deleted_conn_id)
            print(msg)
        return

    if args.add:
        # Check that the conn_id and conn_uri args were passed to the command:
        missing_args = list()
        invalid_args = list()
        if not args.conn_id:
            missing_args.append('conn_id')
        if args.conn_uri:
            for arg in alternative_conn_specs:
                if getattr(args, arg) is not None:
                    invalid_args.append(arg)
        elif not args.conn_type:
            missing_args.append('conn_uri or conn_type')
        if missing_args:
            msg = ('\n\tThe following args are required to add a connection:' +
                   ' {missing!r}\n'.format(missing=missing_args))
            print(msg)
        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the ' +
                   '--add flag and --conn_uri flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
        if missing_args or invalid_args:
            return

        if args.conn_uri:
            new_conn = Connection(conn_id=args.conn_id, uri=args.conn_uri)
        else:
            new_conn = Connection(conn_id=args.conn_id, conn_type=args.conn_type, host=args.conn_host,
                                  login=args.conn_login, password=args.conn_password, schema=args.conn_schema, port=args.conn_port)
        if args.conn_extra is not None:
            new_conn.set_extra(args.conn_extra)

        session = settings.Session()
        if not (session
                    .query(Connection)
                    .filter(Connection.conn_id == new_conn.conn_id).first()):
            session.add(new_conn)
            session.commit()
            msg = '\n\tSuccessfully added `conn_id`={conn_id} : {uri}\n'
            msg = msg.format(conn_id=new_conn.conn_id, uri=args.conn_uri or urlunparse((args.conn_type, '{login}:{password}@{host}:{port}'.format(
                login=args.conn_login or '', password=args.conn_password or '', host=args.conn_host or '', port=args.conn_port or ''), args.conn_schema or '', '', '', '')))
            print(msg)
        else:
            msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
            msg = msg.format(conn_id=new_conn.conn_id)
            print(msg)

        return


def flower(args):
    broka = conf.get('celery', 'BROKER_URL')
    address = '--address={}'.format(args.hostname)
    port = '--port={}'.format(args.port)
    api = ''
    if args.broker_api:
        api = '--broker_api=' + args.broker_api

    url_prefix = ''
    if args.url_prefix:
        url_prefix = '--url-prefix=' + args.url_prefix

    flower_conf = ''
    if args.flower_conf:
        flower_conf = '--conf=' + args.flower_conf

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("flower", args.pid, args.stdout, args.stderr, args.log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            stdout=stdout,
            stderr=stderr,
        )

        with ctx:
            os.execvp("flower", ['flower', '-b',
                                 broka, address, port, api, flower_conf, url_prefix])

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        os.execvp("flower", ['flower', '-b',
                             broka, address, port, api, flower_conf, url_prefix])


def kerberos(args):  # noqa
    print(settings.HEADER)
    import airflow.bin.airflow.security.kerberos

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("kerberos", args.pid, args.stdout, args.stderr, args.log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            stdout=stdout,
            stderr=stderr,
        )

        with ctx:
            airflow.bin.airflow.security.kerberos.run()

        stdout.close()
        stderr.close()
    else:
        airflow.bin.airflow.security.kerberos.run()


