#!/usr/bin/env python
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
"""Command-line interface"""

import argparse
import errno
import json
import logging
import os
import reprlib
import signal
import subprocess
import sys
import textwrap
import threading
import time
import traceback
from argparse import RawTextHelpFormatter
from urllib.parse import urlunparse

import daemon
import psutil
from daemon.pidfile import TimeoutPIDLockFile
from sqlalchemy.orm import exc
from tabulate import tabulate, tabulate_formats

import airflow
from airflow import api, jobs, settings
from airflow.api.client import get_current_api_client
from airflow.cli.commands import (
    pool_command, role_command, rotate_fernet_key_command, sync_perm_command, task_command, user_command,
)
from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowWebServerTimeout
from airflow.models import DAG, Connection, DagBag, DagModel, DagRun, TaskInstance, Variable
from airflow.utils import cli as cli_utils, db
from airflow.utils.cli import get_dag, process_subdir
from airflow.utils.dot_renderer import render_dag
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.timezone import parse as parsedate
from airflow.www.app import cached_app, create_app

api.load_auth()

LOG = LoggingMixin().log

DAGS_FOLDER = settings.DAGS_FOLDER

if "BUILDING_AIRFLOW_DOCS" in os.environ:
    DAGS_FOLDER = '[AIRFLOW_HOME]/dags'


def sigint_handler(sig, frame):  # pylint: disable=unused-argument
    """
    Returns without error on SIGINT or SIGTERM signals in interactive command mode
    e.g. CTRL+C or kill <PID>
    """
    sys.exit(0)


def sigquit_handler(sig, frame):  # pylint: disable=unused-argument
    """
    Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\
    """
    print("Dumping stack traces for all threads in PID {}".format(os.getpid()))
    id_to_name = {th.ident: th.name for th in threading.enumerate()}
    code = []
    for thread_id, stack in sys._current_frames().items():  # pylint: disable=protected-access
        code.append("\n# Thread: {}({})"
                    .format(id_to_name.get(thread_id, ""), thread_id))
        for filename, line_number, name, line in traceback.extract_stack(stack):
            code.append('File: "{}", line {}, in {}'
                        .format(filename, line_number, name))
            if line:
                code.append("  {}".format(line.strip()))
    print("\n".join(code))


def setup_logging(filename):
    """Creates log file handler for daemon process"""
    root = logging.getLogger()
    handler = logging.FileHandler(filename)
    formatter = logging.Formatter(settings.SIMPLE_LOG_FORMAT)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(settings.LOGGING_LEVEL)

    return handler.stream


def setup_locations(process, pid=None, stdout=None, stderr=None, log=None):
    """Creates logging paths"""
    if not stderr:
        stderr = os.path.join(settings.AIRFLOW_HOME, 'airflow-{}.err'.format(process))
    if not stdout:
        stdout = os.path.join(settings.AIRFLOW_HOME, 'airflow-{}.out'.format(process))
    if not log:
        log = os.path.join(settings.AIRFLOW_HOME, 'airflow-{}.log'.format(process))
    if not pid:
        pid = os.path.join(settings.AIRFLOW_HOME, 'airflow-{}.pid'.format(process))

    return pid, stdout, stderr, log


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

    :param args:
    :return:
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

    :param args:
    :return:
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


def variables_list(args):
    """Displays all of the variables"""
    with db.create_session() as session:
        variables = session.query(Variable)
    print("\n".join(var.key for var in variables))


def variables_get(args):
    """Displays variable by a given name"""
    try:
        var = Variable.get(args.key,
                           deserialize_json=args.json,
                           default_var=args.default)
        print(var)
    except ValueError as e:
        print(e)


@cli_utils.action_logging
def variables_set(args):
    """Creates new variable with a given name and value"""
    Variable.set(args.key, args.value, serialize_json=args.json)


@cli_utils.action_logging
def variables_delete(args):
    """Deletes variable by a given name"""
    Variable.delete(args.key)


@cli_utils.action_logging
def variables_import(args):
    """Imports variables from a given file"""
    if os.path.exists(args.file):
        import_helper(args.file)
    else:
        print("Missing variables file.")


def variables_export(args):
    """Exports all of the variables to the file"""
    variable_export_helper(args.file)


def import_helper(filepath):
    """Helps import variables from the file"""
    with open(filepath, 'r') as varfile:
        data = varfile.read()

    try:
        var_json = json.loads(data)
    except Exception:  # pylint: disable=broad-except
        print("Invalid variables file.")
    else:
        suc_count = fail_count = 0
        for k, v in var_json.items():
            try:
                Variable.set(k, v, serialize_json=not isinstance(v, str))
            except Exception as e:  # pylint: disable=broad-except
                print('Variable import failed: {}'.format(repr(e)))
                fail_count += 1
            else:
                suc_count += 1
        print("{} of {} variables successfully updated.".format(suc_count, len(var_json)))
        if fail_count:
            print("{} variable(s) failed to be updated.".format(fail_count))


def variable_export_helper(filepath):
    """Helps export all of the variables to the file"""
    var_dict = {}
    with db.create_session() as session:
        qry = session.query(Variable).all()

        data = json.JSONDecoder()
        for var in qry:
            try:
                val = data.decode(var.val)
            except Exception:  # pylint: disable=broad-except
                val = var.val
            var_dict[var.key] = val

    with open(filepath, 'w') as varfile:
        varfile.write(json.dumps(var_dict, sort_keys=True, indent=4))
    print("{} variables successfully exported to {}".format(len(var_dict), filepath))


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


def get_num_ready_workers_running(gunicorn_master_proc):
    """Returns number of ready Gunicorn workers by looking for READY_PREFIX in process name"""
    workers = psutil.Process(gunicorn_master_proc.pid).children()

    def ready_prefix_on_cmdline(proc):
        try:
            cmdline = proc.cmdline()
            if len(cmdline) > 0:  # pylint: disable=len-as-condition
                return settings.GUNICORN_WORKER_READY_PREFIX in cmdline[0]
        except psutil.NoSuchProcess:
            pass
        return False

    ready_workers = [proc for proc in workers if ready_prefix_on_cmdline(proc)]
    return len(ready_workers)


def get_num_workers_running(gunicorn_master_proc):
    """Returns number of running Gunicorn workers processes"""
    workers = psutil.Process(gunicorn_master_proc.pid).children()
    return len(workers)


def restart_workers(gunicorn_master_proc, num_workers_expected, master_timeout):
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

    def wait_until_true(fn, timeout=0):
        """
        Sleeps until fn is true
        """
        start_time = time.time()
        while not fn():
            if 0 < timeout <= time.time() - start_time:
                raise AirflowWebServerTimeout(
                    "No response from gunicorn master within {0} seconds"
                    .format(timeout))
            time.sleep(0.1)

    def start_refresh(gunicorn_master_proc):
        batch_size = conf.getint('webserver', 'worker_refresh_batch_size')
        LOG.debug('%s doing a refresh of %s workers', state, batch_size)
        sys.stdout.flush()
        sys.stderr.flush()

        excess = 0
        for _ in range(batch_size):
            gunicorn_master_proc.send_signal(signal.SIGTTIN)
            excess += 1
            wait_until_true(lambda: num_workers_expected + excess ==
                            get_num_workers_running(gunicorn_master_proc),
                            master_timeout)

    try:  # pylint: disable=too-many-nested-blocks
        wait_until_true(lambda: num_workers_expected ==
                        get_num_workers_running(gunicorn_master_proc),
                        master_timeout)
        while True:
            num_workers_running = get_num_workers_running(gunicorn_master_proc)
            num_ready_workers_running = \
                get_num_ready_workers_running(gunicorn_master_proc)

            state = '[{0} / {1}]'.format(num_ready_workers_running, num_workers_running)

            # Whenever some workers are not ready, wait until all workers are ready
            if num_ready_workers_running < num_workers_running:
                LOG.debug('%s some workers are starting up, waiting...', state)
                sys.stdout.flush()
                time.sleep(1)

            # Kill a worker gracefully by asking gunicorn to reduce number of workers
            elif num_workers_running > num_workers_expected:
                excess = num_workers_running - num_workers_expected
                LOG.debug('%s killing %s workers', state, excess)

                for _ in range(excess):
                    gunicorn_master_proc.send_signal(signal.SIGTTOU)
                    excess -= 1
                    wait_until_true(lambda: num_workers_expected + excess ==
                                    get_num_workers_running(gunicorn_master_proc),
                                    master_timeout)

            # Start a new worker by asking gunicorn to increase number of workers
            elif num_workers_running == num_workers_expected:
                refresh_interval = conf.getint('webserver', 'worker_refresh_interval')
                LOG.debug(
                    '%s sleeping for %ss starting doing a refresh...',
                    state, refresh_interval
                )
                time.sleep(refresh_interval)
                start_refresh(gunicorn_master_proc)

            else:
                # num_ready_workers_running == num_workers_running < num_workers_expected
                LOG.error((
                    "%s some workers seem to have died and gunicorn"
                    "did not restart them as expected"
                ), state)
                time.sleep(10)
                if len(
                    psutil.Process(gunicorn_master_proc.pid).children()
                ) < num_workers_expected:
                    start_refresh(gunicorn_master_proc)
    except (AirflowWebServerTimeout, OSError) as err:
        LOG.error(err)
        LOG.error("Shutting down webserver")
        try:
            gunicorn_master_proc.terminate()
            gunicorn_master_proc.wait()
        finally:
            sys.exit(1)


@cli_utils.action_logging
def webserver(args):
    """Starts Airflow Webserver"""
    print(settings.HEADER)

    access_logfile = args.access_logfile or conf.get('webserver', 'access_logfile')
    error_logfile = args.error_logfile or conf.get('webserver', 'error_logfile')
    num_workers = args.workers or conf.get('webserver', 'workers')
    worker_timeout = (args.worker_timeout or
                      conf.get('webserver', 'web_server_worker_timeout'))
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
        app, _ = create_app(None, testing=conf.getboolean('core', 'unit_test_mode'))
        app.run(debug=True, use_reloader=not app.config['TESTING'],
                port=args.port, host=args.hostname,
                ssl_context=(ssl_cert, ssl_key) if ssl_cert and ssl_key else None)
    else:
        os.environ['SKIP_DAGS_PARSING'] = 'True'
        app = cached_app(None)
        pid, stdout, stderr, log_file = setup_locations(
            "webserver", args.pid, args.stdout, args.stderr, args.log_file)
        os.environ.pop('SKIP_DAGS_PARSING')
        if args.daemon:
            handle = setup_logging(log_file)
            stdout = open(stdout, 'w+')
            stderr = open(stderr, 'w+')

        print(
            textwrap.dedent('''\
                Running the Gunicorn Server with:
                Workers: {num_workers} {workerclass}
                Host: {hostname}:{port}
                Timeout: {worker_timeout}
                Logfiles: {access_logfile} {error_logfile}
                =================================================================\
            '''.format(num_workers=num_workers, workerclass=args.workerclass,
                       hostname=args.hostname, port=args.port,
                       worker_timeout=worker_timeout, access_logfile=access_logfile,
                       error_logfile=error_logfile)))

        run_args = [
            'gunicorn',
            '-w', str(num_workers),
            '-k', str(args.workerclass),
            '-t', str(worker_timeout),
            '-b', args.hostname + ':' + str(args.port),
            '-n', 'airflow-webserver',
            '-p', str(pid),
            '-c', 'python:airflow.www.gunicorn_config',
        ]

        if args.access_logfile:
            run_args += ['--access-logfile', str(args.access_logfile)]

        if args.error_logfile:
            run_args += ['--error-logfile', str(args.error_logfile)]

        if args.daemon:
            run_args += ['-D']

        if ssl_cert:
            run_args += ['--certfile', ssl_cert, '--keyfile', ssl_key]

        webserver_module = 'www'
        run_args += ["airflow." + webserver_module + ".app:cached_app()"]

        gunicorn_master_proc = None

        def kill_proc(dummy_signum, dummy_frame):  # pylint: disable=unused-argument
            gunicorn_master_proc.terminate()
            gunicorn_master_proc.wait()
            sys.exit(0)

        def monitor_gunicorn(gunicorn_master_proc):
            # These run forever until SIG{INT, TERM, KILL, ...} signal is sent
            if conf.getint('webserver', 'worker_refresh_interval') > 0:
                master_timeout = conf.getint('webserver', 'web_server_master_timeout')
                restart_workers(gunicorn_master_proc, num_workers, master_timeout)
            else:
                while gunicorn_master_proc.poll() is None:
                    time.sleep(1)

                sys.exit(gunicorn_master_proc.returncode)

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
                        with open(pid) as file:
                            gunicorn_master_proc_pid = int(file.read())
                            break
                    except OSError:
                        LOG.debug("Waiting for gunicorn's pid file to be created.")
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


@cli_utils.action_logging
def scheduler(args):
    """Starts Airflow Scheduler"""
    print(settings.HEADER)
    job = jobs.SchedulerJob(
        dag_id=args.dag_id,
        subdir=process_subdir(args.subdir),
        num_runs=args.num_runs,
        do_pickle=args.do_pickle)

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("scheduler",
                                                        args.pid,
                                                        args.stdout,
                                                        args.stderr,
                                                        args.log_file)
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


@cli_utils.action_logging
def serve_logs(args):
    """Serves logs generated by Worker"""
    print("Starting flask")
    import flask
    flask_app = flask.Flask(__name__)

    @flask_app.route('/log/<path:filename>')
    def serve_logs(filename):  # pylint: disable=unused-variable, redefined-outer-name
        log = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        return flask.send_from_directory(
            log,
            filename,
            mimetype="application/json",
            as_attachment=False)

    worker_log_server_port = int(conf.get('celery', 'WORKER_LOG_SERVER_PORT'))
    flask_app.run(host='0.0.0.0', port=worker_log_server_port)


@cli_utils.action_logging
def worker(args):
    """Starts Airflow Celery worker"""
    env = os.environ.copy()
    env['AIRFLOW_HOME'] = settings.AIRFLOW_HOME

    if not settings.validate_session():
        log = LoggingMixin().log
        log.error("Worker exiting... database connection precheck failed! ")
        sys.exit(1)

    # Celery worker
    from airflow.executors.celery_executor import app as celery_app
    from celery.bin import worker  # pylint: disable=redefined-outer-name

    autoscale = args.autoscale
    if autoscale is None and conf.has_option("celery", "worker_autoscale"):
        autoscale = conf.get("celery", "worker_autoscale")
    worker = worker.worker(app=celery_app)   # pylint: disable=redefined-outer-name
    options = {
        'optimization': 'fair',
        'O': 'fair',
        'queues': args.queues,
        'concurrency': args.concurrency,
        'autoscale': autoscale,
        'hostname': args.celery_hostname,
        'loglevel': conf.get('core', 'LOGGING_LEVEL'),
    }

    if conf.has_option("celery", "pool"):
        options["pool"] = conf.get("celery", "pool")

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations("worker",
                                                        args.pid,
                                                        args.stdout,
                                                        args.stderr,
                                                        args.log_file)
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
            sub_proc = subprocess.Popen(['airflow', 'serve_logs'], env=env, close_fds=True)
            worker.run(**options)
            sub_proc.kill()

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        sub_proc = subprocess.Popen(['airflow', 'serve_logs'], env=env, close_fds=True)

        worker.run(**options)
        sub_proc.kill()


def initdb(args):
    """Initializes the metadata database"""
    print("DB: " + repr(settings.engine.url))
    db.initdb()
    print("Done.")


def resetdb(args):
    """Resets the metadata database"""
    print("DB: " + repr(settings.engine.url))
    if args.yes or input("This will drop existing tables "
                         "if they exist. Proceed? "
                         "(y/n)").upper() == "Y":
        db.resetdb()
    else:
        print("Bail.")


@cli_utils.action_logging
def upgradedb(args):
    """Upgrades the metadata database"""
    print("DB: " + repr(settings.engine.url))
    db.upgradedb()


@cli_utils.action_logging
def version(args):
    """Displays Airflow version at the command line"""
    print(settings.HEADER + "  v" + airflow.__version__)


alternative_conn_specs = ['conn_type', 'conn_host',
                          'conn_login', 'conn_password', 'conn_schema', 'conn_port']


def connections_list(args):
    """Lists all connections at the command line"""
    with db.create_session() as session:
        conns = session.query(Connection.conn_id, Connection.conn_type,
                              Connection.host, Connection.port,
                              Connection.is_encrypted,
                              Connection.is_extra_encrypted,
                              Connection.extra).all()
        conns = [map(reprlib.repr, conn) for conn in conns]
        msg = tabulate(conns, ['Conn Id', 'Conn Type', 'Host', 'Port',
                               'Is Encrypted', 'Is Extra Encrypted', 'Extra'],
                       tablefmt=args.output)
        print(msg)


@cli_utils.action_logging
def connections_add(args):
    """Adds new connection"""
    # Check that the conn_id and conn_uri args were passed to the command:
    missing_args = list()
    invalid_args = list()
    if args.conn_uri:
        for arg in alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)
    elif not args.conn_type:
        missing_args.append('conn_uri or conn_type')
    if missing_args:
        msg = ('The following args are required to add a connection:' +
               ' {missing!r}'.format(missing=missing_args))
        raise SystemExit(msg)
    if invalid_args:
        msg = ('The following args are not compatible with the ' +
               '--add flag and --conn_uri flag: {invalid!r}')
        msg = msg.format(invalid=invalid_args)
        raise SystemExit(msg)

    if args.conn_uri:
        new_conn = Connection(conn_id=args.conn_id, uri=args.conn_uri)
    else:
        new_conn = Connection(conn_id=args.conn_id,
                              conn_type=args.conn_type,
                              host=args.conn_host,
                              login=args.conn_login,
                              password=args.conn_password,
                              schema=args.conn_schema,
                              port=args.conn_port)
    if args.conn_extra is not None:
        new_conn.set_extra(args.conn_extra)

    with db.create_session() as session:
        if not (session.query(Connection)
                .filter(Connection.conn_id == new_conn.conn_id).first()):
            session.add(new_conn)
            msg = '\n\tSuccessfully added `conn_id`={conn_id} : {uri}\n'
            msg = msg.format(conn_id=new_conn.conn_id,
                             uri=args.conn_uri or
                             urlunparse((args.conn_type,
                                         '{login}:{password}@{host}:{port}'
                                             .format(login=args.conn_login or '',
                                                     password=args.conn_password or '',
                                                     host=args.conn_host or '',
                                                     port=args.conn_port or ''),
                                         args.conn_schema or '', '', '', '')))
            print(msg)
        else:
            msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
            msg = msg.format(conn_id=new_conn.conn_id)
            print(msg)


@cli_utils.action_logging
def connections_delete(args):
    """Deletes connection from DB"""
    with db.create_session() as session:
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
            msg = '\n\tSuccessfully deleted `conn_id`={conn_id}\n'
            msg = msg.format(conn_id=deleted_conn_id)
            print(msg)


@cli_utils.action_logging
def flower(args):
    """Starts Flower, Celery monitoring tool"""
    broka = conf.get('celery', 'BROKER_URL')
    address = '--address={}'.format(args.hostname)
    port = '--port={}'.format(args.port)
    api = ''  # pylint: disable=redefined-outer-name
    if args.broker_api:
        api = '--broker_api=' + args.broker_api

    url_prefix = ''
    if args.url_prefix:
        url_prefix = '--url-prefix=' + args.url_prefix

    basic_auth = ''
    if args.basic_auth:
        basic_auth = '--basic_auth=' + args.basic_auth

    flower_conf = ''
    if args.flower_conf:
        flower_conf = '--conf=' + args.flower_conf

    if args.daemon:
        pid, stdout, stderr, _ = setup_locations("flower", args.pid, args.stdout, args.stderr, args.log_file)
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            stdout=stdout,
            stderr=stderr,
        )

        with ctx:
            os.execvp("flower", ['flower', '-b',
                                 broka, address, port, api, flower_conf, url_prefix, basic_auth])

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        os.execvp("flower", ['flower', '-b',
                             broka, address, port, api, flower_conf, url_prefix, basic_auth])


@cli_utils.action_logging
def kerberos(args):
    """Start a kerberos ticket renewer"""
    print(settings.HEADER)
    import airflow.security.kerberos  # pylint: disable=redefined-outer-name

    if args.daemon:
        pid, stdout, stderr, _ = setup_locations(
            "kerberos", args.pid, args.stdout, args.stderr, args.log_file
        )
        stdout = open(stdout, 'w+')
        stderr = open(stderr, 'w+')

        ctx = daemon.DaemonContext(
            pidfile=TimeoutPIDLockFile(pid, -1),
            stdout=stdout,
            stderr=stderr,
        )

        with ctx:
            airflow.security.kerberos.run(principal=args.principal, keytab=args.keytab)

        stdout.close()
        stderr.close()
    else:
        airflow.security.kerberos.run(principal=args.principal, keytab=args.keytab)


@cli_utils.action_logging
def dag_list_dag_runs(args, dag=None):
    """Lists dag runs for a given DAG"""
    if dag:
        args.dag_id = dag.dag_id

    dagbag = DagBag()

    if args.dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(args.dag_id)
        raise AirflowException(error_message)

    dag_runs = list()
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


class Arg:
    """Class to keep information about command line argument"""
    # pylint: disable=redefined-builtin
    def __init__(self, flags=None, help=None, action=None, default=None, nargs=None,
                 type=None, choices=None, required=None, metavar=None):
        self.flags = flags
        self.help = help
        self.action = action
        self.default = default
        self.nargs = nargs
        self.type = type
        self.choices = choices
        self.required = required
        self.metavar = metavar
    # pylint: enable=redefined-builtin


class CLIFactory:
    """
    Factory class which generates command line argument parser and holds information
    about all available Airflow commands
    """
    args = {
        # Shared
        'dag_id': Arg(("dag_id",), "The id of the dag"),
        'task_id': Arg(("task_id",), "The id of the task"),
        'execution_date': Arg(
            ("execution_date",), help="The execution date of the DAG",
            type=parsedate),
        'task_regex': Arg(
            ("-t", "--task_regex"),
            "The regex to filter specific task_ids to backfill (optional)"),
        'subdir': Arg(
            ("-sd", "--subdir"),
            "File location or directory from which to look for the dag. "
            "Defaults to '[AIRFLOW_HOME]/dags' where [AIRFLOW_HOME] is the "
            "value you set for 'AIRFLOW_HOME' config you set in 'airflow.cfg' ",
            default=DAGS_FOLDER),
        'start_date': Arg(
            ("-s", "--start_date"), "Override start_date YYYY-MM-DD",
            type=parsedate),
        'end_date': Arg(
            ("-e", "--end_date"), "Override end_date YYYY-MM-DD",
            type=parsedate),
        'dry_run': Arg(
            ("-dr", "--dry_run"), "Perform a dry run", "store_true"),
        'pid': Arg(
            ("--pid",), "PID file location",
            nargs='?'),
        'daemon': Arg(
            ("-D", "--daemon"), "Daemonize instead of running "
                                "in the foreground",
            "store_true"),
        'stderr': Arg(
            ("--stderr",), "Redirect stderr to this file"),
        'stdout': Arg(
            ("--stdout",), "Redirect stdout to this file"),
        'log_file': Arg(
            ("-l", "--log-file"), "Location of the log file"),
        'yes': Arg(
            ("-y", "--yes"),
            "Do not prompt to confirm reset. Use with care!",
            "store_true",
            default=False),
        'output': Arg(
            ("--output",), (
                "Output table format. The specified value is passed to "
                "the tabulate module (https://pypi.org/project/tabulate/). "
                "Valid values are: ({})".format("|".join(tabulate_formats))
            ),
            choices=tabulate_formats,
            default="fancy_grid"),

        # list_dag_runs
        'no_backfill': Arg(
            ("--no_backfill",),
            "filter all the backfill dagruns given the dag id", "store_true"),
        'state': Arg(
            ("--state",),
            "Only list the dag runs corresponding to the state"
        ),

        # list_jobs
        'limit': Arg(
            ("--limit",),
            "Return a limited number of records"
        ),

        # backfill
        'mark_success': Arg(
            ("-m", "--mark_success"),
            "Mark jobs as succeeded without running them", "store_true"),
        'verbose': Arg(
            ("-v", "--verbose"),
            "Make logging output more verbose", "store_true"),
        'local': Arg(
            ("-l", "--local"),
            "Run the task using the LocalExecutor", "store_true"),
        'donot_pickle': Arg(
            ("-x", "--donot_pickle"), (
                "Do not attempt to pickle the DAG object to send over "
                "to the workers, just tell the workers to run their version "
                "of the code."),
            "store_true"),
        'bf_ignore_dependencies': Arg(
            ("-i", "--ignore_dependencies"),
            (
                "Skip upstream tasks, run only the tasks "
                "matching the regexp. Only works in conjunction "
                "with task_regex"),
            "store_true"),
        'bf_ignore_first_depends_on_past': Arg(
            ("-I", "--ignore_first_depends_on_past"),
            (
                "Ignores depends_on_past dependencies for the first "
                "set of tasks only (subsequent executions in the backfill "
                "DO respect depends_on_past)."),
            "store_true"),
        'pool': Arg(("--pool",), "Resource pool to use"),
        'delay_on_limit': Arg(
            ("--delay_on_limit",),
            help=("Amount of time in seconds to wait when the limit "
                  "on maximum active dag runs (max_active_runs) has "
                  "been reached before trying to execute a dag run "
                  "again."),
            type=float,
            default=1.0),
        'reset_dag_run': Arg(
            ("--reset_dagruns",),
            (
                "if set, the backfill will delete existing "
                "backfill-related DAG runs and start "
                "anew with fresh, running DAG runs"),
            "store_true"),
        'rerun_failed_tasks': Arg(
            ("--rerun_failed_tasks",),
            (
                "if set, the backfill will auto-rerun "
                "all the failed tasks for the backfill date range "
                "instead of throwing exceptions"),
            "store_true"),
        'run_backwards': Arg(
            ("-B", "--run_backwards",),
            (
                "if set, the backfill will run tasks from the most "
                "recent day first.  if there are tasks that depend_on_past "
                "this option will throw an exception"),
            "store_true"),

        # list_tasks
        'tree': Arg(("-t", "--tree"), "Tree view", "store_true"),
        # list_dags
        'report': Arg(
            ("-r", "--report"), "Show DagBag loading report", "store_true"),
        # clear
        'upstream': Arg(
            ("-u", "--upstream"), "Include upstream tasks", "store_true"),
        'only_failed': Arg(
            ("-f", "--only_failed"), "Only failed jobs", "store_true"),
        'only_running': Arg(
            ("-r", "--only_running"), "Only running jobs", "store_true"),
        'downstream': Arg(
            ("-d", "--downstream"), "Include downstream tasks", "store_true"),
        'exclude_subdags': Arg(
            ("-x", "--exclude_subdags"),
            "Exclude subdags", "store_true"),
        'exclude_parentdag': Arg(
            ("-xp", "--exclude_parentdag"),
            "Exclude ParentDAGS if the task cleared is a part of a SubDAG",
            "store_true"),
        'dag_regex': Arg(
            ("-dx", "--dag_regex"),
            "Search dag_id as regex instead of exact string", "store_true"),
        # show_dag
        'save': Arg(
            ("-s", "--save"),
            "Saves the result to the indicated file.\n"
            "\n"
            "The file format is determined by the file extension. For more information about supported "
            "format, see: https://www.graphviz.org/doc/info/output.html\n"
            "\n"
            "If you want to create a PNG file then you should execute the following command:\n"
            "airflow dags show <DAG_ID> --save output.png\n"
            "\n"
            "If you want to create a DOT file then you should execute the following command:\n"
            "airflow dags show <DAG_ID> --save output.dot\n"
        ),
        'imgcat': Arg(
            ("--imgcat", ),
            "Displays graph using the imgcat tool. \n"
            "\n"
            "For more information, see: https://www.iterm2.com/documentation-images.html",
            action='store_true'),
        # trigger_dag
        'run_id': Arg(("-r", "--run_id"), "Helps to identify this run"),
        'conf': Arg(
            ('-c', '--conf'),
            "JSON string that gets pickled into the DagRun's conf attribute"),
        'exec_date': Arg(
            ("-e", "--exec_date"), help="The execution date of the DAG",
            type=parsedate),
        # pool
        'pool_name': Arg(
            ("pool",),
            metavar='NAME',
            help="Pool name"),
        'pool_slots': Arg(
            ("slots",),
            type=int,
            help="Pool slots"),
        'pool_description': Arg(
            ("description",),
            help="Pool description"),
        'pool_import': Arg(
            ("file",),
            metavar="FILEPATH",
            help="Import pool from JSON file"),
        'pool_export': Arg(
            ("file",),
            metavar="FILEPATH",
            help="Export pool to JSON file"),
        # variables
        'var': Arg(
            ("key",),
            help="Variable key"),
        'var_value': Arg(
            ("value",),
            metavar='VALUE',
            help="Variable value"),
        'default': Arg(
            ("-d", "--default"),
            metavar="VAL",
            default=None,
            help="Default value returned if variable does not exist"),
        'json': Arg(
            ("-j", "--json"),
            help="Deserialize JSON variable",
            action="store_true"),
        'var_import': Arg(
            ("file",),
            help="Import variables from JSON file"),
        'var_export': Arg(
            ("file",),
            help="Export variables to JSON file"),
        # kerberos
        'principal': Arg(
            ("principal",), "kerberos principal", nargs='?'),
        'keytab': Arg(
            ("-kt", "--keytab"), "keytab",
            nargs='?', default=conf.get('kerberos', 'keytab')),
        # run
        # TODO(aoen): "force" is a poor choice of name here since it implies it overrides
        # all dependencies (not just past success), e.g. the ignore_depends_on_past
        # dependency. This flag should be deprecated and renamed to 'ignore_ti_state' and
        # the "ignore_all_dependencies" command should be called the"force" command
        # instead.
        'interactive': Arg(
            ('-int', '--interactive'),
            help='Do not capture standard output and error streams '
                 '(useful for interactive debugging)',
            action='store_true'),
        'force': Arg(
            ("-f", "--force"),
            "Ignore previous task instance state, rerun regardless if task already "
            "succeeded/failed",
            "store_true"),
        'raw': Arg(("-r", "--raw"), argparse.SUPPRESS, "store_true"),
        'ignore_all_dependencies': Arg(
            ("-A", "--ignore_all_dependencies"),
            "Ignores all non-critical dependencies, including ignore_ti_state and "
            "ignore_task_deps",
            "store_true"),
        # TODO(aoen): ignore_dependencies is a poor choice of name here because it is too
        # vague (e.g. a task being in the appropriate state to be run is also a dependency
        # but is not ignored by this flag), the name 'ignore_task_dependencies' is
        # slightly better (as it ignores all dependencies that are specific to the task),
        # so deprecate the old command name and use this instead.
        'ignore_dependencies': Arg(
            ("-i", "--ignore_dependencies"),
            "Ignore task-specific dependencies, e.g. upstream, depends_on_past, and "
            "retry delay dependencies",
            "store_true"),
        'ignore_depends_on_past': Arg(
            ("-I", "--ignore_depends_on_past"),
            "Ignore depends_on_past dependencies (but respect "
            "upstream dependencies)",
            "store_true"),
        'ship_dag': Arg(
            ("--ship_dag",),
            "Pickles (serializes) the DAG and ships it to the worker",
            "store_true"),
        'pickle': Arg(
            ("-p", "--pickle"),
            "Serialized pickle object of the entire dag (used internally)"),
        'job_id': Arg(("-j", "--job_id"), argparse.SUPPRESS),
        'cfg_path': Arg(
            ("--cfg_path",), "Path to config file to use instead of airflow.cfg"),
        # webserver
        'port': Arg(
            ("-p", "--port"),
            default=conf.get('webserver', 'WEB_SERVER_PORT'),
            type=int,
            help="The port on which to run the server"),
        'ssl_cert': Arg(
            ("--ssl_cert",),
            default=conf.get('webserver', 'WEB_SERVER_SSL_CERT'),
            help="Path to the SSL certificate for the webserver"),
        'ssl_key': Arg(
            ("--ssl_key",),
            default=conf.get('webserver', 'WEB_SERVER_SSL_KEY'),
            help="Path to the key to use with the SSL certificate"),
        'workers': Arg(
            ("-w", "--workers"),
            default=conf.get('webserver', 'WORKERS'),
            type=int,
            help="Number of workers to run the webserver on"),
        'workerclass': Arg(
            ("-k", "--workerclass"),
            default=conf.get('webserver', 'WORKER_CLASS'),
            choices=['sync', 'eventlet', 'gevent', 'tornado'],
            help="The worker class to use for Gunicorn"),
        'worker_timeout': Arg(
            ("-t", "--worker_timeout"),
            default=conf.get('webserver', 'WEB_SERVER_WORKER_TIMEOUT'),
            type=int,
            help="The timeout for waiting on webserver workers"),
        'hostname': Arg(
            ("-hn", "--hostname"),
            default=conf.get('webserver', 'WEB_SERVER_HOST'),
            help="Set the hostname on which to run the web server"),
        'debug': Arg(
            ("-d", "--debug"),
            "Use the server that ships with Flask in debug mode",
            "store_true"),
        'access_logfile': Arg(
            ("-A", "--access_logfile"),
            default=conf.get('webserver', 'ACCESS_LOGFILE'),
            help="The logfile to store the webserver access log. Use '-' to print to "
                 "stderr."),
        'error_logfile': Arg(
            ("-E", "--error_logfile"),
            default=conf.get('webserver', 'ERROR_LOGFILE'),
            help="The logfile to store the webserver error log. Use '-' to print to "
                 "stderr."),
        # scheduler
        'dag_id_opt': Arg(("-d", "--dag_id"), help="The id of the dag to run"),
        'num_runs': Arg(
            ("-n", "--num_runs"),
            default=conf.getint('scheduler', 'num_runs'), type=int,
            help="Set the number of runs to execute before exiting"),
        # worker
        'do_pickle': Arg(
            ("-p", "--do_pickle"),
            default=False,
            help=(
                "Attempt to pickle the DAG object to send over "
                "to the workers, instead of letting workers run their version "
                "of the code."),
            action="store_true"),
        'queues': Arg(
            ("-q", "--queues"),
            help="Comma delimited list of queues to serve",
            default=conf.get('celery', 'DEFAULT_QUEUE')),
        'concurrency': Arg(
            ("-c", "--concurrency"),
            type=int,
            help="The number of worker processes",
            default=conf.get('celery', 'worker_concurrency')),
        'celery_hostname': Arg(
            ("-cn", "--celery_hostname"),
            help=("Set the hostname of celery worker "
                  "if you have multiple workers on a single machine.")),
        # flower
        'broker_api': Arg(("-a", "--broker_api"), help="Broker api"),
        'flower_hostname': Arg(
            ("-hn", "--hostname"),
            default=conf.get('celery', 'FLOWER_HOST'),
            help="Set the hostname on which to run the server"),
        'flower_port': Arg(
            ("-p", "--port"),
            default=conf.get('celery', 'FLOWER_PORT'),
            type=int,
            help="The port on which to run the server"),
        'flower_conf': Arg(
            ("-fc", "--flower_conf"),
            help="Configuration file for flower"),
        'flower_url_prefix': Arg(
            ("-u", "--url_prefix"),
            default=conf.get('celery', 'FLOWER_URL_PREFIX'),
            help="URL prefix for Flower"),
        'flower_basic_auth': Arg(
            ("-ba", "--basic_auth"),
            default=conf.get('celery', 'FLOWER_BASIC_AUTH'),
            help=("Securing Flower with Basic Authentication. "
                  "Accepts user:password pairs separated by a comma. "
                  "Example: flower_basic_auth = user1:password1,user2:password2")),
        'task_params': Arg(
            ("-tp", "--task_params"),
            help="Sends a JSON params dict to the task"),
        'post_mortem': Arg(
            ("-pm", "--post_mortem"),
            action="store_true",
            help="Open debugger on uncaught exception",
        ),
        # connections
        'conn_id': Arg(
            ('conn_id',),
            help='Connection id, required to add/delete a connection',
            type=str),
        'conn_uri': Arg(
            ('--conn_uri',),
            help='Connection URI, required to add a connection without conn_type',
            type=str),
        'conn_type': Arg(
            ('--conn_type',),
            help='Connection type, required to add a connection without conn_uri',
            type=str),
        'conn_host': Arg(
            ('--conn_host',),
            help='Connection host, optional when adding a connection',
            type=str),
        'conn_login': Arg(
            ('--conn_login',),
            help='Connection login, optional when adding a connection',
            type=str),
        'conn_password': Arg(
            ('--conn_password',),
            help='Connection password, optional when adding a connection',
            type=str),
        'conn_schema': Arg(
            ('--conn_schema',),
            help='Connection schema, optional when adding a connection',
            type=str),
        'conn_port': Arg(
            ('--conn_port',),
            help='Connection port, optional when adding a connection',
            type=str),
        'conn_extra': Arg(
            ('--conn_extra',),
            help='Connection `Extra` field, optional when adding a connection',
            type=str),
        # users
        'username': Arg(
            ('--username',),
            help='Username of the user',
            required=True,
            type=str),
        'username_optional': Arg(
            ('--username',),
            help='Username of the user',
            type=str),
        'firstname': Arg(
            ('--firstname',),
            help='First name of the user',
            required=True,
            type=str),
        'lastname': Arg(
            ('--lastname',),
            help='Last name of the user',
            required=True,
            type=str),
        'role': Arg(
            ('--role',),
            help='Role of the user. Existing roles include Admin, '
                 'User, Op, Viewer, and Public.',
            required=True,
            type=str,
        ),
        'email': Arg(
            ('--email',),
            help='Email of the user',
            required=True,
            type=str),
        'email_optional': Arg(
            ('--email',),
            help='Email of the user',
            type=str),
        'password': Arg(
            ('--password',),
            help='Password of the user, required to create a user '
                 'without --use_random_password',
            type=str),
        'use_random_password': Arg(
            ('--use_random_password',),
            help='Do not prompt for password. Use random string instead.'
                 ' Required to create a user without --password ',
            default=False,
            action='store_true'),
        'user_import': Arg(
            ("import",),
            metavar="FILEPATH",
            help="Import users from JSON file. Example format:" +
                    textwrap.dedent('''
                    [
                        {
                            "email": "foo@bar.org",
                            "firstname": "Jon",
                            "lastname": "Doe",
                            "roles": ["Public"],
                            "username": "jondoe"
                        }
                    ]'''),
        ),
        'user_export': Arg(
            ("export",),
            metavar="FILEPATH",
            help="Export users to JSON file"),
        # roles
        'create_role': Arg(
            ('-c', '--create'),
            help='Create a new role',
            action='store_true'),
        'list_roles': Arg(
            ('-l', '--list'),
            help='List roles',
            action='store_true'),
        'roles': Arg(
            ('role',),
            help='The name of a role',
            nargs='*'),
        'autoscale': Arg(
            ('-a', '--autoscale'),
            help="Minimum and Maximum number of worker to autoscale"),
    }
    subparsers = (
        {
            'help': 'List and manage DAGs',
            'name': 'dags',
            'subcommands': (
                {
                    'func': dag_list_dags,
                    'name': 'list',
                    'help': "List all the DAGs",
                    'args': ('subdir', 'report'),
                },
                {
                    'func': dag_list_dag_runs,
                    'name': 'list_runs',
                    'help': "List dag runs given a DAG id. If state option is given, it will only "
                            "search for all the dagruns with the given state. "
                            "If no_backfill option is given, it will filter out "
                            "all backfill dagruns for given dag id.",
                    'args': ('dag_id', 'no_backfill', 'state'),
                },
                {
                    'func': dag_list_jobs,
                    'name': 'list_jobs',
                    'help': "List the jobs",
                    'args': ('dag_id_opt', 'state', 'limit', 'output',),
                },
                {
                    'func': dag_state,
                    'name': 'state',
                    'help': "Get the status of a dag run",
                    'args': ('dag_id', 'execution_date', 'subdir'),
                },
                {
                    'func': dag_next_execution,
                    'name': 'next_execution',
                    'help': "Get the next execution datetime of a DAG.",
                    'args': ('dag_id', 'subdir'),
                },
                {
                    'func': dag_pause,
                    'name': 'pause',
                    'help': 'Pause a DAG',
                    'args': ('dag_id', 'subdir'),
                },
                {
                    'func': dag_unpause,
                    'name': 'unpause',
                    'help': 'Resume a paused DAG',
                    'args': ('dag_id', 'subdir'),
                },
                {
                    'func': dag_trigger,
                    'name': 'trigger',
                    'help': 'Trigger a DAG run',
                    'args': ('dag_id', 'subdir', 'run_id', 'conf', 'exec_date'),
                },
                {
                    'func': dag_delete,
                    'name': 'delete',
                    'help': "Delete all DB records related to the specified DAG",
                    'args': ('dag_id', 'yes'),
                },
                {
                    'func': dag_show,
                    'name': 'show',
                    'help': "Displays DAG's tasks with their dependencies",
                    'args': ('dag_id', 'subdir', 'save', 'imgcat',),
                },
                {
                    'func': dag_backfill,
                    'name': 'backfill',
                    'help': "Run subsections of a DAG for a specified date range. "
                            "If reset_dag_run option is used,"
                            " backfill will first prompt users whether airflow "
                            "should clear all the previous dag_run and task_instances "
                            "within the backfill date range. "
                            "If rerun_failed_tasks is used, backfill "
                            "will auto re-run the previous failed task instances"
                            " within the backfill date range.",
                    'args': (
                        'dag_id', 'task_regex', 'start_date', 'end_date',
                        'mark_success', 'local', 'donot_pickle', 'yes',
                        'bf_ignore_dependencies', 'bf_ignore_first_depends_on_past',
                        'subdir', 'pool', 'delay_on_limit', 'dry_run', 'verbose', 'conf',
                        'reset_dag_run', 'rerun_failed_tasks', 'run_backwards'
                    ),
                },
            ),
        }, {
            'help': 'List and manage tasks',
            'name': 'tasks',
            'subcommands': (
                {
                    'func': task_command.task_list,
                    'name': 'list',
                    'help': "List the tasks within a DAG",
                    'args': ('dag_id', 'tree', 'subdir'),
                },
                {
                    'func': task_command.task_clear,
                    'name': 'clear',
                    'help': "Clear a set of task instance, as if they never ran",
                    'args': (
                        'dag_id', 'task_regex', 'start_date', 'end_date', 'subdir',
                        'upstream', 'downstream', 'yes', 'only_failed',
                        'only_running', 'exclude_subdags', 'exclude_parentdag', 'dag_regex'),
                },
                {
                    'func': task_command.task_state,
                    'name': 'state',
                    'help': "Get the status of a task instance",
                    'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
                },
                {
                    'func': task_command.task_failed_deps,
                    'name': 'failed_deps',
                    'help': (
                        "Returns the unmet dependencies for a task instance from the perspective "
                        "of the scheduler. In other words, why a task instance doesn't get "
                        "scheduled and then queued by the scheduler, and then run by an "
                        "executor)."),
                    'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
                },
                {
                    'func': task_command.task_render,
                    'name': 'render',
                    'help': "Render a task instance's template(s)",
                    'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
                },
                {
                    'func': task_command.task_run,
                    'name': 'run',
                    'help': "Run a single task instance",
                    'args': (
                        'dag_id', 'task_id', 'execution_date', 'subdir',
                        'mark_success', 'force', 'pool', 'cfg_path',
                        'local', 'raw', 'ignore_all_dependencies', 'ignore_dependencies',
                        'ignore_depends_on_past', 'ship_dag', 'pickle', 'job_id', 'interactive',),
                },
                {
                    'func': task_command.task_test,
                    'name': 'test',
                    'help': (
                        "Test a task instance. This will run a task without checking for "
                        "dependencies or recording its state in the database."),
                    'args': (
                        'dag_id', 'task_id', 'execution_date', 'subdir', 'dry_run',
                        'task_params', 'post_mortem'),
                },
            ),
        }, {
            'help': "CRUD operations on pools",
            'name': 'pools',
            'subcommands': (
                {
                    'func': pool_command.pool_list,
                    'name': 'list',
                    'help': 'List pools',
                    'args': ('output',),
                },
                {
                    'func': pool_command.pool_get,
                    'name': 'get',
                    'help': 'Get pool size',
                    'args': ('pool_name', 'output',),
                },
                {
                    'func': pool_command.pool_set,
                    'name': 'set',
                    'help': 'Configure pool',
                    'args': ('pool_name', 'pool_slots', 'pool_description', 'output',),
                },
                {
                    'func': pool_command.pool_delete,
                    'name': 'delete',
                    'help': 'Delete pool',
                    'args': ('pool_name', 'output',),
                },
                {
                    'func': pool_command.pool_import,
                    'name': 'import',
                    'help': 'Import pool',
                    'args': ('pool_import', 'output',),
                },
                {
                    'func': pool_command.pool_export,
                    'name': 'export',
                    'help': 'Export pool',
                    'args': ('pool_export', 'output',),
                },
            ),
        }, {
            'help': "CRUD operations on variables",
            'name': 'variables',
            'subcommands': (
                {
                    'func': variables_list,
                    'name': 'list',
                    'help': 'List variables',
                    'args': (),
                },
                {
                    'func': variables_get,
                    'name': 'get',
                    'help': 'Get variable',
                    'args': ('var', 'json', 'default'),
                },
                {
                    'func': variables_set,
                    'name': 'set',
                    'help': 'Set variable',
                    'args': ('var', 'var_value', 'json'),
                },
                {
                    'func': variables_delete,
                    'name': 'delete',
                    'help': 'Delete variable',
                    'args': ('var',),
                },
                {
                    'func': variables_import,
                    'name': 'import',
                    'help': 'Import variables',
                    'args': ('var_import',),
                },
                {
                    'func': variables_export,
                    'name': 'export',
                    'help': 'Export variables',
                    'args': ('var_export',),
                },
            ),
            "args": ('set', 'get', 'json', 'default',
                     'var_import', 'var_export', 'var_delete'),
        }, {
            'help': "Database operations",
            'name': 'db',
            'subcommands': (
                {
                    'func': initdb,
                    'name': 'init',
                    'help': "Initialize the metadata database",
                    'args': (),
                },
                {
                    'func': resetdb,
                    'name': 'reset',
                    'help': "Burn down and rebuild the metadata database",
                    'args': ('yes',),
                },
                {
                    'func': upgradedb,
                    'name': 'upgrade',
                    'help': "Upgrade the metadata database to latest version",
                    'args': tuple(),
                },
            ),
        }, {
            'func': kerberos,
            'help': "Start a kerberos ticket renewer",
            'args': ('principal', 'keytab', 'pid',
                     'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': serve_logs,
            'help': "Serve logs generate by worker",
            'args': tuple(),
        }, {
            'func': webserver,
            'help': "Start a Airflow webserver instance",
            'args': ('port', 'workers', 'workerclass', 'worker_timeout', 'hostname',
                     'pid', 'daemon', 'stdout', 'stderr', 'access_logfile',
                     'error_logfile', 'log_file', 'ssl_cert', 'ssl_key', 'debug'),
        }, {
            'func': scheduler,
            'help': "Start a scheduler instance",
            'args': ('dag_id_opt', 'subdir', 'num_runs',
                     'do_pickle', 'pid', 'daemon', 'stdout', 'stderr',
                     'log_file'),
        }, {
            'func': worker,
            'help': "Start a Celery worker node",
            'args': ('do_pickle', 'queues', 'concurrency', 'celery_hostname',
                     'pid', 'daemon', 'stdout', 'stderr', 'log_file', 'autoscale'),
        }, {
            'func': flower,
            'help': "Start a Celery Flower",
            'args': ('flower_hostname', 'flower_port', 'flower_conf', 'flower_url_prefix',
                     'flower_basic_auth', 'broker_api', 'pid', 'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': version,
            'help': "Show the version",
            'args': tuple(),
        }, {
            'help': "List/Add/Delete connections",
            'name': 'connections',
            'subcommands': (
                {
                    'func': connections_list,
                    'name': 'list',
                    'help': 'List connections',
                    'args': ('output',),
                },
                {
                    'func': connections_add,
                    'name': 'add',
                    'help': 'Add a connection',
                    'args': ('conn_id', 'conn_uri', 'conn_extra') + tuple(alternative_conn_specs),
                },
                {
                    'func': connections_delete,
                    'name': 'delete',
                    'help': 'Delete a connection',
                    'args': ('conn_id',),
                },
            ),
        }, {
            'help': "List/Create/Delete/Update users",
            'name': 'users',
            'subcommands': (
                {
                    'func': user_command.users_list,
                    'name': 'list',
                    'help': 'List users',
                    'args': ('output',),
                },
                {
                    'func': user_command.users_create,
                    'name': 'create',
                    'help': 'Create a user',
                    'args': ('role', 'username', 'email', 'firstname', 'lastname', 'password',
                             'use_random_password')
                },
                {
                    'func': user_command.users_delete,
                    'name': 'delete',
                    'help': 'Delete a user',
                    'args': ('username',),
                },
                {
                    'func': user_command.add_role,
                    'name': 'add_role',
                    'help': 'Add role to a user',
                    'args': ('username_optional', 'email_optional', 'role'),
                },
                {
                    'func': user_command.remove_role,
                    'name': 'remove_role',
                    'help': 'Remove role from a user',
                    'args': ('username_optional', 'email_optional', 'role'),
                },
                {
                    'func': user_command.users_import,
                    'name': 'import',
                    'help': 'Import a user',
                    'args': ('user_import',),
                },
                {
                    'func': user_command.users_export,
                    'name': 'export',
                    'help': 'Export a user',
                    'args': ('user_export',),
                },
            ),
        }, {
            'help': 'Create/List roles',
            'name': 'roles',
            'subcommands': (
                {
                    'func': role_command.roles_list,
                    'name': 'list',
                    'help': 'List roles',
                    'args': ('output',),
                },
                {
                    'func': role_command.roles_create,
                    'name': 'create',
                    'help': 'Create role',
                    'args': ('roles',),
                },
            ),
        }, {
            'func': sync_perm_command.sync_perm,
            'help': "Update permissions for existing roles and DAGs.",
            'args': tuple(),
        },
        {
            'func': rotate_fernet_key_command.rotate_fernet_key,
            'help': 'Rotate all encrypted connection credentials and variables; see '
                    'https://airflow.readthedocs.io/en/stable/howto/secure-connections.html'
                    '#rotating-encryption-keys.',
            'args': (),
        },
    )
    subparsers_dict = {sp.get('name') or sp['func'].__name__: sp for sp in subparsers}
    dag_subparsers = (
        'list_tasks', 'backfill', 'test', 'run', 'pause', 'unpause', 'list_dag_runs')

    @classmethod
    def get_parser(cls, dag_parser=False):
        """Creates and returns command line argument parser"""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(
            help='sub-command help', dest='subcommand')
        subparsers.required = True

        subparser_list = cls.dag_subparsers if dag_parser else cls.subparsers_dict.keys()
        for sub in sorted(subparser_list):
            sub = cls.subparsers_dict[sub]
            cls._add_subcommand(subparsers, sub)
        return parser

    @classmethod
    def _add_subcommand(cls, subparsers, sub):
        dag_parser = False
        sub_proc = subparsers.add_parser(sub.get('name') or sub['func'].__name__, help=sub['help'])
        sub_proc.formatter_class = RawTextHelpFormatter

        subcommands = sub.get('subcommands', [])
        if subcommands:
            sub_subparsers = sub_proc.add_subparsers(dest='subcommand')
            sub_subparsers.required = True
            for command in subcommands:
                cls._add_subcommand(sub_subparsers, command)
        else:
            for arg in sub['args']:
                if 'dag_id' in arg and dag_parser:
                    continue
                arg = cls.args[arg]
                kwargs = {
                    f: v
                    for f, v in vars(arg).items() if f != 'flags' and v}
                sub_proc.add_argument(*arg.flags, **kwargs)
            sub_proc.set_defaults(func=sub['func'])


def get_parser():
    """Calls static method inside factory which creates argument parser"""
    return CLIFactory.get_parser()
