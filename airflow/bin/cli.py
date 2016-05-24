#!/usr/bin/env python
from __future__ import print_function
import logging
import os
import subprocess
import textwrap
import warnings
from datetime import datetime

import argparse
from builtins import input
from collections import namedtuple
from dateutil.parser import parse as parsedate
import json

import daemon
from daemon.pidfile import TimeoutPIDLockFile
import signal
import sys

import airflow
from airflow import jobs, settings
from airflow import configuration as conf
from airflow.executors import DEFAULT_EXECUTOR
from airflow.models import DagModel, DagBag, TaskInstance, DagPickle, DagRun, Variable
from airflow.utils import db as db_utils
from airflow.utils import logging as logging_utils
from airflow.utils.state import State
from airflow.exceptions import AirflowException

DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))


def sigint_handler(signal, frame):
    sys.exit(0)


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
    dags_folder = conf.get("core", "DAGS_FOLDER")
    dags_folder = os.path.expanduser(dags_folder)
    if subdir:
        if "DAGS_FOLDER" in subdir:
            subdir = subdir.replace("DAGS_FOLDER", dags_folder)
        subdir = os.path.abspath(os.path.expanduser(subdir))
        return subdir


def get_dag(args):
    dagbag = DagBag(process_subdir(args.subdir))
    if args.dag_id not in dagbag.dags:
        raise AirflowException(
            'dag_id could not be found: {}'.format(args.dag_id))
    return dagbag.dags[args.dag_id]


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
            ignore_dependencies=args.ignore_dependencies,
            ignore_first_depends_on_past=args.ignore_first_depends_on_past,
            pool=args.pool)


def trigger_dag(args):
    dag = get_dag(args)

    if not dag:
        logging.error("Cannot find dag {}".format(args.dag_id))
        sys.exit(1)

    execution_date = datetime.now()
    run_id = args.run_id or "manual__{0}".format(execution_date.isoformat())

    dr = DagRun.find(dag_id=args.dag_id, run_id=run_id)
    if dr:
        logging.error("This run_id {} already exists".format(run_id))
        raise AirflowException()

    run_conf = {}
    if args.conf:
        run_conf = json.loads(args.conf)

    trigger = dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.RUNNING,
        conf=run_conf,
        external_trigger=True
    )
    logging.info("Created {}".format(trigger))


def variables(args):
    if args.get:
        try:
            var = Variable.get(args.get,
                               deserialize_json=args.json,
                               default_var=args.default)
            print(var)
        except ValueError as e:
            print(e)
    if args.set:
        Variable.set(args.set[0], args.set[1])
    if not args.set and not args.get:
        # list all variables
        session = settings.Session()
        vars = session.query(Variable)
        msg = "\n".join(var.key for var in vars)
        print(msg)


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
    db_utils.pessimistic_connection_handling()
    if dag:
        args.dag_id = dag.dag_id

    # Setting up logging
    log_base = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
    directory = log_base + "/{args.dag_id}/{args.task_id}".format(args=args)
    if not os.path.exists(directory):
        os.makedirs(directory)
    iso = args.execution_date.isoformat()
    filename = "{directory}/{iso}".format(**locals())

    logging.root.handlers = []
    logging.basicConfig(
        filename=filename,
        level=settings.LOGGING_LEVEL,
        format=settings.LOG_FORMAT)

    if not args.pickle and not dag:
        dag = get_dag(args)
    elif not dag:
        session = settings.Session()
        logging.info('Loading pickle id {args.pickle}'.format(**locals()))
        dag_pickle = session.query(
            DagPickle).filter(DagPickle.id == args.pickle).first()
        if not dag_pickle:
            raise AirflowException("Who hid the pickle!? [missing pickle]")
        dag = dag_pickle.pickle
    task = dag.get_task(task_id=args.task_id)

    ti = TaskInstance(task, args.execution_date)

    if args.local:
        print("Logging into: " + filename)
        run_job = jobs.LocalTaskJob(
            task_instance=ti,
            mark_success=args.mark_success,
            force=args.force,
            pickle_id=args.pickle,
            ignore_dependencies=args.ignore_dependencies,
            ignore_depends_on_past=args.ignore_depends_on_past,
            pool=args.pool)
        run_job.run()
    elif args.raw:
        ti.run(
            mark_success=args.mark_success,
            force=args.force,
            ignore_dependencies=args.ignore_dependencies,
            ignore_depends_on_past=args.ignore_depends_on_past,
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
                print((
                    'Pickled dag {dag} '
                    'as pickle_id:{pickle_id}').format(**locals()))
            except Exception as e:
                print('Could not pickle the DAG')
                print(e)
                raise e

        executor = DEFAULT_EXECUTOR
        executor.start()
        print("Sending to executor.")
        executor.queue_task_instance(
            ti,
            mark_success=args.mark_success,
            pickle_id=pickle_id,
            ignore_dependencies=args.ignore_dependencies,
            ignore_depends_on_past=args.ignore_depends_on_past,
            force=args.force,
            pool=args.pool)
        executor.heartbeat()
        executor.end()

    # Force the log to flush, and set the handler to go back to normal so we
    # don't continue logging to the task's log file. The flush is important
    # because we subsequently read from the log to insert into S3 or Google
    # cloud storage.
    logging.root.handlers[0].flush()
    logging.root.handlers = []

    # store logs remotely
    remote_base = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')

    # deprecated as of March 2016
    if not remote_base and conf.get('core', 'S3_LOG_FOLDER'):
        warnings.warn(
            'The S3_LOG_FOLDER conf key has been replaced by '
            'REMOTE_BASE_LOG_FOLDER. Your conf still works but please '
            'update airflow.cfg to ensure future compatibility.',
            DeprecationWarning)
        remote_base = conf.get('core', 'S3_LOG_FOLDER')

    if os.path.exists(filename):
        # read log and remove old logs to get just the latest additions

        with open(filename, 'r') as logfile:
            log = logfile.read()

        remote_log_location = filename.replace(log_base, remote_base)
        # S3
        if remote_base.startswith('s3:/'):
            logging_utils.S3Log().write(log, remote_log_location)
        # GCS
        elif remote_base.startswith('gs:/'):
            logging_utils.GCSLog().write(
                log,
                remote_log_location,
                append=True)
        # Other
        elif remote_base and remote_base != 'None':
            logging.error(
                'Unsupported remote log location: {}'.format(remote_base))


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
        ti.run(force=True, ignore_dependencies=True, test_mode=True)


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
    dag = get_dag(args)

    if args.task_regex:
        dag = dag.sub_dag(
            task_regex=args.task_regex,
            include_downstream=args.downstream,
            include_upstream=args.upstream,
        )
    dag.clear(
        start_date=args.start_date,
        end_date=args.end_date,
        only_failed=args.only_failed,
        only_running=args.only_running,
        confirm_prompt=not args.no_confirm)


def webserver(args):
    print(settings.HEADER)

    from airflow.www.app import cached_app
    app = cached_app(conf)
    workers = args.workers or conf.get('webserver', 'workers')
    worker_timeout = (args.worker_timeout or
                      conf.get('webserver', 'webserver_worker_timeout'))
    if args.debug:
        print(
            "Starting the web server on port {0} and host {1}.".format(
                args.port, args.hostname))
        app.run(debug=True, port=args.port, host=args.hostname)
    else:
        pid, stdout, stderr, log_file = setup_locations("webserver", pid=args.pid)
        print(
            'Running the Gunicorn server with {workers} {args.workerclass}'
            'workers on host {args.hostname} and port '
            '{args.port} with a timeout of {worker_timeout}...'.format(**locals()))

        run_args = ['gunicorn',
                    '-w ' + str(args.workers),
                    '-k ' + str(args.workerclass),
                    '-t ' + str(args.worker_timeout),
                    '-b ' + args.hostname + ':' + str(args.port),
                    '-n ' + 'airflow-webserver',
                    '-p ' + str(pid)]

        if args.daemon:
            run_args.append("-D")

        module = "airflow.www.app:cached_app()".encode()
        run_args.append(module)
        os.execvp(
            'gunicorn', run_args
        )


def scheduler(args):
    print(settings.HEADER)
    job = jobs.SchedulerJob(
        dag_id=args.dag_id,
        subdir=process_subdir(args.subdir),
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
    from airflow.executors.celery_executor import app as celery_app
    from celery.bin import worker

    worker = worker.worker(app=celery_app)
    options = {
        'optimization': 'fair',
        'O': 'fair',
        'queues': args.queues,
        'concurrency': args.concurrency,
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
            sp = subprocess.Popen(['airflow', 'serve_logs'], env=env)
            worker.run(**options)
            sp.kill()

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        sp = subprocess.Popen(['airflow', 'serve_logs'], env=env)

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
        logging.basicConfig(level=settings.LOGGING_LEVEL,
                            format=settings.SIMPLE_LOG_FORMAT)
        db_utils.resetdb()
    else:
        print("Bail.")


def upgradedb(args):  # noqa
    print("DB: " + repr(settings.engine.url))
    db_utils.upgradedb()


def version(args):  # noqa
    print(settings.HEADER + "  v" + airflow.__version__)


def flower(args):
    broka = conf.get('celery', 'BROKER_URL')
    port = '--port={}'.format(args.port)
    api = ''
    if args.broker_api:
        api = '--broker_api=' + args.broker_api

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
            os.execvp("flower", ['flower', '-b', broka, port, api])

        stdout.close()
        stderr.close()
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)

        os.execvp("flower", ['flower', '-b', broka, port, api])


def kerberos(args):  # noqa
    print(settings.HEADER)
    import airflow.security.kerberos

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
            airflow.security.kerberos.run()

        stdout.close()
        stderr.close()
    else:
        airflow.security.kerberos.run()


Arg = namedtuple(
    'Arg', ['flags', 'help', 'action', 'default', 'nargs', 'type', 'choices', 'metavar'])
Arg.__new__.__defaults__ = (None, None, None, None, None, None, None)


class CLIFactory(object):
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
            "File location or directory from which to look for the dag",
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
            ("--pid", ), "PID file location",
            nargs='?'),
        'daemon': Arg(
            ("-D", "--daemon"), "Daemonize instead of running "
                                "in the foreground",
            "store_true"),
        'stderr': Arg(
            ("--stderr", ), "Redirect stderr to this file"),
        'stdout': Arg(
            ("--stdout", ), "Redirect stdout to this file"),
        'log_file': Arg(
            ("-l", "--log-file"), "Location of the log file"),

        # backfill
        'mark_success': Arg(
            ("-m", "--mark_success"),
            "Mark jobs as succeeded without running them", "store_true"),
        'local': Arg(
            ("-l", "--local"),
            "Run the task using the LocalExecutor", "store_true"),
        'donot_pickle': Arg(
            ("-x", "--donot_pickle"), (
                "Do not attempt to pickle the DAG object to send over "
                "to the workers, just tell the workers to run their version "
                "of the code."),
            "store_true"),
        'include_adhoc': Arg(
            ("-a", "--include_adhoc"),
            "Include dags with the adhoc parameter.", "store_true"),
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
        'no_confirm': Arg(
            ("-c", "--no_confirm"),
            "Do not request confirmation", "store_true"),
        # trigger_dag
        'run_id': Arg(("-r", "--run_id"), "Helps to identify this run"),
        'conf': Arg(
            ('-c', '--conf'),
            "JSON string that gets pickled into the DagRun's conf attribute"),
        # variables
        'set': Arg(
            ("-s", "--set"),
            nargs=2,
            metavar=('KEY', 'VAL'),
            help="Set a variable"),
        'get': Arg(
            ("-g", "--get"),
            metavar='KEY',
            help="Get value of a variable"),
        'default': Arg(
            ("-d", "--default"),
            metavar="VAL",
            default=None,
            help="Default value returned if variable does not exist"),
        'json': Arg(
            ("-j", "--json"),
            help="Deserialize JSON variable",
            action="store_true"),
        # kerberos
        'principal': Arg(
            ("principal",), "kerberos principal",
            nargs='?', default=conf.get('kerberos', 'principal')),
        'keytab': Arg(
            ("-kt", "--keytab"), "keytab",
            nargs='?', default=conf.get('kerberos', 'keytab')),
        # run
        'force': Arg(
            ("-f", "--force"),
            "Force a run regardless of previous success", "store_true"),
        'raw': Arg(("-r", "--raw"), argparse.SUPPRESS, "store_true"),
        'ignore_dependencies': Arg(
            ("-i", "--ignore_dependencies"),
            "Ignore upstream and depends_on_past dependencies", "store_true"),
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
        # webserver
        'port': Arg(
            ("-p", "--port"),
            default=conf.get('webserver', 'WEB_SERVER_PORT'),
            type=int,
            help="The port on which to run the server"),
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
        # resetdb
        'yes': Arg(
            ("-y", "--yes"),
            "Do not prompt to confirm reset. Use with care!",
            "store_true",
            default=False),
        # scheduler
        'dag_id_opt': Arg(("-d", "--dag_id"), help="The id of the dag to run"),
        'num_runs': Arg(
            ("-n", "--num_runs"),
            default=None, type=int,
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
            default=conf.get('celery', 'celeryd_concurrency')),
        # flower
        'broker_api': Arg(("-a", "--broker_api"), help="Broker api"),
        'flower_port': Arg(
            ("-p", "--port"),
            default=conf.get('celery', 'FLOWER_PORT'),
            type=int,
            help="The port on which to run the server"),
        'task_params': Arg(
            ("-tp", "--task_params"),
            help="Sends a JSON params dict to the task"),
    }
    subparsers = (
        {
            'func': backfill,
            'help': "Run subsections of a DAG for a specified date range",
            'args': (
                'dag_id', 'task_regex', 'start_date', 'end_date',
                'mark_success', 'local', 'donot_pickle', 'include_adhoc',
                'bf_ignore_dependencies', 'bf_ignore_first_depends_on_past',
                'subdir', 'pool', 'dry_run')
        }, {
            'func': list_tasks,
            'help': "List the tasks within a DAG",
            'args': ('dag_id', 'tree', 'subdir'),
        }, {
            'func': clear,
            'help': "Clear a set of task instance, as if they never ran",
            'args': (
                'dag_id', 'task_regex', 'start_date', 'end_date', 'subdir',
                'upstream', 'downstream', 'no_confirm', 'only_failed',
                'only_running'),
        }, {
            'func': pause,
            'help': "Pause a DAG",
            'args': ('dag_id', 'subdir'),
        }, {
            'func': unpause,
            'help': "Resume a paused DAG",
            'args': ('dag_id', 'subdir'),
        }, {
            'func': trigger_dag,
            'help': "Trigger a DAG run",
            'args': ('dag_id', 'subdir', 'run_id', 'conf'),
        }, {
            'func': variables,
            'help': "List all variables",
            "args": ('set', 'get', 'json', 'default'),
        }, {
            'func': kerberos,
            'help': "Start a kerberos ticket renewer",
            'args': ('principal', 'keytab', 'pid',
                     'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': render,
            'help': "Render a task instance's template(s)",
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
        }, {
            'func': run,
            'help': "Run a single task instance",
            'args': (
                'dag_id', 'task_id', 'execution_date', 'subdir',
                'mark_success', 'force', 'pool',
                'local', 'raw', 'ignore_dependencies',
                'ignore_depends_on_past', 'ship_dag', 'pickle', 'job_id'),
        }, {
            'func': initdb,
            'help': "Initialize the metadata database",
            'args': tuple(),
        }, {
            'func': list_dags,
            'help': "List all the DAGs",
            'args': ('subdir', 'report'),
        }, {
            'func': dag_state,
            'help': "Get the status of a dag run",
            'args': ('dag_id', 'execution_date', 'subdir'),
        }, {
            'func': task_state,
            'help': "Get the status of a task instance",
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
        }, {
            'func': serve_logs,
            'help': "Serve logs generate by worker",
            'args': tuple(),
        }, {
            'func': test,
            'help': (
                "Test a task instance. This will run a task without checking for "
                "dependencies or recording it's state in the database."),
            'args': (
                'dag_id', 'task_id', 'execution_date', 'subdir', 'dry_run',
                'task_params'),
        }, {
            'func': webserver,
            'help': "Start a Airflow webserver instance",
            'args': ('port', 'workers', 'workerclass', 'worker_timeout', 'hostname',
                     'pid', 'daemon', 'stdout', 'stderr', 'log_file',
                     'debug'),
        }, {
            'func': resetdb,
            'help': "Burn down and rebuild the metadata database",
            'args': ('yes',),
        }, {
            'func': upgradedb,
            'help': "Upgrade the metadata database to latest version",
            'args': tuple(),
        }, {
            'func': scheduler,
            'help': "Start a scheduler instance",
            'args': ('dag_id_opt', 'subdir', 'num_runs', 'do_pickle',
                     'pid', 'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': worker,
            'help': "Start a Celery worker node",
            'args': ('do_pickle', 'queues', 'concurrency',
                     'pid', 'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': flower,
            'help': "Start a Celery Flower",
            'args': ('flower_port', 'broker_api',
                     'pid', 'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': version,
            'help': "Show the version",
            'args': tuple(),
        },
    )
    subparsers_dict = {sp['func'].__name__: sp for sp in subparsers}
    dag_subparsers = (
        'list_tasks', 'backfill', 'test', 'run', 'pause', 'unpause')

    @classmethod
    def get_parser(cls, dag_parser=False):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(
            help='sub-command help', dest='subcommand')
        subparsers.required = True

        subparser_list = cls.dag_subparsers if dag_parser else cls.subparsers_dict.keys()
        for sub in subparser_list:
            sub = cls.subparsers_dict[sub]
            sp = subparsers.add_parser(sub['func'].__name__, help=sub['help'])
            for arg in sub['args']:
                if 'dag_id' in arg and dag_parser:
                    continue
                arg = cls.args[arg]
                kwargs = {
                    f: getattr(arg, f)
                    for f in arg._fields if f != 'flags' and getattr(arg, f)}
                sp.add_argument(*arg.flags, **kwargs)
            sp.set_defaults(func=sub['func'])
        return parser

def get_parser():
    return CLIFactory.get_parser()
