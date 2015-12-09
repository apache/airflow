#!/usr/bin/env python
from __future__ import print_function
import logging
import os
import subprocess
from datetime import datetime

from builtins import input
import argparse
import dateutil.parser

import airflow
from airflow import jobs, settings, utils
from airflow import configuration
from airflow.executors import DEFAULT_EXECUTOR
from airflow.models import DagBag, TaskInstance, DagPickle, DagRun
from airflow.utils import AirflowException, State

DAGS_FOLDER = os.path.expanduser(configuration.get('core', 'DAGS_FOLDER'))

# Common help text across subcommands
mark_success_help = "Mark jobs as succeeded without running them"
subdir_help = "File location or directory from which to look for the dag"


def process_subdir(subdir):
    dags_folder = configuration.get("core", "DAGS_FOLDER")
    dags_folder = os.path.expanduser(dags_folder)
    if subdir:
        subdir = os.path.expanduser(subdir)
        if "DAGS_FOLDER" in subdir:
            subdir = subdir.replace("DAGS_FOLDER", dags_folder)
        if dags_folder not in subdir:
            raise AirflowException(
                "subdir has to be part of your DAGS_FOLDER as defined in your "
                "airflow.cfg")
        return subdir


def backfill(args):
    logging.basicConfig(
        level=settings.LOGGING_LEVEL,
        format=settings.SIMPLE_LOG_FORMAT)
    dagbag = DagBag(process_subdir(args.subdir))
    if args.dag_id not in dagbag.dags:
        raise AirflowException('dag_id could not be found')
    dag = dagbag.dags[args.dag_id]

    if args.start_date:
        args.start_date = dateutil.parser.parse(args.start_date)
    if args.end_date:
        args.end_date = dateutil.parser.parse(args.end_date)

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
            donot_pickle=(args.donot_pickle or configuration.getboolean('core', 'donot_pickle')),
            ignore_dependencies=args.ignore_dependencies,
            pool=args.pool)


def trigger_dag(args):
    utils.log_to_stdout()
    session = settings.Session()
    # TODO: verify dag_id
    execution_date = datetime.now()
    dr = session.query(DagRun).filter(
        DagRun.dag_id==args.dag_id, DagRun.run_id==args.run_id).first()
    if dr:
        logging.error("This run_id already exists")
    else:
        trigger = DagRun(
            dag_id=args.dag_id,
            run_id=args.run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            external_trigger=True)
        session.add(trigger)
        logging.info("Created {}".format(trigger))
    session.commit()


def run(args):

    utils.pessimistic_connection_handling()
    # Setting up logging
    log = os.path.expanduser(configuration.get('core', 'BASE_LOG_FOLDER'))
    directory = log + "/{args.dag_id}/{args.task_id}".format(args=args)
    if not os.path.exists(directory):
        os.makedirs(directory)
    args.execution_date = dateutil.parser.parse(args.execution_date)
    iso = args.execution_date.isoformat()
    filename = "{directory}/{iso}".format(**locals())

    # store old log (to help with S3 appends)
    if os.path.exists(filename):
        with open(filename, 'r') as logfile:
            old_log = logfile.read()
    else:
        old_log = None

    subdir = process_subdir(args.subdir)
    logging.basicConfig(
        filename=filename,
        level=settings.LOGGING_LEVEL,
        format=settings.LOG_FORMAT)
    if not args.pickle:
        dagbag = DagBag(subdir)
        if args.dag_id not in dagbag.dags:
            msg = 'DAG [{0}] could not be found in {1}'.format(args.dag_id, subdir)
            logging.error(msg)
            raise AirflowException(msg)
        dag = dagbag.dags[args.dag_id]
        task = dag.get_task(task_id=args.task_id)
    else:
        session = settings.Session()
        logging.info('Loading pickle id {args.pickle}'.format(**locals()))
        dag_pickle = session.query(
            DagPickle).filter(DagPickle.id == args.pickle).first()
        if not dag_pickle:
            raise AirflowException("Who hid the pickle!? [missing pickle]")
        dag = dag_pickle.pickle
        task = dag.get_task(task_id=args.task_id)

    task_start_date = None
    if args.task_start_date:
        task_start_date = dateutil.parser.parse(args.task_start_date)
        task.start_date = task_start_date
    ti = TaskInstance(task, args.execution_date)

    if args.local:
        print("Logging into: " + filename)
        run_job = jobs.LocalTaskJob(
            task_instance=ti,
            mark_success=args.mark_success,
            force=args.force,
            pickle_id=args.pickle,
            task_start_date=task_start_date,
            ignore_dependencies=args.ignore_dependencies,
            pool=args.pool)
        run_job.run()
    elif args.raw:
        ti.run(
            mark_success=args.mark_success,
            force=args.force,
            ignore_dependencies=args.ignore_dependencies,
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
            force=args.force)
        executor.heartbeat()
        executor.end()

    if configuration.get('core', 'S3_LOG_FOLDER').startswith('s3:'):
        import boto
        s3_log = filename.replace(log, configuration.get('core', 'S3_LOG_FOLDER'))
        bucket, key = s3_log.lstrip('s3:/').split('/', 1)
        if os.path.exists(filename):

            # get logs
            with open(filename, 'r') as logfile:
                new_log = logfile.read()

            # remove old logs (since they are already in S3)
            if old_log:
                new_log.replace(old_log, '')

            try:
                s3 = boto.connect_s3()
                s3_key = boto.s3.key.Key(s3.get_bucket(bucket), key)

                # append new logs to old S3 logs, if available
                if s3_key.exists():
                    old_s3_log = s3_key.get_contents_as_string().decode()
                    new_log = old_s3_log + '\n' + new_log

                # send log to S3
                s3_key.set_contents_from_string(new_log)
            except:
                print('Could not send logs to S3.')


def task_state(args):
    """
    Returns the state of a TaskInstance at the command line.

    >>> airflow task_state tutorial sleep 2015-01-01
    success
    """
    args.execution_date = dateutil.parser.parse(args.execution_date)
    dagbag = DagBag(process_subdir(args.subdir))
    if args.dag_id not in dagbag.dags:
        raise AirflowException('dag_id could not be found')
    dag = dagbag.dags[args.dag_id]
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)
    print(ti.current_state())


def list_dags(args):
    dagbag = DagBag(process_subdir(args.subdir))
    print("\n".join(sorted(dagbag.dags)))


def list_tasks(args):
    dagbag = DagBag(process_subdir(args.subdir))
    if args.dag_id not in dagbag.dags:
        raise AirflowException('dag_id could not be found')
    dag = dagbag.dags[args.dag_id]
    if args.tree:
        dag.tree_view()
    else:
        tasks = sorted([t.task_id for t in dag.tasks])
        print("\n".join(sorted(tasks)))


def test(args):
    utils.log_to_stdout()
    args.execution_date = dateutil.parser.parse(args.execution_date)
    dagbag = DagBag(process_subdir(args.subdir))
    if args.dag_id not in dagbag.dags:
        raise AirflowException('dag_id could not be found')
    dag = dagbag.dags[args.dag_id]
    task = dag.get_task(task_id=args.task_id)
    ti = TaskInstance(task, args.execution_date)

    if args.dry_run:
        ti.dry_run()
    else:
        ti.run(force=True, ignore_dependencies=True, test_mode=True)


def clear(args):
    logging.basicConfig(
        level=settings.LOGGING_LEVEL,
        format=settings.SIMPLE_LOG_FORMAT)
    dagbag = DagBag(process_subdir(args.subdir))

    if args.dag_id not in dagbag.dags:
        raise AirflowException('dag_id could not be found')
    dag = dagbag.dags[args.dag_id]

    if args.start_date:
        args.start_date = dateutil.parser.parse(args.start_date)
    if args.end_date:
        args.end_date = dateutil.parser.parse(args.end_date)

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
    utils.log_to_stdout()
    from airflow.www.app import cached_app
    app = cached_app(configuration)
    workers = args.workers or configuration.get('webserver', 'workers')
    if args.debug:
        print(
            "Starting the web server on port {0} and host {1}.".format(
                args.port, args.hostname))
        app.run(debug=True, port=args.port, host=args.hostname)
    else:
        print(
            'Running the Gunicorn server with {workers} {args.workerclass}'
            'workers on host {args.hostname} and port '
            '{args.port}...'.format(**locals()))
        sp = subprocess.Popen([
            'gunicorn', '-w', str(args.workers), '-k', str(args.workerclass),
            '-t', '120', '-b', args.hostname + ':' + str(args.port),
            'airflow.www.app:cached_app()'])
        sp.wait()


def scheduler(args):
    print(settings.HEADER)
    utils.log_to_stdout()
    job = jobs.SchedulerJob(
        dag_id=args.dag_id,
        subdir=process_subdir(args.subdir),
        num_runs=args.num_runs,
        do_pickle=args.do_pickle)
    job.run()


def serve_logs(args):
    print("Starting flask")
    import flask
    flask_app = flask.Flask(__name__)

    @flask_app.route('/log/<path:filename>')
    def serve_logs(filename):
        log = os.path.expanduser(configuration.get('core', 'BASE_LOG_FOLDER'))
        return flask.send_from_directory(
            log,
            filename,
            mimetype="application/json",
            as_attachment=False)
    WORKER_LOG_SERVER_PORT = \
        int(configuration.get('celery', 'WORKER_LOG_SERVER_PORT'))
    flask_app.run(
        host='0.0.0.0', port=WORKER_LOG_SERVER_PORT)


def worker(args):
    # Worker to serve static log files through this simple flask app
    env = os.environ.copy()
    env['AIRFLOW_HOME'] = settings.AIRFLOW_HOME
    sp = subprocess.Popen(['airflow', 'serve_logs'], env=env)

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
    worker.run(**options)
    sp.kill()


def initdb(args):
    print("DB: " + configuration.get('core', 'SQL_ALCHEMY_CONN'))
    utils.initdb()
    print("Done.")


def resetdb(args):
    print("DB: " + configuration.get('core', 'SQL_ALCHEMY_CONN'))
    if args.yes or input(
            "This will drop existing tables if they exist. "
            "Proceed? (y/n)").upper() == "Y":
        logging.basicConfig(level=settings.LOGGING_LEVEL,
                            format=settings.SIMPLE_LOG_FORMAT)
        utils.resetdb()
    else:
        print("Bail.")


def upgradedb(args):
    print("DB: " + configuration.get('core', 'SQL_ALCHEMY_CONN'))
    utils.upgradedb()


def version(args):
    print(settings.HEADER + "  v" + airflow.__version__)


def flower(args):
    broka = configuration.get('celery', 'BROKER_URL')
    args.port = args.port or configuration.get('celery', 'FLOWER_PORT')
    port = '--port=' + args.port
    api = ''
    if args.broker_api:
        api = '--broker_api=' + args.broker_api
    sp = subprocess.Popen(['flower', '-b', broka, port, api])
    sp.wait()


def kerberos(args):
    print(settings.HEADER)
    utils.log_to_stdout()
    import airflow.security.kerberos
    airflow.security.kerberos.run()


def get_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help')

    ht = "Run subsections of a DAG for a specified date range"
    parser_backfill = subparsers.add_parser('backfill', help=ht)
    parser_backfill.add_argument("dag_id", help="The id of the dag to run")
    parser_backfill.add_argument(
        "-t", "--task_regex",
        help="The regex to filter specific task_ids to backfill (optional)")
    parser_backfill.add_argument(
        "-s", "--start_date", help="Override start_date YYYY-MM-DD")
    parser_backfill.add_argument(
        "-e", "--end_date", help="Override end_date YYYY-MM-DD")
    parser_backfill.add_argument(
        "-m", "--mark_success",
        help=mark_success_help, action="store_true")
    parser_backfill.add_argument(
        "-l", "--local",
        help="Run the task using the LocalExecutor", action="store_true")
    parser_backfill.add_argument(
        "-x", "--donot_pickle",
        help=(
            "Do not attempt to pickle the DAG object to send over "
            "to the workers, just tell the workers to run their version "
            "of the code."),
        action="store_true")
    parser_backfill.add_argument(
        "-a", "--include_adhoc",
        help="Include dags with the adhoc parameter.", action="store_true")
    parser_backfill.add_argument(
        "-i", "--ignore_dependencies",
        help=(
            "Skip upstream tasks, run only the tasks "
            "matching the regexp. Only works in conjunction with task_regex"),
        action="store_true")
    parser_backfill.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=DAGS_FOLDER)
    parser_backfill.add_argument(
        "-p", "--pool", help="Pool to use to run the backfill")
    parser_backfill.add_argument(
        "-dr", "--dry_run", help="Perform a dry run", action="store_true")
    parser_backfill.set_defaults(func=backfill)

    ht = "Clear a set of task instance, as if they never ran"
    parser_clear = subparsers.add_parser('clear', help=ht)
    parser_clear.add_argument("dag_id", help="The id of the dag to run")
    parser_clear.add_argument(
        "-t", "--task_regex",
        help="The regex to filter specific task_ids to clear (optional)")
    parser_clear.add_argument(
        "-s", "--start_date", help="Override start_date YYYY-MM-DD")
    parser_clear.add_argument(
        "-e", "--end_date", help="Override end_date YYYY-MM-DD")
    ht = "Include upstream tasks"
    parser_clear.add_argument(
        "-u", "--upstream", help=ht, action="store_true")
    ht = "Only failed jobs"
    parser_clear.add_argument(
        "-f", "--only_failed", help=ht, action="store_true")
    ht = "Only running jobs"
    parser_clear.add_argument(
        "-r", "--only_running", help=ht, action="store_true")
    ht = "Include downstream tasks"
    parser_clear.add_argument(
        "-d", "--downstream", help=ht, action="store_true")
    parser_clear.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=DAGS_FOLDER)
    parser_clear.add_argument(
        "-c", "--no_confirm", help=ht, action="store_true")
    parser_clear.set_defaults(func=clear)

    ht = "Trigger a DAG"
    parser_trigger_dag = subparsers.add_parser('trigger_dag', help=ht)
    parser_trigger_dag.add_argument("dag_id", help="The id of the dag to run")
    parser_trigger_dag.add_argument(
        "-r", "--run_id",
        help="Helps to indentify this run")
    parser_trigger_dag.set_defaults(func=trigger_dag)

    ht = "Run a single task instance"
    parser_run = subparsers.add_parser('run', help=ht)
    parser_run.add_argument("dag_id", help="The id of the dag to run")
    parser_run.add_argument("task_id", help="The task_id to run")
    parser_run.add_argument(
        "execution_date", help="The execution date to run")
    parser_run.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=DAGS_FOLDER)
    parser_run.add_argument(
        "-s", "--task_start_date",
        help="Override the tasks's start_date (used internally)",)
    parser_run.add_argument(
        "-m", "--mark_success", help=mark_success_help, action="store_true")
    parser_run.add_argument(
        "-f", "--force",
        help="Force a run regardless or previous success",
        action="store_true")
    parser_run.add_argument(
        "-l", "--local",
        help="Runs the task locally, don't use the executor",
        action="store_true")
    parser_run.add_argument(
        "-r", "--raw",
        help=argparse.SUPPRESS,
        action="store_true")
    parser_run.add_argument(
        "--pool", help="Pool to use to run the task instance")
    parser_run.add_argument(
        "-i", "--ignore_dependencies",
        help="Ignore upstream and depends_on_past dependencies",
        action="store_true")
    parser_run.add_argument(
        "--ship_dag",
        help="Pickles (serializes) the DAG and ships it to the worker",
        action="store_true")
    parser_run.add_argument(
        "-p", "--pickle",
        help="Serialized pickle object of the entire dag (used internally)")
    parser_run.add_argument(
        "-j", "--job_id", help=argparse.SUPPRESS)
    parser_run.set_defaults(func=run)

    ht = (
        "Test a task instance. This will run a task without checking for "
        "dependencies or recording it's state in the database."
    )
    parser_test = subparsers.add_parser('test', help=ht)
    parser_test.add_argument("dag_id", help="The id of the dag to run")
    parser_test.add_argument("task_id", help="The task_id to run")
    parser_test.add_argument(
        "execution_date", help="The execution date to run")
    parser_test.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=DAGS_FOLDER)
    parser_test.add_argument(
        "-dr", "--dry_run", help="Perform a dry run", action="store_true")
    parser_test.set_defaults(func=test)

    ht = "Get the status of a task instance."
    parser_task_state = subparsers.add_parser('task_state', help=ht)
    parser_task_state.add_argument("dag_id", help="The id of the dag to check")
    parser_task_state.add_argument("task_id", help="The task_id to check")
    parser_task_state.add_argument(
        "execution_date", help="The execution date to check")
    parser_task_state.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=DAGS_FOLDER)
    parser_task_state.set_defaults(func=task_state)

    ht = "Start a Airflow webserver instance"
    parser_webserver = subparsers.add_parser('webserver', help=ht)
    parser_webserver.add_argument(
        "-p", "--port",
        default=configuration.get('webserver', 'WEB_SERVER_PORT'),
        type=int,
        help="Set the port on which to run the web server")
    parser_webserver.add_argument(
        "-w", "--workers",
        default=configuration.get('webserver', 'WORKERS'),
        type=int,
        help="Number of workers to run the webserver on")
    parser_webserver.add_argument(
        "-k", "--workerclass",
        default=configuration.get('webserver', 'WORKER_CLASS'),
        choices=['sync', 'eventlet', 'gevent', 'tornado'],
        help="The worker class to use for gunicorn")
    parser_webserver.add_argument(
        "-hn", "--hostname",
        default=configuration.get('webserver', 'WEB_SERVER_HOST'),
        help="Set the hostname on which to run the web server")
    ht = "Use the server that ships with Flask in debug mode"
    parser_webserver.add_argument(
        "-d", "--debug", help=ht, action="store_true")
    parser_webserver.set_defaults(func=webserver)

    ht = "Start a scheduler scheduler instance"
    parser_scheduler = subparsers.add_parser('scheduler', help=ht)
    parser_scheduler.add_argument(
        "-d", "--dag_id", help="The id of the dag to run")
    parser_scheduler.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=DAGS_FOLDER)
    parser_scheduler.add_argument(
        "-n", "--num_runs",
        default=None,
        type=int,
        help="Set the number of runs to execute before exiting")
    parser_scheduler.add_argument(
        "-p", "--do_pickle",
        default=False,
        help=(
            "Attempt to pickle the DAG object to send over "
            "to the workers, instead of letting workers run their version "
            "of the code."),
        action="store_true")
    parser_scheduler.set_defaults(func=scheduler)

    ht = "Initialize the metadata database"
    parser_initdb = subparsers.add_parser('initdb', help=ht)
    parser_initdb.set_defaults(func=initdb)

    ht = "Burn down and rebuild the metadata database"
    parser_resetdb = subparsers.add_parser('resetdb', help=ht)
    parser_resetdb.add_argument(
            "-y", "--yes",
            default=False,
            help="Do not prompt to confirm reset. Use with care!",
            action="store_true")
    parser_resetdb.set_defaults(func=resetdb)

    ht = "Upgrade metadata database to latest version"
    parser_upgradedb = subparsers.add_parser('upgradedb', help=ht)
    parser_upgradedb.set_defaults(func=upgradedb)

    ht = "List the DAGs"
    parser_list_dags = subparsers.add_parser('list_dags', help=ht)
    parser_list_dags.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=DAGS_FOLDER)
    parser_list_dags.set_defaults(func=list_dags)

    ht = "List the tasks within a DAG"
    parser_list_tasks = subparsers.add_parser('list_tasks', help=ht)
    parser_list_tasks.add_argument(
        "-t", "--tree", help="Tree view", action="store_true")
    parser_list_tasks.add_argument(
        "dag_id", help="The id of the dag")
    parser_list_tasks.add_argument(
        "-sd", "--subdir", help=subdir_help,
        default=DAGS_FOLDER)
    parser_list_tasks.set_defaults(func=list_tasks)

    ht = "Start a Celery worker node"
    parser_worker = subparsers.add_parser('worker', help=ht)
    parser_worker.add_argument(
        "-q", "--queues",
        help="Comma delimited list of queues to serve",
        default=configuration.get('celery', 'DEFAULT_QUEUE'))
    parser_worker.add_argument(
        "-c", "--concurrency",
        type=int,
        help="The number of worker processes",
        default=configuration.get('celery', 'celeryd_concurrency'))
    parser_worker.set_defaults(func=worker)

    ht = "Serve logs generate by worker"
    parser_logs = subparsers.add_parser('serve_logs', help=ht)
    parser_logs.set_defaults(func=serve_logs)

    ht = "Start a Celery Flower"
    parser_flower = subparsers.add_parser('flower', help=ht)
    parser_flower.add_argument(
        "-p", "--port", help="The port")
    parser_flower.add_argument(
        "-a", "--broker_api", help="Broker api")
    parser_flower.set_defaults(func=flower)

    parser_version = subparsers.add_parser('version', help="Show version")
    parser_version.set_defaults(func=version)

    ht = "Start a kerberos ticket renewer"
    parser_kerberos = subparsers.add_parser('kerberos', help=ht)
    parser_kerberos.add_argument(
        "-kt", "--keytab", help="keytab",
        nargs='?', default=configuration.get('kerberos', 'keytab'))
    parser_kerberos.add_argument(
        "principal", help="kerberos principal",
        nargs='?', default=configuration.get('kerberos', 'principal'))
    parser_kerberos.set_defaults(func=kerberos)

    return parser
