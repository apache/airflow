from collections import namedtuple
from cli import *
from dateutil.parser import parse as parsedate


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
            default=settings.DAGS_FOLDER),
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
        'exclude_subdags': Arg(
            ("-x", "--exclude_subdags"),
            "Exclude subdags", "store_true"),
        # trigger_dag
        'run_id': Arg(("-r", "--run_id"), "Helps to identify this run"),
        'conf': Arg(
            ('-c', '--conf'),
            "JSON string that gets pickled into the DagRun's conf attribute"),
        'exec_date': Arg(
            ("-e", "--exec_date"), help="The execution date of the DAG",
            type=parsedate),
        # pool
        'pool_set': Arg(
            ("-s", "--set"),
            nargs=3,
            metavar=('NAME', 'SLOT_COUNT', 'POOL_DESCRIPTION'),
            help="Set pool slot count and description, respectively"),
        'pool_get': Arg(
            ("-g", "--get"),
            metavar='NAME',
            help="Get pool info"),
        'pool_delete': Arg(
            ("-x", "--delete"),
            metavar="NAME",
            help="Delete a pool"),
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
        'var_import': Arg(
            ("-i", "--import"),
            metavar="FILEPATH",
            help="Import variables from JSON file"),
        'var_export': Arg(
            ("-e", "--export"),
            metavar="FILEPATH",
            help="Export variables to JSON file"),
        'var_delete': Arg(
            ("-x", "--delete"),
            metavar="KEY",
            help="Delete a variable"),
        # kerberos
        'principal': Arg(
            ("principal",), "kerberos principal",
            nargs='?', default=conf.get('kerberos', 'principal')),
        'keytab': Arg(
            ("-kt", "--keytab"), "keytab",
            nargs='?', default=conf.get('kerberos', 'keytab')),
        # run
        # TODO(aoen): "force" is a poor choice of name here since it implies it overrides
        # all dependencies (not just past success), e.g. the ignore_depends_on_past
        # dependency. This flag should be deprecated and renamed to 'ignore_ti_state' and
        # the "ignore_all_dependencies" command should be called the"force" command
        # instead.
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
        'kubernetes_mode': Arg(
            ("-km", "--kubernetes_mode"),
            "Pod will not contact postgres instance. should only be used for kubernetes tasks",
            "store_true"),
        'pickle': Arg(
            ("-p", "--pickle"),
            "Serialized pickle object of the entire dag (used internally)"),
        'job_id': Arg(("-j", "--job_id"), argparse.SUPPRESS),
        'cfg_path': Arg(
            ("--cfg_path", ), "Path to config file to use instead of airflow.cfg"),
        # webserver
        'port': Arg(
            ("-p", "--port"),
            default=conf.get('webserver', 'WEB_SERVER_PORT'),
            type=int,
            help="The port on which to run the server"),
        'ssl_cert': Arg(
            ("--ssl_cert", ),
            default=conf.get('webserver', 'WEB_SERVER_SSL_CERT'),
            help="Path to the SSL certificate for the webserver"),
        'ssl_key': Arg(
            ("--ssl_key", ),
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
        # resetdb
        'yes': Arg(
            ("-y", "--yes"),
            "Do not prompt to confirm reset. Use with care!",
            "store_true",
            default=False),
        # scheduler
        'dag_id_opt': Arg(("-d", "--dag_id"), help="The id of the dag to run"),
        'run_duration': Arg(
            ("-r", "--run-duration"),
            default=None, type=int,
            help="Set number of seconds to execute before exiting"),
        'num_runs': Arg(
            ("-n", "--num_runs"),
            default=-1, type=int,
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
        'task_params': Arg(
            ("-tp", "--task_params"),
            help="Sends a JSON params dict to the task"),
        # connections
        'list_connections': Arg(
            ('-l', '--list'),
            help='List all connections',
            action='store_true'),
        'add_connection': Arg(
            ('-a', '--add'),
            help='Add a connection',
            action='store_true'),
        'delete_connection': Arg(
            ('-d', '--delete'),
            help='Delete a connection',
            action='store_true'),
        'conn_id': Arg(
            ('--conn_id',),
            help='Connection id, required to add/delete a connection',
            type=str),
        'conn_uri': Arg(
            ('--conn_uri',),
            help='Connection URI, required to add a connection',
            type=str),
        'conn_extra': Arg(
            ('--conn_extra',),
            help='Connection `Extra` field, optional when adding a connection',
            type=str),
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
                'only_running', 'exclude_subdags'),
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
            'args': ('dag_id', 'subdir', 'run_id', 'conf', 'exec_date'),
        }, {
            'func': pool,
            'help': "CRUD operations on pools",
            "args": ('pool_set', 'pool_get', 'pool_delete'),
        }, {
            'func': variables,
            'help': "CRUD operations on variables",
            "args": ('set', 'get', 'json', 'default', 'var_import', 'var_export', 'var_delete'),
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
                'mark_success', 'force', 'pool', 'cfg_path',
                'local', 'raw', 'ignore_all_dependencies', 'ignore_dependencies',
                'ignore_depends_on_past', 'ship_dag', 'pickle', 'job_id', 'kubernetes_mode'),
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
            'func': task_failed_deps,
            'help': (
                "Returns the unmet dependencies for a task instance from the perspective "
                "of the scheduler. In other words, why a task instance doesn't get "
                "scheduled and then queued by the scheduler, and then run by an "
                "executor)."),
            'args': ('dag_id', 'task_id', 'execution_date', 'subdir'),
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
                     'pid', 'daemon', 'stdout', 'stderr', 'access_logfile',
                     'error_logfile', 'log_file', 'ssl_cert', 'ssl_key', 'debug'),
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
            'args': ('dag_id_opt', 'subdir', 'run_duration', 'num_runs',
                     'do_pickle', 'pid', 'daemon', 'stdout', 'stderr',
                     'log_file'),
        }, {
            'func': worker,
            'help': "Start a Celery worker node",
            'args': ('do_pickle', 'queues', 'concurrency',
                     'pid', 'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': flower,
            'help': "Start a Celery Flower",
            'args': ('flower_hostname', 'flower_port', 'flower_conf', 'broker_api',
                     'pid', 'daemon', 'stdout', 'stderr', 'log_file'),
        }, {
            'func': version,
            'help': "Show the version",
            'args': tuple(),
        }, {
            'func': connections,
            'help': "List/Add/Delete connections",
            'args': ('list_connections', 'add_connection', 'delete_connection',
                     'conn_id', 'conn_uri', 'conn_extra'),
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
