from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import copy
import errno
import logging
import os
import subprocess
import warnings

from future import standard_library
standard_library.install_aliases()

from builtins import str
from collections import OrderedDict
from configparser import ConfigParser

# show Airflow's deprecation warnings
warnings.filterwarnings(
    action='default', category=DeprecationWarning, module='airflow')
warnings.filterwarnings(
    action='default', category=PendingDeprecationWarning, module='airflow')

class AirflowConfigException(Exception):
    pass

try:
    from cryptography.fernet import Fernet
except ImportError:
    pass


def generate_fernet_key():
    try:
        FERNET_KEY = Fernet.generate_key().decode()
    except NameError:
        FERNET_KEY = "cryptography_not_found_storing_passwords_in_plain_text"
    return FERNET_KEY


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def run_command(command):
    """
    Runs command and returns stdout
    """
    process = subprocess.Popen(
        command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, stderr = process.communicate()

    if process.returncode != 0:
        raise AirflowConfigException(
            "Cannot execute {}. Error code is: {}. Output: {}, Stderr: {}"
            .format(command, process.returncode, output, stderr)
        )

    return output


defaults = {
    'core': {
        'unit_test_mode': False,
        'parallelism': 32,
        'load_examples': True,
        'plugins_folder': None,
        'security': None,
        'donot_pickle': False,
        'remote_base_log_folder': '',
        'remote_log_conn_id': '',
        'encrypt_s3_logs': False,
        's3_log_folder': '', # deprecated!
        'dag_concurrency': 16,
        'max_active_runs_per_dag': 16,
        'executor': 'SequentialExecutor',
        'dags_are_paused_at_creation': True,
        'sql_alchemy_pool_size': 5,
        'sql_alchemy_pool_recycle': 3600,
        'dagbag_import_timeout': 30,
        'non_pooled_task_slot_count': 128,
    },
    'operators': {
        'default_owner': 'airflow'
    },
    'webserver': {
        'base_url': 'http://localhost:8080',
        'web_server_host': '0.0.0.0',
        'web_server_port': '8080',
        'web_server_worker_timeout': 120,
        'authenticate': False,
        'filter_by_owner': False,
        'demo_mode': False,
        'secret_key': 'airflowified',
        'expose_config': False,
        'workers': 4,
        'worker_class': 'sync',
    },
    'scheduler': {
        'statsd_on': False,
        'statsd_host': 'localhost',
        'statsd_port': 8125,
        'statsd_prefix': 'airflow',
        'job_heartbeat_sec': 5,
        'scheduler_heartbeat_sec': 60,
        'authenticate': False,
        'max_threads': 2,
    },
    'celery': {
        'default_queue': 'default',
        'flower_port': '5555'
    },
    'email': {
        'email_backend': 'airflow.utils.email.send_email_smtp',
    },
    'smtp': {
        'smtp_starttls': True,
        'smtp_ssl': False,
        'smtp_user': '',
        'smtp_password': '',
    },
    'kerberos': {
        'ccache': '/tmp/airflow_krb5_ccache',
        'principal': 'airflow',                 # gets augmented with fqdn
        'reinit_frequency': '3600',
        'kinit_path': 'kinit',
        'keytab': 'airflow.keytab',
    },
    'github_enterprise': {
        'api_rev': 'v3'
    }
}

DEFAULT_CONFIG = """\
[core]
# The home folder for airflow, default is ~/airflow
airflow_home = {AIRFLOW_HOME}

# The folder where your airflow pipelines live, most likely a
# subfolder in a code repository
dags_folder = {AIRFLOW_HOME}/dags

# The folder where airflow should store its log files. This location
base_log_folder = {AIRFLOW_HOME}/logs

# Airflow can store logs remotely in AWS S3 or Google Cloud Storage. Users
# must supply a remote location URL (starting with either 's3://...' or
# 'gs://...') and an Airflow connection id that provides access to the storage
# location.
remote_base_log_folder =
remote_log_conn_id =
# Use server-side encryption for logs stored in S3
encrypt_s3_logs = False
# deprecated option for remote log storage, use remote_base_log_folder instead!
# s3_log_folder =

# The executor class that airflow should use. Choices include
# SequentialExecutor, LocalExecutor, CeleryExecutor
executor = SequentialExecutor

# The SqlAlchemy connection string to the metadata database.
# SqlAlchemy supports many different database engine, more information
# their website
sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/airflow.db

# The SqlAlchemy pool size is the maximum number of database connections
# in the pool.
sql_alchemy_pool_size = 5

# The SqlAlchemy pool recycle is the number of seconds a connection
# can be idle in the pool before it is invalidated. This config does
# not apply to sqlite.
sql_alchemy_pool_recycle = 3600

# The amount of parallelism as a setting to the executor. This defines
# the max number of task instances that should run simultaneously
# on this airflow installation
parallelism = 32

# The number of task instances allowed to run concurrently by the scheduler
dag_concurrency = 16

# Are DAGs paused by default at creation
dags_are_paused_at_creation = True

# When not using pools, tasks are run in the "default pool",
# whose size is guided by this config element
non_pooled_task_slot_count = 128

# The maximum number of active DAG runs per DAG
max_active_runs_per_dag = 16

# Whether to load the examples that ship with Airflow. It's good to
# get started, but you probably want to set this to False in a production
# environment
load_examples = True

# Where your Airflow plugins are stored
plugins_folder = {AIRFLOW_HOME}/plugins

# Secret key to save connection passwords in the db
fernet_key = {FERNET_KEY}

# Whether to disable pickling dags
donot_pickle = False

# How long before timing out a python file import while filling the DagBag
dagbag_import_timeout = 30


[operators]
# The default owner assigned to each new operator, unless
# provided explicitly or passed via `default_args`
default_owner = Airflow


[webserver]
# The base url of your website as airflow cannot guess what domain or
# cname you are using. This is used in automated emails that
# airflow sends to point links to the right web server
base_url = http://localhost:8080

# The ip specified when starting the web server
web_server_host = 0.0.0.0

# The port on which to run the web server
web_server_port = 8080

# The time the gunicorn webserver waits before timing out on a worker
web_server_worker_timeout = 120

# Secret key used to run your flask app
secret_key = temporary_key

# Number of workers to run the Gunicorn web server
workers = 4

# The worker class gunicorn should use. Choices include
# sync (default), eventlet, gevent
worker_class = sync

# Expose the configuration file in the web server
expose_config = true

# Set to true to turn on authentication:
# http://pythonhosted.org/airflow/installation.html#web-authentication
authenticate = False

# Filter the list of dags by owner name (requires authentication to be enabled)
filter_by_owner = False

[email]
email_backend = airflow.utils.email.send_email_smtp

[smtp]
# If you want airflow to send emails on retries, failure, and you want to use
# the airflow.utils.email.send_email_smtp function, you have to configure an smtp
# server here
smtp_host = localhost
smtp_starttls = True
smtp_ssl = False
smtp_user = airflow
smtp_port = 25
smtp_password = airflow
smtp_mail_from = airflow@airflow.com

[celery]
# This section only applies if you are using the CeleryExecutor in
# [core] section above

# The app name that will be used by celery
celery_app_name = airflow.executors.celery_executor

# The concurrency that will be used when starting workers with the
# "airflow worker" command. This defines the number of task instances that
# a worker will take, so size up your workers based on the resources on
# your worker box and the nature of your tasks
celeryd_concurrency = 16

# When you start an airflow worker, airflow starts a tiny web server
# subprocess to serve the workers local log files to the airflow main
# web server, who then builds pages and sends them to users. This defines
# the port on which the logs are served. It needs to be unused, and open
# visible from the main web server to connect into the workers.
worker_log_server_port = 8793

# The Celery broker URL. Celery supports RabbitMQ, Redis and experimentally
# a sqlalchemy database. Refer to the Celery documentation for more
# information.
broker_url = sqla+mysql://airflow:airflow@localhost:3306/airflow

# Another key Celery setting
celery_result_backend = db+mysql://airflow:airflow@localhost:3306/airflow

# Celery Flower is a sweet UI for Celery. Airflow has a shortcut to start
# it `airflow flower`. This defines the port that Celery Flower runs on
flower_port = 5555

# Default queue that tasks get assigned to and that worker listen on.
default_queue = default

[scheduler]
# Task instances listen for external kill signal (when you clear tasks
# from the CLI or the UI), this defines the frequency at which they should
# listen (in seconds).
job_heartbeat_sec = 5

# The scheduler constantly tries to trigger new tasks (look at the
# scheduler section in the docs for more information). This defines
# how often the scheduler should run (in seconds).
scheduler_heartbeat_sec = 5

# Statsd (https://github.com/etsy/statsd) integration settings
# statsd_on =  False
# statsd_host =  localhost
# statsd_port =  8125
# statsd_prefix = airflow

# The scheduler can run multiple threads in parallel to schedule dags.
# This defines how many threads will run. However airflow will never
# use more threads than the amount of cpu cores available.
max_threads = 2

[mesos]
# Mesos master address which MesosExecutor will connect to.
master = localhost:5050

# The framework name which Airflow scheduler will register itself as on mesos
framework_name = Airflow

# Number of cpu cores required for running one task instance using
# 'airflow run <dag_id> <task_id> <execution_date> --local -p <pickle_id>'
# command on a mesos slave
task_cpu = 1

# Memory in MB required for running one task instance using
# 'airflow run <dag_id> <task_id> <execution_date> --local -p <pickle_id>'
# command on a mesos slave
task_memory = 256

# Enable framework checkpointing for mesos
# See http://mesos.apache.org/documentation/latest/slave-recovery/
checkpoint = False

# Failover timeout in milliseconds.
# When checkpointing is enabled and this option is set, Mesos waits
# until the configured timeout for
# the MesosExecutor framework to re-register after a failover. Mesos
# shuts down running tasks if the
# MesosExecutor framework fails to re-register within this timeframe.
# failover_timeout = 604800

# Enable framework authentication for mesos
# See http://mesos.apache.org/documentation/latest/configuration/
authenticate = False

# Mesos credentials, if authentication is enabled
# default_principal = admin
# default_secret = admin

"""

TEST_CONFIG = """\
[core]
airflow_home = {AIRFLOW_HOME}
dags_folder = {TEST_DAGS_FOLDER}
base_log_folder = {AIRFLOW_HOME}/logs
executor = SequentialExecutor
sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/unittests.db
unit_test_mode = True
load_examples = True
donot_pickle = False
dag_concurrency = 16
dags_are_paused_at_creation = False
fernet_key = {FERNET_KEY}
non_pooled_task_slot_count = 128

[operators]
default_owner = airflow

[webserver]
base_url = http://localhost:8080
web_server_host = 0.0.0.0
web_server_port = 8080

[email]
email_backend = airflow.utils.email.send_email_smtp

[smtp]
smtp_host = localhost
smtp_user = airflow
smtp_port = 25
smtp_password = airflow
smtp_mail_from = airflow@airflow.com

[celery]
celery_app_name = airflow.executors.celery_executor
celeryd_concurrency = 16
worker_log_server_port = 8793
broker_url = sqla+mysql://airflow:airflow@localhost:3306/airflow
celery_result_backend = db+mysql://airflow:airflow@localhost:3306/airflow
flower_port = 5555
default_queue = default

[scheduler]
job_heartbeat_sec = 1
scheduler_heartbeat_sec = 5
authenticate = true
max_threads = 2
"""


class ConfigParserWithDefaults(ConfigParser):

    # These configuration elements can be fetched as the stdout of commands
    # following the "{section}__{name}__cmd" pattern, the idea behind this is to not
    # store password on boxes in text files.
    as_command_stdout = {
        ('core', 'sql_alchemy_conn'),
        ('core', 'fernet_key'),
        ('celery', 'broker_url'),
        ('celery', 'celery_result_backend')
    }

    def __init__(self, defaults, *args, **kwargs):
        self.defaults = defaults
        ConfigParser.__init__(self, *args, **kwargs)
        self.is_validated = False

    def _validate(self):
        if (
                self.get("core", "executor") != 'SequentialExecutor' and
                "sqlite" in self.get('core', 'sql_alchemy_conn')):
            raise AirflowConfigException("error: cannot use sqlite with the {}".
                format(self.get('core', 'executor')))

        self.is_validated = True

    def _get_env_var_option(self, section, key):
        # must have format AIRFLOW__{SECTION}__{KEY} (note double underscore)
        env_var = 'AIRFLOW__{S}__{K}'.format(S=section.upper(), K=key.upper())
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])

    def _get_cmd_option(self, section, key):
        fallback_key = key + '_cmd'
        if (
                (section, key) in ConfigParserWithDefaults.as_command_stdout and
                self.has_option(section, fallback_key)):
            command = self.get(section, fallback_key)
            return run_command(command)

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        d = self.defaults

        # first check environment variables
        option = self._get_env_var_option(section, key)
        if option:
            return option

        # ...then the config file
        if self.has_option(section, key):
            return expand_env_var(
                ConfigParser.get(self, section, key, **kwargs))

        # ...then commands
        option = self._get_cmd_option(section, key)
        if option:
            return option

        # ...then the defaults
        if section in d and key in d[section]:
            return expand_env_var(d[section][key])

        else:
            logging.warn("section/key [{section}/{key}] not found "
                         "in config".format(**locals()))

            raise AirflowConfigException(
                "section/key [{section}/{key}] not found "
                "in config".format(**locals()))

    def getboolean(self, section, key):
        val = str(self.get(section, key)).lower().strip()
        if '#' in val:
            val = val.split('#')[0].strip()
        if val == "true":
            return True
        elif val == "false":
            return False
        else:
            raise AirflowConfigException("Not a boolean.")

    def getint(self, section, key):
        return int(self.get(section, key))

    def getfloat(self, section, key):
        return float(self.get(section, key))

    def read(self, filenames):
        ConfigParser.read(self, filenames)
        self._validate()

    def as_dict(self, display_source=False, display_sensitive=False):
        """
        Returns the current configuration as an OrderedDict of OrderedDicts.
        :param display_source: If False, the option value is returned. If True,
            a tuple of (option_value, source) is returned. Source is either
            'airflow.cfg' or 'default'.
        :type display_source: bool
        :param display_sensitive: If True, the values of options set by env
            vars and bash commands will be displayed. If False, those options
            are shown as '< hidden >'
        :type display_sensitive: bool
        """
        cfg = copy.deepcopy(self._sections)

        # remove __name__ (affects Python 2 only)
        for options in cfg.values():
            options.pop('__name__', None)

        # add source
        if display_source:
            for section in cfg:
                for k, v in cfg[section].items():
                    cfg[section][k] = (v, 'airflow.cfg')

        # add env vars and overwrite because they have priority
        for ev in [ev for ev in os.environ if ev.startswith('AIRFLOW__')]:
            try:
                _, section, key = ev.split('__')
                opt = self._get_env_var_option(section, key)
            except ValueError:
                opt = None
            if opt:
                if not display_sensitive:
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'env var')
                cfg.setdefault(section.lower(), OrderedDict()).update(
                    {key.lower(): opt})

        # add bash commands
        for (section, key) in ConfigParserWithDefaults.as_command_stdout:
            opt = self._get_cmd_option(section, key)
            if opt:
                if not display_sensitive:
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'bash cmd')
                cfg.setdefault(section, OrderedDict()).update({key: opt})

        # add defaults
        for section in sorted(self.defaults):
            for key in sorted(self.defaults[section].keys()):
                if key not in cfg.setdefault(section, OrderedDict()):
                    opt = str(self.defaults[section][key])
                    if display_source:
                        cfg[section][key] = (opt, 'default')
                    else:
                        cfg[section][key] = opt

        return cfg


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise AirflowConfigException('Had trouble creating a directory')

"""
Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
"~/airflow" and "~/airflow/airflow.cfg" respectively as defaults.
"""

if 'AIRFLOW_HOME' not in os.environ:
    AIRFLOW_HOME = expand_env_var('~/airflow')
else:
    AIRFLOW_HOME = expand_env_var(os.environ['AIRFLOW_HOME'])

mkdir_p(AIRFLOW_HOME)

if 'AIRFLOW_CONFIG' not in os.environ:
    if os.path.isfile(expand_env_var('~/airflow.cfg')):
        AIRFLOW_CONFIG = expand_env_var('~/airflow.cfg')
    else:
        AIRFLOW_CONFIG = AIRFLOW_HOME + '/airflow.cfg'
else:
    AIRFLOW_CONFIG = expand_env_var(os.environ['AIRFLOW_CONFIG'])

# Set up dags folder for unit tests
# this directory won't exist if users install via pip
_TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    'tests',
    'dags')
if os.path.exists(_TEST_DAGS_FOLDER):
    TEST_DAGS_FOLDER = _TEST_DAGS_FOLDER
else:
    TEST_DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')


def parameterized_config(template):
    """
    Generates a configuration from the provided template + variables defined in
    current scope
    :param template: a config content templated with {{variables}}
    """
    FERNET_KEY = generate_fernet_key()
    all_vars = {k: v for d in [globals(), locals()] for k, v in d.items()}
    return template.format(**all_vars)

TEST_CONFIG_FILE = AIRFLOW_HOME + '/unittests.cfg'
if not os.path.isfile(TEST_CONFIG_FILE):
    logging.info("Creating new airflow config file for unit tests in: " +
                 TEST_CONFIG_FILE)
    with open(TEST_CONFIG_FILE, 'w') as f:
        f.write(parameterized_config(TEST_CONFIG))

if not os.path.isfile(AIRFLOW_CONFIG):
    # These configuration options are used to generate a default configuration
    # when it is missing. The right way to change your configuration is to alter
    # your configuration file, not this code.
    logging.info("Creating new airflow config file in: " + AIRFLOW_CONFIG)
    with open(AIRFLOW_CONFIG, 'w') as f:
        f.write(parameterized_config(DEFAULT_CONFIG))

logging.info("Reading the config from " + AIRFLOW_CONFIG)


def test_mode():
    conf = ConfigParserWithDefaults(defaults)
    conf.read(TEST_CONFIG)

conf = ConfigParserWithDefaults(defaults)
conf.read(AIRFLOW_CONFIG)


def get(section, key, **kwargs):
    return conf.get(section, key, **kwargs)


def getboolean(section, key):
    return conf.getboolean(section, key)


def getfloat(section, key):
    return conf.getfloat(section, key)


def getint(section, key):
    return conf.getint(section, key)


def has_option(section, key):
    return conf.has_option(section, key)


def remove_option(section, option):
    return conf.remove_option(section, option)


def as_dict(display_source=False, display_sensitive=False):
    return conf.as_dict(
        display_source=display_source, display_sensitive=display_sensitive)
as_dict.__doc__ = conf.as_dict.__doc__


def set(section, option, value):  # noqa
    return conf.set(section, option, value)

########################
# convenience method to access config entries


def get_dags_folder():
    return os.path.expanduser(get('core', 'DAGS_FOLDER'))
