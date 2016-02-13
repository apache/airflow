from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from future import standard_library

standard_library.install_aliases()
from builtins import str
from configparser import ConfigParser
import errno
import logging
import os

try:
    from cryptography.fernet import Fernet
except:
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


class AirflowConfigException(Exception):
    pass

defaults = {
    'core': {
        'unit_test_mode': False,
        'parallelism': 32,
        'load_examples': True,
        'plugins_folder': None,
        'security': None,
        'donot_pickle': False,
        's3_log_folder': '',
        'dag_concurrency': 16,
        'max_active_runs_per_dag': 16,
    },
    'webserver': {
        'base_url': 'http://localhost:8080',
        'web_server_host': '0.0.0.0',
        'web_server_port': '8080',
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
    },
    'celery': {
        'default_queue': 'default',
        'flower_port': '5555'
    },
    'smtp': {
        'smtp_starttls': True,
    },
    'kerberos': {
        'ccache': '/tmp/airflow_krb5_ccache',
        'principal': 'airflow',                 # gets augmented with fqdn
        'reinit_frequency': '3600',
        'kinit_path': 'kinit',
        'keytab': 'airflow.keytab',
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
# An S3 location can be provided for log backups
# For S3, use the full URL to the base folder (starting with "s3://...")
s3_log_folder = None

# The executor class that airflow should use. Choices include
# SequentialExecutor, LocalExecutor, CeleryExecutor
executor = SequentialExecutor

# The SqlAlchemy connection string to the metadata database.
# SqlAlchemy supports many different database engine, more information
# their website
sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/airflow.db

# The amount of parallelism as a setting to the executor. This defines
# the max number of task instances that should run simultaneously
# on this airflow installation
parallelism = 32

# The number of task instances allowed to run concurrently by the scheduler
dag_concurrency = 16

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

[webserver]
# The base url of your website as airflow cannot guess what domain or
# cname you are using. This is use in automated emails that
# airflow sends to point links to the right web server
base_url = http://localhost:8080

# The ip specified when starting the web server
web_server_host = 0.0.0.0

# The port on which to run the web server
web_server_port = 8080

# Secret key used to run your flask app
secret_key = temporary_key

# Number of workers to run the Gunicorn web server
workers = 4

# The worker class gunicorn should use. Choices include
# sync (default), eventlet, gevent
worker_class = sync

# Expose the configuration file in the web server
expose_config = true

# Set to true to turn on authentication : http://pythonhosted.org/airflow/installation.html#web-authentication
authenticate = False

# Filter the list of dags by owner name (requires authentication to be enabled)
filter_by_owner = False

[smtp]
# If you want airflow to send emails on retries, failure, and you want to
# the airflow.utils.send_email function, you have to configure an smtp
# server here
smtp_host = localhost
smtp_starttls = True
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
# When checkpointing is enabled and this option is set, Mesos waits until the configured timeout for
# the MesosExecutor framework to re-register after a failover. Mesos shuts down running tasks if the
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
dags_folder = {AIRFLOW_HOME}/dags
base_log_folder = {AIRFLOW_HOME}/logs
executor = SequentialExecutor
sql_alchemy_conn = sqlite:///{AIRFLOW_HOME}/unittests.db
unit_test_mode = True
load_examples = True
donot_pickle = False
dag_concurrency = 16
fernet_key = {FERNET_KEY}

[webserver]
base_url = http://localhost:8080
web_server_host = 0.0.0.0
web_server_port = 8080

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
"""


class ConfigParserWithDefaults(ConfigParser):

    def __init__(self, defaults, *args, **kwargs):
        self.defaults = defaults
        ConfigParser.__init__(self, *args, **kwargs)

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()
        d = self.defaults

        # environment variables get precedence
        # must have format AIRFLOW__{SECTION}__{KEY} (note double underscore)
        env_var = 'AIRFLOW__{S}__{K}'.format(S=section.upper(), K=key.upper())
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])

        # ...then the config file
        elif self.has_option(section, key):
            return expand_env_var(ConfigParser.get(self, section, key, **kwargs))

        # ...then the defaults
        elif section in d and key in d[section]:
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
    with open(AIRFLOW_CONFIG, 'w') as f:
        f.write(parameterized_config(TEST_CONFIG))

if not os.path.isfile(AIRFLOW_CONFIG):
    """
    These configuration options are used to generate a default configuration
    when it is missing. The right way to change your configuration is to alter
    your configuration file, not this code.
    """
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


########################
# convenience method to access config entries

def get_dags_folder():
    return os.path.expanduser(get('core', 'DAGS_FOLDER'))

