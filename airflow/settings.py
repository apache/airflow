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
import atexit
import json
import logging
import os
import sys
from typing import Optional

import pendulum
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import Engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.session import Session as SASession
from sqlalchemy.pool import NullPool

# pylint: disable=unused-import
from airflow.configuration import AIRFLOW_HOME, WEBSERVER_CONFIG, conf  # NOQA F401
from airflow.logging_config import configure_logging
from airflow.utils.orm_event_handlers import setup_event_handlers

log = logging.getLogger(__name__)


TIMEZONE = pendulum.tz.timezone('UTC')
try:
    tz = conf.get("core", "default_timezone")
    if tz == "system":
        TIMEZONE = pendulum.tz.local_timezone()
    else:
        TIMEZONE = pendulum.tz.timezone(tz)
except Exception:  # pylint: disable=broad-except
    pass
log.info("Configured default timezone %s", TIMEZONE)


HEADER = '\n'.join([
    r'  ____________       _____________',
    r' ____    |__( )_________  __/__  /________      __',
    r'____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /',
    r'___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /',
    r' _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/',
])

LOGGING_LEVEL = logging.INFO

# the prefix to append to gunicorn worker processes after init
GUNICORN_WORKER_READY_PREFIX = "[ready] "

LOG_FORMAT = conf.get('logging', 'log_format')
SIMPLE_LOG_FORMAT = conf.get('logging', 'simple_log_format')

SQL_ALCHEMY_CONN: Optional[str] = None
PLUGINS_FOLDER: Optional[str] = None
LOGGING_CLASS_PATH: Optional[str] = None
DAGS_FOLDER: str = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))

engine: Optional[Engine] = None
Session: Optional[SASession] = None

# The JSON library to use for DAG Serialization and De-Serialization
json = json  # pylint: disable=self-assigning-variable

# Dictionary containing State and colors associated to each state to
# display on the Webserver
STATE_COLORS = {
    "queued": "gray",
    "running": "lime",
    "success": "green",
    "failed": "red",
    "up_for_retry": "gold",
    "up_for_reschedule": "turquoise",
    "upstream_failed": "orange",
    "skipped": "pink",
    "scheduled": "tan",
}


def policy(task):  # pylint: disable=unused-argument
    """
    This policy setting allows altering tasks after they are loaded in
    the DagBag. It allows administrator to rewire some task parameters.

    To define policy, add a ``airflow_local_settings`` module
    to your PYTHONPATH that defines this ``policy`` function.

    Here are a few examples of how this can be useful:

    * You could enforce a specific queue (say the ``spark`` queue)
        for tasks using the ``SparkOperator`` to make sure that these
        tasks get wired to the right workers
    * You could enforce a task timeout policy, making sure that no tasks run
        for more than 48 hours
    * ...
    """


def task_instance_mutation_hook(task_instance):  # pylint: disable=unused-argument
    """
    This setting allows altering task instances before they are queued by
    the Airflow scheduler.

    To define task_instance_mutation_hook, add a ``airflow_local_settings`` module
    to your PYTHONPATH that defines this ``task_instance_mutation_hook`` function.

    This could be used, for instance, to modify the task instance during retries.
    """


def pod_mutation_hook(pod):  # pylint: disable=unused-argument
    """
    This setting allows altering ``kubernetes.client.models.V1Pod`` object
    before they are passed to the Kubernetes client by the ``PodLauncher``
    for scheduling.

    To define a pod mutation hook, add a ``airflow_local_settings`` module
    to your PYTHONPATH that defines this ``pod_mutation_hook`` function.
    It receives a ``Pod`` object and can alter it where needed.

    This could be used, for instance, to add sidecar or init containers
    to every worker pod launched by KubernetesExecutor or KubernetesPodOperator.
    """


# pylint: disable=global-statement
def configure_vars():
    """Configure Global Variables from airflow.cfg"""
    global SQL_ALCHEMY_CONN
    global DAGS_FOLDER
    global PLUGINS_FOLDER
    SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
    DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))

    PLUGINS_FOLDER = conf.get(
        'core',
        'plugins_folder',
        fallback=os.path.join(AIRFLOW_HOME, 'plugins')
    )


def configure_orm(disable_connection_pool=False):
    """Configure ORM using SQLAlchemy"""
    log.debug("Setting up DB connection pool (PID %s)", os.getpid())
    global engine
    global Session
    engine_args = {}

    pool_connections = conf.getboolean('core', 'SQL_ALCHEMY_POOL_ENABLED')
    if disable_connection_pool or not pool_connections:
        engine_args['poolclass'] = NullPool
        log.debug("settings.configure_orm(): Using NullPool")
    elif 'sqlite' not in SQL_ALCHEMY_CONN:
        # Pool size engine args not supported by sqlite.
        # If no config value is defined for the pool size, select a reasonable value.
        # 0 means no limit, which could lead to exceeding the Database connection limit.
        pool_size = conf.getint('core', 'SQL_ALCHEMY_POOL_SIZE', fallback=5)

        # The maximum overflow size of the pool.
        # When the number of checked-out connections reaches the size set in pool_size,
        # additional connections will be returned up to this limit.
        # When those additional connections are returned to the pool, they are disconnected and discarded.
        # It follows then that the total number of simultaneous connections
        # the pool will allow is pool_size + max_overflow,
        # and the total number of “sleeping” connections the pool will allow is pool_size.
        # max_overflow can be set to -1 to indicate no overflow limit;
        # no limit will be placed on the total number
        # of concurrent connections. Defaults to 10.
        max_overflow = conf.getint('core', 'SQL_ALCHEMY_MAX_OVERFLOW', fallback=10)

        # The DB server already has a value for wait_timeout (number of seconds after
        # which an idle sleeping connection should be killed). Since other DBs may
        # co-exist on the same server, SQLAlchemy should set its
        # pool_recycle to an equal or smaller value.
        pool_recycle = conf.getint('core', 'SQL_ALCHEMY_POOL_RECYCLE', fallback=1800)

        # Check connection at the start of each connection pool checkout.
        # Typically, this is a simple statement like “SELECT 1”, but may also make use
        # of some DBAPI-specific method to test the connection for liveness.
        # More information here:
        # https://docs.sqlalchemy.org/en/13/core/pooling.html#disconnect-handling-pessimistic
        pool_pre_ping = conf.getboolean('core', 'SQL_ALCHEMY_POOL_PRE_PING', fallback=True)

        log.debug("settings.configure_orm(): Using pool settings. pool_size=%d, max_overflow=%d, "
                  "pool_recycle=%d, pid=%d", pool_size, max_overflow, pool_recycle, os.getpid())
        engine_args['pool_size'] = pool_size
        engine_args['pool_recycle'] = pool_recycle
        engine_args['pool_pre_ping'] = pool_pre_ping
        engine_args['max_overflow'] = max_overflow

    # Allow the user to specify an encoding for their DB otherwise default
    # to utf-8 so jobs & users with non-latin1 characters can still use us.
    engine_args['encoding'] = conf.get('core', 'SQL_ENGINE_ENCODING', fallback='utf-8')

    if conf.has_option('core', 'sql_alchemy_connect_args'):
        connect_args = conf.getimport('core', 'sql_alchemy_connect_args')
    else:
        connect_args = {}

    engine = create_engine(SQL_ALCHEMY_CONN, connect_args=connect_args, **engine_args)
    setup_event_handlers(engine)

    Session = scoped_session(
        sessionmaker(autocommit=False,
                     autoflush=False,
                     bind=engine,
                     expire_on_commit=False))


def dispose_orm():
    """Properly close pooled database connections"""
    log.debug("Disposing DB connection pool (PID %s)", os.getpid())
    global engine
    global Session

    if Session:
        Session.remove()
        Session = None
    if engine:
        engine.dispose()
        engine = None


def configure_adapters():
    """Register Adapters and DB Converters"""
    from pendulum import DateTime as Pendulum
    try:
        from sqlite3 import register_adapter
        register_adapter(Pendulum, lambda val: val.isoformat(' '))
    except ImportError:
        pass
    try:
        import MySQLdb.converters
        MySQLdb.converters.conversions[Pendulum] = MySQLdb.converters.DateTime2literal
    except ImportError:
        pass
    try:
        import pymysql.converters
        pymysql.converters.conversions[Pendulum] = pymysql.converters.escape_datetime
    except ImportError:
        pass


def validate_session():
    """Validate ORM Session"""
    worker_precheck = conf.getboolean('core', 'worker_precheck', fallback=False)
    if not worker_precheck:
        return True
    else:
        check_session = sessionmaker(bind=engine)
        session = check_session()
        try:
            session.execute("select 1")  # pylint: disable=no-member
            conn_status = True
        except exc.DBAPIError as err:
            log.error(err)
            conn_status = False
        session.close()  # pylint: disable=no-member
        return conn_status


def configure_action_logging():
    """
    Any additional configuration (register callback) for airflow.utils.action_loggers
    module
    :rtype: None
    """


def prepare_syspath():
    """
    Ensures that certain subfolders of AIRFLOW_HOME are on the classpath
    """

    if DAGS_FOLDER not in sys.path:
        sys.path.append(DAGS_FOLDER)

    # Add ./config/ for loading custom log parsers etc, or
    # airflow_local_settings etc.
    config_path = os.path.join(AIRFLOW_HOME, 'config')
    if config_path not in sys.path:
        sys.path.append(config_path)

    if PLUGINS_FOLDER not in sys.path:
        sys.path.append(PLUGINS_FOLDER)


def import_local_settings():
    """Import airflow_local_settings.py files to allow overriding any configs in settings.py file"""
    try:  # pylint: disable=too-many-nested-blocks
        import airflow_local_settings

        if hasattr(airflow_local_settings, "__all__"):
            for i in airflow_local_settings.__all__:  # pylint: disable=no-member
                globals()[i] = getattr(airflow_local_settings, i)
        else:
            for k, v in airflow_local_settings.__dict__.items():
                if not k.startswith("__"):
                    globals()[k] = v

        log.info("Loaded airflow_local_settings from %s .", airflow_local_settings.__file__)
    except ImportError:
        log.debug("Failed to import airflow_local_settings.", exc_info=True)


def initialize():
    """Initialize Airflow with all the settings from this file"""
    configure_vars()
    prepare_syspath()
    import_local_settings()
    global LOGGING_CLASS_PATH
    LOGGING_CLASS_PATH = configure_logging()
    configure_adapters()
    # The webservers import this file from models.py with the default settings.
    configure_orm()
    configure_action_logging()

    # Ensure we close DB connections at scheduler and gunicon worker terminations
    atexit.register(dispose_orm)
# pylint: enable=global-statement


# Const stuff

KILOBYTE = 1024
MEGABYTE = KILOBYTE * KILOBYTE
WEB_COLORS = {'LIGHTBLUE': '#4d9de0',
              'LIGHTORANGE': '#FF9933'}

# If store_serialized_dags is True, scheduler writes serialized DAGs to DB, and webserver
# reads DAGs from DB instead of importing from files.
STORE_SERIALIZED_DAGS = conf.getboolean('core', 'store_serialized_dags', fallback=False)

# Updating serialized DAG can not be faster than a minimum interval to reduce database
# write rate.
MIN_SERIALIZED_DAG_UPDATE_INTERVAL = conf.getint(
    'core', 'min_serialized_dag_update_interval', fallback=30)

# Fetching serialized DAG can not be faster than a minimum interval to reduce database
# read rate. This config controls when your DAGs are updated in the Webserver
MIN_SERIALIZED_DAG_FETCH_INTERVAL = conf.getint(
    'core', 'min_serialized_dag_fetch_interval', fallback=10)

# Whether to persist DAG files code in DB. If set to True, Webserver reads file contents
# from DB instead of trying to access files in a DAG folder.
# Defaults to same as the store_serialized_dags setting.
STORE_DAG_CODE = conf.getboolean("core", "store_dag_code", fallback=STORE_SERIALIZED_DAGS)

# If donot_modify_handlers=True, we do not modify logging handlers in task_run command
# If the flag is set to False, we remove all handlers from the root logger
# and add all handlers from 'airflow.task' logger to the root Logger. This is done
# to get all the logs from the print & log statements in the DAG files before a task is run
# The handlers are restored after the task completes execution.
DONOT_MODIFY_HANDLERS = conf.getboolean('logging', 'donot_modify_handlers', fallback=False)
