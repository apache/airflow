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

import atexit
import logging
import os
import pendulum
import sys
import socket
import time

from sqlalchemy import create_engine, exc
from sqlalchemy.orm import scoped_session, sessionmaker, Query
from sqlalchemy.pool import NullPool
from typing import Any

from airflow.configuration import conf, AIRFLOW_HOME, WEBSERVER_CONFIG  # NOQA F401
from airflow.kubernetes.pod import Pod
from airflow.logging_config import configure_logging
from airflow.utils.sqlalchemy import setup_event_handlers

log = logging.getLogger(__name__)


TIMEZONE = pendulum.timezone('UTC')
try:
    tz = conf.get("core", "default_timezone")
    if tz == "system":
        TIMEZONE = pendulum.local_timezone()
    else:
        TIMEZONE = pendulum.timezone(tz)
except Exception:
    pass
log.info("Configured default timezone %s" % TIMEZONE)


class RetryingQuery(Query):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.max_tries = 1 + conf.getint('core', 'SQL_ALCHEMY_STATEMENT_MAX_RETRIES')

        try:
            self.max_retry_time_seconds = max(0, conf.getint(
                'core', 'SQL_ALCHEMY_STATEMENT_MAX_RETRY_SECONDS'))
        except conf.AirflowConfigException:
            self.max_retry_time_seconds = 30

    def __iter__(self):
        try_number = 1
        last_exc = None
        while try_number <= self.max_tries:
            try:
                return super().__iter__()

            except Exception as ex:
                log.warning('Try {}/{} failed to perform db action.'.format(try_number, self.max_tries), exc_info=ex)
                last_exc = ex
                time.sleep(min(float(self.max_retry_time_seconds), 0.1 * (1 << try_number)))

            try_number += 1

        log.error('Failed to perform db action after {} attempts.'.format(self.max_tries))
        raise last_exc


class DummyStatsLogger(object):
    @classmethod
    def incr(cls, stat, count=1, rate=1):
        pass

    @classmethod
    def decr(cls, stat, count=1, rate=1):
        pass

    @classmethod
    def gauge(cls, stat, value, rate=1, delta=False):
        pass

    @classmethod
    def timing(cls, stat, dt):
        pass


Stats: Any = DummyStatsLogger

try:
    if conf.getboolean('scheduler', 'statsd_on'):
        from statsd import StatsClient

        statsd = StatsClient(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.getint('scheduler', 'statsd_port'),
            prefix=conf.get('scheduler', 'statsd_prefix'))
        Stats = statsd
except (socket.gaierror, ImportError) as e:
    log.warning("Could not configure StatsClient: %s, using DummyStatsLogger instead.", e)

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

LOG_FORMAT = conf.get('core', 'log_format')
SIMPLE_LOG_FORMAT = conf.get('core', 'simple_log_format')

SQL_ALCHEMY_CONN = None
DAGS_FOLDER = None
PLUGINS_FOLDER = None
LOGGING_CLASS_PATH = None

engine = None
Session = None


def policy(task_instance):
    """
    This policy setting allows altering task instances right before they
    are executed. It allows administrator to rewire some task parameters.

    Note that the ``TaskInstance`` object has an attribute ``task`` pointing
    to its related task object, that in turns has a reference to the DAG
    object. So you can use the attributes of all of these to define your
    policy.

    To define policy, add a ``airflow_local_settings`` module
    to your PYTHONPATH that defines this ``policy`` function. It receives
    a ``TaskInstance`` object and can alter it where needed.

    Here are a few examples of how this can be useful:

    * You could enforce a specific queue (say the ``spark`` queue)
        for tasks using the ``SparkOperator`` to make sure that these
        task instances get wired to the right workers
    * You could force all task instances running on an
        ``execution_date`` older than a week old to run in a ``backfill``
        pool.
    * ...
    """


def pod_mutation_hook(pod: Pod):
    """
    This setting allows altering ``Pod`` objects before they are passed to
    the Kubernetes client by the ``PodLauncher`` for scheduling.

    To define a pod mutation hook, add a ``airflow_local_settings`` module
    to your PYTHONPATH that defines this ``pod_mutation_hook`` function.
    It receives a ``Pod`` object and can alter it where needed.

    This could be used, for instance, to add sidecar or init containers
    to every worker pod launched by KubernetesExecutor or KubernetesPodOperator.
    """


def configure_vars():
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
    log.debug("Setting up DB connection pool (PID %s)" % os.getpid())
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
        try:
            pool_size = conf.getint('core', 'SQL_ALCHEMY_POOL_SIZE')
        except conf.AirflowConfigException:
            pool_size = 5

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
        try:
            max_overflow = conf.getint('core', 'SQL_ALCHEMY_MAX_OVERFLOW')
        except conf.AirflowConfigException:
            max_overflow = 10

        # The DB server already has a value for wait_timeout (number of seconds after
        # which an idle sleeping connection should be killed). Since other DBs may
        # co-exist on the same server, SQLAlchemy should set its
        # pool_recycle to an equal or smaller value.
        try:
            pool_recycle = conf.getint('core', 'SQL_ALCHEMY_POOL_RECYCLE')
        except conf.AirflowConfigException:
            pool_recycle = 1800

        log.info("settings.configure_orm(): Using pool settings. pool_size={}, max_overflow={}, "
                 "pool_recycle={}, pid={}".format(pool_size, max_overflow, pool_recycle, os.getpid()))
        engine_args['pool_size'] = pool_size
        engine_args['pool_recycle'] = pool_recycle
        engine_args['max_overflow'] = max_overflow

    # Allow the user to specify an encoding for their DB otherwise default
    # to utf-8 so jobs & users with non-latin1 characters can still use
    # us.
    engine_args['encoding'] = conf.get('core', 'SQL_ENGINE_ENCODING', fallback='utf-8')
    # For Python2 we get back a newstr and need a str
    engine_args['encoding'] = engine_args['encoding'].__str__()

    # Use the RetryingQuery subclass to perform retries against the database
    # when the maximum number of total tries is greater than 1 (i.e. max_retries > 0).
    query_cls = Query
    try:
        if conf.getint('core', 'SQL_ALCHEMY_STATEMENT_MAX_RETRIES') > 0:
            query_cls = RetryingQuery

    except conf.AirflowConfigException:
        query_cls = Query

    if 'postgres' in SQL_ALCHEMY_CONN:
        # set connect/statement timeouts
        connect_args = {}

        try:
            connect_timeout_seconds = conf.getint('core', 'SQL_ALCHEMY_CONNECT_TIMEOUT_SECONDS')
            connect_args['connect_timeout'] = connect_timeout_seconds
        except conf.AirflowConfigException:
            pass

        try:
            statement_timeout_seconds = conf.getint('core', 'SQL_ALCHEMY_STATEMENT_TIMEOUT_SECONDS')
            connect_args['options'] = '-c statement_timeout={}'.format(statement_timeout_seconds * 1000)
        except conf.AirflowConfigException:
            pass

        if len(connect_args) > 0:
            engine_args['connect_args'] = connect_args

    engine = create_engine(SQL_ALCHEMY_CONN, **engine_args)
    reconnect_timeout = conf.getint('core', 'SQL_ALCHEMY_RECONNECT_TIMEOUT')
    setup_event_handlers(engine, reconnect_timeout)

    Session = scoped_session(
        sessionmaker(autocommit=False,
                     autoflush=False,
                     bind=engine,
                     expire_on_commit=False,
                     query_cls=query_cls))


def dispose_orm():
    """ Properly close pooled database connections """
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
    from pendulum import Pendulum
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
    worker_precheck = conf.getboolean('core', 'worker_precheck', fallback=False)
    if not worker_precheck:
        return True
    else:
        check_session = sessionmaker(bind=engine)
        session = check_session()
        try:
            session.execute("select 1")
            conn_status = True
        except exc.DBAPIError as err:
            log.error(err)
            conn_status = False
        session.close()
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
    try:
        import airflow_local_settings

        if hasattr(airflow_local_settings, "__all__"):
            for i in airflow_local_settings.__all__:
                globals()[i] = getattr(airflow_local_settings, i)
        else:
            for k, v in airflow_local_settings.__dict__.items():
                if not k.startswith("__"):
                    globals()[k] = v

        log.info("Loaded airflow_local_settings from " + airflow_local_settings.__file__ + ".")
    except ImportError:
        log.debug("Failed to import airflow_local_settings.", exc_info=True)


def initialize():
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


# Const stuff

KILOBYTE = 1024
MEGABYTE = KILOBYTE * KILOBYTE
WEB_COLORS = {'LIGHTBLUE': '#4d9de0',
              'LIGHTORANGE': '#FF9933'}

# Used by DAG context_managers
CONTEXT_MANAGER_DAG = None
