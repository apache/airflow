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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import atexit
import logging
import os
import pendulum

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from airflow import configuration as conf
from airflow.logging_config import configure_logging
from airflow.utils.sqlalchemy import setup_event_handlers

log = logging.getLogger(__name__)

RBAC = conf.getboolean('webserver', 'rbac')

TIMEZONE = pendulum.timezone('UTC')
try:
    tz = conf.get("core", "default_timezone")
    if tz == "system":
        TIMEZONE = pendulum.local_timezone()
    else:
        TIMEZONE = pendulum.timezone(tz)
except:
    pass
log.info("Configured default timezone %s" % TIMEZONE)


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


Stats = DummyStatsLogger

if conf.getboolean('scheduler', 'statsd_on'):
    from statsd import StatsClient

    statsd = StatsClient(
        host=conf.get('scheduler', 'statsd_host'),
        port=conf.getint('scheduler', 'statsd_port'),
        prefix=conf.get('scheduler', 'statsd_prefix'))
    Stats = statsd
else:
    Stats = DummyStatsLogger

HEADER = """\
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
 """

LOGGING_LEVEL = logging.INFO

# the prefix to append to gunicorn worker processes after init
GUNICORN_WORKER_READY_PREFIX = "[ready] "

LOG_FORMAT = conf.get('core', 'log_format')
SIMPLE_LOG_FORMAT = conf.get('core', 'simple_log_format')

AIRFLOW_HOME = None
SQL_ALCHEMY_CONN = None
DAGS_FOLDER = None

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
    pass


def configure_vars():
    global AIRFLOW_HOME
    global SQL_ALCHEMY_CONN
    global DAGS_FOLDER
    AIRFLOW_HOME = os.path.expanduser(conf.get('core', 'AIRFLOW_HOME'))
    SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
    DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))


def configure_orm(disable_connection_pool=False):
    log.debug("Setting up DB connection pool (PID %s)" % os.getpid())
    global engine
    global Session
    engine_args = {}

    pool_connections = conf.getboolean('core', 'SQL_ALCHEMY_POOL_ENABLED')
    if disable_connection_pool or not pool_connections:
        engine_args['poolclass'] = NullPool
    elif 'sqlite' not in SQL_ALCHEMY_CONN:
        # Engine args not supported by sqlite
        engine_args['pool_size'] = conf.getint('core', 'SQL_ALCHEMY_POOL_SIZE')
        engine_args['pool_recycle'] = conf.getint('core',
                                                  'SQL_ALCHEMY_POOL_RECYCLE')

    engine = create_engine(SQL_ALCHEMY_CONN, **engine_args)
    reconnect_timeout = conf.getint('core', 'SQL_ALCHEMY_RECONNECT_TIMEOUT')
    setup_event_handlers(engine, reconnect_timeout)

    Session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine))


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


def configure_action_logging():
    """
    Any additional configuration (register callback) for airflow.utils.action_loggers
    module
    :return: None
    """
    pass


try:
    from airflow_local_settings import *
    log.info("Loaded airflow_local_settings.")
except:
    pass

configure_logging()
configure_vars()
configure_adapters()
configure_orm()
configure_action_logging()

# Ensure we close DB connections at scheduler and gunicon worker terminations
atexit.register(dispose_orm)

# Const stuff

KILOBYTE = 1024
MEGABYTE = KILOBYTE * KILOBYTE
WEB_COLORS = {'LIGHTBLUE': '#4d9de0',
              'LIGHTORANGE': '#FF9933'}
