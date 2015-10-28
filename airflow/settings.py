from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import sys

from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine

from airflow.configuration import conf

HEADER = """\
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
 """

BASE_LOG_URL = '/admin/airflow/log'
AIRFLOW_HOME = os.path.expanduser(conf.get('core', 'AIRFLOW_HOME'))
SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
LOGGING_LEVEL = logging.INFO
DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))

engine_args = {}
if 'sqlite' not in SQL_ALCHEMY_CONN:
    # Engine args not supported by sqlite
    engine_args['pool_size'] = 50
    engine_args['pool_recycle'] = 3600

engine = create_engine(
    SQL_ALCHEMY_CONN, **engine_args)
Session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine))

# can't move this to configuration due to ConfigParser interpolation
LOG_FORMAT = (
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
SIMPLE_LOG_FORMAT = '%(asctime)s %(levelname)s - %(message)s'


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


try:
    from airflow_local_settings import *
    logging.info("Loaded airflow_local_settings.")
except:
    pass
