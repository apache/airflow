from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

HEADER = """\
  ____________       _____________
 ____    |__( )_________  __/__  /________      __
____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
 _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
 """

BASE_LOG_URL = '/admin/airflow/log'
LOGGING_LEVEL = logging.INFO

# can't move this to conf due to ConfigParser interpolation
LOG_FORMAT = (
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
SIMPLE_LOG_FORMAT = '%(asctime)s %(levelname)s - %(message)s'


engine = None
Session = None


def connect(sql_alchemy_conn, pool_size, pool_recycle):
    """
    builds the Session object to connect to the SQL-alchemy backend.
    """
    global Session, engine

    if Session:
        Session.remove()

    engine_args = {}
    if 'sqlite' not in sql_alchemy_conn:
        # Engine args not supported by sqlite
        engine_args['pool_size'] = pool_size
        engine_args['pool_recycle'] = pool_recycle

    engine = create_engine(sql_alchemy_conn, **engine_args)
    Session = scoped_session(sessionmaker(autocommit=False, autoflush=False,
                                          bind=engine))


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

def configure_logging():
    logging.root.handlers = []
    logging.basicConfig(
        format=LOG_FORMAT, stream=sys.stdout, level=LOGGING_LEVEL)

try:
    from airflow_local_settings import *
    logging.info("Loaded airflow_local_settings.")
except:
    pass

configure_logging()
