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
DAGS_FOLDER = os.path.expanduser(conf.get('core', 'DAGS_FOLDER'))
SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')

if DAGS_FOLDER not in sys.path:
    sys.path.append(DAGS_FOLDER)

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
LOG_FORMAT =  \
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
SIMPLE_LOG_FORMAT = \
    '%(asctime)s %(levelname)s - %(message)s'
