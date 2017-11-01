# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
from functools import wraps
import logging
import os
import time

import sys, traceback

from alembic.config import Config
from alembic import command
from alembic.migration import MigrationContext

from sqlalchemy import event, exc
from sqlalchemy.pool import Pool
from sqlalchemy import select

from airflow import settings


def provide_session(func):
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        needs_session = False
        arg_session = 'session'
        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and \
            func_params.index(arg_session) < len(args)
        if not (arg_session in kwargs or session_in_args):
            needs_session = True
            session = settings.Session()
            kwargs[arg_session] = session
        result = func(*args, **kwargs)
        if needs_session:
            session.expunge_all()
            session.commit()
            session.close()
        return result
    return wrapper


def enable_connection_reconnect(db_reconnect_limit):

    # from: http://docs.sqlalchemy.org/en/rel_1_1/core/pooling.html#disconnect-handling-pessimistic
    @event.listens_for(settings.engine, "engine_connect")
    def reconnect_on_engine_connect(connection, branch):
        logging.info("reconnect_on_engine_connect() Checking db connection; branch: {}".format(branch))

        if branch:
            logging.info("reconnect_on_engine_connect(): returning since branch: {}".format(branch))
            # "branch" refers to a sub-connection of a connection,
            # we don't want to bother pinging on these.
            return

        # turn off "close with result".  This flag is only used with
        # "connectionless" execution, otherwise will be False in any case
        save_should_close_with_result = connection.should_close_with_result
        connection.should_close_with_result = False

        try:
            # run a SELECT 1.   use a core select() so that
            # the SELECT of a scalar value without a table is
            # appropriately formatted for the backend
            connection.scalar(select([1]))
        except exc.DBAPIError as err:
            logging.warning("reconnect_on_engine_connect() exception: {}; connection_invalidated: {}"
                            .format(err, err.connection_invalidated))
            # catch SQLAlchemy's DBAPIError, which is a wrapper
            # for the DBAPI's exception.  It includes a .connection_invalidated
            # attribute which specifies if this connection is a "disconnect"
            # condition, which is based on inspection of the original exception
            # by the dialect in use.
            if err.connection_invalidated:
                if not retry_connection_for_period(connection, db_reconnect_limit):
                    raise
            else:
                logging.warning("reconnect_on_engine_connect(): Not retrying as connection_invalidated is False")
                raise
        finally:
            # restore "close with result"
            logging.debug("reconnect_on_engine_connect() done")
            connection.should_close_with_result = save_should_close_with_result

    @event.listens_for(Pool, "checkout")
    def reconnect_on_checkout(dbapi_connection, connection_record, connection_proxy):
        """
        Disconnect Handling - Pessimistic, taken from:
        http://docs.sqlalchemy.org/en/rel_0_9/core/pooling.html
        """

        logging.info("reconnect_on_checkout()")

        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
        except exc.DBAPIError as err:
            logging.info("reconnect_on_checkout(): connection_invalidated; not retrying", err)
            raise exc.DisconnectionError()
        except Exception as e:
            logging.warning("reconnect_on_checkout(): Error type: ", type(e), "; ", e.args)

            if not retry_connection_for_period(None, db_reconnect_limit):
                raise exc.DisconnectionError()

        cursor.close()


def retry_connection_for_period(connection, db_reconnect_limit):
    logging.debug("retryConnectionForPeriod")
    retry_time_spent = 0
    succeeded = False
    retry_time_to_sleep = 10  # starts with 10 seconds; grows to 2 mins after 10 retries

    logging.info("retry_connection_for_period(): Retry limit (seconds): {}"
                 .format(db_reconnect_limit if db_reconnect_limit is not None else "infinite"))
    while (not succeeded) and ((db_reconnect_limit is None) or
                                       (retry_time_spent + retry_time_to_sleep) < db_reconnect_limit):
        # run the same SELECT again - the connection will re-validate
        # itself and establish a new connection.  The disconnect detection
        # here also causes the whole connection pool to be invalidated
        # so that all stale connections are discarded.

        logging.info("retry_connection_for_period(): sleeping for {}s; retry_time_spent: {}s"
                     .format(retry_time_to_sleep, retry_time_spent))
        time.sleep(retry_time_to_sleep)
        retry_time_spent += retry_time_to_sleep
        logging.info("retry_connection_for_period(): calling select 1; retry_time_spent: {}"
            .format(retry_time_spent))
        try:
            if connection is None:
                connection = settings.engine.connect()
            connection.scalar(select([1]))
            logging.info("retry_connection_for_period(): SUCCESS calling select 1")
            succeeded = True
        except Exception as e:
            logging.info("retry_connection_for_period(): ERROR calling select 1; Error type: ", type(e), "; ", e.args)

        if retry_time_spent >= 100:
            retry_time_to_sleep = 120

    if succeeded:
        logging.info("retry_connection_for_period(): Connection re-established")
        return True

    logging.error("retry_connection_for_period(): Connection could NOT be re-established")
    return False


def pessimistic_connection_handling():
    @event.listens_for(Pool, "checkout")
    def ping_connection(dbapi_connection, connection_record, connection_proxy):
        '''
        Disconnect Handling - Pessimistic, taken from:
        http://docs.sqlalchemy.org/en/rel_0_9/core/pooling.html
        '''
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
        except:
            raise exc.DisconnectionError()
        cursor.close()


@provide_session
def merge_conn(conn, session=None):
    from airflow import models
    C = models.Connection
    if not session.query(C).filter(C.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()


@event.listens_for(settings.engine, "connect")
def connect(dbapi_connection, connection_record):
    connection_record.info['pid'] = os.getpid()


@event.listens_for(settings.engine, "checkout")
def checkout(dbapi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        connection_record.connection = connection_proxy.connection = None
        raise exc.DisconnectionError(
            "Connection record belongs to pid {}, "
            "attempting to check out in pid {}".format(connection_record.info['pid'], pid)
        )


def initdb():
    session = settings.Session()

    from airflow import models
    upgradedb()

    merge_conn(
        models.Connection(
            conn_id='airflow_db', conn_type='mysql',
            host='localhost', login='root', password='',
            schema='airflow'))
    merge_conn(
        models.Connection(
            conn_id='airflow_ci', conn_type='mysql',
            host='localhost', login='root', extra="{\"local_infile\": true}",
            schema='airflow_ci'))
    merge_conn(
        models.Connection(
            conn_id='beeline_default', conn_type='beeline', port="10000",
            host='localhost', extra="{\"use_beeline\": true, \"auth\": \"\"}",
            schema='default'))
    merge_conn(
        models.Connection(
            conn_id='bigquery_default', conn_type='bigquery'))
    merge_conn(
        models.Connection(
            conn_id='local_mysql', conn_type='mysql',
            host='localhost', login='airflow', password='airflow',
            schema='airflow'))
    merge_conn(
        models.Connection(
            conn_id='presto_default', conn_type='presto',
            host='localhost',
            schema='hive', port=3400))
    merge_conn(
        models.Connection(
            conn_id='hive_cli_default', conn_type='hive_cli',
            schema='default',))
    merge_conn(
        models.Connection(
            conn_id='hiveserver2_default', conn_type='hiveserver2',
            host='localhost',
            schema='default', port=10000))
    merge_conn(
        models.Connection(
            conn_id='metastore_default', conn_type='hive_metastore',
            host='localhost', extra="{\"authMechanism\": \"PLAIN\"}",
            port=9083))
    merge_conn(
        models.Connection(
            conn_id='mysql_default', conn_type='mysql',
            login='root',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='postgres_default', conn_type='postgres',
            login='postgres',
            schema='airflow',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='sqlite_default', conn_type='sqlite',
            host='/tmp/sqlite_default.db'))
    merge_conn(
        models.Connection(
            conn_id='http_default', conn_type='http',
            host='https://www.google.com/'))
    merge_conn(
        models.Connection(
            conn_id='mssql_default', conn_type='mssql',
            host='localhost', port=1433))
    merge_conn(
        models.Connection(
            conn_id='vertica_default', conn_type='vertica',
            host='localhost', port=5433))
    merge_conn(
        models.Connection(
            conn_id='webhdfs_default', conn_type='hdfs',
            host='localhost', port=50070))
    merge_conn(
        models.Connection(
            conn_id='ssh_default', conn_type='ssh',
            host='localhost'))
    merge_conn(
        models.Connection(
            conn_id='fs_default', conn_type='fs',
            extra='{"path": "/"}'))
    merge_conn(
        models.Connection(
            conn_id='aws_default', conn_type='aws',
            extra='{"region_name": "us-east-1"}'))
    merge_conn(
        models.Connection(
            conn_id='spark_default', conn_type='spark',
            host='yarn', extra='{"queue": "root.default"}'))
    merge_conn(
        models.Connection(
            conn_id='emr_default', conn_type='emr',
            extra='''
                {   "Name": "default_job_flow_name",
                    "LogUri": "s3://my-emr-log-bucket/default_job_flow_location",
                    "ReleaseLabel": "emr-4.6.0",
                    "Instances": {
                        "InstanceGroups": [
                            {
                                "Name": "Master nodes",
                                "Market": "ON_DEMAND",
                                "InstanceRole": "MASTER",
                                "InstanceType": "r3.2xlarge",
                                "InstanceCount": 1
                            },
                            {
                                "Name": "Slave nodes",
                                "Market": "ON_DEMAND",
                                "InstanceRole": "CORE",
                                "InstanceType": "r3.2xlarge",
                                "InstanceCount": 1
                            }
                        ]
                    },
                    "Ec2KeyName": "mykey",
                    "KeepJobFlowAliveWhenNoSteps": false,
                    "TerminationProtected": false,
                    "Ec2SubnetId": "somesubnet",
                    "Applications":[
                        { "Name": "Spark" }
                    ],
                    "VisibleToAllUsers": true,
                    "JobFlowRole": "EMR_EC2_DefaultRole",
                    "ServiceRole": "EMR_DefaultRole",
                    "Tags": [
                        {
                            "Key": "app",
                            "Value": "analytics"
                        },
                        {
                            "Key": "environment",
                            "Value": "development"
                        }
                    ]
                }
            '''))

    # Known event types
    KET = models.KnownEventType
    if not session.query(KET).filter(KET.know_event_type == 'Holiday').first():
        session.add(KET(know_event_type='Holiday'))
    if not session.query(KET).filter(KET.know_event_type == 'Outage').first():
        session.add(KET(know_event_type='Outage'))
    if not session.query(KET).filter(
            KET.know_event_type == 'Natural Disaster').first():
        session.add(KET(know_event_type='Natural Disaster'))
    if not session.query(KET).filter(
            KET.know_event_type == 'Marketing Campaign').first():
        session.add(KET(know_event_type='Marketing Campaign'))
    session.commit()

    dagbag = models.DagBag()
    # Save individual DAGs in the ORM
    now = datetime.utcnow()
    for dag in dagbag.dags.values():
        models.DAG.sync_to_db(dag, dag.owner, now)
    # Deactivate the unknown ones
    models.DAG.deactivate_unknown_dags(dagbag.dags.keys())

    Chart = models.Chart
    chart_label = "Airflow task instance by type"
    chart = session.query(Chart).filter(Chart.label == chart_label).first()
    if not chart:
        chart = Chart(
            label=chart_label,
            conn_id='airflow_db',
            chart_type='bar',
            x_is_date=False,
            sql=(
                "SELECT state, COUNT(1) as number "
                "FROM task_instance "
                "WHERE dag_id LIKE 'example%' "
                "GROUP BY state"),
        )
        session.add(chart)
        session.commit()


def upgradedb():
    logging.info("Creating tables")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    package_dir = os.path.normpath(os.path.join(current_dir, '..'))
    directory = os.path.join(package_dir, 'migrations')
    config = Config(os.path.join(package_dir, 'alembic.ini'))
    config.set_main_option('script_location', directory)
    config.set_main_option('sqlalchemy.url', settings.SQL_ALCHEMY_CONN)
    command.upgrade(config, 'heads')


def resetdb():
    '''
    Clear out the database
    '''
    from airflow import models

    logging.info("Dropping tables that exist")
    models.Base.metadata.drop_all(settings.engine)
    mc = MigrationContext.configure(settings.engine)
    if mc._version.exists(settings.engine):
        mc._version.drop(settings.engine)
    initdb()
