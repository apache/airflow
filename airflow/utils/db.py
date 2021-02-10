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

from functools import wraps

import logging
import os
import contextlib

from airflow import settings
from airflow.configuration import conf

log = logging.getLogger(__name__)


@contextlib.contextmanager
def create_session():
    """
    Contextmanager that will create and teardown a session.
    """
    session = settings.Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def provide_session(func):
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = 'session'

        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and \
            func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session() as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)

    return wrapper


@provide_session
def merge_conn(conn, session=None):
    from airflow.models import Connection
    if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()


@provide_session
def add_default_pool_if_not_exists(session=None):
    from airflow.models.pool import Pool
    if not Pool.get_pool(Pool.DEFAULT_POOL_NAME, session=session):
        default_pool = Pool(
            pool=Pool.DEFAULT_POOL_NAME,
            slots=conf.getint(section='core', key='non_pooled_task_slot_count',
                              fallback=128),
            description="Default pool",
        )
        session.add(default_pool)
        session.commit()


@provide_session
def create_default_connections(session=None):
    from airflow.models import Connection

    merge_conn(
        Connection(
            conn_id='airflow_db', conn_type='mysql',
            host='mysql', login='root', password='',
            schema='airflow'), session)
    merge_conn(
        Connection(
            conn_id='beeline_default', conn_type='beeline', port=10000,
            host='localhost', extra="{\"use_beeline\": true, \"auth\": \"\"}",
            schema='default'), session)
    merge_conn(
        Connection(
            conn_id='bigquery_default', conn_type='google_cloud_platform',
            schema='default'), session)
    merge_conn(
        Connection(
            conn_id='local_mysql', conn_type='mysql',
            host='localhost', login='airflow', password='airflow',
            schema='airflow'), session)
    merge_conn(
        Connection(
            conn_id='presto_default', conn_type='presto',
            host='localhost',
            schema='hive', port=3400), session)
    merge_conn(
        Connection(
            conn_id='google_cloud_default', conn_type='google_cloud_platform',
            schema='default', ), session)
    merge_conn(
        Connection(
            conn_id='hive_cli_default', conn_type='hive_cli',
            schema='default', ), session)
    merge_conn(
        Connection(
            conn_id='pig_cli_default', conn_type='pig_cli',
            schema='default', ), session)
    merge_conn(
        Connection(
            conn_id='hiveserver2_default', conn_type='hiveserver2',
            host='localhost',
            schema='default', port=10000))
    merge_conn(
        Connection(
            conn_id='metastore_default', conn_type='hive_metastore',
            host='localhost', extra="{\"authMechanism\": \"PLAIN\"}",
            port=9083), session)
    merge_conn(
        Connection(
            conn_id='mongo_default', conn_type='mongo',
            host='mongo', port=27017), session)
    merge_conn(
        Connection(
            conn_id='mysql_default', conn_type='mysql',
            login='root',
            schema='airflow',
            host='mysql'), session)
    merge_conn(
        Connection(
            conn_id='postgres_default', conn_type='postgres',
            login='postgres',
            password='airflow',
            schema='airflow',
            host='postgres'), session)
    merge_conn(
        Connection(
            conn_id='sqlite_default', conn_type='sqlite',
            host='/tmp/sqlite_default.db'), session)
    merge_conn(
        Connection(
            conn_id='http_default', conn_type='http',
            host='https://www.httpbin.org/'), session)
    merge_conn(
        Connection(
            conn_id='mssql_default', conn_type='mssql',
            host='localhost', port=1433), session)
    merge_conn(
        Connection(
            conn_id='vertica_default', conn_type='vertica',
            host='localhost', port=5433), session)
    merge_conn(
        Connection(
            conn_id='wasb_default', conn_type='wasb',
            extra='{"sas_token": null}'), session)
    merge_conn(
        Connection(
            conn_id='webhdfs_default', conn_type='hdfs',
            host='localhost', port=50070), session)
    merge_conn(
        Connection(
            conn_id='ssh_default', conn_type='ssh',
            host='localhost'), session)
    merge_conn(
        Connection(
            conn_id='sftp_default', conn_type='sftp',
            host='localhost', port=22, login='airflow',
            extra='''
                    {"key_file": "~/.ssh/id_rsa", "no_host_key_check": true}
                '''), session)
    merge_conn(
        Connection(
            conn_id='fs_default', conn_type='fs',
            extra='{"path": "/"}'), session)
    merge_conn(
        Connection(
            conn_id='aws_default', conn_type='aws'), session)
    merge_conn(
        Connection(
            conn_id='spark_default', conn_type='spark',
            host='yarn', extra='{"queue": "root.default"}'), session)
    merge_conn(
        Connection(
            conn_id='druid_broker_default', conn_type='druid',
            host='druid-broker', port=8082, extra='{"endpoint": "druid/v2/sql"}'), session)
    merge_conn(
        Connection(
            conn_id='druid_ingest_default', conn_type='druid',
            host='druid-overlord', port=8081,
            extra='{"endpoint": "druid/indexer/v1/task"}'), session)
    merge_conn(
        Connection(
            conn_id='redis_default', conn_type='redis',
            host='redis', port=6379,
            extra='{"db": 0}'), session)
    merge_conn(
        Connection(
            conn_id='sqoop_default', conn_type='sqoop',
            host='rmdbs', extra=''), session)
    merge_conn(
        Connection(
            conn_id='emr_default', conn_type='emr',
            extra='''
                    {   "Name": "default_job_flow_name",
                        "LogUri": "s3://my-emr-log-bucket/default_job_flow_location",
                        "ReleaseLabel": "emr-4.6.0",
                        "Instances": {
                            "Ec2KeyName": "mykey",
                            "Ec2SubnetId": "somesubnet",
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
                            ],
                            "TerminationProtected": false,
                            "KeepJobFlowAliveWhenNoSteps": false
                        },
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
                '''), session)
    merge_conn(
        Connection(
            conn_id='databricks_default', conn_type='databricks',
            host='localhost'), session)
    merge_conn(
        Connection(
            conn_id='qubole_default', conn_type='qubole',
            host='localhost'), session)
    merge_conn(
        Connection(
            conn_id='segment_default', conn_type='segment',
            extra='{"write_key": "my-segment-write-key"}'), session),
    merge_conn(
        Connection(
            conn_id='azure_data_lake_default', conn_type='azure_data_lake',
            extra='{"tenant": "<TENANT>", "account_name": "<ACCOUNTNAME>" }'), session)
    merge_conn(
        Connection(
            conn_id='azure_cosmos_default', conn_type='azure_cosmos',
            extra='{"database_name": "<DATABASE_NAME>", "collection_name": "<COLLECTION_NAME>" }'),
        session
    )
    merge_conn(
        Connection(
            conn_id='azure_container_instances_default', conn_type='azure_container_instances',
            extra='{"tenantId": "<TENANT>", "subscriptionId": "<SUBSCRIPTION ID>" }'), session)
    merge_conn(
        Connection(
            conn_id='cassandra_default', conn_type='cassandra',
            host='cassandra', port=9042), session)
    merge_conn(
        Connection(
            conn_id='dingding_default', conn_type='http',
            host='', password=''), session)
    merge_conn(
        Connection(
            conn_id='opsgenie_default', conn_type='http',
            host='', password=''), session)


def initdb(rbac=False):
    session = settings.Session()

    from airflow import models
    upgradedb()

    if conf.getboolean('core', 'LOAD_DEFAULT_CONNECTIONS', fallback=True):
        create_default_connections()

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
    for dag in dagbag.dags.values():
        dag.sync_to_db()
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

    if rbac:
        from flask_appbuilder.security.sqla import models
        from flask_appbuilder.models.sqla import Base
        Base.metadata.create_all(settings.engine)


def upgradedb():
    # alembic adds significant import time, so we import it lazily
    from alembic import command
    from alembic.config import Config

    log.info("Creating tables")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    package_dir = os.path.normpath(os.path.join(current_dir, '..'))
    directory = os.path.join(package_dir, 'migrations')
    config = Config(os.path.join(package_dir, 'alembic.ini'))
    config.set_main_option('script_location', directory.replace('%', '%%'))
    config.set_main_option('sqlalchemy.url', settings.SQL_ALCHEMY_CONN.replace('%', '%%'))
    command.upgrade(config, 'heads')
    add_default_pool_if_not_exists()


def resetdb(rbac):
    """
    Clear out the database
    """
    from airflow import models
    # We need to add this model manually to get reset working well
    # noinspection PyUnresolvedReferences
    from airflow.models.serialized_dag import SerializedDagModel  # noqa: F401
    # noinspection PyUnresolvedReferences
    from airflow.jobs.base_job import BaseJob  # noqa: F401

    # alembic adds significant import time, so we import it lazily
    from alembic.migration import MigrationContext

    log.info("Dropping tables that exist")

    connection = settings.engine.connect()
    models.base.Base.metadata.drop_all(connection)
    mc = MigrationContext.configure(connection)
    if mc._version.exists(connection):
        mc._version.drop(connection)

    if rbac:
        # drop rbac security tables
        from flask_appbuilder.security.sqla import models
        from flask_appbuilder.models.sqla import Base
        Base.metadata.drop_all(connection)
    from flask_appbuilder.models.sqla import Base
    Base.metadata.drop_all(connection)

    initdb(rbac)


@provide_session
def checkdb(session=None):
    """
    Checks if the database works.
    :param session: session of the sqlalchemy
    """
    session.execute('select 1 as is_alive;')
    log.info("Connection successful.")
