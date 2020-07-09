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

import os
import contextlib

from airflow import settings
from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


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
def merge_error_tag(err_tag, session=None):
    from airflow.models import ErrorTag
    if not session.query(ErrorTag).filter(ErrorTag.label == err_tag.label).first():
        session.add(err_tag)
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
def create_default_error_tags(session=None):
    from airflow.models import ErrorTag
    # todo: error tag init
    merge_error_tag(
        ErrorTag(lable='测试错误代码1', value='100')
    )
    merge_error_tag(
        ErrorTag(lable='测试错误代码2', value='101')
    )


@provide_session
def create_default_nd_line_controller_map_var(session=None):
    from airflow.models import Variable
    val = {
        "1C1": [
            "CON001@1T101/内饰1工位"
        ],
        "T1": [
            "T1-06M1@T1-06M1/内饰一1工位",
            "T1-007L@T1-007L/内饰一2工位",
            "T1-007R@T1-007R/内饰一3工位",
            "T1-016M1@T1-016M1/内饰一4工位",
            "T1-006L2@T1-006L2/内饰一5工位",
            "T1-006R2@T1-006R2/内饰一6工位",
            "T1-022L@T1-022L/内饰一7工位",
            "T1-012L@T1-012L/内饰一8工位",
            "T1-012R@T1-012R/内饰一9工位"
        ],
        "T2": [
            "T2-004L@T2-004L/内饰二1工位",
            "T2-004R@T2-004R/内饰二2工位",
            "T2-010M1@T2-010M1/内饰二3工位",
            "T2-019L2@T2-019L2/内饰二4工位",
            "T2-019R2@T2-019R2/内饰二5工位"
        ],
        "C1": [
            "C1-005L@C1-005L/底盘一1工位",
            "C1-006L@C1-006L/底盘一2工位",
            "C1-005R@C1-005R/底盘一3工位"
        ],
        "C2": [
            "C2-001L@C2-001L/底盘二1工位",
            "C2-001R1@C2-001R1/底盘二2工位",
            "C2-01RJ@C2-01机器人工位/底盘二3工位",
            "C2-004L@C2-004L/底盘二4工位",
            "C2-004R@C2-004R/底盘二5工位",
            "C2-005L@C2-005L/底盘二6工位",
            "C2-005R@C2-005R/底盘二7工位",
            "C2-006R@C2-006R/底盘二8工位",
            "C2-006R@C2-006R/底盘二9工位",
            "C2-007M1@C2-007M1/底盘二10工位",
            "C2-008M3@C2-008M3/底盘二11工位",
            "C2-015L2@C2-015L2/底盘二12工位",
            "C2-015R2@C2-015R2/底盘二13工位",
            "C2-016L@C2-016L/底盘二14工位",
            "C2-016R@C2-016R/底盘二15工位",
            "C2-016R1@C2-016R1/底盘二16工位",
            "C2-016L1@C2-016L1/底盘二17工位",
            "C2-020L@C2-020L/底盘二18工位",
            "C2-020R@C2-020R/底盘二19工位",
            "C2-023R@C2-023R/底盘二20工位",
            "C2-024L@C2-024L/底盘二21工位",
            "C2-025L@C2-025L/底盘二22工位",
            "C2-025R@C2-025R/底盘二23工位"
        ],
        "W1": [
            "W1-017L@W1-017L/外装一1工位",
            "W1-012L@W1-012L/外装一2工位",
            "W1-007L@W1-007L/外装一3工位",
            "W1-021L@W1-021L/外装一4工位",
            "W1-021R@W1-021R/外装一5工位",
            "W1-020L1@W1-020L1/外装一6工位",
            "W1-020R1@W1-020R1/外装一7工位"
        ],
        "W2": [
            "W2-008L@W2-008L/外装二1工位"
        ],
        "FL": [
            "FL-013R@FL-013R/最终线1工位"
        ],
        "E0": [
            "E0-012R@E0-012R/发动机1工位",
            "E0-012R@E0-012R/发动机2工位",
            "E0-013R@E0-013R/发动机3工位",
            "E0-014R@E0-014R/发动机4工位"
        ],
        "E1": [
            "E1-004R@E1-004R/发动机5工位",
            "E1-005R@E1-005R/发动机6工位",
            "E1-005L@E1-005L/发动机7工位",
            "E1-007R@E1-007R/发动机8工位"
        ],
        "CA": [
            "CA-004L@CA-004L/底盘模块1工位",
            "CA-005L@CA-005L/底盘模块2工位",
            "CA-007L@CA-007L/底盘模块3工位",
            "CA-007R@CA-007R/底盘模块4工位",
            "CA-007L@CA-007L/底盘模块5工位",
            "CA-007R@CA-007R/底盘模块6工位",
            "CA-008L@CA-008L/底盘模块7工位",
            "CA-008R@CA-008R/底盘模块8工位",
            "CA-008L@CA-008L/底盘模块9工位",
            "CA-08L@CA-08L/底盘模块10工位",
            "CA-008R@CA-008R/底盘模块11工位",
            "CA-010L@CA-010L/底盘模块12工位",
            "CA-010R@CA-010R/底盘模块13工位"
        ],
        "D1": [
            "D1-008L@D1-008L/门线1工位",
            "D1-008R@D1-008R/门线2工位",
            "D1-007L@D1-007L/门线3工位",
            "D1-007R@D1-007R/门线4工位"
        ],
        "IP": [
            "IP-005R1@IP-005R1/仪表线1工位",
            "IP-015R@IP-015R/仪表线2工位",
            "IP-011R@IP-011R/仪表线3工位"
        ],
        "MFE": [
            "MFE-006R@MFE-006R/前装模块线1工位"
        ]
    }
    line_code_controllers_map = Variable.get('line_code_controllers_map', {} , deserialize_json=True)
    if not line_code_controllers_map:
        # 不存在时候才进行设定
        Variable.set(key='line_code_controllers_map', value=val, serialize_json=True)


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
    from airflow.models import Connection
    session = settings.Session()

    from airflow import models
    upgradedb()

    if conf.getboolean('core', 'LOAD_DEFAULT_CONNECTIONS', fallback=True):
        create_default_connections()

    if conf.getboolean('core', 'LOAD_DEFAULT_ERROR_TAG', fallback=True):
        create_default_error_tags()

    if os.environ.get('FACTORY_CODE', '') in ['nd', '7200']:
        create_default_nd_line_controller_map_var()

    merge_conn(
        Connection(
            conn_id='rabbitmq_default', conn_type='rabbitmq',
            login='guest',
            schema='amqp',
            extra='{"user_id": "guest", "vhost": "/", heartbeat: 0 }',
            host='localhost', port=5672))

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
