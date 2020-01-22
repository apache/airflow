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

import os

from airflow import models, settings
from airflow.configuration import conf
from airflow.jobs.base_job import BaseJob  # noqa: F401  # pylint: disable=unused-import
from airflow.models import Connection
from airflow.models.pool import Pool
# We need to add this model manually to get reset working well
from airflow.models.serialized_dag import SerializedDagModel  # noqa: F401  # pylint: disable=unused-import
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import create_session, provide_session  # noqa  # pylint: disable=unused-import

log = LoggingMixin().log


@provide_session
def merge_conn(conn, session=None):
    """
    Add new Connection.
    """
    if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()


@provide_session
def add_default_pool_if_not_exists(session=None):
    """
    Add default pool if it does not exist.
    """
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
    """
    Create default Airflow connections.
    """
    merge_conn(
        Connection(
            conn_id="airflow_db",
            conn_type="mysql",
            host="mysql",
            login="root",
            password="",
            schema="airflow",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="aws_default",
            conn_type="aws",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="azure_container_instances_default",
            conn_type="azure_container_instances",
            extra='{"tenantId": "<TENANT>", "subscriptionId": "<SUBSCRIPTION ID>" }',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="azure_cosmos_default",
            conn_type="azure_cosmos",
            extra='{"database_name": "<DATABASE_NAME>", "collection_name": "<COLLECTION_NAME>" }',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="azure_data_lake_default",
            conn_type="azure_data_lake",
            extra='{"tenant": "<TENANT>", "account_name": "<ACCOUNTNAME>" }',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="cassandra_default",
            conn_type="cassandra",
            host="cassandra",
            port=9042,
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="databricks_default",
            conn_type="databricks",
            host="localhost",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="dingding_default",
            conn_type="http",
            host="",
            password="",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="druid_broker_default",
            conn_type="druid",
            host="druid-broker",
            port=8082,
            extra='{"endpoint": "druid/v2/sql"}',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="druid_ingest_default",
            conn_type="druid",
            host="druid-overlord",
            port=8081,
            extra='{"endpoint": "druid/indexer/v1/task"}',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="emr_default",
            conn_type="emr",
            extra="""
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
            """,
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="fs_default",
            conn_type="fs",
            extra='{"path": "/"}',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="google_cloud_default",
            conn_type="google_cloud_platform",
            schema="default",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="hive_cli_default",
            conn_type="hive_cli",
            port=10000,
            host="localhost",
            extra='{"use_beeline": true, "auth": ""}',
            schema="default",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="hiveserver2_default",
            conn_type="hiveserver2",
            host="localhost",
            schema="default",
            port=10000,
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="http_default",
            conn_type="http",
            host="https://www.httpbin.org/",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="local_mysql",
            conn_type="mysql",
            host="localhost",
            login="airflow",
            password="airflow",
            schema="airflow",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="metastore_default",
            conn_type="hive_metastore",
            host="localhost",
            extra='{"authMechanism": "PLAIN"}',
            port=9083,
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="mongo_default",
            conn_type="mongo",
            host="mongo",
            port=27017
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="mssql_default",
            conn_type="mssql",
            host="localhost",
            port=1433,
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="mysql_default",
            conn_type="mysql",
            login="root",
            schema="airflow",
            host="mysql",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="opsgenie_default",
            conn_type="http",
            host="",
            password="",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="pig_cli_default",
            conn_type="pig_cli",
            schema="default",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="pinot_admin_default",
            conn_type="pinot",
            host="localhost",
            port=9000,
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="postgres_default",
            conn_type="postgres",
            login="postgres",
            password="airflow",
            schema="airflow",
            host="postgres",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="presto_default",
            conn_type="presto",
            host="localhost",
            schema="hive",
            port=3400,
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="qubole_default",
            conn_type="qubole",
            host="localhost",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="redis_default",
            conn_type="redis",
            host="redis",
            port=6379,
            extra='{"db": 0}',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="segment_default",
            conn_type="segment",
            extra='{"write_key": "my-segment-write-key"}',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="sftp_default",
            conn_type="sftp",
            host="localhost",
            port=22,
            login="airflow",
            extra='{"key_file": "~/.ssh/id_rsa", "no_host_key_check": true}',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="spark_default",
            conn_type="spark",
            host="yarn",
            extra='{"queue": "root.default"}',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="sqlite_default",
            conn_type="sqlite",
            host="/tmp/sqlite_default.db",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="sqoop_default",
            conn_type="sqoop",
            host="rdbms",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="ssh_default",
            conn_type="ssh",
            host="localhost",
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="vertica_default",
            conn_type="vertica",
            host="localhost",
            port=5433,
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="wasb_default",
            conn_type="wasb",
            extra='{"sas_token": null}',
        ),
        session
    )
    merge_conn(
        Connection(
            conn_id="webhdfs_default",
            conn_type="hdfs",
            host="localhost",
            port=50070,
        ),
        session
    )


def initdb():
    """
    Initialize Airflow database.
    """
    upgradedb()

    create_default_connections()

    dagbag = models.DagBag()
    # Save individual DAGs in the ORM
    for dag in dagbag.dags.values():
        dag.sync_to_db()
    # Deactivate the unknown ones
    models.DAG.deactivate_unknown_dags(dagbag.dags.keys())

    from flask_appbuilder.models.sqla import Base
    Base.metadata.create_all(settings.engine)  # pylint: disable=no-member


def upgradedb():
    """
    Upgrade the database.
    """
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


def resetdb():
    """
    Clear out the database
    """
    # alembic adds significant import time, so we import it lazily
    # noinspection PyUnresolvedReferences
    from alembic.migration import MigrationContext

    log.info("Dropping tables that exist")

    connection = settings.engine.connect()
    models.base.Base.metadata.drop_all(connection)
    migartion_ctx = MigrationContext.configure(connection)
    version = migartion_ctx._version  # pylint: disable=protected-access
    if version.exists(connection):
        version.drop(connection)

    from flask_appbuilder.models.sqla import Base
    Base.metadata.drop_all(connection)  # pylint: disable=no-member

    initdb()


@provide_session
def check(session=None):
    """
    Checks if the database works.
    :param session: session of the sqlalchemy
    """
    session.execute('select 1 as is_alive;')
    log.info("Connection successful.")
