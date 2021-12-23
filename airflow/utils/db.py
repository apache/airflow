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
import logging
import os
import time
from tempfile import gettempdir
from typing import Iterable

from sqlalchemy import Table, exc, func, inspect, or_, text

from airflow import settings
from airflow.configuration import conf
from airflow.jobs.base_job import BaseJob  # noqa: F401
from airflow.models import (  # noqa: F401
    DAG,
    XCOM_RETURN_KEY,
    BaseOperator,
    BaseOperatorLink,
    Connection,
    DagBag,
    DagModel,
    DagPickle,
    DagRun,
    DagTag,
    Log,
    Pool,
    SkipMixin,
    SlaMiss,
    TaskFail,
    TaskInstance,
    TaskReschedule,
    Variable,
    XCom,
)

# We need to add this model manually to get reset working well
from airflow.models.serialized_dag import SerializedDagModel  # noqa: F401

# TODO: remove create_session once we decide to break backward compatibility
from airflow.utils.session import create_global_lock, create_session, provide_session  # noqa: F401

log = logging.getLogger(__name__)


def _format_airflow_moved_table_name(source_table, version):
    return "__".join([settings.AIRFLOW_MOVED_TABLE_PREFIX, version.replace(".", "_"), source_table])


@provide_session
def merge_conn(conn, session=None):
    """Add new Connection."""
    if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()


@provide_session
def add_default_pool_if_not_exists(session=None):
    """Add default pool if it does not exist."""
    if not Pool.get_pool(Pool.DEFAULT_POOL_NAME, session=session):
        default_pool = Pool(
            pool=Pool.DEFAULT_POOL_NAME,
            slots=conf.getint(section='core', key='default_pool_task_slot_count'),
            description="Default pool",
        )
        session.add(default_pool)
        session.commit()


@provide_session
def create_default_connections(session=None):
    """Create default Airflow connections."""
    merge_conn(
        Connection(
            conn_id="airflow_db",
            conn_type="mysql",
            host="mysql",
            login="root",
            password="",
            schema="airflow",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="aws_default",
            conn_type="aws",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="azure_batch_default",
            conn_type="azure_batch",
            login="<ACCOUNT_NAME>",
            password="",
            extra='''{"account_url": "<ACCOUNT_URL>"}''',
        )
    )
    merge_conn(
        Connection(
            conn_id="azure_cosmos_default",
            conn_type="azure_cosmos",
            extra='{"database_name": "<DATABASE_NAME>", "collection_name": "<COLLECTION_NAME>" }',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id='azure_data_explorer_default',
            conn_type='azure_data_explorer',
            host='https://<CLUSTER>.kusto.windows.net',
            extra='''{"auth_method": "<AAD_APP | AAD_APP_CERT | AAD_CREDS | AAD_DEVICE>",
                    "tenant": "<TENANT ID>", "certificate": "<APPLICATION PEM CERTIFICATE>",
                    "thumbprint": "<APPLICATION CERTIFICATE THUMBPRINT>"}''',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="azure_data_lake_default",
            conn_type="azure_data_lake",
            extra='{"tenant": "<TENANT>", "account_name": "<ACCOUNTNAME>" }',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="azure_default",
            conn_type="azure",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="cassandra_default",
            conn_type="cassandra",
            host="cassandra",
            port=9042,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="databricks_default",
            conn_type="databricks",
            host="localhost",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="dingding_default",
            conn_type="http",
            host="",
            password="",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="drill_default",
            conn_type="drill",
            host="localhost",
            port=8047,
            extra='{"dialect_driver": "drill+sadrill", "storage_plugin": "dfs"}',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="druid_broker_default",
            conn_type="druid",
            host="druid-broker",
            port=8082,
            extra='{"endpoint": "druid/v2/sql"}',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="druid_ingest_default",
            conn_type="druid",
            host="druid-overlord",
            port=8081,
            extra='{"endpoint": "druid/indexer/v1/task"}',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="elasticsearch_default",
            conn_type="elasticsearch",
            host="localhost",
            schema="http",
            port=9200,
        ),
        session,
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
                                "Name": "Core nodes",
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
        session,
    )
    merge_conn(
        Connection(
            conn_id="facebook_default",
            conn_type="facebook_social",
            extra="""
                {   "account_id": "<AD_ACCOUNT_ID>",
                    "app_id": "<FACEBOOK_APP_ID>",
                    "app_secret": "<FACEBOOK_APP_SECRET>",
                    "access_token": "<FACEBOOK_AD_ACCESS_TOKEN>"
                }
            """,
        ),
        session,
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
        session,
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
        session,
    )
    merge_conn(
        Connection(
            conn_id="hiveserver2_default",
            conn_type="hiveserver2",
            host="localhost",
            schema="default",
            port=10000,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="http_default",
            conn_type="http",
            host="https://www.httpbin.org/",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id='kubernetes_default',
            conn_type='kubernetes',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id='kylin_default',
            conn_type='kylin',
            host='localhost',
            port=7070,
            login="ADMIN",
            password="KYLIN",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="leveldb_default",
            conn_type="leveldb",
            host="localhost",
        ),
        session,
    )
    merge_conn(Connection(conn_id="livy_default", conn_type="livy", host="livy", port=8998), session)
    merge_conn(
        Connection(
            conn_id="local_mysql",
            conn_type="mysql",
            host="localhost",
            login="airflow",
            password="airflow",
            schema="airflow",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="metastore_default",
            conn_type="hive_metastore",
            host="localhost",
            extra='{"authMechanism": "PLAIN"}',
            port=9083,
        ),
        session,
    )
    merge_conn(Connection(conn_id="mongo_default", conn_type="mongo", host="mongo", port=27017), session)
    merge_conn(
        Connection(
            conn_id="mssql_default",
            conn_type="mssql",
            host="localhost",
            port=1433,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="mysql_default",
            conn_type="mysql",
            login="root",
            schema="airflow",
            host="mysql",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="opsgenie_default",
            conn_type="http",
            host="",
            password="",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="oss_default",
            conn_type="oss",
            extra='''{
                "auth_type": "AK",
                "access_key_id": "<ACCESS_KEY_ID>",
                "access_key_secret": "<ACCESS_KEY_SECRET>"}
                ''',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="pig_cli_default",
            conn_type="pig_cli",
            schema="default",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="pinot_admin_default",
            conn_type="pinot",
            host="localhost",
            port=9000,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="pinot_broker_default",
            conn_type="pinot",
            host="localhost",
            port=9000,
            extra='{"endpoint": "/query", "schema": "http"}',
        ),
        session,
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
        session,
    )
    merge_conn(
        Connection(
            conn_id="presto_default",
            conn_type="presto",
            host="localhost",
            schema="hive",
            port=3400,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="qubole_default",
            conn_type="qubole",
            host="localhost",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="redis_default",
            conn_type="redis",
            host="redis",
            port=6379,
            extra='{"db": 0}',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="segment_default",
            conn_type="segment",
            extra='{"write_key": "my-segment-write-key"}',
        ),
        session,
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
        session,
    )
    merge_conn(
        Connection(
            conn_id="spark_default",
            conn_type="spark",
            host="yarn",
            extra='{"queue": "root.default"}',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="sqlite_default",
            conn_type="sqlite",
            host=os.path.join(gettempdir(), "sqlite_default.db"),
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="sqoop_default",
            conn_type="sqoop",
            host="rdbms",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="ssh_default",
            conn_type="ssh",
            host="localhost",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="tableau_default",
            conn_type="tableau",
            host="https://tableau.server.url",
            login="user",
            password="password",
            extra='{"site_id": "my_site"}',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="trino_default",
            conn_type="trino",
            host="localhost",
            schema="hive",
            port=3400,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="vertica_default",
            conn_type="vertica",
            host="localhost",
            port=5433,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="wasb_default",
            conn_type="wasb",
            extra='{"sas_token": null}',
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="webhdfs_default",
            conn_type="hdfs",
            host="localhost",
            port=50070,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id='yandexcloud_default',
            conn_type='yandexcloud',
            schema='default',
        ),
        session,
    )


@provide_session
def initdb(session=None):
    """Initialize Airflow database."""
    upgradedb(session=session)

    if conf.getboolean('core', 'LOAD_DEFAULT_CONNECTIONS'):
        create_default_connections(session=session)

    with create_global_lock(session=session):

        dagbag = DagBag()
        # Save DAGs in the ORM
        dagbag.sync_to_db(session=session)

        # Deactivate the unknown ones
        DAG.deactivate_unknown_dags(dagbag.dags.keys(), session=session)

        from flask_appbuilder.models.sqla import Base

        Base.metadata.create_all(settings.engine)


def _get_alembic_config():
    from alembic.config import Config

    current_dir = os.path.dirname(os.path.abspath(__file__))
    package_dir = os.path.normpath(os.path.join(current_dir, '..'))
    directory = os.path.join(package_dir, 'migrations')
    config = Config(os.path.join(package_dir, 'alembic.ini'))
    config.set_main_option('script_location', directory.replace('%', '%%'))
    config.set_main_option('sqlalchemy.url', settings.SQL_ALCHEMY_CONN.replace('%', '%%'))
    return config


def check_migrations(timeout):
    """
    Function to wait for all airflow migrations to complete.

    :param timeout: Timeout for the migration in seconds
    :return: None
    """
    from alembic.runtime.migration import MigrationContext
    from alembic.script import ScriptDirectory

    config = _get_alembic_config()
    script_ = ScriptDirectory.from_config(config)
    with settings.engine.connect() as connection:
        context = MigrationContext.configure(connection)
        ticker = 0
        while True:
            source_heads = set(script_.get_heads())
            db_heads = set(context.get_current_heads())
            if source_heads == db_heads:
                break
            if ticker >= timeout:
                raise TimeoutError(
                    f"There are still unapplied migrations after {ticker} seconds. "
                    f"Migration Head(s) in DB: {db_heads} | Migration Head(s) in Source Code: {source_heads}"
                )
            ticker += 1
            time.sleep(1)
            log.info('Waiting for migrations... %s second(s)', ticker)


def check_conn_id_duplicates(session=None) -> Iterable[str]:
    """
    Check unique conn_id in connection table

    :param session:  session of the sqlalchemy
    :rtype: str
    """
    dups = []
    try:
        dups = session.query(Connection.conn_id).group_by(Connection.conn_id).having(func.count() > 1).all()
    except (exc.OperationalError, exc.ProgrammingError):
        # fallback if tables hasn't been created yet
        session.rollback()
    if dups:
        yield (
            'Seems you have non unique conn_id in connection table.\n'
            'You have to manage those duplicate connections '
            'before upgrading the database.\n'
            f'Duplicated conn_id: {[dup.conn_id for dup in dups]}'
        )


def check_conn_type_null(session=None) -> Iterable[str]:
    """
    Check nullable conn_type column in Connection table

    :param session:  session of the sqlalchemy
    :rtype: str
    """
    n_nulls = []
    try:
        n_nulls = session.query(Connection.conn_id).filter(Connection.conn_type.is_(None)).all()
    except (exc.OperationalError, exc.ProgrammingError, exc.InternalError):
        # fallback if tables hasn't been created yet
        session.rollback()

    if n_nulls:
        yield (
            'The conn_type column in the connection '
            'table must contain content.\n'
            'Make sure you don\'t have null '
            'in the conn_type column.\n'
            f'Null conn_type conn_id: {list(n_nulls)}'
        )


def _format_dangling_error(source_table, target_table, invalid_count, reason):
    noun = "row" if invalid_count == 1 else "rows"
    return (
        f"The {source_table} table has {invalid_count} {noun} {reason}, which "
        f"is invalid. We could not move them out of the way because the "
        f"{target_table} table already exists in your database. Please either "
        f"drop the {target_table} table, or manually delete the invalid rows "
        f"from the {source_table} table."
    )


def _move_dangling_run_data_to_new_table(session, source_table: "Table", target_table_name: str):
    where_clause = "where dag_id is null or run_id is null or execution_date is null"
    _move_dangling_table(session, source_table, target_table_name, where_clause)


def _move_dangling_table(session, source_table: "Table", target_table_name: str, where_clause: str):
    dialect_name = session.get_bind().dialect.name

    delete_where = " AND ".join(
        f"{source_table.name}.{c.name} = d.{c.name}" for c in source_table.primary_key.columns
    )
    if dialect_name == "mssql":
        session.execute(
            text(f"select source.* into {target_table_name} from {source_table} as source {where_clause}")
        )
        session.execute(
            text(
                f"delete from {source_table} from {source_table} join {target_table_name} AS d ON "
                + delete_where
            )
        )
    else:
        if dialect_name == "mysql":
            # CREATE TABLE AS SELECT must be broken into two queries for  MySQL as the single query
            # approach fails when replication is enabled ("Statement violates GTID consistency")```
            session.execute(text(f"create table {target_table_name} like {source_table}"))
            session.execute(
                text(
                    f"INSERT INTO {target_table_name} select source.* from {source_table} as source "
                    + where_clause
                )
            )
        # Postgres and SQLite have the same CREATE TABLE a AS SELECT ... syntax
        else:
            session.execute(
                text(
                    f"create table {target_table_name} as select source.* from {source_table} as source "
                    + where_clause
                )
            )

        # But different join-delete syntax.
        if dialect_name == "mysql":
            session.execute(
                text(
                    f"delete {source_table} from {source_table} join {target_table_name} as d on "
                    + delete_where
                )
            )
        elif dialect_name == "sqlite":
            session.execute(
                text(
                    f"delete from {source_table} where ROWID in (select {source_table}.ROWID from "
                    f"{source_table} as source join {target_table_name} as d on {delete_where})"
                )
            )
        else:
            session.execute(
                text(f"delete from {source_table} using {target_table_name} as d where {delete_where}")
            )


def check_run_id_null(session) -> Iterable[str]:
    import sqlalchemy.schema

    metadata = sqlalchemy.schema.MetaData(session.bind)
    try:
        metadata.reflect(only=[DagRun.__tablename__], extend_existing=True, resolve_fks=False)
    except exc.InvalidRequestError:
        # Table doesn't exist -- empty db
        return

    # We can't use the model here since it may differ from the db state due to
    # this function is run prior to migration. Use the reflected table instead.
    dagrun_table = metadata.tables[DagRun.__tablename__]

    invalid_dagrun_filter = or_(
        dagrun_table.c.dag_id.is_(None),
        dagrun_table.c.run_id.is_(None),
        dagrun_table.c.execution_date.is_(None),
    )
    invalid_dagrun_count = session.query(dagrun_table.c.id).filter(invalid_dagrun_filter).count()
    if invalid_dagrun_count > 0:
        dagrun_dangling_table_name = _format_airflow_moved_table_name(dagrun_table.name, "2.2")
        if dagrun_dangling_table_name in inspect(session.get_bind()).get_table_names():
            yield _format_dangling_error(
                source_table=dagrun_table.name,
                target_table=dagrun_dangling_table_name,
                invalid_count=invalid_dagrun_count,
                reason="with a NULL dag_id, run_id, or execution_date",
            )
            return
        _move_dangling_run_data_to_new_table(session, dagrun_table, dagrun_dangling_table_name)


def _move_dangling_task_data_to_new_table(session, source_table: "Table", target_table_name: str):
    where_clause = """
        left join dag_run as dr
        on (source.dag_id = dr.dag_id and source.execution_date = dr.execution_date)
        where dr.id is null
    """
    _move_dangling_table(session, source_table, target_table_name, where_clause)


def check_task_tables_without_matching_dagruns(session) -> Iterable[str]:
    import sqlalchemy.schema
    from sqlalchemy import and_, outerjoin

    metadata = sqlalchemy.schema.MetaData(session.bind)
    models_to_dagrun = [TaskInstance, TaskReschedule]
    for model in models_to_dagrun + [DagRun]:
        try:
            metadata.reflect(only=[model.__tablename__], extend_existing=True, resolve_fks=False)
        except exc.InvalidRequestError:
            # Table doesn't exist, but try the other ones incase the user is upgrading from an _old_ DB
            # version
            pass

    # Key table doesn't exist -- likely empty DB.
    if DagRun.__tablename__ not in metadata or TaskInstance.__tablename__ not in metadata:
        return

    # We can't use the model here since it may differ from the db state due to
    # this function is run prior to migration. Use the reflected table instead.
    dagrun_table = metadata.tables[DagRun.__tablename__]

    existing_table_names = set(inspect(session.get_bind()).get_table_names())
    errored = False

    for model in models_to_dagrun:
        # We can't use the model here since it may differ from the db state due to
        # this function is run prior to migration. Use the reflected table instead.
        source_table = metadata.tables.get(model.__tablename__)
        if source_table is None:
            continue

        # Migration already applied, don't check again.
        if "run_id" in source_table.columns:
            continue

        source_to_dag_run_join_cond = and_(
            source_table.c.dag_id == dagrun_table.c.dag_id,
            source_table.c.execution_date == dagrun_table.c.execution_date,
        )
        invalid_row_count = (
            session.query(source_table.c.dag_id, source_table.c.task_id, source_table.c.execution_date)
            .select_from(outerjoin(source_table, dagrun_table, source_to_dag_run_join_cond))
            .filter(dagrun_table.c.dag_id.is_(None))
            .count()
        )
        if invalid_row_count <= 0:
            continue

        dangling_table_name = _format_airflow_moved_table_name(source_table.name, "2.2")
        if dangling_table_name in existing_table_names:
            yield _format_dangling_error(
                source_table=source_table.name,
                target_table=dangling_table_name,
                invalid_count=invalid_row_count,
                reason=f"without a corresponding {dagrun_table.name} row",
            )
            errored = True
            continue
        _move_dangling_task_data_to_new_table(session, source_table, dangling_table_name)

    if errored:
        session.rollback()
    else:
        session.commit()


@provide_session
def _check_migration_errors(session=None) -> Iterable[str]:
    """
    :session: session of the sqlalchemy
    :rtype: list[str]
    """
    for check_fn in (
        check_conn_id_duplicates,
        check_conn_type_null,
        check_run_id_null,
        check_task_tables_without_matching_dagruns,
    ):
        yield from check_fn(session)
        # Ensure there is no "active" transaction. Seems odd, but without this MSSQL can hang
        session.commit()


@provide_session
def upgradedb(session=None):
    """Upgrade the database."""
    # alembic adds significant import time, so we import it lazily
    from alembic import command

    config = _get_alembic_config()

    config.set_main_option('sqlalchemy.url', settings.SQL_ALCHEMY_CONN.replace('%', '%%'))

    errors_seen = False
    for err in _check_migration_errors(session=session):
        if not errors_seen:
            log.error("Automatic migration is not available")
            errors_seen = True
        log.error("%s", err)

    if errors_seen:
        exit(1)

    with create_global_lock(session=session, pg_lock_id=2, lock_name="upgrade"):
        log.info("Creating tables")
        command.upgrade(config, 'heads')
    add_default_pool_if_not_exists()


@provide_session
def resetdb(session=None):
    """Clear out the database"""
    log.info("Dropping tables that exist")

    connection = settings.engine.connect()

    with create_global_lock(session=session, pg_lock_id=4, lock_name="reset"):
        drop_airflow_models(connection)
        drop_flask_models(connection)

    initdb(session=session)


def drop_airflow_models(connection):
    """
    Drops all airflow models.

    :param connection: SQLAlchemy Connection
    :return: None
    """
    from airflow.models.base import Base

    # Drop connection and chart - those tables have been deleted and in case you
    # run resetdb on schema with chart or users table will fail
    chart = Table('chart', Base.metadata)
    chart.drop(settings.engine, checkfirst=True)
    user = Table('user', Base.metadata)
    user.drop(settings.engine, checkfirst=True)
    users = Table('users', Base.metadata)
    users.drop(settings.engine, checkfirst=True)
    dag_stats = Table('dag_stats', Base.metadata)
    dag_stats.drop(settings.engine, checkfirst=True)

    Base.metadata.drop_all(connection)
    # we remove the Tables here so that if resetdb is run metadata does not keep the old tables.
    Base.metadata.remove(dag_stats)
    Base.metadata.remove(users)
    Base.metadata.remove(user)
    Base.metadata.remove(chart)
    # alembic adds significant import time, so we import it lazily
    from alembic.migration import MigrationContext

    migration_ctx = MigrationContext.configure(connection)
    version = migration_ctx._version
    if version.exists(connection):
        version.drop(connection)


def drop_flask_models(connection):
    """
    Drops all Flask models.

    :param connection: SQLAlchemy Connection
    :return: None
    """
    from flask_appbuilder.models.sqla import Base

    Base.metadata.drop_all(connection)


@provide_session
def check(session=None):
    """
    Checks if the database works.

    :param session: session of the sqlalchemy
    """
    session.execute('select 1 as is_alive;')
    log.info("Connection successful.")
