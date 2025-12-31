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
from __future__ import annotations

import collections.abc
import contextlib
import enum
import itertools
import json
import logging
import os
import signal
import sys
import time
import traceback
import warnings
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from tempfile import gettempdir
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeVar,
    overload,
)

import attrs
from sqlalchemy import (
    Table,
    exc,
    func,
    inspect,
    literal,
    or_,
    select,
    text,
)

import airflow
from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import import_all_models
from airflow.utils import helpers
from airflow.utils.db_manager import RunDBManager
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import get_dialect_name

_USE_PSYCOPG3: bool
try:
    from importlib.util import find_spec

    import sqlalchemy
    from packaging.version import Version

    is_psycopg3 = find_spec("psycopg") is not None
    sqlalchemy_version = Version(sqlalchemy.__version__)
    is_sqla2 = (sqlalchemy_version.major, sqlalchemy_version.minor, sqlalchemy_version.micro) >= (2, 0, 0)

    _USE_PSYCOPG3 = is_psycopg3 and is_sqla2
except (ImportError, ModuleNotFoundError):
    _USE_PSYCOPG3 = False

if TYPE_CHECKING:
    from alembic.runtime.environment import EnvironmentContext
    from alembic.script import ScriptDirectory
    from sqlalchemy.engine import Row
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement, TextClause
    from sqlalchemy.sql.selectable import Select

    from airflow.models.connection import Connection
    from airflow.typing_compat import Self

    # TODO: Import this from sqlalchemy.orm instead when switching to SQLA 2.
    # https://docs.sqlalchemy.org/en/20/orm/mapping_api.html#sqlalchemy.orm.MappedClassProtocol
    class MappedClassProtocol(Protocol):
        """Protocol for SQLALchemy model base."""

        __tablename__: str


T = TypeVar("T")

log = logging.getLogger(__name__)

_REVISION_HEADS_MAP: dict[str, str] = {
    "2.6.2": "4bc4d934e2bc",
    "2.7.0": "405de8318b3a",
    "2.8.0": "10b52ebd31f7",
    "2.8.1": "88344c1d9134",
    "2.9.0": "1949afb29106",
    "2.9.2": "686269002441",
    "2.10.0": "22ed7efa9da2",
    "2.10.3": "5f2621c13b39",
    "3.0.0": "29ce7909c52b",
    "3.0.3": "fe199e1abd77",
    "3.1.0": "cc92b33c6709",
    "3.2.0": "658517c60c7f",
}

# Prefix used to identify tables holding data moved during migration.
AIRFLOW_MOVED_TABLE_PREFIX = "_airflow_moved"


@contextlib.contextmanager
def timeout_with_traceback(seconds, message="Operation timed out"):
    """
    Raise a TimeoutException after specified seconds.

    Logs the full call stack when timeout occurs.

    Note: This uses SIGALRM and only works on Unix systems (not Windows).
    """

    class TimeoutException(Exception):
        """Exception raised when a timeout occurs."""

    def timeout_handler(signum, frame):
        # Capture the full call stack
        stack_trace = "".join(traceback.format_stack(frame))

        # Log the timeout and stack trace
        log.error(
            "\n%s after %s seconds\nFull call stack at timeout:\n%s",
            message,
            seconds,
            stack_trace,
        )

        raise TimeoutException(message)

    # Set the signal handler
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        # Cancel the alarm and restore the old handler
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


@provide_session
def merge_conn(conn: Connection, session: Session = NEW_SESSION):
    """Add new Connection."""
    if not session.scalar(select(1).where(conn.__class__.conn_id == conn.conn_id)):
        session.add(conn)
        session.commit()


@provide_session
def add_default_pool_if_not_exists(session: Session = NEW_SESSION):
    """Add default pool if it does not exist."""
    from airflow.models.pool import Pool

    if not session.scalar(select(Pool.id).where(Pool.pool == Pool.DEFAULT_POOL_NAME)):
        default_pool = Pool(
            pool=Pool.DEFAULT_POOL_NAME,
            slots=conf.getint(section="core", key="default_pool_task_slot_count"),
            description="Default pool",
            include_deferred=False,
        )
        session.add(default_pool)
        session.commit()


@provide_session
def create_default_connections(session: Session = NEW_SESSION):
    """Create default Airflow connections."""
    conns = get_default_connections()

    for c in conns:
        merge_conn(c, session)


def get_default_connections():
    from airflow.models.connection import Connection

    conns = [
        Connection(
            conn_id="airflow_db",
            conn_type="mysql",
            host="mysql",
            login="root",
            password="",
            schema="airflow",
        ),
        Connection(
            conn_id="athena_default",
            conn_type="athena",
        ),
        Connection(
            conn_id="aws_default",
            conn_type="aws",
        ),
        Connection(
            conn_id="azure_batch_default",
            conn_type="azure_batch",
            login="<ACCOUNT_NAME>",
            password="",
            extra="""{"account_url": "<ACCOUNT_URL>"}""",
        ),
        Connection(
            conn_id="azure_cosmos_default",
            conn_type="azure_cosmos",
            extra='{"database_name": "<DATABASE_NAME>", "collection_name": "<COLLECTION_NAME>" }',
        ),
        Connection(
            conn_id="azure_data_explorer_default",
            conn_type="azure_data_explorer",
            host="https://<CLUSTER>.kusto.windows.net",
            extra="""{"auth_method": "<AAD_APP | AAD_APP_CERT | AAD_CREDS | AAD_DEVICE>",
                    "tenant": "<TENANT ID>", "certificate": "<APPLICATION PEM CERTIFICATE>",
                    "thumbprint": "<APPLICATION CERTIFICATE THUMBPRINT>"}""",
        ),
        Connection(
            conn_id="azure_data_lake_default",
            conn_type="azure_data_lake",
            extra='{"tenant": "<TENANT>", "account_name": "<ACCOUNTNAME>" }',
        ),
        Connection(
            conn_id="azure_default",
            conn_type="azure",
        ),
        Connection(
            conn_id="cassandra_default",
            conn_type="cassandra",
            host="cassandra",
            port=9042,
        ),
        Connection(
            conn_id="databricks_default",
            conn_type="databricks",
            host="localhost",
        ),
        Connection(
            conn_id="dingding_default",
            conn_type="http",
            host="",
            password="",
        ),
        Connection(
            conn_id="drill_default",
            conn_type="drill",
            host="localhost",
            port=8047,
            extra='{"dialect_driver": "drill+sadrill", "storage_plugin": "dfs"}',
        ),
        Connection(
            conn_id="druid_broker_default",
            conn_type="druid",
            host="druid-broker",
            port=8082,
            extra='{"endpoint": "druid/v2/sql"}',
        ),
        Connection(
            conn_id="druid_ingest_default",
            conn_type="druid",
            host="druid-overlord",
            port=8081,
            extra='{"endpoint": "druid/indexer/v1/task"}',
        ),
        Connection(
            conn_id="elasticsearch_default",
            conn_type="elasticsearch",
            host="localhost",
            schema="http",
            port=9200,
        ),
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
        Connection(
            conn_id="fs_default",
            conn_type="fs",
            extra='{"path": "/"}',
        ),
        Connection(
            conn_id="ftp_default",
            conn_type="ftp",
            host="localhost",
            port=21,
            login="airflow",
            password="airflow",
            extra='{"key_file": "~/.ssh/id_rsa", "no_host_key_check": true}',
        ),
        Connection(
            conn_id="google_cloud_default",
            conn_type="google_cloud_platform",
            schema="default",
        ),
        Connection(
            conn_id="gremlin_default",
            conn_type="gremlin",
            host="gremlin",
            port=8182,
        ),
        Connection(
            conn_id="hive_cli_default",
            conn_type="hive_cli",
            port=10000,
            host="localhost",
            extra='{"use_beeline": true, "auth": ""}',
            schema="default",
        ),
        Connection(
            conn_id="hiveserver2_default",
            conn_type="hiveserver2",
            host="localhost",
            schema="default",
            port=10000,
        ),
        Connection(
            conn_id="http_default",
            conn_type="http",
            host="https://www.httpbin.org/",
        ),
        Connection(
            conn_id="iceberg_default",
            conn_type="iceberg",
            host="https://api.iceberg.io/ws/v1",
        ),
        Connection(conn_id="impala_default", conn_type="impala", host="localhost", port=21050),
        Connection(
            conn_id="kafka_default",
            conn_type="kafka",
            extra=json.dumps({"bootstrap.servers": "broker:29092", "group.id": "my-group"}),
        ),
        Connection(
            conn_id="kubernetes_default",
            conn_type="kubernetes",
        ),
        Connection(
            conn_id="kylin_default",
            conn_type="kylin",
            host="localhost",
            port=7070,
            login="ADMIN",
            password="KYLIN",
        ),
        Connection(
            conn_id="leveldb_default",
            conn_type="leveldb",
            host="localhost",
        ),
        Connection(conn_id="livy_default", conn_type="livy", host="livy", port=8998),
        Connection(
            conn_id="local_mysql",
            conn_type="mysql",
            host="localhost",
            login="airflow",
            password="airflow",
            schema="airflow",
        ),
        Connection(
            conn_id="metastore_default",
            conn_type="hive_metastore",
            host="localhost",
            extra='{"authMechanism": "PLAIN"}',
            port=9083,
        ),
        Connection(conn_id="mongo_default", conn_type="mongo", host="mongo", port=27017),
        Connection(
            conn_id="mssql_default",
            conn_type="mssql",
            host="localhost",
            port=1433,
        ),
        Connection(
            conn_id="mysql_default",
            conn_type="mysql",
            login="root",
            schema="airflow",
            host="mysql",
        ),
        Connection(
            conn_id="opensearch_default",
            conn_type="opensearch",
            host="localhost",
            schema="http",
            port=9200,
        ),
        Connection(
            conn_id="opsgenie_default",
            conn_type="http",
            host="",
            password="",
        ),
        Connection(
            conn_id="oracle_default",
            conn_type="oracle",
            host="localhost",
            login="root",
            password="password",
            schema="schema",
            port=1521,
        ),
        Connection(
            conn_id="oss_default",
            conn_type="oss",
            extra="""
                {
                "auth_type": "AK",
                "access_key_id": "<ACCESS_KEY_ID>",
                "access_key_secret": "<ACCESS_KEY_SECRET>",
                "region": "<YOUR_OSS_REGION>"}
                """,
        ),
        Connection(
            conn_id="pig_cli_default",
            conn_type="pig_cli",
            schema="default",
        ),
        Connection(
            conn_id="pinot_admin_default",
            conn_type="pinot",
            host="localhost",
            port=9000,
        ),
        Connection(
            conn_id="pinot_broker_default",
            conn_type="pinot",
            host="localhost",
            port=9000,
            extra='{"endpoint": "/query", "schema": "http"}',
        ),
        Connection(
            conn_id="postgres_default",
            conn_type="postgres",
            login="postgres",
            password="airflow",
            schema="airflow",
            host="postgres",
        ),
        Connection(
            conn_id="presto_default",
            conn_type="presto",
            host="localhost",
            schema="hive",
            port=3400,
        ),
        Connection(
            conn_id="qdrant_default",
            conn_type="qdrant",
            host="qdrant",
            port=6333,
        ),
        Connection(
            conn_id="redis_default",
            conn_type="redis",
            host="redis",
            port=6379,
            extra='{"db": 0}',
        ),
        Connection(
            conn_id="redshift_default",
            conn_type="redshift",
            extra="""
{
    "iam": true,
    "cluster_identifier": "<REDSHIFT_CLUSTER_IDENTIFIER>",
    "port": 5439,
    "profile": "default",
    "db_user": "awsuser",
    "database": "dev",
    "region": ""
}""",
        ),
        Connection(
            conn_id="salesforce_default",
            conn_type="salesforce",
            login="username",
            password="password",
            extra='{"security_token": "security_token"}',
        ),
        Connection(
            conn_id="segment_default",
            conn_type="segment",
            extra='{"write_key": "my-segment-write-key"}',
        ),
        Connection(
            conn_id="sftp_default",
            conn_type="sftp",
            host="localhost",
            port=22,
            login="airflow",
            extra='{"key_file": "~/.ssh/id_rsa", "no_host_key_check": true}',
        ),
        Connection(
            conn_id="spark_default",
            conn_type="spark",
            host="yarn",
            extra='{"queue": "root.default"}',
        ),
        Connection(
            conn_id="sqlite_default",
            conn_type="sqlite",
            host=os.path.join(gettempdir(), "sqlite_default.db"),
        ),
        Connection(
            conn_id="ssh_default",
            conn_type="ssh",
            host="localhost",
        ),
        Connection(
            conn_id="tableau_default",
            conn_type="tableau",
            host="https://tableau.server.url",
            login="user",
            password="password",
            extra='{"site_id": "my_site"}',
        ),
        Connection(
            conn_id="teradata_default",
            conn_type="teradata",
            host="localhost",
            login="user",
            password="password",
            schema="schema",
        ),
        Connection(
            conn_id="trino_default",
            conn_type="trino",
            host="localhost",
            schema="hive",
            port=3400,
        ),
        Connection(
            conn_id="vertica_default",
            conn_type="vertica",
            host="localhost",
            port=5433,
        ),
        Connection(
            conn_id="wasb_default",
            conn_type="wasb",
            extra='{"sas_token": null}',
        ),
        Connection(
            conn_id="webhdfs_default",
            conn_type="hdfs",
            host="localhost",
            port=50070,
        ),
        Connection(
            conn_id="yandexcloud_default",
            conn_type="yandexcloud",
            schema="default",
        ),
        Connection(
            conn_id="ydb_default",
            conn_type="ydb",
            host="grpc://localhost",
            port=2135,
            extra={"database": "/local"},
        ),
    ]
    return conns


class AutocommitEngineForMySQL:
    """
    Context manager to temporarily use AUTOCOMMIT isolation level for MySQL.

    This is needed to work around MySQL 8.4 metadata lock issues with SQLAlchemy 2.0.
    """

    def __init__(self):
        self.is_mysql = settings.SQL_ALCHEMY_CONN and settings.SQL_ALCHEMY_CONN.lower().startswith("mysql")
        self.original_prepare_engine_args = None

    def __enter__(self):
        if not self.is_mysql:
            return self

        log.info("Entering AUTOCOMMIT mode for MySQL DDL operations")

        # Save and replace prepare_engine_args
        self.original_prepare_engine_args = settings.prepare_engine_args

        def autocommit_prepare_engine_args(disable_connection_pool=False, pool_class=None):
            # Call with keyword arguments to preserve the calling convention
            args = self.original_prepare_engine_args(
                disable_connection_pool=disable_connection_pool, pool_class=pool_class
            )
            args["isolation_level"] = "AUTOCOMMIT"
            return args

        settings.prepare_engine_args = autocommit_prepare_engine_args

        # Recreate engine with AUTOCOMMIT
        settings.dispose_orm(do_log=False)
        settings.configure_orm()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.is_mysql:
            return

        log.info("Exiting AUTOCOMMIT mode, restoring normal transaction engine")

        # Restore original function
        settings.prepare_engine_args = self.original_prepare_engine_args

        # Recreate engine with normal settings
        settings.dispose_orm(do_log=False)
        settings.configure_orm()


def _create_db_from_orm(session):
    """Create database tables from ORM models and stamp alembic version."""
    from airflow.models.base import Base

    log.info("Creating Airflow database tables from the ORM")

    # Debug setup if requested
    _setup_debug_logging_if_needed()

    log.info("Creating context")
    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
        log.info("Binding engine")
        engine = session.get_bind().engine
        log.info("Pool status: %s", engine.pool.status())

        log.info("Creating metadata")
        Base.metadata.create_all(engine)

        log.info("Getting alembic config")
        config = _get_alembic_config()

        # Use AUTOCOMMIT for DDL to avoid metadata lock issues
        with AutocommitEngineForMySQL():  # TODO: enable for sqlite too
            from alembic import command

            log.info("Stamping migration head")
            command.stamp(config, "head")

        log.info("Airflow database tables created")


def _setup_debug_logging_if_needed():
    """Set up debug logging and stack trace dumping if SQLALCHEMY_ENGINE_DEBUG is set."""
    if not os.environ.get("SQLALCHEMY_ENGINE_DEBUG"):
        return

    import faulthandler
    import threading

    # Enable SQLA debug logging
    logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

    # Enable Fault Handler
    faulthandler.enable(file=sys.stderr, all_threads=True)

    # Print Active Threads and Stack Traces Periodically
    def dump_stacks():
        while True:
            for thread_id, frame in sys._current_frames().items():
                log.info("\nThread %s stack:", thread_id)
                traceback.print_stack(frame)
            time.sleep(300)

    threading.Thread(target=dump_stacks, daemon=True).start()


@provide_session
def initdb(session: Session = NEW_SESSION):
    """Initialize Airflow database."""
    # First validate external DB managers before running migration
    external_db_manager = RunDBManager()
    external_db_manager.validate()

    import_all_models()

    db_exists = _get_current_revision(session)
    with timeout_with_traceback(60 * 20, "DB upgrade/creation timed out."):
        if db_exists:
            upgradedb(session=session)
        else:
            _create_db_from_orm(session=session)

    external_db_manager.initdb(session)
    # Add default pool & sync log_template
    add_default_pool_if_not_exists(session=session)
    synchronize_log_template(session=session)


def _get_alembic_config():
    from alembic.config import Config

    package_dir = os.path.dirname(airflow.__file__)
    directory = os.path.join(package_dir, "migrations")
    alembic_file = conf.get("database", "alembic_ini_file_path")
    if os.path.isabs(alembic_file):
        config = Config(alembic_file)
    else:
        config = Config(os.path.join(package_dir, alembic_file))
    config.set_main_option("script_location", directory.replace("%", "%%"))
    config.set_main_option("sqlalchemy.url", settings.SQL_ALCHEMY_CONN.replace("%", "%%"))
    return config


def _get_script_object(config=None) -> ScriptDirectory:
    from alembic.script import ScriptDirectory

    if not config:
        config = _get_alembic_config()
    return ScriptDirectory.from_config(config)


def _get_current_revision(session):
    from alembic.migration import MigrationContext

    conn = session.connection()

    migration_ctx = MigrationContext.configure(conn)

    return migration_ctx.get_current_revision()


def check_migrations(timeout):
    """
    Wait for all airflow migrations to complete.

    :param timeout: Timeout for the migration in seconds
    :return: None
    """
    timeout = timeout or 1  # run the loop at least 1
    external_db_manager = RunDBManager()
    with _configured_alembic_environment() as env:
        context = env.get_context()
        source_heads = None
        db_heads = None
        for ticker in range(timeout):
            source_heads = set(env.script.get_heads())
            db_heads = set(context.get_current_heads())
            if source_heads == db_heads and external_db_manager.check_migration(settings.Session()):
                return
            time.sleep(1)
            log.info("Waiting for migrations... %s second(s)", ticker)
        raise TimeoutError(
            f"There are still unapplied migrations after {timeout} seconds. Migration"
            f"Head(s) in DB: {db_heads} | Migration Head(s) in Source Code: {source_heads}"
        )


@contextlib.contextmanager
def _configured_alembic_environment() -> Generator[EnvironmentContext, None, None]:
    from alembic.runtime.environment import EnvironmentContext

    config = _get_alembic_config()
    script = _get_script_object(config)

    with (
        EnvironmentContext(
            config,
            script,
        ) as env,
        settings.get_engine().connect() as connection,
    ):
        alembic_logger = logging.getLogger("alembic")
        level = alembic_logger.level
        alembic_logger.setLevel(logging.WARNING)
        env.configure(connection)
        alembic_logger.setLevel(level)

        yield env


def check_and_run_migrations():
    """Check and run migrations if necessary. Only use in a tty."""
    with _configured_alembic_environment() as env:
        context = env.get_context()
        source_heads = set(env.script.get_heads())
        db_heads = set(context.get_current_heads())
        db_command = None
        command_name = None
        verb = None
    if len(db_heads) < 1:
        db_command = initdb
        command_name = "migrate"
        verb = "initialize"
    elif source_heads != db_heads:
        db_command = upgradedb
        command_name = "migrate"
        verb = "migrate"

    if sys.stdout.isatty() and verb:
        print()
        question = f"Please confirm database {verb} (or wait 4 seconds to skip it). Are you sure? [y/N]"
        try:
            answer = helpers.prompt_with_timeout(question, timeout=4, default=False)
            if answer:
                try:
                    db_command()
                    print(f"DB {verb} done")
                except Exception as error:
                    from airflow.version import version

                    print(error)
                    print(
                        "You still have unapplied migrations. "
                        f"You may need to {verb} the database by running `airflow db {command_name}`. ",
                        f"Make sure the command is run using Airflow version {version}.",
                        file=sys.stderr,
                    )
                    sys.exit(1)
        except AirflowException:
            pass
    elif source_heads != db_heads:
        from airflow.version import version

        print(
            f"ERROR: You need to {verb} the database. Please run `airflow db {command_name}`. "
            f"Make sure the command is run using Airflow version {version}.",
            file=sys.stderr,
        )
        sys.exit(1)


@provide_session
def synchronize_log_template(*, session: Session = NEW_SESSION) -> None:
    """
    Synchronize log template configs with table.

    This checks if the last row fully matches the current config values, and
    insert a new row if not.
    """
    # NOTE: SELECT queries in this function are INTENTIONALLY written with the
    # SQL builder style, not the ORM query API. This avoids configuring the ORM
    # unless we need to insert something, speeding up CLI in general.

    from airflow.models.tasklog import LogTemplate

    metadata = reflect_tables([LogTemplate], session)
    log_template_table: Table | None = metadata.tables.get(LogTemplate.__tablename__)

    if log_template_table is None:
        log.info("Log template table does not exist (added in 2.3.0); skipping log template sync.")
        return

    filename = conf.get("logging", "log_filename_template")
    elasticsearch_id = conf.get("elasticsearch", "log_id_template")

    stored = session.execute(
        select(
            log_template_table.c.filename,
            log_template_table.c.elasticsearch_id,
        )
        .order_by(log_template_table.c.id.desc())
        .limit(1)
    ).first()

    # If we have an empty table, and the default values exist, we will seed the
    # table with values from pre 2.3.0, so old logs will still be retrievable.
    if not stored:
        is_default_log_id = elasticsearch_id == conf.get_default_value("elasticsearch", "log_id_template")
        is_default_filename = filename == conf.get_default_value("logging", "log_filename_template")
        if is_default_log_id and is_default_filename:
            session.add(
                LogTemplate(
                    filename="{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log",
                    elasticsearch_id="{dag_id}-{task_id}-{logical_date}-{try_number}",
                )
            )

    # Before checking if the _current_ value exists, we need to check if the old config value we upgraded in
    # place exists!
    pre_upgrade_filename = conf.upgraded_values.get(("logging", "log_filename_template"), filename)
    pre_upgrade_elasticsearch_id = conf.upgraded_values.get(
        ("elasticsearch", "log_id_template"), elasticsearch_id
    )
    if pre_upgrade_filename != filename or pre_upgrade_elasticsearch_id != elasticsearch_id:
        # The previous non-upgraded value likely won't be the _latest_ value (as after we've recorded the
        # recorded the upgraded value it will be second-to-newest), so we'll have to just search which is okay
        # as this is a table with a tiny number of rows
        row = session.execute(
            select(log_template_table.c.id)
            .where(
                or_(
                    log_template_table.c.filename == pre_upgrade_filename,
                    log_template_table.c.elasticsearch_id == pre_upgrade_elasticsearch_id,
                )
            )
            .order_by(log_template_table.c.id.desc())
            .limit(1)
        ).first()
        if not row:
            session.add(
                LogTemplate(filename=pre_upgrade_filename, elasticsearch_id=pre_upgrade_elasticsearch_id)
            )

    if not stored or stored.filename != filename or stored.elasticsearch_id != elasticsearch_id:
        session.add(LogTemplate(filename=filename, elasticsearch_id=elasticsearch_id))


def reflect_tables(tables: list[MappedClassProtocol | str] | None, session):
    """
    When running checks prior to upgrades, we use reflection to determine current state of the database.

    This function gets the current state of each table in the set of models
    provided and returns a SqlAlchemy metadata object containing them.
    """
    import sqlalchemy.schema

    bind = session.bind
    metadata = sqlalchemy.schema.MetaData()

    if tables is None:
        metadata.reflect(bind=bind, resolve_fks=False)
    else:
        for tbl in tables:
            try:
                table_name = tbl if isinstance(tbl, str) else tbl.__tablename__
                metadata.reflect(bind=bind, only=[table_name], extend_existing=True, resolve_fks=False)
            except exc.InvalidRequestError:
                continue
    return metadata


@provide_session
def _check_migration_errors(session: Session = NEW_SESSION) -> Iterable[str]:
    """:session: session of the sqlalchemy."""
    check_functions: Iterable[Callable[..., Iterable[str]]] = ()
    for check_fn in check_functions:
        log.debug("running check function %s", check_fn.__name__)
        yield from check_fn(session=session)


def _offline_migration(migration_func: Callable, config, revision):
    """
    Run offline migration.

    :meta private:
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        logging.disable(logging.CRITICAL)
        migration_func(config, revision, sql=True)
        logging.disable(logging.NOTSET)


def print_happy_cat(message):
    if sys.stdout.isatty():
        size = os.get_terminal_size().columns
    else:
        size = 0
    print(message.center(size))
    print("""/\\_/\\""".center(size))
    print("""(='_' )""".center(size))
    print("""(,(") (")""".center(size))
    print("""^^^""".center(size))
    return


def _revision_greater(config, this_rev, base_rev):
    # Check if there is history between the revisions and the start revision
    # This ensures that the revisions are above `min_revision`
    script = _get_script_object(config)
    try:
        list(script.revision_map.iterate_revisions(upper=this_rev, lower=base_rev))
        return True
    except Exception:
        return False


def _revisions_above_min_for_offline(config, revisions) -> None:
    """
    Check that all supplied revision ids are above the minimum revision for the dialect.

    :param config: Alembic config
    :param revisions: list of Alembic revision ids
    :return: None
    """
    dbname = settings.get_engine().dialect.name
    if dbname == "sqlite":
        raise SystemExit("Offline migration not supported for SQLite.")
    min_version, min_revision = ("2.7.0", "937cbd173ca1")

    # Check if there is history between the revisions and the start revision
    # This ensures that the revisions are above `min_revision`
    for rev in revisions:
        if not _revision_greater(config, rev, min_revision):
            raise ValueError(
                f"Error while checking history for revision range {min_revision}:{rev}. "
                f"Check that {rev} is a valid revision. "
                f"For dialect {dbname!r}, supported revision for offline migration is from {min_revision} "
                f"which corresponds to Airflow {min_version}."
            )


@provide_session
def upgradedb(
    *,
    to_revision: str | None = None,
    from_revision: str | None = None,
    show_sql_only: bool = False,
    session: Session = NEW_SESSION,
):
    """
    Upgrades the DB.

    :param to_revision: Optional Alembic revision ID to upgrade *to*.
        If omitted, upgrades to latest revision.
    :param from_revision: Optional Alembic revision ID to upgrade *from*.
        Not compatible with ``sql_only=False``.
    :param show_sql_only: if True, migration statements will be printed but not executed.
    :param session: sqlalchemy session with connection to Airflow metadata database
    :return: None
    """
    if from_revision and not show_sql_only:
        raise AirflowException("`from_revision` only supported with `sql_only=True`.")

    # alembic adds significant import time, so we import it lazily
    if not settings.SQL_ALCHEMY_CONN:
        raise RuntimeError("The settings.SQL_ALCHEMY_CONN not set. This is a critical assertion.")
    from alembic import command

    import_all_models()

    config = _get_alembic_config()

    if show_sql_only:
        if not from_revision:
            from_revision = _get_current_revision(session)

        if not to_revision:
            script = _get_script_object()
            to_revision = script.get_current_head()

        if to_revision == from_revision:
            print_happy_cat("No migrations to apply; nothing to do.")
            return

        _revisions_above_min_for_offline(config=config, revisions=[from_revision, to_revision])

        _offline_migration(command.upgrade, config, f"{from_revision}:{to_revision}")
        return  # only running sql; our job is done

    errors_seen = False
    for err in _check_migration_errors(session=session):
        if not errors_seen:
            log.error("Automatic migration is not available")
            errors_seen = True
        log.error("%s", err)

    if errors_seen:
        exit(1)

    if not _get_current_revision(session=session) and not to_revision:
        # Don't load default connections
        # New DB; initialize and exit
        initdb(session=session)
        return
    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
        import sqlalchemy.pool

        log.info("Migrating the Airflow database")
        val = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_SIZE")
        try:
            # Reconfigure the ORM to use _EXACTLY_ one connection, otherwise some db engines hang forever
            # trying to ALTER TABLEs
            os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_SIZE"] = "1"
            settings.reconfigure_orm(pool_class=sqlalchemy.pool.SingletonThreadPool)
            command.upgrade(config, revision=to_revision or "heads")
            current_revision = _get_current_revision(session=session)
            with _configured_alembic_environment() as env:
                source_heads = env.script.get_heads()
            if current_revision == source_heads[0]:
                # Only run external DB upgrade migration if user upgraded to heads
                external_db_manager = RunDBManager()
                external_db_manager.upgradedb(session)

        finally:
            if val is None:
                os.environ.pop("AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_SIZE")
            else:
                os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_SIZE"] = val
            settings.reconfigure_orm()

        add_default_pool_if_not_exists(session=session)
        synchronize_log_template(session=session)


@provide_session
def resetdb(session: Session = NEW_SESSION, skip_init: bool = False):
    """Clear out the database."""
    if not settings.engine:
        raise RuntimeError("The settings.engine must be set. This is a critical assertion")
    log.info("Dropping Airflow tables that exist")

    import_all_models()

    connection = settings.engine.connect()

    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS), connection.begin():
        drop_airflow_models(connection)
        drop_airflow_moved_tables(connection)
        log.info("Dropped all Airflow tables")
        external_db_manager = RunDBManager()
        external_db_manager.drop_tables(session, connection)

    if not skip_init:
        initdb(session=session)


@provide_session
def downgrade(*, to_revision, from_revision=None, show_sql_only=False, session: Session = NEW_SESSION):
    """
    Downgrade the airflow metastore schema to a prior version.

    :param to_revision: The alembic revision to downgrade *to*.
    :param show_sql_only: if True, print sql statements but do not run them
    :param from_revision: if supplied, alembic revision to dawngrade *from*. This may only
        be used in conjunction with ``sql=True`` because if we actually run the commands,
        we should only downgrade from the *current* revision.
    :param session: sqlalchemy session for connection to airflow metadata database
    """
    if from_revision and not show_sql_only:
        raise ValueError(
            "`from_revision` can't be combined with `sql=False`. When actually "
            "applying a downgrade (instead of just generating sql), we always "
            "downgrade from current revision."
        )

    if not settings.SQL_ALCHEMY_CONN:
        raise RuntimeError("The settings.SQL_ALCHEMY_CONN not set.")

    # alembic adds significant import time, so we import it lazily
    from alembic import command

    log.info("Attempting downgrade to revision %s", to_revision)
    config = _get_alembic_config()
    # If downgrading to less than 3.0.0, we need to handle the FAB provider
    if _revision_greater(config, _REVISION_HEADS_MAP["2.10.3"], to_revision):
        _handle_fab_downgrade(session=session)
    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
        if show_sql_only:
            log.warning("Generating sql scripts for manual migration.")
            if not from_revision:
                from_revision = _get_current_revision(session)
            revision_range = f"{from_revision}:{to_revision}"
            _offline_migration(command.downgrade, config=config, revision=revision_range)
        else:
            log.info("Applying downgrade migrations to Airflow database.")
            command.downgrade(config, revision=to_revision, sql=show_sql_only)


def _get_fab_migration_version(*, session: Session) -> str | None:
    """
    Get the current FAB migration version from the database.

    This intentionally queries the db directly, as the FAB provider and FABDBManager may not even be installed.

    :param session: sqlalchemy session for connection to airflow metadata database
    :return: The current FAB migration revision, or None if not found
    """
    try:
        result = session.execute(text("SELECT version_num FROM alembic_version_fab LIMIT 1"))
        row = result.fetchone()
        return row[0] if row else None
    except Exception:
        # Table might not exist or other database error
        return None


def _handle_fab_downgrade(*, session: Session) -> None:
    """
    Handle FAB downgrade requirements for downgrades to Airflow versions < 3.0.0.

    First, checks if the FAB db version matches the known version from 1.4.0.
    If it matches, no FAB db tables need to be touched.
    Otherwise, imports the FABDBManager and calls its downgrade method.

    :param session: sqlalchemy session for connection to airflow metadata database
    :raises RuntimeError: If FAB provider is required but cannot be imported
    """
    fab_version = _get_fab_migration_version(session=session)
    if fab_version == "6709f7a774b9":  # 1.4.0
        # FAB version matches - we can proceed without touching the FAB db tables
        log.info(
            "FAB migration version %s matches known version from 1.4.0. "
            "FAB provider is not required for downgrade.",
            fab_version,
        )
        return
    connection = settings.get_engine().connect()
    insp = inspect(connection)
    if not fab_version and insp.has_table("ab_user"):
        log.info(
            "FAB migration version not found, but FAB tables exist. "
            "FAB provider is not required for downgrade.",
        )
        return

    # FAB db version is different or not found - require the FAB provider
    try:
        from airflow.providers.fab.auth_manager.models.db import FABDBManager
    except ImportError:
        raise RuntimeError(
            "Import error occurred while importing FABDBManager. The apache-airflow-provider-fab package must be installed before we can "
            "downgrade to <3.0.0."
        )
    dbm = FABDBManager(session)
    if hasattr(dbm, "reset_to_2_x"):
        dbm.reset_to_2_x()
    else:
        # Older version before we added that function, it only has a single migration so we can just create the tables
        # to ensure they are there
        dbm.create_db_from_orm()


def drop_airflow_models(connection):
    """
    Drop all airflow models.

    :param connection: SQLAlchemy Connection
    :return: None
    """
    from airflow.models.base import Base

    Base.metadata.drop_all(connection)
    # alembic adds significant import time, so we import it lazily
    from alembic.migration import MigrationContext

    migration_ctx = MigrationContext.configure(connection)
    version = migration_ctx._version
    if inspect(connection).has_table(version.name):
        version.drop(connection)


def drop_airflow_moved_tables(connection):
    from airflow.models.base import Base

    tables = set(inspect(connection).get_table_names())
    to_delete = [Table(x, Base.metadata) for x in tables if x.startswith(AIRFLOW_MOVED_TABLE_PREFIX)]
    for tbl in to_delete:
        tbl.drop(settings.engine, checkfirst=False)
        Base.metadata.remove(tbl)


@provide_session
def check(session: Session = NEW_SESSION):
    """
    Check if the database works.

    :param session: session of the sqlalchemy
    """
    session.execute(text("select 1 as is_alive;"))
    log.info("Connection successful.")


@enum.unique
class DBLocks(enum.IntEnum):
    """
    Cross-db Identifiers for advisory global database locks.

    Postgres uses int64 lock ids so we use the integer value, MySQL uses names, so we
    call ``str()`, which is implemented using the ``_name_`` field.
    """

    MIGRATIONS = enum.auto()
    SCHEDULER_CRITICAL_SECTION = enum.auto()

    def __str__(self):
        return f"airflow_{self._name_}"


@contextlib.contextmanager
def create_global_lock(
    session: Session,
    lock: DBLocks,
    lock_timeout: int = 1800,
) -> Generator[None, None, None]:
    """Contextmanager that will create and teardown a global db lock."""
    bind = session.get_bind()
    if hasattr(bind, "connect"):
        conn = bind.connect()
    else:
        conn = bind
    dialect_name = get_dialect_name(session)
    try:
        if dialect_name == "postgresql":
            if _USE_PSYCOPG3:
                # psycopg3 doesn't support parameters for `SET`. Use `set_config` instead.
                # The timeout value must be passed as a string of milliseconds.
                conn.execute(
                    text("SELECT set_config('lock_timeout', :timeout, false)"),
                    {"timeout": str(lock_timeout)},
                )
                conn.execute(text("SELECT pg_advisory_lock(:id)"), {"id": lock.value})
            else:
                conn.execute(text("SET LOCK_TIMEOUT to :timeout"), {"timeout": lock_timeout})
                conn.execute(text("SELECT pg_advisory_lock(:id)"), {"id": lock.value})
        elif (
            dialect_name == "mysql"
            and conn.dialect.server_version_info
            and conn.dialect.server_version_info >= (5, 6)
        ):
            conn.execute(text("SELECT GET_LOCK(:id, :timeout)"), {"id": str(lock), "timeout": lock_timeout})

        yield
    finally:
        if dialect_name == "postgresql":
            if _USE_PSYCOPG3:
                # Use set_config() to reset the timeout to its default (0 = off/wait forever).
                conn.execute(text("SELECT set_config('lock_timeout', '0', false)"))
            else:
                conn.execute(text("SET LOCK_TIMEOUT TO DEFAULT"))
            result = conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": lock.value}).fetchone()
            if result is None:
                raise RuntimeError("Error releasing DB lock!")
            (unlocked,) = result
            if not unlocked:
                raise RuntimeError("Error releasing DB lock!")
        elif (
            dialect_name == "mysql"
            and conn.dialect.server_version_info
            and conn.dialect.server_version_info >= (5, 6)
        ):
            conn.execute(text("select RELEASE_LOCK(:id)"), {"id": str(lock)})


def compare_type(context, inspected_column, metadata_column, inspected_type, metadata_type):
    """
    Compare types between ORM and DB .

    return False if the metadata_type is the same as the inspected_type
    or None to allow the default implementation to compare these
    types. a return value of True means the two types do not
    match and should result in a type change operation.
    """
    if context.dialect.name == "mysql":
        from sqlalchemy import String
        from sqlalchemy.dialects import mysql

        if isinstance(inspected_type, mysql.VARCHAR) and isinstance(metadata_type, String):
            # This is a hack to get around MySQL VARCHAR collation
            # not being possible to change from utf8_bin to utf8mb3_bin.
            # We only make sure lengths are the same
            if inspected_type.length != metadata_type.length:
                return True
            return False
    return None


def compare_server_default(
    context, inspected_column, metadata_column, inspected_default, metadata_default, rendered_metadata_default
):
    """
    Compare server defaults between ORM and DB .

    return True if the defaults are different, False if not, or None to allow the default implementation
    to compare these defaults

    In SQLite: task_instance.map_index & task_reschedule.map_index
    are not comparing accurately. Sometimes they are equal, sometimes they are not.
    Alembic warned that this feature has varied accuracy depending on backends.
    See: (https://alembic.sqlalchemy.org/en/latest/api/runtime.html#alembic.runtime.environment.EnvironmentContext.configure.params.compare_server_default)
    """
    dialect_name = context.connection.dialect.name
    if dialect_name in ["sqlite"]:
        return False
    if (
        dialect_name == "mysql"
        and metadata_column.name == "pool_slots"
        and metadata_column.table.name == "task_instance"
    ):
        # We removed server_default value in ORM to avoid expensive migration
        # (it was removed in postgres DB in migration head 7b2661a43ba3 ).
        # As a side note, server default value here was only actually needed for the migration
        # where we added the column in the first place -- now that it exists and all
        # existing rows are populated with a value this server default is never used.
        return False
    return None


def get_sqla_model_classes():
    """
    Get all SQLAlchemy class mappers.

    SQLAlchemy < 1.4 does not support registry.mappers so we use
    try/except to handle it.
    """
    from airflow.models.base import Base

    try:
        return [mapper.class_ for mapper in Base.registry.mappers]
    except AttributeError:
        return Base._decl_class_registry.values()


def get_query_count(query_stmt: Select, *, session: Session) -> int:
    """
    Get count of a query.

    A SELECT COUNT() FROM is issued against the subquery built from the
    given statement. The ORDER BY clause is stripped from the statement
    since it's unnecessary for COUNT, and can impact query planning and
    degrade performance.

    :meta private:
    """
    count_stmt = select(func.count()).select_from(query_stmt.order_by(None).subquery())
    result = session.scalar(count_stmt)
    return result or 0


async def get_query_count_async(statement: Select, *, session: AsyncSession) -> int:
    """
    Get count of a query.

    A SELECT COUNT() FROM is issued against the subquery built from the
    given statement. The ORDER BY clause is stripped from the statement
    since it's unnecessary for COUNT, and can impact query planning and
    degrade performance.

    :meta private:
    """
    count_stmt = select(func.count()).select_from(statement.order_by(None).subquery())
    result = await session.scalar(count_stmt)
    return result or 0


def check_query_exists(query_stmt: Select, *, session: Session) -> bool:
    """
    Check whether there is at least one row matching a query.

    A SELECT 1 FROM is issued against the subquery built from the given
    statement. The ORDER BY clause is stripped from the statement since it's
    unnecessary, and can impact query planning and degrade performance.

    :meta private:
    """
    count_stmt = select(literal(True)).select_from(query_stmt.order_by(None).subquery())
    # we must cast to bool because scalar() can return None
    return bool(session.scalar(count_stmt))


def exists_query(*where: ColumnElement[bool], session: Session) -> bool:
    """
    Check whether there is at least one row matching given clauses.

    This does a SELECT 1 WHERE ... LIMIT 1 and check the result.

    :meta private:
    """
    stmt = select(literal(True)).where(*where).limit(1)
    return session.scalar(stmt) is not None


@attrs.define(slots=True)
class LazySelectSequence(Sequence[T]):
    """
    List-like interface to lazily access a database model query.

    The intended use case is inside a task execution context, where we manage an
    active SQLAlchemy session in the background.

    This is an abstract base class. Each use case should subclass, and implement
    the following static methods:

    * ``_rebuild_select`` is called when a lazy sequence is unpickled. Since it
      is not easy to pickle SQLAlchemy constructs, this class serializes the
      SELECT statements into plain text to storage. This method is called on
      deserialization to convert the textual clause back into an ORM SELECT.
    * ``_process_row`` is called when an item is accessed. The lazy sequence
      uses ``session.execute()`` to fetch rows from the database, and this
      method should know how to process each row into a value.

    :meta private:
    """

    _select_asc: Select
    _select_desc: Select
    _session: Session
    _len: int | None = attrs.field(init=False, default=None)

    @classmethod
    def from_select(
        cls,
        select: Select,
        *,
        order_by: Sequence[ColumnElement],
        session: Session,
    ) -> Self:
        s1 = select
        for col in order_by:
            s1 = s1.order_by(col.asc())
        s2 = select
        for col in order_by:
            s2 = s2.order_by(col.desc())
        return cls(s1, s2, session=session)

    @staticmethod
    def _rebuild_select(stmt: TextClause) -> Select:
        """
        Rebuild a textual statement into an ORM-configured SELECT statement.

        This should do something like ``select(field).from_statement(stmt)`` to
        reconfigure ORM information to the textual SQL statement.
        """
        raise NotImplementedError

    @staticmethod
    def _process_row(row: Row) -> T:
        """Process a SELECT-ed row into the end value."""
        raise NotImplementedError

    def __repr__(self) -> str:
        counter = "item" if (length := len(self)) == 1 else "items"
        return f"LazySelectSequence([{length} {counter}])"

    def __str__(self) -> str:
        counter = "item" if (length := len(self)) == 1 else "items"
        return f"LazySelectSequence([{length} {counter}])"

    def __getstate__(self) -> Any:
        # We don't want to go to the trouble of serializing SQLAlchemy objects.
        # Converting the statement into a SQL string is the best we can get.
        # The literal_binds compile argument inlines all the values into the SQL
        # string to simplify cross-process commuinication as much as possible.
        # Theoratically we can do the same for count(), but I think it should be
        # performant enough to calculate only that eagerly.
        s1 = str(self._select_asc.compile(self._session.get_bind(), compile_kwargs={"literal_binds": True}))
        s2 = str(self._select_desc.compile(self._session.get_bind(), compile_kwargs={"literal_binds": True}))
        return (s1, s2, len(self))

    def __setstate__(self, state: Any) -> None:
        s1, s2, self._len = state
        self._select_asc = self._rebuild_select(text(s1))
        self._select_desc = self._rebuild_select(text(s2))

    def __bool__(self) -> bool:
        return check_query_exists(self._select_asc, session=self._session)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, collections.abc.Sequence):
            return NotImplemented
        z = itertools.zip_longest(iter(self), iter(other), fillvalue=object())
        return all(x == y for x, y in z)

    def __hash__(self):
        return hash(tuple(x for x in iter(self)))

    def __reversed__(self) -> Iterator[T]:
        return iter(self._process_row(r) for r in self._session.execute(self._select_desc))

    def __iter__(self) -> Iterator[T]:
        return iter(self._process_row(r) for r in self._session.execute(self._select_asc))

    def __len__(self) -> int:
        if self._len is None:
            self._len = get_query_count(self._select_asc, session=self._session)
        return self._len

    @overload
    def __getitem__(self, key: int) -> T: ...

    @overload
    def __getitem__(self, key: slice) -> Sequence[T]: ...

    def __getitem__(self, key: int | slice) -> T | Sequence[T]:
        if isinstance(key, int):
            if key >= 0:
                stmt = self._select_asc.offset(key)
            else:
                stmt = self._select_desc.offset(-1 - key)
            if (row := self._session.execute(stmt.limit(1)).one_or_none()) is None:
                raise IndexError(key)
            return self._process_row(row)
        if isinstance(key, slice):
            # This implements the slicing syntax. We want to optimize negative
            # slicing (e.g. seq[-10:]) by not doing an additional COUNT query
            # if possible. We can do this unless the start and stop have
            # different signs (i.e. one is positive and another negative).
            start, stop, reverse = _coerce_slice(key)
            if start >= 0:
                if stop is None:
                    stmt = self._select_asc.offset(start)
                elif stop >= 0:
                    stmt = self._select_asc.slice(start, stop)
                else:
                    stmt = self._select_asc.slice(start, len(self) + stop)
                rows = [self._process_row(row) for row in self._session.execute(stmt)]
                if reverse:
                    rows.reverse()
            else:
                if stop is None:
                    stmt = self._select_desc.limit(-start)
                elif stop < 0:
                    stmt = self._select_desc.slice(-stop, -start)
                else:
                    stmt = self._select_desc.slice(len(self) - stop, -start)
                rows = [self._process_row(row) for row in self._session.execute(stmt)]
                if not reverse:
                    rows.reverse()
            return rows
        raise TypeError(f"Sequence indices must be integers or slices, not {type(key).__name__}")


def _coerce_index(value: Any) -> int | None:
    """
    Check slice attribute's type and convert it to int.

    See CPython documentation on this:
    https://docs.python.org/3/reference/datamodel.html#object.__index__
    """
    if value is None or isinstance(value, int):
        return value
    if (index := getattr(value, "__index__", None)) is not None:
        return index()
    raise TypeError("slice indices must be integers or None or have an __index__ method")


def _coerce_slice(key: slice) -> tuple[int, int | None, bool]:
    """
    Check slice content and convert it for SQL.

    See CPython documentation on this:
    https://docs.python.org/3/reference/datamodel.html#slice-objects
    """
    if key.step is None or key.step == 1:
        reverse = False
    elif key.step == -1:
        reverse = True
    else:
        raise ValueError("non-trivial slice step not supported")
    return _coerce_index(key.start) or 0, _coerce_index(key.stop), reverse
