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

import contextlib
import enum
import logging
import os
import sys
import time
import warnings
from dataclasses import dataclass
from tempfile import gettempdir
from typing import TYPE_CHECKING, Callable, Generator, Iterable

from sqlalchemy import Table, and_, column, exc, func, inspect, or_, select, table, text, tuple_
from sqlalchemy.orm.session import Session

import airflow
from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import import_all_models
from airflow.utils import helpers

# TODO: remove create_session once we decide to break backward compatibility
from airflow.utils.session import NEW_SESSION, create_session, provide_session  # noqa: F401

if TYPE_CHECKING:
    from alembic.runtime.environment import EnvironmentContext
    from alembic.script import ScriptDirectory
    from sqlalchemy.orm import Query

    from airflow.models.base import Base


log = logging.getLogger(__name__)

REVISION_HEADS_MAP = {
    "2.0.0": "e959f08ac86c",
    "2.0.1": "82b7c48c147f",
    "2.0.2": "2e42bb497a22",
    "2.1.0": "a13f7613ad25",
    "2.1.1": "a13f7613ad25",
    "2.1.2": "a13f7613ad25",
    "2.1.3": "97cdd93827b8",
    "2.1.4": "ccde3e26fe78",
    "2.2.0": "7b2661a43ba3",
    "2.2.1": "7b2661a43ba3",
    "2.2.2": "7b2661a43ba3",
    "2.2.3": "be2bfac3da23",
    "2.2.4": "587bdf053233",
    "2.2.5": "587bdf053233",
    "2.3.0": "b1b348e02d07",
    "2.3.1": "1de7bc13c950",
    "2.3.2": "3c94c427fdf6",
    "2.3.3": "f5fcbda3e651",
    "2.3.4": "f5fcbda3e651",
    "2.4.0": "ecb43d2a1842",
    "2.4.1": "ecb43d2a1842",
    "2.4.2": "b0d31815b5a6",
    "2.4.3": "e07f49787c9d",
    "2.5.0": "290244fb8b83",
}


def _format_airflow_moved_table_name(source_table, version, category):
    return "__".join([settings.AIRFLOW_MOVED_TABLE_PREFIX, version.replace(".", "_"), category, source_table])


@provide_session
def merge_conn(conn, session: Session = NEW_SESSION):
    """Add new Connection."""
    if not session.query(conn.__class__).filter_by(conn_id=conn.conn_id).first():
        session.add(conn)
        session.commit()


@provide_session
def add_default_pool_if_not_exists(session: Session = NEW_SESSION):
    """Add default pool if it does not exist."""
    from airflow.models.pool import Pool

    if not Pool.get_pool(Pool.DEFAULT_POOL_NAME, session=session):
        default_pool = Pool(
            pool=Pool.DEFAULT_POOL_NAME,
            slots=conf.getint(section="core", key="default_pool_task_slot_count"),
            description="Default pool",
        )
        session.add(default_pool)
        session.commit()


@provide_session
def create_default_connections(session: Session = NEW_SESSION):
    """Create default Airflow connections."""
    from airflow.models.connection import Connection

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
            extra="""{"account_url": "<ACCOUNT_URL>"}""",
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
            conn_id="azure_data_explorer_default",
            conn_type="azure_data_explorer",
            host="https://<CLUSTER>.kusto.windows.net",
            extra="""{"auth_method": "<AAD_APP | AAD_APP_CERT | AAD_CREDS | AAD_DEVICE>",
                    "tenant": "<TENANT ID>", "certificate": "<APPLICATION PEM CERTIFICATE>",
                    "thumbprint": "<APPLICATION CERTIFICATE THUMBPRINT>"}""",
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
            conn_id="kubernetes_default",
            conn_type="kubernetes",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="kylin_default",
            conn_type="kylin",
            host="localhost",
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
            conn_id="oracle_default",
            conn_type="oracle",
            host="localhost",
            login="root",
            password="password",
            schema="schema",
            port=1521,
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="oss_default",
            conn_type="oss",
            extra="""{
                "auth_type": "AK",
                "access_key_id": "<ACCESS_KEY_ID>",
                "access_key_secret": "<ACCESS_KEY_SECRET>",
                "region": "<YOUR_OSS_REGION>"}
                """,
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
            conn_id="redshift_default",
            conn_type="redshift",
            extra="""{
    "iam": true,
    "cluster_identifier": "<REDSHIFT_CLUSTER_IDENTIFIER>",
    "port": 5439,
    "profile": "default",
    "db_user": "awsuser",
    "database": "dev",
    "region": ""
}""",
        ),
        session,
    )
    merge_conn(
        Connection(
            conn_id="salesforce_default",
            conn_type="salesforce",
            login="username",
            password="password",
            extra='{"security_token": "security_token"}',
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
            conn_id="tabular_default",
            conn_type="tabular",
            host="https://api.tabulardata.io/ws/v1",
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
            conn_id="yandexcloud_default",
            conn_type="yandexcloud",
            schema="default",
        ),
        session,
    )


def _create_db_from_orm(session):
    from alembic import command
    from flask import Flask
    from flask_sqlalchemy import SQLAlchemy

    from airflow.models.base import Base
    from airflow.www.fab_security.sqla.models import Model
    from airflow.www.session import AirflowDatabaseSessionInterface

    def _create_flask_session_tbl():
        flask_app = Flask(__name__)
        flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")
        flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
        db = SQLAlchemy(flask_app)
        AirflowDatabaseSessionInterface(app=flask_app, db=db, table="session", key_prefix="")
        db.create_all()

    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
        Base.metadata.create_all(settings.engine)
        Model.metadata.create_all(settings.engine)
        _create_flask_session_tbl()
        # stamp the migration head
        config = _get_alembic_config()
        command.stamp(config, "head")


@provide_session
def initdb(session: Session = NEW_SESSION, load_connections: bool = True):
    """Initialize Airflow database."""
    import_all_models()

    db_exists = _get_current_revision(session)
    if db_exists:
        upgradedb(session=session)
    else:
        _create_db_from_orm(session=session)
    # Load default connections
    if conf.getboolean("database", "LOAD_DEFAULT_CONNECTIONS") and load_connections:
        create_default_connections(session=session)
    # Add default pool & sync log_template
    add_default_pool_if_not_exists()
    synchronize_log_template()


def _get_alembic_config():
    from alembic.config import Config

    package_dir = os.path.dirname(airflow.__file__)
    directory = os.path.join(package_dir, "migrations")
    config = Config(os.path.join(package_dir, "alembic.ini"))
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
    Function to wait for all airflow migrations to complete.
    :param timeout: Timeout for the migration in seconds
    :return: None
    """
    timeout = timeout or 1  # run the loop at least 1
    with _configured_alembic_environment() as env:
        context = env.get_context()
        source_heads = None
        db_heads = None
        for ticker in range(timeout):
            source_heads = set(env.script.get_heads())
            db_heads = set(context.get_current_heads())
            if source_heads == db_heads:
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

    with EnvironmentContext(
        config,
        script,
    ) as env, settings.engine.connect() as connection:

        alembic_logger = logging.getLogger("alembic")
        level = alembic_logger.level
        alembic_logger.setLevel(logging.WARNING)
        env.configure(connection)
        alembic_logger.setLevel(level)

        yield env


def check_and_run_migrations():
    """Check and run migrations if necessary. Only use in a tty"""
    with _configured_alembic_environment() as env:
        context = env.get_context()
        source_heads = set(env.script.get_heads())
        db_heads = set(context.get_current_heads())
        db_command = None
        command_name = None
        verb = None
    if len(db_heads) < 1:
        db_command = initdb
        command_name = "init"
        verb = "initialize"
    elif source_heads != db_heads:
        db_command = upgradedb
        command_name = "upgrade"
        verb = "upgrade"

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


def _reserialize_dags(*, session: Session) -> None:
    from airflow.models.dagbag import DagBag
    from airflow.models.serialized_dag import SerializedDagModel

    session.query(SerializedDagModel).delete(synchronize_session=False)
    dagbag = DagBag(collect_dags=False)
    dagbag.collect_dags(only_if_updated=False)
    dagbag.sync_to_db(session=session)


@provide_session
def synchronize_log_template(*, session: Session = NEW_SESSION) -> None:
    """Synchronize log template configs with table.

    This checks if the last row fully matches the current config values, and
    insert a new row if not.
    """
    from airflow.models.tasklog import LogTemplate

    def log_template_exists():
        metadata = reflect_tables([LogTemplate], session)
        log_template_table = metadata.tables.get(LogTemplate.__tablename__)
        return log_template_table is not None

    if not log_template_exists():
        log.info("Log template table does not exist (added in 2.3.0); skipping log template sync.")
        return

    filename = conf.get("logging", "log_filename_template")
    elasticsearch_id = conf.get("elasticsearch", "log_id_template")

    # First check if we have an empty table. If so, and the default values exist,
    # we will seed the table with the values from pre 2.3.0, so old logs will
    # still be retrievable.
    if not session.query(LogTemplate.id).first():
        is_default_log_id = elasticsearch_id == conf.airflow_defaults.get("elasticsearch", "log_id_template")
        is_default_filename = filename == conf.airflow_defaults.get("logging", "log_filename_template")
        if is_default_log_id and is_default_filename:
            session.add(
                LogTemplate(
                    filename="{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log",
                    elasticsearch_id="{dag_id}-{task_id}-{execution_date}-{try_number}",
                )
            )
            session.flush()

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
        row = (
            session.query(LogTemplate.id)
            .filter(
                or_(
                    LogTemplate.filename == pre_upgrade_filename,
                    LogTemplate.elasticsearch_id == pre_upgrade_elasticsearch_id,
                )
            )
            .order_by(LogTemplate.id.desc())
            .first()
        )
        if not row:
            session.add(
                LogTemplate(filename=pre_upgrade_filename, elasticsearch_id=pre_upgrade_elasticsearch_id)
            )
            session.flush()

    stored = session.query(LogTemplate).order_by(LogTemplate.id.desc()).first()

    if not stored or stored.filename != filename or stored.elasticsearch_id != elasticsearch_id:
        session.add(LogTemplate(filename=filename, elasticsearch_id=elasticsearch_id))


def check_conn_id_duplicates(session: Session) -> Iterable[str]:
    """
    Check unique conn_id in connection table

    :param session:  session of the sqlalchemy
    """
    from airflow.models.connection import Connection

    dups = []
    try:
        dups = session.query(Connection.conn_id).group_by(Connection.conn_id).having(func.count() > 1).all()
    except (exc.OperationalError, exc.ProgrammingError):
        # fallback if tables hasn't been created yet
        session.rollback()
    if dups:
        yield (
            "Seems you have non unique conn_id in connection table.\n"
            "You have to manage those duplicate connections "
            "before upgrading the database.\n"
            f"Duplicated conn_id: {[dup.conn_id for dup in dups]}"
        )


def check_username_duplicates(session: Session) -> Iterable[str]:
    """
    Check unique username in User & RegisterUser table

    :param session:  session of the sqlalchemy
    :rtype: str
    """
    from airflow.www.fab_security.sqla.models import RegisterUser, User

    for model in [User, RegisterUser]:
        dups = []
        try:
            dups = (
                session.query(model.username)  # type: ignore[attr-defined]
                .group_by(model.username)  # type: ignore[attr-defined]
                .having(func.count() > 1)
                .all()
            )
        except (exc.OperationalError, exc.ProgrammingError):
            # fallback if tables hasn't been created yet
            session.rollback()
        if dups:
            yield (
                f"Seems you have mixed case usernames in {model.__table__.name} table.\n"  # type: ignore
                "You have to rename or delete those mixed case usernames "
                "before upgrading the database.\n"
                f"usernames with mixed cases: {[dup.username for dup in dups]}"
            )


def reflect_tables(tables: list[Base | str] | None, session):
    """
    When running checks prior to upgrades, we use reflection to determine current state of the
    database.
    This function gets the current state of each table in the set of models provided and returns
    a SqlAlchemy metadata object containing them.
    """
    import sqlalchemy.schema

    metadata = sqlalchemy.schema.MetaData(session.bind)

    if tables is None:
        metadata.reflect(resolve_fks=False)
    else:
        for tbl in tables:
            try:
                table_name = tbl if isinstance(tbl, str) else tbl.__tablename__
                metadata.reflect(only=[table_name], extend_existing=True, resolve_fks=False)
            except exc.InvalidRequestError:
                continue
    return metadata


def check_table_for_duplicates(
    *, session: Session, table_name: str, uniqueness: list[str], version: str
) -> Iterable[str]:
    """
    Check table for duplicates, given a list of columns which define the uniqueness of the table.

    Usage example:

    .. code-block:: python

        def check_task_fail_for_duplicates(session):
            from airflow.models.taskfail import TaskFail

            metadata = reflect_tables([TaskFail], session)
            task_fail = metadata.tables.get(TaskFail.__tablename__)  # type: ignore
            if task_fail is None:  # table not there
                return
            if "run_id" in task_fail.columns:  # upgrade already applied
                return
            yield from check_table_for_duplicates(
                table_name=task_fail.name,
                uniqueness=["dag_id", "task_id", "execution_date"],
                session=session,
                version="2.3",
            )

    :param table_name: table name to check
    :param uniqueness: uniqueness constraint to evaluate against
    :param session:  session of the sqlalchemy
    """
    minimal_table_obj = table(table_name, *[column(x) for x in uniqueness])
    try:
        subquery = (
            session.query(minimal_table_obj, func.count().label("dupe_count"))
            .group_by(*[text(x) for x in uniqueness])
            .having(func.count() > text("1"))
            .subquery()
        )
        dupe_count = session.query(func.sum(subquery.c.dupe_count)).scalar()
        if not dupe_count:
            # there are no duplicates; nothing to do.
            return

        log.warning("Found %s duplicates in table %s.  Will attempt to move them.", dupe_count, table_name)

        metadata = reflect_tables(tables=[table_name], session=session)
        if table_name not in metadata.tables:
            yield f"Table {table_name} does not exist in the database."

        # We can't use the model here since it may differ from the db state due to
        # this function is run prior to migration. Use the reflected table instead.
        table_obj = metadata.tables[table_name]

        _move_duplicate_data_to_new_table(
            session=session,
            source_table=table_obj,
            subquery=subquery,
            uniqueness=uniqueness,
            target_table_name=_format_airflow_moved_table_name(table_name, version, "duplicates"),
        )
    except (exc.OperationalError, exc.ProgrammingError):
        # fallback if `table_name` hasn't been created yet
        session.rollback()


def check_conn_type_null(session: Session) -> Iterable[str]:
    """
    Check nullable conn_type column in Connection table

    :param session:  session of the sqlalchemy
    """
    from airflow.models.connection import Connection

    n_nulls = []
    try:
        n_nulls = session.query(Connection.conn_id).filter(Connection.conn_type.is_(None)).all()
    except (exc.OperationalError, exc.ProgrammingError, exc.InternalError):
        # fallback if tables hasn't been created yet
        session.rollback()

    if n_nulls:
        yield (
            "The conn_type column in the connection "
            "table must contain content.\n"
            "Make sure you don't have null "
            "in the conn_type column.\n"
            f"Null conn_type conn_id: {list(n_nulls)}"
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


def check_run_id_null(session: Session) -> Iterable[str]:
    from airflow.models.dagrun import DagRun

    metadata = reflect_tables([DagRun], session)

    # We can't use the model here since it may differ from the db state due to
    # this function is run prior to migration. Use the reflected table instead.
    dagrun_table = metadata.tables.get(DagRun.__tablename__)
    if dagrun_table is None:
        return

    invalid_dagrun_filter = or_(
        dagrun_table.c.dag_id.is_(None),
        dagrun_table.c.run_id.is_(None),
        dagrun_table.c.execution_date.is_(None),
    )
    invalid_dagrun_count = session.query(func.count(dagrun_table.c.id)).filter(invalid_dagrun_filter).scalar()
    if invalid_dagrun_count > 0:
        dagrun_dangling_table_name = _format_airflow_moved_table_name(dagrun_table.name, "2.2", "dangling")
        if dagrun_dangling_table_name in inspect(session.get_bind()).get_table_names():
            yield _format_dangling_error(
                source_table=dagrun_table.name,
                target_table=dagrun_dangling_table_name,
                invalid_count=invalid_dagrun_count,
                reason="with a NULL dag_id, run_id, or execution_date",
            )
            return

        bind = session.get_bind()
        dialect_name = bind.dialect.name
        _create_table_as(
            dialect_name=dialect_name,
            source_query=dagrun_table.select(invalid_dagrun_filter),
            target_table_name=dagrun_dangling_table_name,
            source_table_name=dagrun_table.name,
            session=session,
        )
        delete = dagrun_table.delete().where(invalid_dagrun_filter)
        session.execute(delete)


def _create_table_as(
    *,
    session,
    dialect_name: str,
    source_query: Query,
    target_table_name: str,
    source_table_name: str,
):
    """
    Create a new table with rows from query.
    We have to handle CTAS differently for different dialects.
    """
    from sqlalchemy import column, select, table

    if dialect_name == "mssql":
        cte = source_query.cte("source")
        moved_data_tbl = table(target_table_name, *(column(c.name) for c in cte.columns))
        ins = moved_data_tbl.insert().from_select(list(cte.columns), select([cte]))

        stmt = ins.compile(bind=session.get_bind())
        cte_sql = stmt.ctes[cte]

        session.execute(f"WITH {cte_sql} SELECT source.* INTO {target_table_name} FROM source")
    elif dialect_name == "mysql":
        # MySQL with replication needs this split in to two queries, so just do it for all MySQL
        # ERROR 1786 (HY000): Statement violates GTID consistency: CREATE TABLE ... SELECT.
        session.execute(f"CREATE TABLE {target_table_name} LIKE {source_table_name}")
        session.execute(
            f"INSERT INTO {target_table_name} {source_query.selectable.compile(bind=session.get_bind())}"
        )
    else:
        # Postgres and SQLite both support the same "CREATE TABLE a AS SELECT ..." syntax
        session.execute(
            f"CREATE TABLE {target_table_name} AS {source_query.selectable.compile(bind=session.get_bind())}"
        )


def _move_dangling_data_to_new_table(
    session, source_table: Table, source_query: Query, target_table_name: str
):

    bind = session.get_bind()
    dialect_name = bind.dialect.name

    # First: Create moved rows from new table
    log.debug("running CTAS for table %s", target_table_name)
    _create_table_as(
        dialect_name=dialect_name,
        source_query=source_query,
        target_table_name=target_table_name,
        source_table_name=source_table.name,
        session=session,
    )
    session.commit()

    target_table = source_table.to_metadata(source_table.metadata, name=target_table_name)
    log.debug("checking whether rows were moved for table %s", target_table_name)
    moved_rows_exist_query = select([1]).select_from(target_table).limit(1)
    first_moved_row = session.execute(moved_rows_exist_query).all()
    session.commit()

    if not first_moved_row:
        log.debug("no rows moved; dropping %s", target_table_name)
        # no bad rows were found; drop moved rows table.
        target_table.drop(bind=session.get_bind(), checkfirst=True)
    else:
        log.debug("rows moved; purging from %s", source_table.name)
        if dialect_name == "sqlite":
            pk_cols = source_table.primary_key.columns

            delete = source_table.delete().where(
                tuple_(*pk_cols).in_(session.query(*target_table.primary_key.columns).subquery())
            )
        else:
            delete = source_table.delete().where(
                and_(col == target_table.c[col.name] for col in source_table.primary_key.columns)
            )
        log.debug(delete.compile())
        session.execute(delete)
    session.commit()

    log.debug("exiting move function")


def _dangling_against_dag_run(session, source_table, dag_run):
    """
    Given a source table, we generate a subquery that will return 1 for every row that
    has a dagrun.
    """
    source_to_dag_run_join_cond = and_(
        source_table.c.dag_id == dag_run.c.dag_id,
        source_table.c.execution_date == dag_run.c.execution_date,
    )
    return (
        session.query(*[c.label(c.name) for c in source_table.c])
        .join(dag_run, source_to_dag_run_join_cond, isouter=True)
        .filter(dag_run.c.dag_id.is_(None))
    )


def _dangling_against_task_instance(session, source_table, dag_run, task_instance):
    """
    Given a source table, we generate a subquery that will return 1 for every row that
    has a valid task instance (and associated dagrun).

    This is used to identify rows that need to be removed from tables prior to adding a TI fk.

    Since this check is applied prior to running the migrations, we have to use different
    query logic depending on which revision the database is at.

    """
    if "run_id" not in task_instance.c:
        # db is < 2.2.0
        dr_join_cond = and_(
            source_table.c.dag_id == dag_run.c.dag_id,
            source_table.c.execution_date == dag_run.c.execution_date,
        )
        ti_join_cond = and_(
            dag_run.c.dag_id == task_instance.c.dag_id,
            dag_run.c.execution_date == task_instance.c.execution_date,
            source_table.c.task_id == task_instance.c.task_id,
        )
    else:
        # db is 2.2.0 <= version < 2.3.0
        dr_join_cond = and_(
            source_table.c.dag_id == dag_run.c.dag_id,
            source_table.c.execution_date == dag_run.c.execution_date,
        )
        ti_join_cond = and_(
            dag_run.c.dag_id == task_instance.c.dag_id,
            dag_run.c.run_id == task_instance.c.run_id,
            source_table.c.task_id == task_instance.c.task_id,
        )

    return (
        session.query(*[c.label(c.name) for c in source_table.c])
        .join(dag_run, dr_join_cond, isouter=True)
        .join(task_instance, ti_join_cond, isouter=True)
        .filter(or_(task_instance.c.dag_id.is_(None), dag_run.c.dag_id.is_(None)))
    )


def _move_duplicate_data_to_new_table(
    session, source_table: Table, subquery: Query, uniqueness: list[str], target_table_name: str
):
    """
    When adding a uniqueness constraint we first should ensure that there are no duplicate rows.

    This function accepts a subquery that should return one record for each row with duplicates (e.g.
    a group by with having count(*) > 1).  We select from ``source_table`` getting all rows matching the
    subquery result and store in ``target_table_name``.  Then to purge the duplicates from the source table,
    we do a DELETE FROM with a join to the target table (which now contains the dupes).

    :param session: sqlalchemy session for metadata db
    :param source_table: table to purge dupes from
    :param subquery: the subquery that returns the duplicate rows
    :param uniqueness: the string list of columns used to define the uniqueness for the table. used in
        building the DELETE FROM join condition.
    :param target_table_name: name of the table in which to park the duplicate rows
    """
    bind = session.get_bind()
    dialect_name = bind.dialect.name
    query = (
        session.query(source_table)
        .with_entities(*[getattr(source_table.c, x.name).label(str(x.name)) for x in source_table.columns])
        .select_from(source_table)
        .join(subquery, and_(*[getattr(source_table.c, x) == getattr(subquery.c, x) for x in uniqueness]))
    )

    _create_table_as(
        session=session,
        dialect_name=dialect_name,
        source_query=query,
        target_table_name=target_table_name,
        source_table_name=source_table.name,
    )

    # we must ensure that the CTAS table is created prior to the DELETE step since we have to join to it
    session.commit()

    metadata = reflect_tables([target_table_name], session)
    target_table = metadata.tables[target_table_name]
    where_clause = and_(*[getattr(source_table.c, x) == getattr(target_table.c, x) for x in uniqueness])

    if dialect_name == "sqlite":
        subq = query.selectable.with_only_columns([text(f"{source_table}.ROWID")])
        delete = source_table.delete().where(column("ROWID").in_(subq))
    else:
        delete = source_table.delete(where_clause)

    session.execute(delete)


def check_bad_references(session: Session) -> Iterable[str]:
    """
    Starting in Airflow 2.2, we began a process of replacing `execution_date` with `run_id`
    in many tables.
    Here we go through each table and look for records that can't be mapped to a dag run.
    When we find such "dangling" rows we back them up in a special table and delete them
    from the main table.
    """
    from airflow.models.dagrun import DagRun
    from airflow.models.renderedtifields import RenderedTaskInstanceFields
    from airflow.models.taskfail import TaskFail
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskreschedule import TaskReschedule
    from airflow.models.xcom import XCom

    @dataclass
    class BadReferenceConfig:
        """
        :param bad_rows_func: function that returns subquery which determines whether bad rows exist
        :param join_tables: table objects referenced in subquery
        :param ref_table: information-only identifier for categorizing the missing ref
        """

        bad_rows_func: Callable
        join_tables: list[str]
        ref_table: str

    missing_dag_run_config = BadReferenceConfig(
        bad_rows_func=_dangling_against_dag_run,
        join_tables=["dag_run"],
        ref_table="dag_run",
    )

    missing_ti_config = BadReferenceConfig(
        bad_rows_func=_dangling_against_task_instance,
        join_tables=["dag_run", "task_instance"],
        ref_table="task_instance",
    )

    models_list: list[tuple[Base, str, BadReferenceConfig]] = [
        (TaskInstance, "2.2", missing_dag_run_config),
        (TaskReschedule, "2.2", missing_ti_config),
        (RenderedTaskInstanceFields, "2.3", missing_ti_config),
        (TaskFail, "2.3", missing_ti_config),
        (XCom, "2.3", missing_ti_config),
    ]
    metadata = reflect_tables([*[x[0] for x in models_list], DagRun, TaskInstance], session)

    if (
        not metadata.tables
        or metadata.tables.get(DagRun.__tablename__) is None
        or metadata.tables.get(TaskInstance.__tablename__) is None
    ):
        # Key table doesn't exist -- likely empty DB.
        return

    existing_table_names = set(inspect(session.get_bind()).get_table_names())
    errored = False

    for model, change_version, bad_ref_cfg in models_list:
        log.debug("checking model %s", model.__tablename__)
        # We can't use the model here since it may differ from the db state due to
        # this function is run prior to migration. Use the reflected table instead.
        source_table = metadata.tables.get(model.__tablename__)  # type: ignore
        if source_table is None:
            continue

        # Migration already applied, don't check again.
        if "run_id" in source_table.columns:
            continue

        func_kwargs = {x: metadata.tables[x] for x in bad_ref_cfg.join_tables}
        bad_rows_query = bad_ref_cfg.bad_rows_func(session, source_table, **func_kwargs)

        dangling_table_name = _format_airflow_moved_table_name(source_table.name, change_version, "dangling")
        if dangling_table_name in existing_table_names:
            invalid_row_count = bad_rows_query.count()
            if invalid_row_count <= 0:
                continue
            else:
                yield _format_dangling_error(
                    source_table=source_table.name,
                    target_table=dangling_table_name,
                    invalid_count=invalid_row_count,
                    reason=f"without a corresponding {bad_ref_cfg.ref_table} row",
                )
                errored = True
            continue

        log.debug("moving data for table %s", source_table.name)
        _move_dangling_data_to_new_table(
            session,
            source_table,
            bad_rows_query,
            dangling_table_name,
        )

    if errored:
        session.rollback()
    else:
        session.commit()


@provide_session
def _check_migration_errors(session: Session = NEW_SESSION) -> Iterable[str]:
    """:session: session of the sqlalchemy"""
    check_functions: tuple[Callable[..., Iterable[str]], ...] = (
        check_conn_id_duplicates,
        check_conn_type_null,
        check_run_id_null,
        check_bad_references,
        check_username_duplicates,
    )
    for check_fn in check_functions:
        log.debug("running check function %s", check_fn.__name__)
        yield from check_fn(session=session)
        # Ensure there is no "active" transaction. Seems odd, but without this MSSQL can hang
        session.commit()


def _offline_migration(migration_func: Callable, config, revision):
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
    Checks that all supplied revision ids are above the minimum revision for the dialect.

    :param config: Alembic config
    :param revisions: list of Alembic revision ids
    :return: None
    """
    dbname = settings.engine.dialect.name
    if dbname == "sqlite":
        raise AirflowException("Offline migration not supported for SQLite.")
    min_version, min_revision = ("2.2.0", "7b2661a43ba3") if dbname == "mssql" else ("2.0.0", "e959f08ac86c")

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
    reserialize_dags: bool = True,
    session: Session = NEW_SESSION,
):
    """

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

        if not _revision_greater(config, to_revision, from_revision):
            raise ValueError(
                f"Requested *to* revision {to_revision} is older than *from* revision {from_revision}. "
                "Please check your requested versions / revisions."
            )
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

    if not to_revision and not _get_current_revision(session=session):
        # Don't load default connections
        # New DB; initialize and exit
        initdb(session=session, load_connections=False)
        return
    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
        import sqlalchemy.pool

        log.info("Creating tables")
        val = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_SIZE")
        try:
            # Reconfigure the ORM ot use _EXACTLY_ one connection, otherwise some db engines hang forever
            # trying to ALTER TABLEs
            os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_SIZE"] = "1"
            settings.reconfigure_orm(pool_class=sqlalchemy.pool.SingletonThreadPool)
            command.upgrade(config, revision=to_revision or "heads")
        finally:
            if val is None:
                os.environ.pop("AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_SIZE")
            else:
                os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_SIZE"] = val
            settings.reconfigure_orm()

    if reserialize_dags:
        _reserialize_dags(session=session)
    add_default_pool_if_not_exists(session=session)
    synchronize_log_template(session=session)


@provide_session
def resetdb(session: Session = NEW_SESSION, skip_init: bool = False):
    """Clear out the database"""
    if not settings.engine:
        raise RuntimeError("The settings.engine must be set. This is a critical assertion")
    log.info("Dropping tables that exist")

    import_all_models()

    connection = settings.engine.connect()

    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
        drop_airflow_models(connection)
        drop_airflow_moved_tables(session)

    if not skip_init:
        initdb(session=session)


@provide_session
def bootstrap_dagbag(session: Session = NEW_SESSION):
    from airflow.models.dag import DAG
    from airflow.models.dagbag import DagBag

    dagbag = DagBag()
    # Save DAGs in the ORM
    dagbag.sync_to_db(session=session)

    # Deactivate the unknown ones
    DAG.deactivate_unknown_dags(dagbag.dags.keys(), session=session)


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

    with create_global_lock(session=session, lock=DBLocks.MIGRATIONS):
        if show_sql_only:
            log.warning("Generating sql scripts for manual migration.")
            if not from_revision:
                from_revision = _get_current_revision(session)
            revision_range = f"{from_revision}:{to_revision}"
            _offline_migration(command.downgrade, config=config, revision=revision_range)
        else:
            log.info("Applying downgrade migrations.")
            command.downgrade(config, revision=to_revision, sql=show_sql_only)


def drop_airflow_models(connection):
    """
    Drops all airflow models.

    :param connection: SQLAlchemy Connection
    :return: None
    """
    from airflow.models.base import Base

    # Drop connection and chart - those tables have been deleted and in case you
    # run resetdb on schema with chart or users table will fail
    chart = Table("chart", Base.metadata)
    chart.drop(settings.engine, checkfirst=True)
    user = Table("user", Base.metadata)
    user.drop(settings.engine, checkfirst=True)
    users = Table("users", Base.metadata)
    users.drop(settings.engine, checkfirst=True)
    dag_stats = Table("dag_stats", Base.metadata)
    dag_stats.drop(settings.engine, checkfirst=True)
    session = Table("session", Base.metadata)
    session.drop(settings.engine, checkfirst=True)

    Base.metadata.drop_all(connection)
    # we remove the Tables here so that if resetdb is run metadata does not keep the old tables.
    Base.metadata.remove(session)
    Base.metadata.remove(dag_stats)
    Base.metadata.remove(users)
    Base.metadata.remove(user)
    Base.metadata.remove(chart)
    # alembic adds significant import time, so we import it lazily
    from alembic.migration import MigrationContext

    migration_ctx = MigrationContext.configure(connection)
    version = migration_ctx._version
    if inspect(connection).has_table(version.name):
        version.drop(connection)


def drop_airflow_moved_tables(session):
    from airflow.models.base import Base
    from airflow.settings import AIRFLOW_MOVED_TABLE_PREFIX

    tables = set(inspect(session.get_bind()).get_table_names())
    to_delete = [Table(x, Base.metadata) for x in tables if x.startswith(AIRFLOW_MOVED_TABLE_PREFIX)]
    for tbl in to_delete:
        tbl.drop(settings.engine, checkfirst=False)
        Base.metadata.remove(tbl)


@provide_session
def check(session: Session = NEW_SESSION):
    """
    Checks if the database works.

    :param session: session of the sqlalchemy
    """
    session.execute("select 1 as is_alive;")
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
    conn = session.get_bind().connect()
    dialect = conn.dialect
    try:
        if dialect.name == "postgresql":
            conn.execute(text("SET LOCK_TIMEOUT to :timeout"), timeout=lock_timeout)
            conn.execute(text("SELECT pg_advisory_lock(:id)"), id=lock.value)
        elif dialect.name == "mysql" and dialect.server_version_info >= (5, 6):
            conn.execute(text("SELECT GET_LOCK(:id, :timeout)"), id=str(lock), timeout=lock_timeout)
        elif dialect.name == "mssql":
            # TODO: make locking work for MSSQL
            pass

        yield
    finally:
        if dialect.name == "postgresql":
            conn.execute("SET LOCK_TIMEOUT TO DEFAULT")
            (unlocked,) = conn.execute(text("SELECT pg_advisory_unlock(:id)"), id=lock.value).fetchone()
            if not unlocked:
                raise RuntimeError("Error releasing DB lock!")
        elif dialect.name == "mysql" and dialect.server_version_info >= (5, 6):
            conn.execute(text("select RELEASE_LOCK(:id)"), id=str(lock))
        elif dialect.name == "mssql":
            # TODO: make locking work for MSSQL
            pass


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

    Comparing server_default is not accurate in MSSQL because the
    inspected_default above != metadata_default, while in Postgres/MySQL they are equal.
    This is an issue with alembic
    In SQLite: task_instance.map_index & task_reschedule.map_index
    are not comparing accurately. Sometimes they are equal, sometimes they are not.
    Alembic warned that this feature has varied accuracy depending on backends.
    See: (https://alembic.sqlalchemy.org/en/latest/api/runtime.html#alembic.runtime.
        environment.EnvironmentContext.configure.params.compare_server_default)
    """
    dialect_name = context.connection.dialect.name
    if dialect_name in ["mssql", "sqlite"]:
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
