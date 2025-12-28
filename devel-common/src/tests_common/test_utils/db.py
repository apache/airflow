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

import json
import os
from tempfile import gettempdir
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from airflow.configuration import conf
from airflow.jobs.job import Job
from airflow.models import (
    Connection,
    DagModel,
    DagRun,
    DagTag,
    DbCallbackRequest,
    Log,
    Pool,
    RenderedTaskInstanceFields,
    TaskInstance,
    TaskReschedule,
    Trigger,
    Variable,
)
from airflow.models.dag import DagOwnerAttributes
from airflow.models.dagcode import DagCode
from airflow.models.dagwarning import DagWarning
from airflow.models.serialized_dag import SerializedDagModel
from airflow.security.permissions import RESOURCE_DAG_PREFIX
from airflow.utils.db import (
    add_default_pool_if_not_exists,
    create_default_connections,
    reflect_tables,
)
from airflow.utils.session import create_session

from tests_common.test_utils.compat import (
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
    ParseImportError,
    TaskOutletAssetReference,
)
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_2_PLUS

if TYPE_CHECKING:
    from pathlib import Path

if AIRFLOW_V_3_0_PLUS:
    from airflow.models.xcom import XComModel as XCom
else:
    from airflow.models.xcom import XCom  # type: ignore[no-redef]

if AIRFLOW_V_3_1_PLUS:
    from airflow.models.dag_favorite import DagFavorite


def _deactivate_unknown_dags(active_dag_ids, session):
    """
    Given a list of known DAGs, deactivate any other DAGs that are marked as active in the ORM.

    :param active_dag_ids: list of DAG IDs that are active
    :return: None
    """
    if not active_dag_ids:
        return
    for dag in session.scalars(select(DagModel).where(~DagModel.dag_id.in_(active_dag_ids))):
        dag.is_stale = True
        session.merge(dag)
    session.commit()


def _bootstrap_dagbag():
    if AIRFLOW_V_3_2_PLUS:
        from airflow.dag_processing.dagbag import DagBag
    else:  # back-compat for Airflow <3.2
        from airflow.models.dagbag import DagBag  # type: ignore[no-redef, attribute-defined]

    if AIRFLOW_V_3_0_PLUS:
        from airflow.dag_processing.bundles.manager import DagBundlesManager

    with create_session() as session:
        if AIRFLOW_V_3_0_PLUS:
            DagBundlesManager().sync_bundles_to_db(session=session)
            session.commit()

        dagbag = DagBag()
        # Save DAGs in the ORM
        if AIRFLOW_V_3_1_PLUS:
            try:
                from airflow.dag_processing.dagbag import sync_bag_to_db
            except ImportError:
                from airflow.models.dagbag import sync_bag_to_db

            sync_bag_to_db(dagbag, bundle_name="dags-folder", bundle_version=None, session=session)
        elif AIRFLOW_V_3_0_PLUS:
            dagbag.sync_to_db(  # type: ignore[attr-defined]
                bundle_name="dags-folder",
                bundle_version=None,
                session=session,
            )
        else:
            dagbag.sync_to_db(session=session)  # type: ignore[attr-defined]

        # Deactivate the unknown ones
        _deactivate_unknown_dags(dagbag.dags, session=session)


def initial_db_init():
    from airflow.configuration import conf
    from airflow.utils import db

    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    db.resetdb()

    if AIRFLOW_V_3_0_PLUS:
        try:
            from airflow.providers.fab.auth_manager.models.db import FABDBManager
        except ModuleNotFoundError:
            # Reasons it might fail: we're in a provider bundle without FAB, or we're on a version of Python
            # where FAB isn't yet supported
            pass
        else:
            if os.getenv("TEST_GROUP") != "providers":
                # If we loaded the provider, and we're running core (or running via breeze where TEST_GROUP
                # isn't specified) run the downgrade+upgrade to ensure migrations are in sync with Model
                # classes
                db.downgrade(to_revision="5f2621c13b39")
                db.upgradedb(to_revision="head")
            else:
                # Just create the tables so they are there
                with create_session() as session:
                    FABDBManager(session).create_db_from_orm()
                    session.commit()
    else:
        from flask import Flask

        from airflow.www.extensions.init_appbuilder import init_appbuilder
        from airflow.www.extensions.init_auth_manager import get_auth_manager

        # minimal app to add roles
        flask_app = Flask(__name__)
        flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")
        init_appbuilder(flask_app)

        get_auth_manager().init()

    _bootstrap_dagbag()


def parse_and_sync_to_db(folder: Path | str, include_examples: bool = False):
    if AIRFLOW_V_3_2_PLUS:
        from airflow.dag_processing.dagbag import DagBag
    else:
        from airflow.models.dagbag import DagBag  # type: ignore[no-redef, attribute-defined]

    if AIRFLOW_V_3_0_PLUS:
        from airflow.dag_processing.bundles.manager import DagBundlesManager

    with create_session() as session:
        if AIRFLOW_V_3_0_PLUS:
            DagBundlesManager().sync_bundles_to_db(session=session)
            session.flush()

        dagbag = DagBag(dag_folder=folder, include_examples=include_examples)
        if AIRFLOW_V_3_1_PLUS:
            try:
                from airflow.dag_processing.dagbag import sync_bag_to_db
            except ImportError:
                from airflow.models.dagbag import sync_bag_to_db  # type: ignore[no-redef, attribute-defined]

            sync_bag_to_db(dagbag, "dags-folder", None, session=session)
        elif AIRFLOW_V_3_0_PLUS:
            dagbag.sync_to_db("dags-folder", None, session)  # type: ignore[attr-defined]
        else:
            dagbag.sync_to_db(session=session)  # type: ignore[attr-defined]

    return dagbag


def clear_db_runs():
    with create_session() as session:
        session.execute(delete(Job))
        session.execute(delete(Trigger))
        session.execute(delete(DagRun))
        session.execute(delete(TaskInstance))
        try:
            from airflow.models import TaskInstanceHistory

            session.execute(delete(TaskInstanceHistory))
        except ImportError:
            pass


def clear_db_backfills():
    from airflow.models.backfill import Backfill, BackfillDagRun

    with create_session() as session:
        session.execute(delete(BackfillDagRun))
        session.execute(delete(Backfill))


def clear_db_assets():
    with create_session() as session:
        session.execute(delete(AssetEvent))
        session.execute(delete(AssetModel))
        session.execute(delete(AssetDagRunQueue))
        session.execute(delete(DagScheduleAssetReference))
        session.execute(delete(TaskOutletAssetReference))
        if AIRFLOW_V_3_1_PLUS:
            from airflow.models.asset import TaskInletAssetReference

            session.execute(delete(TaskInletAssetReference))
        from tests_common.test_utils.compat import AssetAliasModel, DagScheduleAssetAliasReference

        session.execute(delete(AssetAliasModel))
        session.execute(delete(DagScheduleAssetAliasReference))
        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.asset import (
                AssetActive,
                DagScheduleAssetNameReference,
                DagScheduleAssetUriReference,
            )

            session.execute(delete(AssetActive))
            session.execute(delete(DagScheduleAssetNameReference))
            session.execute(delete(DagScheduleAssetUriReference))
        if AIRFLOW_V_3_2_PLUS:
            from airflow.models.asset import AssetWatcherModel

            session.execute(delete(AssetWatcherModel))


def clear_db_triggers():
    with create_session() as session:
        if AIRFLOW_V_3_2_PLUS:
            from airflow.models.asset import AssetWatcherModel

            session.execute(delete(AssetWatcherModel))
        session.execute(delete(Trigger))


def clear_db_dags():
    with create_session() as session:
        if AIRFLOW_V_3_1_PLUS:
            session.execute(delete(DagFavorite))
        session.execute(delete(DagTag))
        session.execute(delete(DagOwnerAttributes))
        session.execute(
            delete(DagRun)
        )  # todo: this should not be necessary because the fk to DagVersion should be ON DELETE SET NULL
        session.execute(delete(DagModel))


def clear_db_deadline():
    with create_session() as session:
        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.deadline import Deadline

            session.execute(delete(Deadline))


def drop_tables_with_prefix(prefix):
    with create_session() as session:
        metadata = reflect_tables(None, session)
        for table_name, table in metadata.tables.items():
            if table_name.startswith(prefix):
                table.drop(session.bind)


def clear_db_serialized_dags():
    with create_session() as session:
        session.execute(delete(SerializedDagModel))


def clear_db_pools():
    with create_session() as session:
        session.execute(delete(Pool))
        add_default_pool_if_not_exists(session)


def clear_test_connections(add_default_connections_back=True):
    # clear environment variables with AIRFLOW_CONN prefix
    import os

    env_vars_to_remove = [key for key in os.environ.keys() if key.startswith("AIRFLOW_CONN_")]
    for env_var in env_vars_to_remove:
        del os.environ[env_var]

    if add_default_connections_back:
        create_default_connections_for_tests()


def clear_db_connections(add_default_connections_back=True):
    with create_session() as session:
        session.execute(delete(Connection))
        if add_default_connections_back:
            create_default_connections(session)


def clear_db_variables():
    with create_session() as session:
        session.execute(delete(Variable))


def clear_db_dag_code():
    with create_session() as session:
        session.execute(delete(DagCode))


def clear_db_callbacks():
    with create_session() as session:
        if AIRFLOW_V_3_2_PLUS:
            from airflow.models.callback import Callback

            session.execute(delete(Callback))

        else:
            session.execute(delete(DbCallbackRequest))


def set_default_pool_slots(slots):
    with create_session() as session:
        default_pool = Pool.get_default_pool(session)
        default_pool.slots = slots


def clear_rendered_ti_fields():
    with create_session() as session:
        session.execute(delete(RenderedTaskInstanceFields))


def clear_db_import_errors():
    with create_session() as session:
        session.execute(delete(ParseImportError))


def clear_db_dag_warnings():
    with create_session() as session:
        session.execute(delete(DagWarning))


def clear_db_xcom():
    with create_session() as session:
        session.execute(delete(XCom))


def clear_db_pakl():
    if not AIRFLOW_V_3_2_PLUS:
        return
    from airflow.models.asset import PartitionedAssetKeyLog

    with create_session() as session:
        session.execute(delete(PartitionedAssetKeyLog))


def clear_db_apdr():
    if not AIRFLOW_V_3_2_PLUS:
        return
    from airflow.models.asset import AssetPartitionDagRun

    with create_session() as session:
        session.execute(delete(AssetPartitionDagRun))


def clear_db_logs():
    with create_session() as session:
        session.execute(delete(Log))


def clear_db_jobs():
    with create_session() as session:
        session.execute(delete(Job))


def clear_db_task_reschedule():
    with create_session() as session:
        session.execute(delete(TaskReschedule))


def clear_db_dag_parsing_requests():
    with create_session() as session:
        from airflow.models.dagbag import DagPriorityParsingRequest

        session.execute(delete(DagPriorityParsingRequest))


def clear_db_dag_bundles():
    with create_session() as session:
        from airflow.models.dagbundle import DagBundleModel

        session.execute(delete(DagBundleModel))


def clear_db_teams():
    with create_session() as session:
        from airflow.models.team import Team

        session.execute(delete(Team))


def clear_dag_specific_permissions():
    if "FabAuthManager" not in conf.get("core", "auth_manager"):
        return
    try:
        from airflow.providers.fab.auth_manager.models import Permission, Resource, assoc_permission_role
    except ImportError:
        # Handle Pre-airflow 2.9 case where FAB was part of the core airflow
        from airflow.providers.fab.auth.managers.fab.models import (
            Permission,
            Resource,
            assoc_permission_role,
        )
    except RuntimeError as e:
        # Handle case where FAB provider is not even usable
        if "needs Apache Airflow 2.9.0" in str(e):
            from airflow.providers.fab.auth.managers.fab.models import (
                Permission,
                Resource,
                assoc_permission_role,
            )
        else:
            raise
    with create_session() as session:
        dag_resources = session.scalars(
            select(Resource).where(Resource.name.like(f"{RESOURCE_DAG_PREFIX}%"))
        ).all()
        dag_resource_ids = [d.id for d in dag_resources]

        dag_permissions = session.scalars(
            select(Permission).where(Permission.resource_id.in_(dag_resource_ids))
        ).all()
        dag_permission_ids = [d.id for d in dag_permissions]

        session.execute(
            delete(assoc_permission_role).where(
                assoc_permission_role.c.permission_view_id.in_(dag_permission_ids)
            )
        )
        session.execute(delete(Permission).where(Permission.resource_id.in_(dag_resource_ids)))
        session.execute(delete(Resource).where(Resource.id.in_(dag_resource_ids)))


def create_default_connections_for_tests():
    """
    Create default Airflow connections for tests.

    For testing purposes, we do not need to have the connections setup in the database, using environment
    variables instead would provide better lookup speeds and is easier too.
    """
    import os

    try:
        from airflow.utils.db import get_default_connections

        conns = get_default_connections()
    except ImportError:
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

    for c in conns:
        envvar = f"AIRFLOW_CONN_{c.conn_id.upper()}"
        os.environ[envvar] = c.as_json()


def clear_all():
    clear_db_runs()
    clear_db_assets()
    clear_db_apdr()
    clear_db_pakl()
    clear_db_triggers()
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_dag_code()
    clear_db_callbacks()
    clear_rendered_ti_fields()
    clear_db_import_errors()
    clear_db_dag_warnings()
    clear_db_logs()
    clear_db_jobs()
    clear_db_task_reschedule()
    clear_db_xcom()
    clear_db_variables()
    clear_db_pools()
    clear_test_connections(add_default_connections_back=True)
    clear_db_deadline()
    clear_dag_specific_permissions()
    if AIRFLOW_V_3_0_PLUS:
        clear_db_backfills()
        clear_db_dag_bundles()
        clear_db_dag_parsing_requests()
