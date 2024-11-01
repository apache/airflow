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

import os

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
    XCom,
)
from airflow.models.dag import DagOwnerAttributes
from airflow.models.dagcode import DagCode
from airflow.models.dagwarning import DagWarning
from airflow.models.serialized_dag import SerializedDagModel
from airflow.security.permissions import RESOURCE_DAG_PREFIX
from airflow.utils.db import add_default_pool_if_not_exists, create_default_connections, reflect_tables
from airflow.utils.session import create_session

from tests_common.test_utils.compat import (
    AIRFLOW_V_2_10_PLUS,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetReference,
    ParseImportError,
    TaskOutletAssetReference,
)


def initial_db_init():
    from flask import Flask

    from airflow.configuration import conf
    from airflow.utils import db
    from airflow.www.extensions.init_appbuilder import init_appbuilder
    from airflow.www.extensions.init_auth_manager import get_auth_manager

    from tests_common.test_utils.compat import AIRFLOW_V_2_8_PLUS

    db.resetdb()
    db.bootstrap_dagbag()
    # minimal app to add roles
    flask_app = Flask(__name__)
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")
    init_appbuilder(flask_app)
    if AIRFLOW_V_2_8_PLUS:
        get_auth_manager().init()


def clear_db_runs():
    with create_session() as session:
        session.query(Job).delete()
        session.query(Trigger).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()
        try:
            from airflow.models import TaskInstanceHistory

            session.query(TaskInstanceHistory).delete()
        except ImportError:
            pass


def clear_db_backfills():
    from airflow.models.backfill import Backfill, BackfillDagRun

    with create_session() as session:
        session.query(BackfillDagRun).delete()
        session.query(Backfill).delete()


def clear_db_assets():
    with create_session() as session:
        session.query(AssetEvent).delete()
        session.query(AssetModel).delete()
        session.query(AssetDagRunQueue).delete()
        session.query(DagScheduleAssetReference).delete()
        session.query(TaskOutletAssetReference).delete()
        if AIRFLOW_V_2_10_PLUS:
            from tests_common.test_utils.compat import AssetAliasModel

            session.query(AssetAliasModel).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagOwnerAttributes).delete()
        session.query(DagModel).delete()


def drop_tables_with_prefix(prefix):
    with create_session() as session:
        metadata = reflect_tables(None, session)
        for table_name, table in metadata.tables.items():
            if table_name.startswith(prefix):
                table.drop(session.bind)


def clear_db_serialized_dags():
    with create_session() as session:
        session.query(SerializedDagModel).delete()


def clear_db_pools():
    with create_session() as session:
        session.query(Pool).delete()
        add_default_pool_if_not_exists(session)


def clear_db_connections(add_default_connections_back=True):
    with create_session() as session:
        session.query(Connection).delete()
        if add_default_connections_back:
            create_default_connections(session)


def clear_db_variables():
    with create_session() as session:
        session.query(Variable).delete()


def clear_db_dag_code():
    with create_session() as session:
        session.query(DagCode).delete()


def clear_db_callbacks():
    with create_session() as session:
        session.query(DbCallbackRequest).delete()


def set_default_pool_slots(slots):
    with create_session() as session:
        default_pool = Pool.get_default_pool(session)
        default_pool.slots = slots


def clear_rendered_ti_fields():
    with create_session() as session:
        session.query(RenderedTaskInstanceFields).delete()


def clear_db_import_errors():
    with create_session() as session:
        session.query(ParseImportError).delete()


def clear_db_dag_warnings():
    with create_session() as session:
        session.query(DagWarning).delete()


def clear_db_xcom():
    with create_session() as session:
        session.query(XCom).delete()


def clear_db_logs():
    with create_session() as session:
        session.query(Log).delete()


def clear_db_jobs():
    with create_session() as session:
        session.query(Job).delete()


def clear_db_task_reschedule():
    with create_session() as session:
        session.query(TaskReschedule).delete()


def clear_db_dag_parsing_requests():
    with create_session() as session:
        from airflow.models.dagbag import DagPriorityParsingRequest

        session.query(DagPriorityParsingRequest).delete()


def clear_dag_specific_permissions():
    try:
        from airflow.providers.fab.auth_manager.models import Permission, Resource, assoc_permission_role
    except ImportError:
        # Handle Pre-airflow 2.9 case where FAB was part of the core airflow
        from airflow.auth.managers.fab.models import (  # type: ignore[no-redef]
            Permission,
            Resource,
            assoc_permission_role,
        )
    except RuntimeError as e:
        # Handle case where FAB provider is not even usable
        if "needs Apache Airflow 2.9.0" in str(e):
            from airflow.auth.managers.fab.models import (  # type: ignore[no-redef]
                Permission,
                Resource,
                assoc_permission_role,
            )
        else:
            raise
    with create_session() as session:
        dag_resources = session.query(Resource).filter(Resource.name.like(f"{RESOURCE_DAG_PREFIX}%")).all()
        dag_resource_ids = [d.id for d in dag_resources]

        dag_permissions = session.query(Permission).filter(Permission.resource_id.in_(dag_resource_ids)).all()
        dag_permission_ids = [d.id for d in dag_permissions]

        session.query(assoc_permission_role).filter(
            assoc_permission_role.c.permission_view_id.in_(dag_permission_ids)
        ).delete(synchronize_session=False)
        session.query(Permission).filter(Permission.resource_id.in_(dag_resource_ids)).delete(
            synchronize_session=False
        )
        session.query(Resource).filter(Resource.id.in_(dag_resource_ids)).delete(synchronize_session=False)


def clear_all():
    clear_db_runs()
    clear_db_assets()
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
    clear_db_connections(add_default_connections_back=True)
    clear_dag_specific_permissions()


def is_db_isolation_mode():
    return os.environ.get("RUN_TESTS_WITH_DATABASE_ISOLATION", "false").lower() == "true"
