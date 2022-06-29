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
from airflow.jobs.base_job import BaseJob
from airflow.jobs.triggerer_job import TriggererJob
from airflow.models import (
    Connection,
    DagModel,
    DagRun,
    DagTag,
    DagWarning,
    DbCallbackRequest,
    Log,
    Pool,
    RenderedTaskInstanceFields,
    SlaMiss,
    TaskFail,
    TaskInstance,
    TaskReschedule,
    Trigger,
    Variable,
    XCom,
    errors,
)
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel
from airflow.security.permissions import RESOURCE_DAG_PREFIX
from airflow.utils.db import add_default_pool_if_not_exists, create_default_connections, reflect_tables
from airflow.utils.session import create_session
from airflow.www.fab_security.sqla.models import Permission, Resource, assoc_permission_role


def clear_db_runs():
    with create_session() as session:
        session.query(TriggererJob).delete()
        session.query(Trigger).delete()
        session.query(DagRun).delete()
        session.query(TaskInstance).delete()


def clear_db_dags():
    with create_session() as session:
        session.query(DagTag).delete()
        session.query(DagModel).delete()


def drop_tables_with_prefix(prefix):
    with create_session() as session:
        metadata = reflect_tables(None, session)
        for table_name, table in metadata.tables.items():
            if table_name.startswith(prefix):
                table.drop()


def clear_db_serialized_dags():
    with create_session() as session:
        session.query(SerializedDagModel).delete()


def clear_db_sla_miss():
    with create_session() as session:
        session.query(SlaMiss).delete()


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
        session.query(errors.ImportError).delete()


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
        session.query(BaseJob).delete()


def clear_db_task_fail():
    with create_session() as session:
        session.query(TaskFail).delete()


def clear_db_task_reschedule():
    with create_session() as session:
        session.query(TaskReschedule).delete()


def clear_dag_specific_permissions():
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
