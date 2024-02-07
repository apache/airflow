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

from sqlalchemy import delete, select

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
    SlaMiss,
    TaskFail,
    TaskInstance,
    TaskReschedule,
    Trigger,
    Variable,
    XCom,
    errors,
)
from airflow.models.dag import DagOwnerAttributes
from airflow.models.dagcode import DagCode
from airflow.models.dagwarning import DagWarning
from airflow.models.dataset import (
    DagScheduleDatasetReference,
    DatasetDagRunQueue,
    DatasetEvent,
    DatasetModel,
    TaskOutletDatasetReference,
)
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.fab.auth_manager.models import Permission, Resource, assoc_permission_role
from airflow.security.permissions import RESOURCE_DAG_PREFIX
from airflow.typing_compat import Literal
from airflow.utils.db import add_default_pool_if_not_exists, create_default_connections, reflect_tables
from airflow.utils.session import create_session

SyncSessionTypeDef = Literal["auto", "fetch", "evaluate", False]

DELETE_CONNECTION_STMT = delete(Connection)
DELETE_DAG_CODE_STMT = delete(DagCode)
DELETE_DAG_MODEL_STMT = delete(DagModel)
DELETE_DAG_OWNER_ATTRIBUTES_STMT = delete(DagOwnerAttributes)
DELETE_DAG_RUN_STMT = delete(DagRun)
DELETE_DAG_SCHEDULE_DATASET_REFERENCE_STMT = delete(DagScheduleDatasetReference)
DELETE_DAG_TAG_STMT = delete(DagTag)
DELETE_DAG_WARNING_STMT = delete(DagWarning)
DELETE_DATASET_DAG_RUN_QUEUE_STMT = delete(DatasetDagRunQueue)
DELETE_DATASET_EVENT_STMT = delete(DatasetEvent)
DELETE_DATASET_MODEL_STMT = delete(DatasetModel)
DELETE_DB_CALLBACK_REQUEST_STMT = delete(DbCallbackRequest)
DELETE_IMPORT_ERROR_STMT = delete(errors.ImportError)
DELETE_JOB_STMT = delete(Job)
DELETE_LOG_STMT = delete(Log)
DELETE_POOL_STMT = delete(Pool)
DELETE_RENDERED_TI_FIELDS_STMT = delete(RenderedTaskInstanceFields)
DELETE_SERIALIZED_DAG_MODEL_STMT = delete(SerializedDagModel)
DELETE_SLA_MISS_STMT = delete(SlaMiss)
DELETE_TASK_FAIL_STMT = delete(TaskFail)
DELETE_TASK_OUTLET_DATASET_REFERENCE_STMT = delete(TaskOutletDatasetReference)
DELETE_TASK_RESCHEDULE_STMT = delete(TaskReschedule)
DELETE_TI_STMT = delete(TaskInstance)
DELETE_TRIGGER_STMT = delete(Trigger)
DELETE_VARIABLE_STMT = delete(Variable)
DELETE_XCOM_STMT = delete(XCom)


def _run_statements(*statements, synchronize_session: SyncSessionTypeDef):
    with create_session() as session:
        for stmt in statements:
            session.execute(stmt.execution_options(synchronize_session=synchronize_session))


def clear_db_runs(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(
        DELETE_JOB_STMT,
        DELETE_TRIGGER_STMT,
        DELETE_DAG_RUN_STMT,
        DELETE_TI_STMT,
        synchronize_session=synchronize_session,
    )


def clear_db_datasets(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(
        DELETE_DATASET_EVENT_STMT,
        DELETE_DATASET_MODEL_STMT,
        DELETE_DATASET_DAG_RUN_QUEUE_STMT,
        DELETE_DAG_SCHEDULE_DATASET_REFERENCE_STMT,
        DELETE_TASK_OUTLET_DATASET_REFERENCE_STMT,
        synchronize_session=synchronize_session,
    )


def clear_db_dags(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(
        DELETE_DAG_TAG_STMT,
        DELETE_DAG_OWNER_ATTRIBUTES_STMT,
        DELETE_DAG_MODEL_STMT,
        synchronize_session=synchronize_session,
    )


def drop_tables_with_prefix(prefix):
    with create_session() as session:
        metadata = reflect_tables(None, session)
        for table_name, table in metadata.tables.items():
            if table_name.startswith(prefix):
                table.drop(session.bind)


def clear_db_serialized_dags(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_SERIALIZED_DAG_MODEL_STMT, synchronize_session=synchronize_session)


def clear_db_sla_miss(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_SLA_MISS_STMT, synchronize_session=synchronize_session)


def clear_db_pools(*, add_default_poll=True, synchronize_session: SyncSessionTypeDef = False):
    with create_session() as session:
        session.execute(DELETE_POOL_STMT.execution_options(synchronize_session=synchronize_session))
        if add_default_poll:
            add_default_pool_if_not_exists(session)


def clear_db_connections(add_default_connections_back=True, synchronize_session: SyncSessionTypeDef = False):
    with create_session() as session:
        session.execute(DELETE_CONNECTION_STMT.execution_options(synchronize_session=synchronize_session))
        if add_default_connections_back:
            create_default_connections(session)


def clear_db_variables(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_VARIABLE_STMT, synchronize_session=synchronize_session)


def clear_db_dag_code(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_DAG_CODE_STMT, synchronize_session=synchronize_session)


def clear_db_callbacks(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_DB_CALLBACK_REQUEST_STMT, synchronize_session=synchronize_session)


def set_default_pool_slots(slots):
    with create_session() as session:
        default_pool = Pool.get_default_pool(session)
        default_pool.slots = slots


def clear_rendered_ti_fields(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_RENDERED_TI_FIELDS_STMT, synchronize_session=synchronize_session)


def clear_db_import_errors(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_IMPORT_ERROR_STMT, synchronize_session=synchronize_session)


def clear_db_dag_warnings(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_DAG_WARNING_STMT, synchronize_session=synchronize_session)


def clear_db_xcom(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_XCOM_STMT, synchronize_session=synchronize_session)


def clear_db_logs(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_LOG_STMT, synchronize_session=synchronize_session)


def clear_db_jobs(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_JOB_STMT, synchronize_session=synchronize_session)


def clear_db_task_fail(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_TASK_FAIL_STMT, synchronize_session=synchronize_session)


def clear_db_task_reschedule(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_TASK_RESCHEDULE_STMT, synchronize_session=synchronize_session)


def clear_dag_specific_permissions(*, synchronize_session: SyncSessionTypeDef = False):
    with create_session() as session:
        dag_resource_ids = tuple(
            session.scalars(select(Resource.id).where(Resource.name.like(f"{RESOURCE_DAG_PREFIX}%")))
        )
        if not dag_resource_ids:
            return

        dag_permission_ids = tuple(
            session.scalars(select(Permission.id).where(Permission.resource_id.in_(dag_resource_ids)))
        )

        delete_assoc_perm_role_stmt = delete(assoc_permission_role).where(
            assoc_permission_role.c.permission_view_id.in_(dag_permission_ids)
        )
        delete_permissions_stmt = delete(Permission).where(Permission.resource_id.in_(dag_resource_ids))
        delete_resource_stmt = delete(Resource).where(Resource.id.in_(dag_resource_ids))

        for stmt in (delete_assoc_perm_role_stmt, delete_permissions_stmt, delete_resource_stmt):
            session.execute(stmt.execution_options(synchronize_session=synchronize_session))


def clear_db_tags(*, synchronize_session: SyncSessionTypeDef = False):
    _run_statements(DELETE_DAG_TAG_STMT, synchronize_session=synchronize_session)


def clear_all():
    clear_db_runs()
    clear_db_datasets()
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_sla_miss()
    clear_db_dag_code()
    clear_db_callbacks()
    clear_rendered_ti_fields()
    clear_db_import_errors()
    clear_db_dag_warnings()
    clear_db_logs()
    clear_db_jobs()
    clear_db_task_fail()
    clear_db_task_reschedule()
    clear_db_xcom()
    clear_db_variables()
    clear_db_pools()
    clear_db_connections(add_default_connections_back=True)
    clear_dag_specific_permissions()
