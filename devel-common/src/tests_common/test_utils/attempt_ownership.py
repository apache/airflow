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

from datetime import datetime, timezone
from uuid import UUID

import pytest
import sqlalchemy as sa

from tests_common.test_utils.db import clear_db_runs

CURRENT_ID = UUID("01960000-0000-7000-8000-000000000002")
HISTORY_ID = UUID("01960000-0000-7000-8000-000000000001")
DANGLING_VERSION = UUID("01960000-0000-7000-8000-000000000099")
NOW = datetime(2026, 9, 29, tzinfo=timezone.utc)
COORDINATES = {"dag_id": "ownership", "task_id": "task", "run_id": "manual", "map_index": -1}


def table(connection, name, *uuid_columns):
    reflected = sa.Table(name, sa.MetaData(), autoload_with=connection)
    for column in uuid_columns:
        reflected.c[column].type = sa.Uuid()
    if "working_set" in reflected.c:
        reflected.c.working_set.type = sa.Boolean()
    return reflected


def _drop_archive_tables(session):
    from airflow.utils.db_cleanup import ARCHIVE_TABLE_PREFIX

    bind = session.get_bind()
    for name in sa.inspect(bind).get_table_names():
        if name.startswith(ARCHIVE_TABLE_PREFIX):
            sa.Table(name, sa.MetaData()).drop(bind=bind)


def _clear(session):
    from airflow.models.renderedtifields import LegacyRenderedTaskInstanceFields
    from airflow.models.taskinstance import LegacyTaskDataOwner
    from airflow.models.xcom import XComModelV1

    session.rollback()
    for model in (XComModelV1, LegacyRenderedTaskInstanceFields, LegacyTaskDataOwner):
        session.execute(sa.delete(model))
    session.commit()
    clear_db_runs()
    _drop_archive_tables(session)


@pytest.fixture
def ownership_session(session):
    """Hold the rows migration 0142 leaves behind for one archived and one current attempt."""
    from airflow.models.dagrun import DagRun
    from airflow.models.hitl import HITLDetail
    from airflow.models.renderedtifields import LegacyRenderedTaskInstanceFields
    from airflow.models.taskinstance import (
        LegacyTaskDataOwner,
        TaskInstance,
        TaskInstanceNote,
    )
    from airflow.models.taskreschedule import TaskReschedule
    from airflow.models.xcom import XComModelV1

    _clear(session)
    dag_run_id = session.execute(
        DagRun.__table__.insert().values(
            dag_id=COORDINATES["dag_id"],
            run_id=COORDINATES["run_id"],
            run_type="manual",
            run_after=NOW,
            state="running",
            start_date=NOW,
        )
    ).inserted_primary_key[0]
    attempts = TaskInstance.__table__
    session.execute(
        attempts.insert().values(
            **COORDINATES,
            id=CURRENT_ID,
            try_number=2,
            pool="default_pool",
            pool_slots=1,
            state="running",
            max_tries=3,
            working_set=True,
        )
    )
    session.execute(
        attempts.insert().values(
            **COORDINATES,
            id=HISTORY_ID,
            try_number=1,
            pool="default_pool",
            pool_slots=1,
            state="failed",
            max_tries=0,
            duration=12.5,
            working_set=None,
            archived_reason="legacy",
        )
    )
    session.execute(LegacyTaskDataOwner.__table__.insert().values(**COORDINATES, task_instance_id=CURRENT_ID))
    session.execute(
        XComModelV1.__table__.insert().values(
            **COORDINATES,
            dag_run_id=dag_run_id,
            key="return_value",
            value={"legacy": True},
            timestamp=NOW,
            mapped_length=3,
        )
    )
    session.execute(
        LegacyRenderedTaskInstanceFields.__table__.insert().values(
            **COORDINATES,
            rendered_fields={"field": "legacy"},
            k8s_pod_yaml={"kind": "Pod"},
        )
    )
    session.execute(
        TaskInstanceNote.__table__.insert().values(
            ti_id=CURRENT_ID, content="keep this note", created_at=NOW, updated_at=NOW
        )
    )
    session.execute(
        TaskReschedule.__table__.insert().values(
            ti_id=CURRENT_ID, start_date=NOW, end_date=NOW, duration=0, reschedule_date=NOW
        )
    )
    session.execute(
        HITLDetail.__table__.insert().values(
            ti_id=HISTORY_ID,
            options=["yes", "no"],
            subject="approval",
            params={},
            params_input={"answer": 1},
            created_at=NOW,
        )
    )
    session.commit()
    yield session
    _clear(session)
