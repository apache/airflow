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
"""
Tests for the pending partition key migration, written against the schema as it stood at that revision.

The tables are declared here instead of taken from ``airflow.models``, and alembic moves the database
to the revision, so later migrations and model changes cannot change what these tests exercise.
"""

from __future__ import annotations

from importlib import import_module

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.utils.db import downgrade, upgradedb
from airflow.utils.sqlalchemy import UtcDateTime

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0148_3_4_0_add_pending_partition_key_to_apdr")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
DAG_ID = "migration_0148"
OTHER_DAG_ID = "migration_0148_other"
PARTITION_DATE = timezone.datetime(2026, 5, 20)

metadata = sa.MetaData()
dag_run = sa.Table(
    "dag_run",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("run_type", sa.String(50)),
    sa.Column("run_after", UtcDateTime),
)
apdr = sa.Table(
    "asset_partition_dag_run",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("target_dag_id", sa.String(250)),
    sa.Column("created_dag_run_id", sa.Integer),
    sa.Column("partition_key", sa.String(250)),
    sa.Column("partition_date", UtcDateTime),
    sa.Column("created_at", UtcDateTime),
    sa.Column("updated_at", UtcDateTime),
)
key_log = sa.Table(
    "partitioned_asset_key_log",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("asset_id", sa.Integer),
    sa.Column("asset_event_id", sa.Integer),
    sa.Column("asset_partition_dag_run_id", sa.Integer),
    sa.Column("source_partition_key", sa.String(250)),
    sa.Column("target_dag_id", sa.String(250)),
    sa.Column("target_partition_key", sa.String(250)),
    sa.Column("created_at", UtcDateTime),
)


def _delete_rows(connection):
    dag_ids = (DAG_ID, OTHER_DAG_ID)
    connection.execute(key_log.delete().where(key_log.c.target_dag_id.in_(dag_ids)))
    connection.execute(apdr.delete().where(apdr.c.target_dag_id.in_(dag_ids)))
    connection.execute(dag_run.delete().where(dag_run.c.dag_id == DAG_ID))


@pytest.fixture
def database_before_revision():
    """Move the database to the revision before the migration for one test, then back to the current head."""
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        downgrade(to_revision=PREVIOUS_REVISION)
        with settings.engine.begin() as connection:
            _delete_rows(connection)
        yield settings.engine
    finally:
        try:
            with settings.engine.begin() as connection:
                _delete_rows(connection)
            upgradedb()
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


def test_upgrade_marks_only_the_latest_pending_row_per_key_and_keeps_every_row(database_before_revision):
    now = timezone.utcnow()
    with database_before_revision.begin() as connection:
        dag_run_id = connection.execute(
            dag_run.insert().values(dag_id=DAG_ID, run_id="run", run_type="asset_triggered", run_after=now)
        ).inserted_primary_key[0]

        def add_apdr(partition_key, *, dag_id=DAG_ID, created_dag_run_id=None, partition_date=None):
            return connection.execute(
                apdr.insert().values(
                    target_dag_id=dag_id,
                    partition_key=partition_key,
                    created_dag_run_id=created_dag_run_id,
                    partition_date=partition_date,
                    created_at=now,
                    updated_at=now,
                )
            ).inserted_primary_key[0]

        completed = add_apdr("day-1", created_dag_run_id=dag_run_id)
        older_pending = add_apdr("day-1", partition_date=PARTITION_DATE)
        other_dag = add_apdr("day-1", dag_id=OTHER_DAG_ID)
        latest_pending = add_apdr("day-1")
        other_key = add_apdr("day-2")
        apdr_ids = (completed, older_pending, other_dag, latest_pending, other_key)
        for asset_event_id, apdr_id in enumerate(apdr_ids):
            connection.execute(
                key_log.insert().values(
                    asset_id=asset_event_id,
                    asset_event_id=asset_event_id,
                    asset_partition_dag_run_id=apdr_id,
                    source_partition_key="source",
                    target_dag_id=OTHER_DAG_ID if apdr_id == other_dag else DAG_ID,
                    target_partition_key="target",
                    created_at=now,
                )
            )

    upgradedb(to_revision=REVISION)

    upgraded_apdr = sa.Table(apdr.name, sa.MetaData(), autoload_with=database_before_revision)
    with database_before_revision.connect() as connection:
        apdr_rows = {
            row.id: (row.partition_key, row.created_dag_run_id, row.pending_partition_key)
            for row in connection.execute(
                sa.select(upgraded_apdr).where(upgraded_apdr.c.target_dag_id.in_((DAG_ID, OTHER_DAG_ID)))
            )
        }
        partition_dates = dict(
            connection.execute(
                sa.select(apdr.c.id, apdr.c.partition_date).where(
                    apdr.c.target_dag_id.in_((DAG_ID, OTHER_DAG_ID))
                )
            ).all()
        )
        log_rows = dict(
            connection.execute(
                sa.select(key_log.c.asset_event_id, key_log.c.asset_partition_dag_run_id).where(
                    key_log.c.target_dag_id.in_((DAG_ID, OTHER_DAG_ID))
                )
            ).all()
        )

    # A Dag without batch_asset_events has one pending row per event, so none are merged or deleted:
    # only the latest pending row of each key is marked, and a completed row never is.
    assert apdr_rows == {
        completed: ("day-1", dag_run_id, None),
        older_pending: ("day-1", None, None),
        other_dag: ("day-1", None, "day-1"),
        latest_pending: ("day-1", None, "day-1"),
        other_key: ("day-2", None, "day-2"),
    }
    assert partition_dates[older_pending] == PARTITION_DATE
    assert log_rows == dict(enumerate(apdr_ids))
    assert _migration._UQ_NAME in {
        constraint["name"]
        for constraint in inspect(database_before_revision).get_unique_constraints(apdr.name)
    }
