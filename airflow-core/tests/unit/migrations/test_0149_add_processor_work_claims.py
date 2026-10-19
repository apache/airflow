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

from importlib import import_module
from uuid import UUID

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.pool import NullPool

pytestmark = pytest.mark.db_test

migration = import_module("airflow.migrations.versions.0149_3_5_0_add_processor_work_claims")
CALLBACK_ID = UUID(int=1)
CLAIM_ID = UUID(int=2)
PRIORITY_ID = "priority-request"

metadata = sa.MetaData()
job = sa.Table("job", metadata, sa.Column("id", sa.Integer, primary_key=True))
callback = sa.Table("callback", metadata, sa.Column("id", sa.Uuid, primary_key=True))
priority = sa.Table(
    "dag_priority_parsing_request", metadata, sa.Column("id", sa.String(32), primary_key=True)
)
deadline = sa.Table(
    "deadline",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("callback_id", sa.Uuid, sa.ForeignKey("callback.id", ondelete="CASCADE"), nullable=False),
)
claim = sa.table(
    "dag_processor_work_claim",
    sa.column("job_id", sa.Integer),
    sa.column("kind", sa.String(10)),
    sa.column("claim_id", sa.Uuid),
    sa.column("claim_sequence", sa.BigInteger),
    sa.column("payload_hash", sa.String(64)),
)


def _run_migration(engine, migrate):
    with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
        migrate()


@pytest.fixture
def migrated_database(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'work_claims.db'}", poolclass=NullPool)

    @sa.event.listens_for(engine, "connect")
    def _enable_foreign_keys(connection, connection_record):
        connection.execute("PRAGMA foreign_keys=ON")

    try:
        metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(job.insert().values(id=1))
            connection.execute(callback.insert().values(id=CALLBACK_ID))
            connection.execute(priority.insert().values(id=PRIORITY_ID))
            connection.execute(deadline.insert().values(id=1, callback_id=CALLBACK_ID))
        _run_migration(engine, migration.upgrade)
        yield engine
    finally:
        engine.dispose()


def _get_retained_rows(engine):
    with engine.connect() as connection:
        return (
            connection.scalars(sa.select(callback.c.id)).all(),
            connection.scalars(sa.select(priority.c.id)).all(),
            connection.scalars(sa.select(deadline.c.callback_id)).all(),
        )


def test_upgrade_and_downgrade_preserve_requests_and_callback_children(migrated_database):
    expected = ([CALLBACK_ID], [PRIORITY_ID], [CALLBACK_ID])
    assert _get_retained_rows(migrated_database) == expected
    with migrated_database.begin() as connection:
        connection.execute(
            claim.insert(),
            [
                {
                    "job_id": 1,
                    "kind": kind,
                    "claim_id": CLAIM_ID,
                    "claim_sequence": 2**53 - 1,
                    "payload_hash": "a" * 64,
                }
                for kind in ("callbacks", "priority")
            ],
        )
        assert connection.execute(sa.select(claim.c.claim_id, claim.c.claim_sequence)).all() == [
            (CLAIM_ID, 2**53 - 1),
            (CLAIM_ID, 2**53 - 1),
        ]

    _run_migration(migrated_database, migration.downgrade)

    inspector = sa.inspect(migrated_database)
    assert not inspector.has_table(claim.name)
    for table in (callback, priority):
        assert {column["name"] for column in inspector.get_columns(table.name)} == {"id"}
    assert _get_retained_rows(migrated_database) == expected

    _run_migration(migrated_database, migration.upgrade)
    assert sa.inspect(migrated_database).has_table(claim.name)
    assert _get_retained_rows(migrated_database) == expected


def test_deleting_job_removes_receipts_and_releases_requests(migrated_database):
    requests = [
        sa.Table(table.name, sa.MetaData(), autoload_with=migrated_database) for table in (callback, priority)
    ]
    with migrated_database.begin() as connection:
        for kind, table in zip(("callbacks", "priority"), requests, strict=True):
            connection.execute(
                claim.insert().values(
                    job_id=1,
                    kind=kind,
                    claim_id=CLAIM_ID,
                    claim_sequence=1,
                    payload_hash="a" * 64,
                )
            )
            connection.execute(table.update().values(processor_job_id=1))

        connection.execute(job.delete().where(job.c.id == 1))

        assert connection.scalars(sa.select(claim.c.job_id)).all() == []
        for table in requests:
            assert connection.scalars(sa.select(table.c.processor_job_id)).all() == [None]
    assert _get_retained_rows(migrated_database) == ([CALLBACK_ID], [PRIORITY_ID], [CALLBACK_ID])
