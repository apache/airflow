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
from io import StringIO
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic.config import Config
from alembic.environment import EnvironmentContext
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory

from airflow.migrations import db_types

migration = import_module(
    "airflow.providers.edge3.migrations.versions.0006_5_0_0_add_task_instance_id_to_edge_job"
)
COORDINATES = ["dag_id", "task_id", "run_id", "map_index", "try_number"]


def reflected_columns(connection):
    return [
        column | {"type": str(column["type"])} for column in sa.inspect(connection).get_columns("edge_job")
    ]


@pytest.fixture
def legacy_jobs(monkeypatch):
    for name in ("TIMESTAMP", "StringID"):
        monkeypatch.delitem(vars(db_types), name, raising=False)
    for name in ("TIMESTAMP", "StringID"):
        monkeypatch.setitem(vars(db_types), name, getattr(db_types, name))
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        metadata = sa.MetaData(naming_convention={"pk": "%(table_name)s_pkey"})
        scripts = ScriptDirectory(str(Path(migration.__file__).parents[1]))
        with EnvironmentContext(Config(), scripts) as environment:
            environment.configure(connection=connection, target_metadata=metadata)
            with Operations.context(environment.get_context()):
                for revision in reversed(
                    list(scripts.walk_revisions(base="base", head=migration.down_revision))
                ):
                    revision.module.upgrade()
                jobs = sa.Table("edge_job", sa.MetaData(), autoload_with=connection)
                row = dict(
                    dag_id="dag",
                    task_id="task",
                    run_id="run",
                    map_index=-1,
                    try_number=1,
                    state="queued",
                    queue="default",
                    concurrency_slots=1,
                    command='{"ti":{"id":"00000000-0000-0000-0000-000000000001"}}',
                )
                connection.execute(jobs.insert().values(**row))
                yield connection, row
    engine.dispose()


def test_upgrade_preserves_existing_job_and_default_legacy_identity(legacy_jobs):
    connection, original = legacy_jobs
    migration.upgrade()
    jobs = sa.Table("edge_job", sa.MetaData(), autoload_with=connection)
    row = connection.execute(sa.select(jobs)).mappings().one()
    assert row["task_instance_id"] == ""
    assert all(row[key] == value for key, value in original.items())
    connection.execute(jobs.insert().values(**(original | {"task_id": "another"})))
    assert connection.scalar(sa.select(jobs.c.task_instance_id).where(jobs.c.task_id == "another")) == ""
    inspector = sa.inspect(connection)
    assert inspector.get_pk_constraint("edge_job")["constrained_columns"] == [
        *COORDINATES,
        "task_instance_id",
    ]
    assert "rj_order" in {index["name"] for index in inspector.get_indexes("edge_job")}


def test_upgrade_allows_distinct_attempts_at_identical_coordinates(legacy_jobs):
    connection, original = legacy_jobs
    migration.upgrade()
    jobs = sa.Table("edge_job", sa.MetaData(), autoload_with=connection)
    identities = {"", str(uuid4()), str(uuid4())}
    for identity in identities - {""}:
        connection.execute(jobs.insert().values(**original, task_instance_id=identity))
    assert set(connection.scalars(sa.select(jobs.c.task_instance_id))) == identities
    with pytest.raises(sa.exc.IntegrityError):
        connection.execute(jobs.insert().values(**original, task_instance_id=""))


def test_downgrade_rejects_duplicate_legacy_keys_before_changing_schema(legacy_jobs):
    connection, original = legacy_jobs
    migration.upgrade()
    jobs = sa.Table("edge_job", sa.MetaData(), autoload_with=connection)
    identity = str(uuid4())
    connection.execute(jobs.insert().values(**original, task_instance_id=identity))
    before_columns = reflected_columns(connection)
    before_pk = sa.inspect(connection).get_pk_constraint("edge_job")
    with pytest.raises(RuntimeError, match="multiple task instances.*same coordinates"):
        migration.downgrade()
    assert reflected_columns(connection) == before_columns
    assert sa.inspect(connection).get_pk_constraint("edge_job") == before_pk
    assert set(connection.scalars(sa.select(jobs.c.task_instance_id))) == {"", identity}


@pytest.mark.parametrize("native", [False, True])
def test_safe_downgrade_restores_original_schema_and_preserves_jobs(legacy_jobs, native):
    connection, original = legacy_jobs
    before_columns = reflected_columns(connection)
    before_pk = sa.inspect(connection).get_pk_constraint("edge_job")
    before_indexes = sa.inspect(connection).get_indexes("edge_job")
    migration.upgrade()
    jobs = sa.Table("edge_job", sa.MetaData(), autoload_with=connection)
    if native:
        connection.execute(jobs.update().values(task_instance_id=str(uuid4())))
    migration.downgrade()
    inspector = sa.inspect(connection)
    assert reflected_columns(connection) == before_columns
    assert inspector.get_pk_constraint("edge_job") == before_pk
    assert inspector.get_indexes("edge_job") == before_indexes
    restored = sa.Table("edge_job", sa.MetaData(), autoload_with=connection)
    row = connection.execute(sa.select(restored)).mappings().one()
    assert all(row[key] == value for key, value in original.items())
    migration.upgrade()
    assert "task_instance_id" in {column["name"] for column in sa.inspect(connection).get_columns("edge_job")}


@pytest.mark.parametrize("dialect", ["postgresql", "mysql"])
def test_upgrade_compiles_primary_key_ddl_for_server_databases(dialect):
    output = StringIO()
    context = MigrationContext.configure(dialect_name=dialect, opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        migration.upgrade()
    ddl = output.getvalue()
    assert "PRIMARY KEY (dag_id, task_id, run_id, map_index, try_number, task_instance_id)" in ddl
    assert "task_instance_id VARCHAR(36)" in ddl
    assert "NOT NULL" in ddl
    assert "DEFAULT ''" in ddl
    if dialect == "mysql":
        assert "CHARACTER SET ascii COLLATE ascii_bin" in ddl
        assert "DROP PRIMARY KEY" in ddl
    else:
        assert "DROP CONSTRAINT edge_job_pkey" in ddl


@pytest.mark.parametrize("dialect", ["postgresql", "mysql"])
def test_offline_downgrade_refuses_before_emitting_schema_changes(dialect):
    output = StringIO()
    context = MigrationContext.configure(dialect_name=dialect, opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context), pytest.raises(RuntimeError, match="requires an online check"):
        migration.downgrade()
    assert output.getvalue() == ""
