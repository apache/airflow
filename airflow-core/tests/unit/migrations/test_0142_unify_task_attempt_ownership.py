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

from shutil import copyfile
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError, OperationalError

from airflow import settings
from airflow.utils.db import _get_alembic_config

from tests_common.test_utils.attempt_ownership import (
    COORDINATES,
    CURRENT_ID,
    DANGLING_VERSION,
    HISTORY_ID,
    NOW,
    table,
)

pytestmark = [pytest.mark.db_test, pytest.mark.execution_timeout(60)]

PREDECESSOR = "90e4d18ccadf"
REVISION = "e7c2a91bd540"
DAG_RUN_ID = 41
SECOND_DAG_RUN_ID = 42
THIRD_DAG_RUN_ID = 43
FOURTH_DAG_RUN_ID = 44


@pytest.fixture(scope="module")
def predecessor_template_db(tmp_path_factory):
    templates = {}

    def clone(revision, name, target_path):
        source = settings.engine
        dialect = source.dialect.name
        if dialect == "mysql":
            return False
        if revision not in templates:
            template_name = f"ti141_template_{uuid4().hex}"
            if dialect == "sqlite":
                template_path = tmp_path_factory.mktemp("ti141_template") / f"{template_name}.db"
                template_url = source.url.set(database=str(template_path))
                templates[revision] = template_path
            else:
                template_url = source.url.set(database=template_name)
                with source.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
                    admin.exec_driver_sql(f"CREATE DATABASE {template_name}")
                templates[revision] = template_name
            template_engine = sa.create_engine(template_url)
            try:
                with template_engine.connect() as connection:
                    config = _get_alembic_config()
                    config.attributes["connection"] = connection
                    command.upgrade(config, revision)
                    connection.commit()
            finally:
                template_engine.dispose()
        if dialect == "sqlite":
            copyfile(templates[revision], target_path)
        else:
            with source.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
                admin.exec_driver_sql(f"CREATE DATABASE {name} TEMPLATE {templates[revision]}")
        return True

    yield clone

    if settings.engine.dialect.name == "postgresql":
        with settings.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
            for template_name in templates.values():
                admin.exec_driver_sql(f"DROP DATABASE {template_name}")


@pytest.fixture
def predecessor(tmp_path, request, predecessor_template_db):
    source = settings.engine
    name = f"ti141_{uuid4().hex}"
    revision = getattr(request, "param", PREDECESSOR)
    if source.dialect.name == "sqlite":
        database_path = tmp_path / "predecessor.db"
        url = source.url.set(database=str(database_path))
    else:
        url = source.url.set(database=name)
    cloned = predecessor_template_db(
        revision, name, database_path if source.dialect.name == "sqlite" else None
    )
    if not cloned:
        with source.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
            admin.exec_driver_sql(f"CREATE DATABASE {name}")
    engine = sa.create_engine(url)
    try:
        with engine.connect() as connection:
            config = _get_alembic_config()
            config.attributes["connection"] = connection
            if not cloned:
                command.upgrade(config, revision)
            connection.commit()
            if engine.dialect.name == "sqlite":
                connection.exec_driver_sql("PRAGMA foreign_keys=ON")
                connection.commit()

                def enable_foreign_keys(dbapi_connection, connection_record):
                    dbapi_connection.execute("PRAGMA foreign_keys=ON")

                sa.event.listen(engine, "connect", enable_foreign_keys)
            yield connection, config
    finally:
        engine.dispose()
        if source.dialect.name != "sqlite":
            with source.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
                admin.exec_driver_sql(f"DROP DATABASE {name}")


@pytest.fixture
def populated_predecessor(predecessor):
    connection, config = predecessor
    dag_run = table(connection, "dag_run")
    connection.execute(
        dag_run.insert().values(
            id=DAG_RUN_ID,
            dag_id="ownership",
            run_id="manual",
            run_type="manual",
            run_after=NOW,
            state="running",
            start_date=NOW,
        )
    )
    coordinates = dict(dag_id="ownership", task_id="task", run_id="manual", map_index=-1)
    live = table(connection, "task_instance", "id")
    connection.execute(
        live.insert().values(
            **coordinates,
            id=CURRENT_ID,
            try_number=2,
            pool="default_pool",
            pool_slots=1,
            state="running",
            max_tries=3,
        )
    )
    history = table(connection, "task_instance_history", "task_instance_id", "dag_version_id")
    connection.execute(
        history.insert().values(
            **coordinates,
            task_instance_id=HISTORY_ID,
            try_number=1,
            pool="default_pool",
            pool_slots=1,
            state="failed",
            max_tries=None,
            trigger_id=987654,
            dag_version_id=DANGLING_VERSION,
            hostname=None,
            duration=12.5,
        )
    )
    connection.execute(
        table(connection, "xcom")
        .insert()
        .values(
            **coordinates,
            dag_run_id=DAG_RUN_ID,
            key="return_value",
            value={"legacy": True},
            timestamp=NOW,
            mapped_length=3,
        )
    )
    connection.execute(
        table(connection, "rendered_task_instance_fields")
        .insert()
        .values(
            **coordinates,
            rendered_fields={"field": "legacy"},
            k8s_pod_yaml={"kind": "Pod"},
        )
    )
    connection.execute(
        table(connection, "task_instance_note", "ti_id")
        .insert()
        .values(
            ti_id=CURRENT_ID,
            content="keep this note",
            created_at=NOW,
            updated_at=NOW,
        )
    )
    connection.execute(
        table(connection, "task_reschedule", "ti_id")
        .insert()
        .values(
            ti_id=CURRENT_ID,
            start_date=NOW,
            end_date=NOW,
            duration=0,
            reschedule_date=NOW,
        )
    )
    connection.execute(
        table(connection, "hitl_detail_history", "ti_history_id")
        .insert()
        .values(
            ti_history_id=HISTORY_ID,
            options=["yes", "no"],
            subject="approval",
            params={},
            params_input={"answer": 1},
            created_at=NOW,
        )
    )
    connection.commit()
    return connection, config


def test_upgrade_emits_mysql_sql_offline_without_live_schema_reads(capsys, monkeypatch):
    monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", "mysql://")

    command.upgrade(_get_alembic_config(), f"{PREDECESSOR}:{REVISION}", sql=True)

    emitted = capsys.readouterr().out
    assert "CREATE TABLE legacy_task_data_owner" in emitted
    assert "INSERT INTO task_instance" in emitted
    assert "CREATE TABLE xcom_v2" in emitted
    assert emitted.count("ALGORITHM=INPLACE, LOCK=NONE") == 4
    assert emitted.count("DROP FOREIGN KEY") >= 2
    assert emitted.count("ADD CONSTRAINT") >= 2
    assert emitted.count("SET SESSION foreign_key_checks = 0") == 2
    assert emitted.count("SET SESSION foreign_key_checks = @ti141_foreign_key_checks") == 2
    assert "ALGORITHM=COPY" not in emitted


def test_offline_sql_refuses_what_it_cannot_verify_or_render(monkeypatch):
    monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", "postgresql://")
    with pytest.raises(RuntimeError, match="Offline downgrade cannot verify retained attempt ownership"):
        command.downgrade(_get_alembic_config(), f"{REVISION}:{PREDECESSOR}", sql=True)

    monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", "sqlite://")
    with pytest.raises(
        RuntimeError, match="SQLite offline SQL cannot render this migration's table rebuilds"
    ):
        command.upgrade(_get_alembic_config(), f"{PREDECESSOR}:{REVISION}", sql=True)


def test_upgrade_retains_history_and_legacy_owner(populated_predecessor):
    connection, config = populated_predecessor
    foreign_key_checks = (
        connection.scalar(sa.text("SELECT @@SESSION.foreign_key_checks"))
        if connection.dialect.name == "mysql"
        else None
    )
    source_history = (
        connection.execute(sa.select(table(connection, "task_instance_history"))).mappings().one()
    )
    valid_version = uuid4()
    versioned_history_id = uuid4()
    connection.execute(table(connection, "dag_bundle").insert().values(name="test_bundle"))
    connection.execute(
        table(connection, "dag")
        .insert()
        .values(
            dag_id="ownership",
            bundle_name="test_bundle",
            is_paused=False,
            is_stale=False,
            max_active_tasks=16,
            max_consecutive_failed_dag_runs=0,
            has_task_concurrency_limits=False,
            timetable_type="",
            partition_mapper_info=[],
        )
    )
    connection.execute(
        table(connection, "dag_version", "id")
        .insert()
        .values(
            id=valid_version,
            dag_id="ownership",
            version_number=1,
            created_at=NOW,
            last_updated=NOW,
        )
    )
    connection.execute(
        table(connection, "task_instance_history", "task_instance_id", "dag_version_id")
        .insert()
        .values(
            dag_id="ownership",
            task_id="task",
            run_id="manual",
            map_index=-1,
            task_instance_id=versioned_history_id,
            try_number=3,
            pool="default_pool",
            pool_slots=1,
            state="failed",
            max_tries=1,
            dag_version_id=valid_version,
        )
    )
    connection.commit()
    command.upgrade(config, REVISION)
    ti = table(connection, "task_instance", "id", "dag_version_id")
    rows = {row.id: row for row in connection.execute(sa.select(ti))}
    assert set(rows) == {CURRENT_ID, HISTORY_ID, versioned_history_id}
    assert rows[versioned_history_id].dag_version_id == valid_version
    assert rows[CURRENT_ID].working_set is True
    old = rows[HISTORY_ID]
    assert old.working_set is None
    assert old.max_tries == 0
    assert old.duration == 12.5
    assert old.trigger_id is None
    assert old.dag_version_id is None
    for name, value in source_history.items():
        if name not in {"task_instance_id", "trigger_id", "dag_version_id", "max_tries"}:
            assert old._mapping[name] == value
    owner = table(connection, "legacy_task_data_owner", "task_instance_id")
    assert connection.scalar(sa.select(owner.c.task_instance_id)) == CURRENT_ID
    hitl = table(connection, "hitl_detail", "ti_id")
    assert connection.scalar(sa.select(hitl.c.ti_id)) == HISTORY_ID
    for name in ("xcom_v1", "rtif_v1", "task_instance_note", "task_reschedule"):
        assert connection.scalar(sa.select(sa.func.count()).select_from(table(connection, name))) == 1
    assert "task_instance_history" not in sa.inspect(connection).get_table_names()
    assert "hitl_detail_history" not in sa.inspect(connection).get_table_names()
    if foreign_key_checks is not None:
        assert connection.scalar(sa.text("SELECT @@SESSION.foreign_key_checks")) == foreign_key_checks
        inspector = sa.inspect(connection)
        for name, constraint in (
            ("xcom_v1", "xcom_task_instance_fkey"),
            ("rtif_v1", "rtif_ti_fkey"),
        ):
            foreign_key = next(fk for fk in inspector.get_foreign_keys(name) if fk["name"] == constraint)
            assert foreign_key["referred_table"] == "legacy_task_data_owner"


def archive_attempt_at_live_try(connection) -> UUID:
    archived_id = uuid4()
    connection.execute(
        table(connection, "task_instance_history", "task_instance_id")
        .insert()
        .values(
            dag_id="ownership",
            task_id="task",
            run_id="manual",
            map_index=-1,
            task_instance_id=archived_id,
            try_number=2,
            pool="default_pool",
            pool_slots=1,
            state="success",
            max_tries=7,
        )
    )
    connection.execute(
        table(connection, "hitl_detail_history", "ti_history_id")
        .insert()
        .values(
            ti_history_id=archived_id,
            options=["yes"],
            subject="conflicting review",
            params={},
            params_input={},
            created_at=NOW,
        )
    )
    connection.commit()
    return archived_id


def test_upgrade_discards_only_history_conflicting_with_live_try(populated_predecessor):
    connection, config = populated_predecessor
    conflicting_id = archive_attempt_at_live_try(connection)

    command.upgrade(config, REVISION)

    ti = table(connection, "task_instance", "id")
    rows = {row.id: row for row in connection.execute(sa.select(ti))}
    assert set(rows) == {CURRENT_ID, HISTORY_ID}
    assert (rows[CURRENT_ID].state, rows[CURRENT_ID].try_number, rows[CURRENT_ID].max_tries) == (
        "running",
        2,
        3,
    )
    assert rows[CURRENT_ID].working_set is True
    assert rows[HISTORY_ID].working_set is None
    assert rows[HISTORY_ID].try_number == 1
    assert set(connection.scalars(sa.select(table(connection, "hitl_detail", "ti_id").c.ti_id))) == {
        HISTORY_ID
    }
    assert conflicting_id not in rows


@pytest.mark.parametrize(
    "live_state", ["skipped", "upstream_failed", "removed", "success", "failed", "restarting"]
)
def test_upgrade_keeps_history_archived_at_the_try_of_a_live_row_that_never_ran_it(
    populated_predecessor, live_state
):
    connection, config = populated_predecessor
    archived_id = archive_attempt_at_live_try(connection)
    live = table(connection, "task_instance", "id")
    connection.execute(live.update().where(live.c.id == CURRENT_ID).values(state=live_state))
    connection.commit()

    command.upgrade(config, REVISION)

    ti = table(connection, "task_instance", "id")
    rows = {row.id: row for row in connection.execute(sa.select(ti))}
    assert set(rows) == {CURRENT_ID, HISTORY_ID, archived_id}
    assert (rows[CURRENT_ID].state, rows[CURRENT_ID].try_number, rows[CURRENT_ID].working_set) == (
        live_state,
        3,
        True,
    )
    assert (rows[archived_id].state, rows[archived_id].try_number, rows[archived_id].working_set) == (
        "success",
        2,
        None,
    )
    assert set(connection.scalars(sa.select(table(connection, "hitl_detail", "ti_id").c.ti_id))) == {
        HISTORY_ID,
        archived_id,
    }


def test_upgrade_does_not_suppress_unrelated_history_identity_conflict(populated_predecessor):
    connection, config = populated_predecessor
    history = table(connection, "task_instance_history", "task_instance_id")
    connection.execute(history.update().values(task_instance_id=CURRENT_ID))
    connection.commit()

    with pytest.raises((IntegrityError, OperationalError), match="(?i)unique|duplicate"):
        command.upgrade(config, REVISION)
    if connection.dialect.name == "sqlite":
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        assert "xcom" in sa.inspect(connection).get_table_names()
        assert "legacy_task_data_owner" not in sa.inspect(connection).get_table_names()
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []


@pytest.mark.parametrize(
    ("try_number", "source_max_tries", "expected_max_tries"),
    [
        pytest.param(0, None, 0, id="null-pending-attempt"),
        pytest.param(4, None, 3, id="null-later-attempt"),
        pytest.param(1, -1, -1, id="existing-negative-budget"),
    ],
)
def test_upgrade_normalizes_only_null_historical_retry_budget(
    populated_predecessor, try_number, source_max_tries, expected_max_tries
):
    connection, config = populated_predecessor
    history = table(connection, "task_instance_history", "task_instance_id")
    connection.execute(
        history.update()
        .where(history.c.task_instance_id == HISTORY_ID)
        .values(try_number=try_number, max_tries=source_max_tries)
    )
    connection.commit()

    source_column = next(
        column
        for column in sa.inspect(connection).get_columns("task_instance")
        if column["name"] == "max_tries"
    )
    assert source_column["nullable"] is False
    connection.commit()
    command.upgrade(config, REVISION)

    attempts = table(connection, "task_instance", "id")
    values = {
        row.id: row.max_tries for row in connection.execute(sa.select(attempts.c.id, attempts.c.max_tries))
    }
    assert values[HISTORY_ID] == expected_max_tries
    assert values[CURRENT_ID] == 3
    target_column = next(
        column
        for column in sa.inspect(connection).get_columns("task_instance")
        if column["name"] == "max_tries"
    )
    assert target_column["nullable"] is False


def test_empty_downgrade_restores_predecessor_constraints(predecessor):
    connection, config = predecessor

    def constraints():
        inspector = sa.inspect(connection)
        return {
            name: (
                sorted(inspector.get_foreign_keys(name), key=lambda fk: fk["name"]),
                sorted(inspector.get_unique_constraints(name), key=lambda constraint: constraint["name"]),
                inspector.get_pk_constraint(name),
                {
                    column["name"]: (
                        str(column["type"]),
                        column["nullable"],
                        str(column["default"]).translate(str.maketrans("", "", "()'")),
                    )
                    for column in inspector.get_columns(name)
                },
            )
            for name in (
                "task_instance",
                "task_instance_history",
                "hitl_detail_history",
                "xcom",
                "rendered_task_instance_fields",
            )
        }

    before = constraints()
    connection.commit()
    command.upgrade(config, REVISION)
    command.downgrade(config, PREDECESSOR)
    assert constraints() == before


def test_postgresql_upgrade_preserves_legacy_heap_and_index_files(populated_predecessor):
    connection, config = populated_predecessor
    if connection.dialect.name != "postgresql":
        pytest.skip("PostgreSQL physical storage invariant")
    relations = connection.execute(
        sa.text("""
        SELECT oid, relfilenode FROM pg_class
        WHERE oid IN ('xcom'::regclass, 'rendered_task_instance_fields'::regclass)
           OR oid IN (SELECT indexrelid FROM pg_index
                      WHERE indrelid IN ('xcom'::regclass, 'rendered_task_instance_fields'::regclass))
    """)
    ).all()
    connection.commit()
    command.upgrade(config, REVISION)
    for oid, filenode in relations:
        assert (
            connection.scalar(sa.text("SELECT relfilenode FROM pg_class WHERE oid=:oid"), {"oid": oid})
            == filenode
        )
    constraints = connection.execute(
        sa.text("""
        SELECT convalidated, confrelid::regclass::text FROM pg_constraint
        WHERE conrelid IN ('xcom_v1'::regclass, 'rtif_v1'::regclass) AND contype='f'
    """)
    ).all()
    assert constraints == [(False, "legacy_task_data_owner"), (False, "legacy_task_data_owner")]


def test_upgrade_enforces_current_and_public_try_uniqueness(populated_predecessor):
    connection, config = populated_predecessor
    command.upgrade(config, REVISION)
    ti = table(connection, "task_instance", "id")
    row = dict(
        dag_id="ownership",
        task_id="task",
        run_id="manual",
        map_index=-1,
        pool="default_pool",
        pool_slots=1,
        try_number=3,
    )
    for values in ({}, {"working_set": None, "try_number": 1}):
        with (
            pytest.raises((IntegrityError, OperationalError), match="(?i)unique|duplicate"),
            connection.begin_nested(),
        ):
            connection.execute(ti.insert().values(**(row | values), id=uuid4()))
    connection.execute(ti.insert().values(**row, id=uuid4(), working_set=None))
    connection.execute(ti.insert().values(**(row | {"try_number": 4}), id=uuid4(), working_set=None))


def test_upgrade_preserves_uuid_children_and_legacy_cascades(populated_predecessor):
    connection, config = populated_predecessor
    command.upgrade(config, REVISION)
    if connection.dialect.name == "sqlite":
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    ti = table(connection, "task_instance", "id")
    for attempt_id in (CURRENT_ID, HISTORY_ID):
        connection.execute(
            table(connection, "xcom_v2", "task_instance_id", "id")
            .insert()
            .values(
                id=uuid4(),
                task_instance_id=attempt_id,
                key="value",
                value=1,
                timestamp=NOW,
            )
        )
        connection.execute(
            table(connection, "rtif_v2", "task_instance_id", "id")
            .insert()
            .values(
                id=uuid4(),
                task_instance_id=attempt_id,
                rendered_fields={},
            )
        )
    connection.execute(table(connection, "dag_run").delete())
    for name in ("legacy_task_data_owner", "xcom_v1", "rtif_v1", "task_instance_note", "task_reschedule"):
        assert connection.scalar(sa.select(sa.func.count()).select_from(table(connection, name))) == 0
    assert connection.scalar(sa.select(sa.func.count()).select_from(ti)) == 0
    assert connection.scalar(sa.select(sa.func.count()).select_from(table(connection, "hitl_detail"))) == 0
    for name in ("xcom_v2", "rtif_v2"):
        data = table(connection, name, "task_instance_id")
        assert connection.scalars(sa.select(data.c.task_instance_id)).all() == []


COORDINATE_KEY = ("dag_id", "task_id", "run_id", "map_index")
NEIGHBOURS = {
    "task_id": {"task_id": "neighbour"},
    "run_id": {"run_id": "second"},
    "map_index": {"map_index": 0},
    "dag_id": {"dag_id": "other"},
}


def insert_attempt(connection, **values) -> UUID:
    attempt_id = uuid4()
    row = (
        COORDINATES | {"try_number": 1, "state": "success", "pool": "default_pool", "pool_slots": 1} | values
    )
    connection.execute(table(connection, "task_instance", "id").insert().values(id=attempt_id, **row))
    return attempt_id


def insert_dag_run(connection, dag_run_id, dag_id, run_id):
    connection.execute(
        table(connection, "dag_run")
        .insert()
        .values(
            id=dag_run_id,
            dag_id=dag_id,
            run_id=run_id,
            run_type="manual",
            run_after=NOW,
            state="running",
            start_date=NOW,
        )
    )


def insert_neighbour_dag_runs(connection) -> dict[tuple[str, str], int]:
    insert_dag_run(connection, SECOND_DAG_RUN_ID, "ownership", "second")
    insert_dag_run(connection, THIRD_DAG_RUN_ID, "other", "manual")
    return {
        ("ownership", "manual"): DAG_RUN_ID,
        ("ownership", "second"): SECOND_DAG_RUN_ID,
        ("other", "manual"): THIRD_DAG_RUN_ID,
    }


def insert_xcom_v2(connection, attempt_id, key, value, **values):
    connection.execute(
        table(connection, "xcom_v2", "id", "task_instance_id")
        .insert()
        .values(id=uuid4(), task_instance_id=attempt_id, key=key, value=value, timestamp=NOW, **values)
    )


def insert_rtif_v2(connection, attempt_id, rendered_fields, **values):
    connection.execute(
        table(connection, "rtif_v2", "id", "task_instance_id")
        .insert()
        .values(id=uuid4(), task_instance_id=attempt_id, rendered_fields=rendered_fields, **values)
    )


def retry_current_attempt(connection, **values) -> UUID:
    ti = table(connection, "task_instance", "id")
    connection.execute(
        ti.update().where(ti.c.id == CURRENT_ID).values(working_set=None, archived_reason="retry")
    )
    return insert_attempt(connection, try_number=3, state="running", **values)


def read_legacy_xcom(connection):
    rows = connection.execute(table(connection, "xcom").select())
    return {(row.dag_id, row.run_id, row.task_id, row.map_index, row.key): row for row in rows}


def read_legacy_rendered_fields(connection):
    rows = connection.execute(table(connection, "rendered_task_instance_fields").select())
    return {(row.dag_id, row.run_id, row.task_id, row.map_index): row for row in rows}


def drop_unique_constraint(connection, name):
    sqlite = connection.dialect.name == "sqlite"
    connection.commit()
    if sqlite:
        connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
    with (
        Operations.context(MigrationContext.configure(connection)) as ops,
        ops.batch_alter_table("task_instance") as batch,
    ):
        batch.drop_constraint(name, type_="unique")
    connection.commit()
    if sqlite:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")


@pytest.mark.parametrize("legacy_store", ["xcom", "rendered_task_instance_fields"])
def test_downgrade_rejects_owner_that_changed_coordinates(populated_predecessor, legacy_store):
    connection, config = populated_predecessor
    other_store = {"xcom", "rendered_task_instance_fields"} - {legacy_store}
    for name in ("hitl_detail_history", "task_instance_history", *other_store):
        connection.execute(table(connection, name).delete())
    connection.commit()
    command.upgrade(config, REVISION)
    ti = table(connection, "task_instance", "id")
    connection.execute(ti.update().where(ti.c.id == CURRENT_ID).values(map_index=0))
    connection.commit()
    with pytest.raises(RuntimeError, match="legacy owners changed coordinates"):
        command.downgrade(config, PREDECESSOR)


def test_downgrade_deletes_stale_owner_of_an_attempt_retried_after_promotion(populated_predecessor):
    connection, config = populated_predecessor
    for name in ("xcom", "rendered_task_instance_fields", "hitl_detail_history", "task_instance_history"):
        connection.execute(table(connection, name).delete())
    connection.commit()
    command.upgrade(config, REVISION)
    ti = table(connection, "task_instance", "id")
    connection.execute(ti.update().where(ti.c.id == CURRENT_ID).values(map_index=0))
    successor_id = retry_current_attempt(connection, map_index=0)
    insert_xcom_v2(connection, successor_id, "k", "v")
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    assert {key: row.value for key, row in read_legacy_xcom(connection).items()} == {
        ("ownership", "manual", "task", 0, "k"): "v"
    }


def test_downgrade_ignores_owner_that_changed_coordinates_without_legacy_rows(populated_predecessor):
    connection, config = populated_predecessor
    for name in ("xcom", "rendered_task_instance_fields", "hitl_detail_history", "task_instance_history"):
        connection.execute(table(connection, name).delete())
    connection.commit()
    command.upgrade(config, REVISION)
    ti = table(connection, "task_instance", "id")
    connection.execute(ti.update().where(ti.c.id == CURRENT_ID).values(map_index=0))
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    live = table(connection, "task_instance", "id")
    assert [(row.id, row.map_index) for row in connection.execute(live.select())] == [(CURRENT_ID, 0)]


@pytest.mark.parametrize(
    ("dropped_constraint", "duplicate"),
    [
        pytest.param("task_instance_current_key", {"try_number": 5}, id="second-current-attempt"),
        pytest.param(
            "task_instance_try_key", {"try_number": 1, "working_set": None}, id="repeated-try-number"
        ),
    ],
)
def test_downgrade_rejects_attempts_sharing_coordinates(populated_predecessor, dropped_constraint, duplicate):
    connection, config = populated_predecessor
    command.upgrade(config, REVISION)
    drop_unique_constraint(connection, dropped_constraint)
    insert_attempt(connection, **duplicate)
    connection.commit()
    with pytest.raises(RuntimeError, match="multiple attempts sharing coordinates"):
        command.downgrade(config, PREDECESSOR)


def test_downgrade_rejects_archived_attempts_without_a_current_attempt(populated_predecessor):
    connection, config = populated_predecessor
    insert_neighbour_dag_runs(connection)
    connection.commit()
    command.upgrade(config, REVISION)
    for overrides in NEIGHBOURS.values():
        insert_attempt(connection, **(COORDINATES | overrides))
    ti = table(connection, "task_instance", "id")
    connection.execute(ti.delete().where(ti.c.id == CURRENT_ID))
    connection.commit()
    with pytest.raises(RuntimeError, match="1 archived attempts"):
        command.downgrade(config, PREDECESSOR)


def test_downgrade_replaces_legacy_xcom_key_by_key(populated_predecessor):
    connection, config = populated_predecessor
    dag_runs = insert_neighbour_dag_runs(connection)
    legacy_xcom = table(connection, "xcom")
    legacy_coordinates = [
        COORDINATES | {"task_id": "other"},
        *(
            COORDINATES | {"task_id": "mapped", "run_id": run_id, "map_index": map_index}
            for run_id in ("manual", "second")
            for map_index in (0, 1)
        ),
    ]
    for coordinates in legacy_coordinates:
        insert_attempt(connection, **coordinates)
        connection.execute(
            legacy_xcom.insert().values(
                **coordinates,
                dag_run_id=dag_runs[(coordinates["dag_id"], coordinates["run_id"])],
                key="legacy",
                value="legacy",
                timestamp=NOW,
            )
        )
    connection.commit()
    command.upgrade(config, REVISION)

    ti = table(connection, "task_instance", "id")
    mapped_attempts = {
        (row.run_id, row.map_index): row.id
        for row in connection.execute(ti.select().where(ti.c.task_id == "mapped"))
    }
    for attempt_id, key, value in (
        (HISTORY_ID, "from_archived_attempt", "archived"),
        (CURRENT_ID, "return_value", "latest"),
        (CURRENT_ID, "added", "added"),
        (mapped_attempts[("manual", 0)], "extra", "extra"),
        (mapped_attempts[("second", 1)], "legacy", "replaced"),
    ):
        insert_xcom_v2(connection, attempt_id, key, value)
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    xcom = read_legacy_xcom(connection)
    assert {key: (row.value, row.dag_run_id) for key, row in xcom.items()} == {
        ("ownership", "manual", "task", -1, "return_value"): ("latest", DAG_RUN_ID),
        ("ownership", "manual", "task", -1, "added"): ("added", DAG_RUN_ID),
        ("ownership", "manual", "other", -1, "legacy"): ("legacy", DAG_RUN_ID),
        ("ownership", "manual", "mapped", 0, "legacy"): ("legacy", DAG_RUN_ID),
        ("ownership", "manual", "mapped", 0, "extra"): ("extra", DAG_RUN_ID),
        ("ownership", "manual", "mapped", 1, "legacy"): ("legacy", DAG_RUN_ID),
        ("ownership", "second", "mapped", 0, "legacy"): ("legacy", SECOND_DAG_RUN_ID),
        ("ownership", "second", "mapped", 1, "legacy"): ("replaced", SECOND_DAG_RUN_ID),
    }


def test_downgrade_carries_payload_columns_back(populated_predecessor):
    connection, config = populated_predecessor
    command.upgrade(config, REVISION)
    created_id = insert_attempt(connection, task_id="created_after_upgrade")
    insert_xcom_v2(connection, CURRENT_ID, "return_value", "latest", dag_result=True, mapped_length=3)
    insert_xcom_v2(connection, created_id, "return_value", "created")
    insert_rtif_v2(connection, HISTORY_ID, {"f": "archived"})
    insert_rtif_v2(connection, CURRENT_ID, {"f": "latest"}, k8s_pod_yaml={"kind": "Pod"})
    insert_rtif_v2(connection, created_id, {"f": "created"})
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    xcom = read_legacy_xcom(connection)
    assert {key: (row.dag_result, row.mapped_length) for key, row in xcom.items()} == {
        ("ownership", "manual", "task", -1, "return_value"): (True, 3),
        ("ownership", "manual", "created_after_upgrade", -1, "return_value"): (None, None),
    }
    rendered_fields = read_legacy_rendered_fields(connection)
    assert {key: (row.rendered_fields, row.k8s_pod_yaml) for key, row in rendered_fields.items()} == {
        ("ownership", "manual", "task", -1): ({"f": "latest"}, {"kind": "Pod"}),
        ("ownership", "manual", "created_after_upgrade", -1): ({"f": "created"}, None),
    }


def test_downgrade_copies_attempts_that_wrote_only_one_store(populated_predecessor):
    connection, config = populated_predecessor
    command.upgrade(config, REVISION)
    insert_xcom_v2(connection, insert_attempt(connection, task_id="only_xcom"), "k", "v")
    insert_rtif_v2(connection, insert_attempt(connection, task_id="only_rtif"), {"f": "v"})
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    assert {key[2:] for key in read_legacy_xcom(connection) if key[2] != "task"} == {("only_xcom", -1, "k")}
    assert {key[2:] for key in read_legacy_rendered_fields(connection) if key[2] != "task"} == {
        ("only_rtif", -1)
    }


def test_downgrade_copies_attempts_created_next_to_an_owned_coordinate(populated_predecessor):
    connection, config = populated_predecessor
    insert_neighbour_dag_runs(connection)
    connection.commit()
    command.upgrade(config, REVISION)
    for overrides in NEIGHBOURS.values():
        insert_xcom_v2(connection, insert_attempt(connection, **(COORDINATES | overrides)), "k", "v")
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    legacy_xcom = table(connection, "xcom")
    copied = connection.execute(legacy_xcom.select().where(legacy_xcom.c.key == "k")).all()
    assert sorted(tuple(getattr(row, name) for name in COORDINATE_KEY) for row in copied) == sorted(
        tuple((COORDINATES | overrides)[name] for name in COORDINATE_KEY) for overrides in NEIGHBOURS.values()
    )


def test_downgrade_keeps_legacy_data_of_neighbouring_coordinates(populated_predecessor):
    connection, config = populated_predecessor
    dag_runs = insert_neighbour_dag_runs(connection)
    for overrides in NEIGHBOURS.values():
        coordinates = COORDINATES | overrides
        insert_attempt(connection, **coordinates)
        connection.execute(
            table(connection, "xcom")
            .insert()
            .values(
                **coordinates,
                dag_run_id=dag_runs[(coordinates["dag_id"], coordinates["run_id"])],
                key="legacy",
                value="legacy",
                timestamp=NOW,
            )
        )
        connection.execute(
            table(connection, "rendered_task_instance_fields")
            .insert()
            .values(**coordinates, rendered_fields={"f": "legacy"})
        )
    connection.commit()
    command.upgrade(config, REVISION)
    retry_current_attempt(connection)
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    expected = sorted(tuple((COORDINATES | o)[name] for name in COORDINATE_KEY) for o in NEIGHBOURS.values())
    xcom = connection.execute(table(connection, "xcom").select()).all()
    assert sorted(tuple(getattr(row, name) for name in COORDINATE_KEY) for row in xcom) == expected
    rendered_fields = connection.execute(table(connection, "rendered_task_instance_fields").select()).all()
    assert sorted(tuple(getattr(row, name) for name in COORDINATE_KEY) for row in rendered_fields) == expected


def test_downgrade_keeps_dags_sharing_a_run_id_apart(populated_predecessor):
    connection, config = populated_predecessor
    insert_neighbour_dag_runs(connection)
    insert_dag_run(connection, FOURTH_DAG_RUN_ID, "third", "manual")
    insert_attempt(connection, dag_id="third")
    connection.execute(
        table(connection, "xcom")
        .insert()
        .values(
            **(COORDINATES | {"dag_id": "third"}),
            dag_run_id=FOURTH_DAG_RUN_ID,
            key="legacy",
            value="third",
            timestamp=NOW,
        )
    )
    connection.commit()
    command.upgrade(config, REVISION)
    other_id = insert_attempt(connection, dag_id="other", try_number=2)
    insert_xcom_v2(connection, CURRENT_ID, "k", "ownership")
    insert_xcom_v2(connection, other_id, "k", "other")
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    xcom = read_legacy_xcom(connection)
    assert {(key[0], key[4]): (row.value, row.dag_run_id) for key, row in xcom.items()} == {
        ("other", "k"): ("other", THIRD_DAG_RUN_ID),
        ("ownership", "k"): ("ownership", DAG_RUN_ID),
        ("ownership", "return_value"): ({"legacy": True}, DAG_RUN_ID),
        ("third", "legacy"): ("third", FOURTH_DAG_RUN_ID),
    }


def test_downgrade_drops_legacy_data_of_a_retried_attempt_whose_successor_wrote_nothing(
    populated_predecessor,
):
    connection, config = populated_predecessor
    command.upgrade(config, REVISION)
    retry_current_attempt(connection)
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    assert read_legacy_xcom(connection) == {}
    assert read_legacy_rendered_fields(connection) == {}


def test_downgrade_replaces_legacy_data_of_a_retried_attempt_with_its_successors(populated_predecessor):
    connection, config = populated_predecessor
    command.upgrade(config, REVISION)
    successor_id = retry_current_attempt(connection)
    insert_xcom_v2(connection, successor_id, "from_successor", "new")
    insert_rtif_v2(connection, successor_id, {"f": "new"})
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    assert {key: row.value for key, row in read_legacy_xcom(connection).items()} == {
        ("ownership", "manual", "task", -1, "from_successor"): "new"
    }
    assert {key: row.rendered_fields for key, row in read_legacy_rendered_fields(connection).items()} == {
        ("ownership", "manual", "task", -1): {"f": "new"}
    }


def test_downgrade_restores_archived_attempts_to_history(populated_predecessor):
    connection, config = populated_predecessor
    history = table(connection, "task_instance_history", "task_instance_id", "dag_version_id")
    hitl_history = table(connection, "hitl_detail_history", "ti_history_id")
    source_history = connection.execute(history.select()).one()._mapping
    source_hitl = connection.execute(hitl_history.select()).one()._mapping
    connection.commit()
    command.upgrade(config, REVISION)
    connection.execute(
        table(connection, "task_instance_note", "ti_id")
        .insert()
        .values(ti_id=HISTORY_ID, content="archived note", created_at=NOW, updated_at=NOW)
    )
    connection.execute(
        table(connection, "task_reschedule", "ti_id")
        .insert()
        .values(ti_id=HISTORY_ID, start_date=NOW, end_date=NOW, duration=0, reschedule_date=NOW)
    )
    connection.execute(
        table(connection, "hitl_detail", "ti_id")
        .insert()
        .values(ti_id=CURRENT_ID, options=["a"], subject="live", params={}, params_input={}, created_at=NOW)
    )
    connection.commit()

    command.downgrade(config, PREDECESSOR)

    restored = connection.execute(history.select()).one()._mapping
    for name, value in source_history.items():
        if name not in {"trigger_id", "dag_version_id", "max_tries"}:
            assert restored[name] == value
    assert connection.execute(hitl_history.select()).one()._mapping == source_hitl
    live = table(connection, "task_instance", "id")
    assert connection.scalars(sa.select(live.c.id)).all() == [CURRENT_ID]
    for name in ("task_instance_note", "task_reschedule", "hitl_detail"):
        child = table(connection, name, "ti_id")
        assert connection.scalars(sa.select(child.c.ti_id)).all() == [CURRENT_ID]

    connection.commit()
    command.upgrade(config, REVISION)
    live = table(connection, "task_instance", "id")
    assert connection.scalars(sa.select(live.c.id).where(live.c.working_set.is_(None))).all() == [HISTORY_ID]
    hitl = table(connection, "hitl_detail", "ti_id")
    assert sorted(connection.scalars(sa.select(hitl.c.ti_id)).all()) == sorted([CURRENT_ID, HISTORY_ID])


@pytest.mark.parametrize("source_problem", ["unvalidated", "reanchored"])
def test_postgresql_rejects_unsupported_source_before_ddl(populated_predecessor, source_problem):
    connection, config = populated_predecessor
    if connection.dialect.name != "postgresql":
        pytest.skip("PostgreSQL source constraint validation")
    connection.exec_driver_sql("ALTER TABLE xcom DROP CONSTRAINT xcom_task_instance_fkey")
    target = (
        "task_instance (dag_id, task_id, run_id, map_index) ON DELETE CASCADE NOT VALID"
        if source_problem == "unvalidated"
        else "dag_run (dag_id, run_id) ON DELETE CASCADE"
    )
    columns = "dag_id, task_id, run_id, map_index" if source_problem == "unvalidated" else "dag_id, run_id"
    connection.exec_driver_sql(
        f"ALTER TABLE xcom ADD CONSTRAINT xcom_task_instance_fkey FOREIGN KEY ({columns}) REFERENCES {target}"
    )
    connection.commit()
    with pytest.raises(RuntimeError, match="source"):
        command.upgrade(config, REVISION)
    assert "xcom" in sa.inspect(connection).get_table_names()
    assert "legacy_task_data_owner" not in sa.inspect(connection).get_table_names()
