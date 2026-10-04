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
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import joinedload

from airflow.models import xcom
from airflow.models.dagrun import DagRun
from airflow.models.renderedtifields import (
    LegacyRenderedTaskInstanceFields,
    RenderedTaskInstanceFields,
    load_legacy_rendered_fields,
)
from airflow.models.taskinstance import LegacyTaskDataOwner, TaskInstance
from airflow.utils.db import check_query_exists, get_query_count

from tests_common.test_utils.asserts import assert_no_cartesian_products, assert_queries_count
from tests_common.test_utils.attempt_ownership import (
    COORDINATES,
    CURRENT_ID,
    HISTORY_ID,
    NOW,
    ownership_session as ownership_session,
    table,
)

pytestmark = [pytest.mark.db_test, pytest.mark.execution_timeout(10)]


def ti_coordinates(coordinates):
    """Rename the legacy ``map_index`` key to the live ``region_index`` column."""
    return {("region_index" if key == "map_index" else key): value for key, value in coordinates.items()}


def producers(session, *ids):
    ti = table(session.connection(), "task_instance", "id")
    return sa.select(ti.c.id).where(ti.c.id.in_(ids))


def read(session, *ids, key=None):
    query = xcom.build_xcom_read_query(producer_ids=producers(session, *ids), key=key)
    return session.scalars(query).all()


def seed_many_legacy_attempts(session):
    ti = table(session.connection(), "task_instance", "id")
    owner = table(session.connection(), "legacy_task_data_owner", "task_instance_id")
    legacy = table(session.connection(), "xcom_v1")
    ids = [uuid4() for _ in range(1000)]
    coordinates = [
        dict(dag_id="ownership", task_id="task", run_id="manual", map_index=i) for i in range(1000)
    ]
    session.execute(
        ti.insert(),
        [
            dict(**ti_coordinates(coord), id=attempt_id, try_number=1, pool="default_pool", pool_slots=1)
            for coord, attempt_id in zip(coordinates, ids)
        ],
    )
    session.execute(
        owner.insert(),
        [dict(**coord, task_instance_id=attempt_id) for coord, attempt_id in zip(coordinates, ids)],
    )
    session.execute(
        legacy.insert(),
        [dict(**coord, dag_run_id=41, key="return_value", value=1, timestamp=NOW) for coord in coordinates],
    )
    return ("task_instance", "legacy_task_data_owner", "xcom_v1", "xcom_v2")


def test_get_many_is_one_composable_deduplicated_read(ownership_session):
    session = ownership_session
    query = xcom.XComModel.get_many(run_id="manual", dag_ids="ownership", task_ids="task")
    entity = xcom.xcom_entity(query)

    assert isinstance(query, sa.sql.Select)
    assert session.scalars(query).one().value == {"legacy": True}
    projection = query.with_only_columns(entity.key, entity.value).where(entity.key == "return_value")
    assert len(projection.get_final_froms()) == 1
    assert session.execute(projection).one() == ("return_value", {"legacy": True})
    assert session.execute(projection.where(entity.key == "missing")).all() == []

    keyed = xcom.XComModel.get_many(
        run_id="manual", dag_ids="ownership", task_ids="task", map_indexes=-1, key="return_value"
    )
    assert session.scalar(sa.select(sa.func.count()).select_from(keyed.subquery())) == 1
    assert get_query_count(keyed, session=session) == 1
    assert check_query_exists(keyed, session=session)
    assert not check_query_exists(
        xcom.XComModel.get_many(
            run_id="manual", dag_ids="ownership", task_ids="task", map_indexes=-1, key="missing"
        ),
        session=session,
    )
    keyed_entity = xcom.xcom_entity(keyed)
    keyed_values = xcom.LazyXComSelectSequence.from_select(
        keyed.with_only_columns(keyed_entity.value).order_by(None),
        order_by=[keyed_entity.key.expression],
        session=session,
    )
    assert len(keyed_values) == 1

    for key, value in (("a", 1), ("b", 2), ("return_value", 3)):
        xcom.XComModel.set_for_attempt(
            task_instance_id=CURRENT_ID,
            key=key,
            value=value,
            serialize=False,
            session=session,
        )
    query = xcom.build_xcom_read_query(producer_ids=producers(session, CURRENT_ID))
    assert session.scalar(sa.select(sa.func.count()).select_from(query.subquery())) == 3
    page = session.scalars(query.order_by(xcom.xcom_entity(query).key).offset(1).limit(2)).all()
    assert [row.key for row in page] == ["b", "return_value"]
    entity = xcom.xcom_entity(query)
    values = xcom.LazyXComSelectSequence.from_select(
        query.with_only_columns(entity.value).order_by(None),
        order_by=[entity.key.expression],
        session=session,
    )
    assert len(values) == 3
    assert values[1:3] == [2, 3]
    assert values[::-1] == [3, 2, 1]


OTHER_ID = UUID("01960000-0000-7000-8000-000000000003")


def add_other_task_with_legacy_xcom(session, run_id):
    if run_id != COORDINATES["run_id"]:
        session.execute(
            DagRun.__table__.insert().values(
                dag_id=COORDINATES["dag_id"],
                run_id=run_id,
                run_type="manual",
                run_after=NOW,
                state="running",
                start_date=NOW,
            )
        )
    coordinates = {**COORDINATES, "task_id": "other", "run_id": run_id}
    session.execute(
        TaskInstance.__table__.insert().values(
            **ti_coordinates(coordinates),
            id=OTHER_ID,
            try_number=1,
            pool="default_pool",
            pool_slots=1,
            working_set=True,
        )
    )
    session.execute(LegacyTaskDataOwner.__table__.insert().values(**coordinates, task_instance_id=OTHER_ID))
    session.execute(
        xcom.XComModelV1.__table__.insert().values(
            **coordinates,
            dag_run_id=session.scalar(sa.select(DagRun.id).where(DagRun.run_id == run_id)),
            key="return_value",
            value={"legacy": "other"},
            timestamp=NOW,
        )
    )


@pytest.mark.parametrize(
    ("legacy_id", "v2_id", "other_run_id"),
    [
        pytest.param(CURRENT_ID, HISTORY_ID, "manual", id="same-coordinates"),
        pytest.param(CURRENT_ID, OTHER_ID, "manual", id="same-run-legacy-first"),
        pytest.param(OTHER_ID, HISTORY_ID, "manual", id="same-run-v2-first"),
        pytest.param(CURRENT_ID, OTHER_ID, "second", id="mixed-runs-legacy-first"),
        pytest.param(OTHER_ID, HISTORY_ID, "second", id="mixed-runs-v2-first"),
    ],
)
def test_joinedload_resolves_relationships_for_rows_from_both_stores(
    ownership_session, legacy_id, v2_id, other_run_id
):
    session = ownership_session
    add_other_task_with_legacy_xcom(session, other_run_id)
    xcom.XComModel.set_for_attempt(
        task_instance_id=v2_id, key="return_value", value={"v2": True}, serialize=False, session=session
    )
    session.expire_all()
    query = xcom.build_xcom_read_query(producer_ids=producers(session, legacy_id, v2_id))
    entity = xcom.xcom_entity(query)

    with assert_no_cartesian_products():
        rows = session.scalars(
            query.options(
                joinedload(entity.task), joinedload(entity.dag_run).joinedload(DagRun.dag_model)
            ).execution_options(include_all_attempts=True)
        ).unique()
        by_ti = {row.task_instance_id: row for row in rows}

    legacy_value = {"legacy": "other"} if legacy_id == OTHER_ID else {"legacy": True}
    assert {ti_id: row.value for ti_id, row in by_ti.items()} == {
        legacy_id: legacy_value,
        v2_id: {"v2": True},
    }
    for ti_id, row in by_ti.items():
        assert row.task.id == ti_id
        assert row.dag_run.run_id == (other_run_id if ti_id == OTHER_ID else COORDINATES["run_id"])


def test_mysql_xcom_point_read_uses_owner_uuid_index(ownership_session):
    session = ownership_session
    if session.get_bind().dialect.name != "mysql":
        pytest.skip("MySQL optimizer regression")
    for name in seed_many_legacy_attempts(session):
        session.execute(sa.text(f"ANALYZE TABLE {name}"))
    query = xcom.build_xcom_read_query(producer_ids=producers(session, CURRENT_ID), key="return_value")
    compiled = query.compile(dialect=session.get_bind().dialect, compile_kwargs={"literal_binds": True})
    plan = json.loads(session.scalar(sa.text(f"EXPLAIN FORMAT=JSON {compiled}")))

    def table_nodes(node):
        if isinstance(node, dict):
            if "table_name" in node:
                yield node
            for child in node.values():
                yield from table_nodes(child)
        elif isinstance(node, list):
            for child in node:
                yield from table_nodes(child)

    owners = [node for node in table_nodes(plan) if node["table_name"] == "legacy_task_data_owner"]
    assert len(owners) == 1
    assert owners[0]["key"] == "idx_legacy_task_data_owner_ti"
    assert owners[0]["rows_examined_per_scan"] <= 10


def test_postgresql_point_read_uses_the_legacy_coordinate_index(ownership_session):
    session = ownership_session
    if session.get_bind().dialect.name != "postgresql":
        pytest.skip("PostgreSQL query plan")
    for name in seed_many_legacy_attempts(session):
        session.execute(sa.text(f"ANALYZE {name}"))
    query = xcom.build_xcom_read_query(producer_ids=producers(session, CURRENT_ID), key="return_value")
    compiled = query.compile(dialect=session.get_bind().dialect, compile_kwargs={"literal_binds": True})
    plan = session.scalar(sa.text(f"EXPLAIN (FORMAT JSON) {compiled}"))[0]["Plan"]

    def scans(node):
        if node.get("Relation Name") == "xcom_v1":
            yield node
        for child in node.get("Plans", []):
            yield from scans(child)

    legacy_scans = list(scans(plan))
    assert legacy_scans
    assert all(
        node["Node Type"] in {"Index Scan", "Index Only Scan", "Bitmap Heap Scan"} for node in legacy_scans
    ), plan


def test_legacy_data_is_visible_only_to_its_migration_owner(ownership_session):
    session = ownership_session
    rows = read(session, CURRENT_ID)
    assert len(rows) == 1
    assert rows[0].task_instance_id == CURRENT_ID
    assert rows[0].value == {"legacy": True}
    assert rows[0].mapped_length == 3
    assert (rows[0].dag_id, rows[0].task_id, rows[0].run_id, rows[0].map_index) == (
        "ownership",
        "task",
        "manual",
        -1,
    )
    assert read(session, HISTORY_ID) == []


@pytest.mark.parametrize("value", [{"v2": True}, None, {}, 0, False, ""])
def test_uuid_write_shadows_legacy_without_modifying_it(ownership_session, value):
    session = ownership_session
    original = read(session, CURRENT_ID)[0]
    xcom.XComModel.set_for_attempt(
        task_instance_id=CURRENT_ID,
        key="return_value",
        value=value,
        serialize=False,
        mapped_length=5,
        dag_result=True,
        session=session,
    )
    rows = read(session, CURRENT_ID)
    assert len(rows) == 1
    assert rows[0].value == value
    assert rows[0].mapped_length == 5
    assert rows[0].dag_result is True
    assert rows[0] is original
    legacy = table(session.connection(), "xcom_v1")
    assert session.scalar(sa.select(legacy.c.value)) == {"legacy": True}
    physical = xcom.XComModelV2.get_for_attempt(CURRENT_ID, "return_value", session=session)
    assert physical is not original


def test_delete_removes_both_copies_without_touching_a_successor(ownership_session):
    session = ownership_session
    ti = table(session.connection(), "task_instance", "id")
    session.execute(ti.update().where(ti.c.id == CURRENT_ID).values(working_set=None))
    successor_id = uuid4()
    session.execute(
        ti.insert().values(
            id=successor_id,
            dag_id="ownership",
            task_id="task",
            run_id="manual",
            region_index=-1,
            try_number=3,
            pool="default_pool",
            pool_slots=1,
        )
    )
    for attempt_id, value in ((CURRENT_ID, "late write to predecessor"), (successor_id, "successor")):
        xcom.XComModel.set_for_attempt(
            task_instance_id=attempt_id,
            key="return_value",
            value=value,
            serialize=False,
            session=session,
        )
    xcom.XComModel.delete_for_attempts(
        producer_ids=producers(session, CURRENT_ID),
        key="return_value",
        session=session,
    )
    assert read(session, CURRENT_ID) == []
    assert [row.value for row in read(session, successor_id)] == ["successor"]
    assert session.scalar(sa.select(sa.func.count()).select_from(table(session.connection(), "xcom_v1"))) == 0


@pytest.mark.parametrize("operation", ["insert", "update", "delete"])
def test_read_projection_cannot_be_flushed_as_a_physical_row(ownership_session, operation):
    session = ownership_session
    result = read(session, CURRENT_ID)[0]
    if operation == "insert":
        session.add(type(result)(task_instance_id=CURRENT_ID, key="new", value="must not write"))
    elif operation == "update":
        result.value = "must not write"
    else:
        session.delete(result)
    with pytest.raises(TypeError, match="read.only"):
        session.flush()


def test_legacy_lookup_uses_fixed_owner_coordinates_after_mapping_promotion(ownership_session):
    session = ownership_session
    ti = table(session.connection(), "task_instance", "id")
    session.execute(ti.update().where(ti.c.id == CURRENT_ID).values(region_index=0))
    rows = read(session, CURRENT_ID)
    assert len(rows) == 1
    assert rows[0].map_index == 0
    assert rows[0].value == {"legacy": True}
    owner = table(session.connection(), "legacy_task_data_owner")
    assert session.scalar(sa.select(owner.c.map_index)) == -1


@pytest.mark.parametrize("partially_loaded", [False, True])
@pytest.mark.parametrize(
    ("store", "first", "second"),
    [
        pytest.param("xcom", 1, 2, id="xcom"),
        pytest.param("rtif", {"first": 1}, {"second": 2}, id="rendered-fields"),
    ],
)
def test_replacement_refreshes_a_loaded_physical_row(
    ownership_session, store, first, second, partially_loaded
):
    session = ownership_session
    if store == "xcom":
        model, value_column = xcom.XComModelV2, "value"

        def write(value):
            xcom.XComModel.set_for_attempt(
                task_instance_id=CURRENT_ID, key="new", value=value, serialize=False, session=session
            )

        def current():
            return xcom.XComModelV2.get_for_attempt(CURRENT_ID, "new", session=session)

    else:
        model, value_column = RenderedTaskInstanceFields, "rendered_fields"

        def write(value):
            RenderedTaskInstanceFields.set_for_attempt(
                task_instance_id=CURRENT_ID, rendered_fields=value, session=session
            )

        def current():
            return session.scalar(
                sa.select(RenderedTaskInstanceFields).where(
                    RenderedTaskInstanceFields.task_instance_id == CURRENT_ID
                )
            )

    write(first)
    original = current()
    original_id = original.id
    if partially_loaded:
        session.expunge(original)
        original = session.scalar(
            sa.select(model)
            .options(sa.orm.load_only(model.id, getattr(model, value_column)))
            .where(model.id == original_id)
        )
        assert "task_instance_id" not in original.__dict__
    write(second)

    assert session.get(model, original_id) is original
    assert original.id == original_id
    assert getattr(original, value_column) == second


def test_rendered_fields_fall_back_only_to_the_legacy_owner(ownership_session):
    fields = RenderedTaskInstanceFields.get_for_attempt(CURRENT_ID, session=ownership_session)
    assert fields.rendered_fields == {"field": "legacy"}
    assert fields.k8s_pod_yaml == {"kind": "Pod"}
    assert RenderedTaskInstanceFields.get_for_attempt(HISTORY_ID, session=ownership_session) is None


def test_rendered_fields_v2_empty_values_shadow_legacy(ownership_session):
    session = ownership_session
    RenderedTaskInstanceFields.set_for_attempt(
        task_instance_id=CURRENT_ID,
        rendered_fields={},
        k8s_pod_yaml=None,
        session=session,
    )
    fields = RenderedTaskInstanceFields.get_for_attempt(CURRENT_ID, session=session)
    assert fields.rendered_fields == {}
    assert fields.k8s_pod_yaml is None


def test_rendered_field_delete_prevents_legacy_fallback(ownership_session):
    session = ownership_session
    RenderedTaskInstanceFields.set_for_attempt(
        task_instance_id=CURRENT_ID,
        rendered_fields={"v2": "data"},
        session=session,
    )
    RenderedTaskInstanceFields.delete_for_attempts(
        producer_ids=producers(session, CURRENT_ID), session=session
    )
    assert RenderedTaskInstanceFields.get_for_attempt(CURRENT_ID, session=session) is None
    assert session.scalar(sa.select(sa.func.count()).select_from(table(session.connection(), "rtif_v1"))) == 0


def test_load_legacy_rendered_fields_fills_only_attempts_without_a_joined_row(ownership_session):
    session = ownership_session
    tis = list(
        session.scalars(
            sa.select(TaskInstance)
            .where(TaskInstance.id.in_([CURRENT_ID, HISTORY_ID]))
            .options(joinedload(TaskInstance.rendered_task_instance_fields))
            .execution_options(include_all_attempts=True)
        )
    )

    load_legacy_rendered_fields(tis, session=session)

    by_id = {ti.id: ti.rendered_task_instance_fields for ti in tis}
    assert by_id[CURRENT_ID].rendered_fields == {"field": "legacy"}
    assert isinstance(by_id[CURRENT_ID], LegacyRenderedTaskInstanceFields)
    assert by_id[HISTORY_ID] is None
    with assert_queries_count(0):
        load_legacy_rendered_fields([], session=session)


def test_load_legacy_rendered_fields_keeps_the_joined_v2_row(ownership_session):
    session = ownership_session
    RenderedTaskInstanceFields.set_for_attempt(
        task_instance_id=CURRENT_ID, rendered_fields={"v2": "data"}, session=session
    )
    ti = session.scalars(
        sa.select(TaskInstance)
        .where(TaskInstance.id == CURRENT_ID)
        .options(joinedload(TaskInstance.rendered_task_instance_fields))
        .execution_options(populate_existing=True)
    ).one()

    with assert_queries_count(0):
        load_legacy_rendered_fields([ti], session=session)

    assert isinstance(ti.rendered_task_instance_fields, RenderedTaskInstanceFields)
    assert ti.rendered_task_instance_fields.rendered_fields == {"v2": "data"}


def test_load_legacy_rendered_fields_loads_attempts_that_were_not_joined(ownership_session):
    session = ownership_session
    ti = session.scalars(
        sa.select(TaskInstance).where(TaskInstance.id == CURRENT_ID).execution_options(populate_existing=True)
    ).one()

    load_legacy_rendered_fields([ti], session=session)

    assert ti.rendered_task_instance_fields.rendered_fields == {"field": "legacy"}


def test_rendered_retention_deletes_both_stores_and_keeps_a_whole_mapped_run(ownership_session):
    session = ownership_session
    run = table(session.connection(), "dag_run")
    session.execute(
        run.insert().values(
            id=42,
            dag_id="ownership",
            run_id="next",
            run_type="manual",
            run_after=NOW + timedelta(days=1),
            state="running",
        )
    )
    ti = table(session.connection(), "task_instance", "id")
    keep_ids = [uuid4(), uuid4()]
    for map_index, attempt_id in enumerate(keep_ids):
        session.execute(
            ti.insert().values(
                id=attempt_id,
                dag_id="ownership",
                task_id="task",
                run_id="next",
                region_index=map_index,
                try_number=1,
                pool="default_pool",
                pool_slots=1,
            )
        )
    for attempt_id in (CURRENT_ID, *keep_ids):
        RenderedTaskInstanceFields.set_for_attempt(
            task_instance_id=attempt_id,
            rendered_fields={"field": "v2"},
            session=session,
        )
    unrelated_ids = [uuid4(), uuid4()]
    session.execute(
        run.insert().values(
            id=43,
            dag_id="other",
            run_id="manual",
            run_type="manual",
            run_after=NOW,
            state="running",
        )
    )
    unrelated_coordinates = [
        dict(dag_id="ownership", task_id="other", run_id="manual", map_index=-1),
        dict(dag_id="other", task_id="task", run_id="manual", map_index=-1),
    ]
    owner = table(session.connection(), "legacy_task_data_owner", "task_instance_id")
    legacy = table(session.connection(), "rtif_v1")
    for attempt_id, coordinates in zip(unrelated_ids, unrelated_coordinates):
        session.execute(
            ti.insert().values(
                id=attempt_id,
                **ti_coordinates(coordinates),
                try_number=1,
                pool="default_pool",
                pool_slots=1,
            )
        )
        session.execute(owner.insert().values(**coordinates, task_instance_id=attempt_id))
        session.execute(legacy.insert().values(**coordinates, rendered_fields={"field": "legacy"}))
        RenderedTaskInstanceFields.set_for_attempt(
            task_instance_id=attempt_id, rendered_fields={"field": "other"}, session=session
        )

    RenderedTaskInstanceFields.delete_old_records("task", "ownership", num_to_keep=1, session=session)

    assert RenderedTaskInstanceFields.get_for_attempt(CURRENT_ID, session=session) is None
    for attempt_id in keep_ids:
        assert RenderedTaskInstanceFields.get_for_attempt(attempt_id, session=session).rendered_fields == {
            "field": "v2"
        }
    assert session.scalar(sa.select(sa.func.count()).select_from(legacy)) == 2
    for attempt_id in unrelated_ids:
        assert RenderedTaskInstanceFields.get_for_attempt(attempt_id, session=session).rendered_fields == {
            "field": "other"
        }


def test_uuid_store_preserves_custom_serialization(ownership_session):
    session = ownership_session
    xcom.XComModel.set_for_attempt(task_instance_id=CURRENT_ID, key="tuple", value=(1, 2), session=session)
    assert xcom.XComModel.deserialize_value(read(session, CURRENT_ID, key="tuple")[0]) == (1, 2)
