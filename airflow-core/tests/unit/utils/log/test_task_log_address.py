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

from types import SimpleNamespace
from unittest import mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event

from airflow.configuration import conf
from airflow.models.dagbag import DBDagBag
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.serialization.serialized_objects import DagSerialization
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.task_log_address import (
    TaskLogContext,
    prepare_task_log_contexts,
    region_log_position,
    render_task_log_filename,
)

from tests_common.test_utils.asserts import assert_queries_count


@pytest.mark.parametrize(
    ("map_index", "token", "suffix"),
    [(-1, "", "attempt=3.log"), (0, "", "map_index=0/attempt=3.log"), (-1, "pass=2", "pass=2/attempt=3.log")],
)
def test_default_template_preserves_exact_filename(map_index, token, suffix):
    ti = SimpleNamespace(dag_id="dag", run_id="run", task_id="task", region_index=map_index, try_number=3)
    context = TaskLogContext(conf.get("logging", "log_filename_template"), "", "", "", token, map_index)

    assert render_task_log_filename(ti, 3, context=context) == f"dag_id=dag/run_id=run/task_id=task/{suffix}"


def test_region_log_position_preserves_old_fork_addresses():
    first = DynamicRegion(id=uuid4(), dag_id="d", run_id="r", node_id="loop")
    second = DynamicRegion(id=uuid4(), dag_id="d", run_id="r", node_id="loop", forked_from_region_id=first.id)
    mapped = DynamicRegion(
        id=uuid4(),
        dag_id="d",
        run_id="r",
        node_id="mapped",
        parent_region_id=second.id,
        parent_region_index=1,
    )
    regions = {region.id: region for region in (first, second, mapped)}
    kinds = {"loop": "loop", "mapped": "map"}
    assert region_log_position(first.id, 1, regions=regions, node_kinds=kinds) == "pass=1"
    assert region_log_position(second.id, 1, regions=regions, node_kinds=kinds) == "pass=1.2"
    assert region_log_position(mapped.id, 0, regions=regions, node_kinds=kinds) == "pass=1.2/map=0"
    third = DynamicRegion(id=uuid4(), dag_id="d", run_id="r", node_id="loop", forked_from_region_id=second.id)
    regions[third.id] = third
    assert region_log_position(first.id, 1, regions=regions, node_kinds=kinds) == "pass=1"
    assert region_log_position(mapped.id, 0, regions=regions, node_kinds=kinds) == "pass=1.2/map=0"


@pytest.mark.parametrize(
    ("template", "expected"),
    [
        ("custom/{{ ti.task_id }}/{{ try_number }}.log", "custom/t/pass=1.2/map=0/3.log"),
        ("custom/{task_id}/{try_number}.log", "custom/t/pass=1.2/map=0/3.log"),
        ("custom/{{ log_position }}/{{ try_number }}.log", "custom/pass=1.2/map=0/3.log"),
        ("custom/{log_position}/{try_number}.log", "custom/pass=1.2/map=0/3.log"),
        ("custom/{{ ti.map_index }}/{{ try_number }}.log", "custom/0/pass=1.2/map=0/3.log"),
    ],
)
def test_regional_template_uses_token_exactly_once(template, expected):
    ti = SimpleNamespace(dag_id="d", run_id="r", task_id="t", map_index=0, try_number=3)
    context = TaskLogContext(template, "2026-01-01T00:00:00+00:00", "", "", "pass=1.2/map=0", 0)
    assert render_task_log_filename(ti, 3, context=context) == expected


def test_unmapped_loop_public_map_index_does_not_expose_pass():
    ti = SimpleNamespace(dag_id="d", run_id="r", task_id="t", map_index=4, try_number=1)
    context = TaskLogContext("{{ ti.map_index }}/{{ try_number }}.log", "", "", "", "pass=4", -1)
    assert render_task_log_filename(ti, 1, context=context) == "-1/pass=4/1.log"
    assert ti.map_index == 4


@pytest.mark.parametrize(
    "template",
    [
        "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/"
        "{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}attempt={{ try_number }}.log",
        "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/"
        "map_index={{ ti.map_index }}/attempt={{ try_number }}.log",
    ],
)
def test_mapped_expansion_outside_loop_keeps_pinned_template_path(template):
    ti = SimpleNamespace(dag_id="dag", run_id="run", task_id="task", try_number=3)
    context = TaskLogContext(template, "", "", "", "", 3)

    path = render_task_log_filename(ti, 3, context=context)

    assert path.endswith("task_id=task/map_index=3/attempt=3.log")


def test_mapped_expansion_outside_loop_has_no_regional_position():
    first = DynamicRegion(id=uuid4(), dag_id="d", run_id="r", node_id="mapped")
    second = DynamicRegion(
        id=uuid4(), dag_id="d", run_id="r", node_id="mapped", forked_from_region_id=first.id
    )
    regions = {region.id: region for region in (first, second)}

    assert region_log_position(second.id, 3, regions=regions, node_kinds={"mapped": "map"}) == ""


def test_sentinel_has_no_regional_position():
    assert region_log_position(UUID(int=0), 0, regions={}, node_kinds={}) == ""


@pytest.mark.db_test
def test_batched_loop_contexts_and_archived_tries_keep_original_addresses(dag_maker, session):
    @task_group(group_id="body")
    def body():
        EmptyOperator(task_id="work")

    with dag_maker():
        create_loop(body, max_iterations=3)
    run = dag_maker.create_dagrun()
    ti = next(ti for ti in run.task_instances if ti.task_id == "body.work")
    first = DynamicRegion(dag_id=run.dag_id, run_id=run.run_id, node_id="body")
    session.add(first)
    session.flush()
    ti.region_id, ti.region_index, ti.try_number = first.id, 1, 1
    session.flush()

    with assert_queries_count(4, session=session):
        contexts = prepare_task_log_contexts([ti], session=session)
    context = contexts[ti.id]
    assert context.log_position == "pass=1"
    assert context.map_index == -1
    path = render_task_log_filename(ti, 1, context=context)
    assert "pass=1/attempt=1.log" in path
    assert "map_index=" not in path

    with dag_maker(dag_id=run.dag_id, serialized=True):
        PythonOperator.partial(task_id="body.work", python_callable=str).expand(op_args=[[1], [2]])
    dag_maker.sync_dag_to_db()

    successor = DynamicRegion(
        dag_id=run.dag_id, run_id=run.run_id, node_id="body", forked_from_region_id=first.id
    )
    session.add(successor)
    session.flush()
    ti.archive(reason="test", session=session)
    replacement = TaskInstance(ti.task, ti.dag_version_id, run_id=run.run_id, region_id=successor.id)
    replacement.region_index, replacement.try_number = 1, 1
    session.add(replacement)
    session.flush()
    handler = FileTaskHandler("")
    assert handler._render_filename(ti, 1, session=session) == path
    replacement_path = handler._render_filename(replacement, 1, session=session)
    assert "pass=1.2/attempt=1.log" in replacement_path
    assert replacement_path != path


@pytest.mark.db_test
@pytest.mark.parametrize("inside_loop", [False, True])
def test_mapped_log_context_projects_expansion_position(dag_maker, session, inside_loop):
    @task_group(group_id="body")
    def body():
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])

    with dag_maker():
        if inside_loop:
            create_loop(body, max_iterations=3)
        else:
            body()
    run = dag_maker.create_dagrun()
    ti = next(ti for ti in run.task_instances if ti.task_id == "body.mapped")
    parent_id = None
    if inside_loop:
        parent = DynamicRegion(dag_id=run.dag_id, run_id=run.run_id, node_id="body")
        session.add(parent)
        session.flush()
        parent_id = parent.id
    expansion = DynamicRegion(
        dag_id=run.dag_id,
        run_id=run.run_id,
        node_id=ti.task_id,
        parent_region_id=parent_id,
        parent_region_index=2 if inside_loop else None,
    )
    session.add(expansion)
    session.flush()
    predecessor = DynamicRegion(
        dag_id=run.dag_id,
        run_id=run.run_id,
        node_id=ti.task_id,
        parent_region_id=parent_id,
        parent_region_index=2 if inside_loop else None,
    )
    session.add(predecessor)
    session.flush()
    expansion.forked_from_region_id = predecessor.id
    session.add_all(
        DynamicRegion(dag_id=run.dag_id, run_id=run.run_id, node_id="unrelated") for _ in range(20)
    )
    ti.region_id, ti.region_index = expansion.id, 0
    session.flush()
    expected_regions = {expansion.id, predecessor.id}
    if parent_id is not None:
        expected_regions.add(parent_id)
    session.expunge_all()
    loaded_region_ids = set()

    def capture_region_load(session, instance):
        if isinstance(instance, DynamicRegion):
            loaded_region_ids.add(instance.id)

    event.listen(session, "loaded_as_persistent", capture_region_load)
    try:
        context = prepare_task_log_contexts([ti], session=session)[ti.id]
    finally:
        event.remove(session, "loaded_as_persistent", capture_region_load)
    assert loaded_region_ids == expected_regions
    token = "pass=2/map=0.2" if inside_loop else ""
    assert context.log_position == token
    assert context.map_index == 0
    path = render_task_log_filename(ti, 1, context=context)
    if inside_loop:
        assert path.endswith(f"map_index=0/{token}/attempt=1.log")
    else:
        assert path.endswith("task_id=body.mapped/map_index=0/attempt=1.log")


@pytest.mark.db_test
@pytest.mark.parametrize(("attach_dag", "expected_reads"), [(True, 0), (False, 1)])
def test_mapped_log_contexts_deserialize_the_dag_at_most_once(dag_maker, session, attach_dag, expected_reads):
    with dag_maker(serialized=True):
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
    run = dag_maker.create_dagrun()
    tis = run.task_instances
    dag = DBDagBag().get_dag_for_run(run, session=session)
    for ti in tis:
        ti.task = dag.get_task(ti.task_id) if attach_dag else None

    with mock.patch.object(
        DagSerialization, "from_dict", autospec=True, side_effect=DagSerialization.from_dict
    ) as read:
        prepare_task_log_contexts(tis, session=session)

    assert read.call_count == expected_reads
