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

from unittest import mock

import pytest

from airflow.models.expandinput import NotFullyPopulated
from airflow.sdk import task, task_group
from airflow.serialization.definitions import mappedoperator
from airflow.serialization.definitions.mappedoperator import get_mapped_ti_count
from airflow.ti_deps.dep_context import DepContext

from tests_common.test_utils.mapping import push_mapped_length

pytestmark = pytest.mark.db_test


def _create_mapped_group_run(dag_maker, session):
    with dag_maker(session=session, serialized=True):

        @task
        def get_values():
            return [1, 2, 3]

        @task_group
        def group(value):
            @task
            def stage(value):
                return value

            stage(value)

        group.expand(value=get_values())

    return dag_maker.create_dagrun(), dag_maker.serialized_dag.get_task("group.stage")


@pytest.mark.parametrize(
    "node_id",
    [
        pytest.param("group.stage", id="mapped task"),
        pytest.param("group", id="mapped task group"),
    ],
)
@mock.patch.object(
    mappedoperator, "get_mapped_ti_count", autospec=True, side_effect=mappedoperator.get_mapped_ti_count
)
def test_get_mapped_ti_count_is_memoized(mock_get_mapped_ti_count, dag_maker, session, node_id):
    dr, stage = _create_mapped_group_run(dag_maker, session)
    push_mapped_length(dr.get_task_instance("get_values", session=session), [1, 2, 3], session=session)
    node = stage if node_id == stage.task_id else stage.get_closest_mapped_task_group()
    expected = get_mapped_ti_count(node, dr.run_id, session=session)
    dep_context = DepContext()

    assert dep_context.get_mapped_ti_count(node, dr.run_id, session=session) == expected == 3
    # First call is a miss and calls the original once for this node. A task's count also recurses into
    # its group's through the patched name, so count the exact call only.
    assert (
        mock_get_mapped_ti_count.call_args_list.count(
            mock.call(node, dr.run_id, session=session, producer_contexts=None)
        )
        == 1
    )
    mock_get_mapped_ti_count.reset_mock()

    # Second call is a hit: the count is read from the memo and the original is not called.
    assert dep_context.get_mapped_ti_count(node, dr.run_id, session=session) == expected
    mock_get_mapped_ti_count.assert_not_called()
    assert dep_context.mapped_ti_counts == {(dr.dag_id, dr.run_id, node_id, frozenset()): 3}


def test_get_mapped_ti_count_does_not_store_unknown_count(dag_maker, session):
    dr, stage = _create_mapped_group_run(dag_maker, session)
    dep_context = DepContext()

    with pytest.raises(NotFullyPopulated):
        dep_context.get_mapped_ti_count(stage, dr.run_id, session=session)
    assert dep_context.mapped_ti_counts == {}

    push_mapped_length(dr.get_task_instance("get_values", session=session), [1, 2, 3], session=session)
    assert dep_context.get_mapped_ti_count(stage, dr.run_id, session=session) == 3
    assert dep_context.mapped_ti_counts == {(dr.dag_id, dr.run_id, "group.stage", frozenset()): 3}


def test_invalidate_drops_mapped_ti_counts():
    dep_context = DepContext(mapped_ti_counts={("dag", "run", "group.stage", frozenset()): 3})

    dep_context.invalidate_upstream_task_id_counts()

    assert dep_context.mapped_ti_counts == {}
