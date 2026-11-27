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

from datetime import timedelta
from unittest import mock

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
from airflow.api_fastapi.core_api.datamodels.common import BulkActionResponse, BulkBody
from airflow.api_fastapi.core_api.datamodels.task_instances import (
    BulkTaskInstanceBody,
    ClearTaskInstancesBody,
)
from airflow.api_fastapi.core_api.services.public.task_instances import (
    BulkTaskInstanceService,
    ExternalClearDepthExceeded,
    clear_dependent_runs,
    find_dependent_clears,
)
from airflow.models import DagModel
from airflow.models.dagbag import DBDagBag
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import EXTERNAL_CLEAR_TARGET_XCOM_KEY, XComModel
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskMarker
from airflow.sdk import ExternalClearTarget, task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.utils.state import DagRunState, TaskInstanceState

from tests_common.test_utils.db import (
    clear_db_runs,
)
from tests_common.test_utils.mock_operators import MockOperator

pytestmark = pytest.mark.db_test
DAG_ID_1 = "TEST_DAG_1"
DAG_ID_2 = "TEST_DAG_2"
DAG_RUN_ID_1 = "TEST_DAG_RUN_1"
DAG_RUN_ID_2 = "TEST_DAG_RUN_2"
TASK_ID_1 = "TEST_TASK_1"
TASK_ID_2 = "TEST_TASK_2"


class TestTaskInstanceEndpoint:
    @staticmethod
    def clear_db():
        clear_db_runs()


class TestCategorizeTaskInstances(TestTaskInstanceEndpoint):
    """Tests for the categorize_task_instances method in BulkTaskInstanceService."""

    def setup_method(self):
        self.clear_db()

    def teardown_method(self):
        self.clear_db()

    class MockUser:
        username = "test_user"
        role = "admin"
        teams = ["team1"]

        def get_id(self) -> str:
            return "test_user"

        def get_name(self) -> str:
            return "test_user"

    @pytest.mark.parametrize(
        (
            "dags_to_create",
            "dag_id",
            "dag_run_id",
            "task_keys",
            "expected_matched_keys",
            "expected_not_found_keys",
        ),
        [
            pytest.param(
                [(DAG_ID_1, DAG_RUN_ID_1, [TASK_ID_1, TASK_ID_2])],
                DAG_ID_1,
                DAG_RUN_ID_1,
                {(DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1), (DAG_ID_1, DAG_RUN_ID_1, TASK_ID_2, -1)},
                {(DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1), (DAG_ID_1, DAG_RUN_ID_1, TASK_ID_2, -1)},
                set(),
                id="single_dag_run_all_found",
            ),
            pytest.param(
                [(DAG_ID_1, DAG_RUN_ID_1, [TASK_ID_1, TASK_ID_2])],
                DAG_ID_1,
                DAG_RUN_ID_1,
                {(DAG_ID_1, DAG_RUN_ID_1, "nonexistent_task", -1)},
                set(),
                {(DAG_ID_1, DAG_RUN_ID_1, "nonexistent_task", -1)},
                id="single_dag_run_not_found",
            ),
            pytest.param(
                [(DAG_ID_1, DAG_RUN_ID_1, [TASK_ID_1, TASK_ID_2])],
                DAG_ID_1,
                DAG_RUN_ID_1,
                {(DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1), (DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, 0)},
                {(DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1)},
                {(DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, 0)},
                id="single_dag_run_mixed_map_index",
            ),
            pytest.param(
                [(DAG_ID_1, DAG_RUN_ID_1, [TASK_ID_1]), (DAG_ID_2, DAG_RUN_ID_2, [TASK_ID_1])],
                "~",
                "~",
                {(DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1), (DAG_ID_2, DAG_RUN_ID_2, TASK_ID_1, -1)},
                {(DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1), (DAG_ID_2, DAG_RUN_ID_2, TASK_ID_1, -1)},
                set(),
                id="wildcard_multiple_dags_all_found",
            ),
            pytest.param(
                [(DAG_ID_1, DAG_RUN_ID_1, [TASK_ID_1]), (DAG_ID_2, DAG_RUN_ID_2, [TASK_ID_1])],
                "~",
                "~",
                {
                    (DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1),
                    (DAG_ID_2, DAG_RUN_ID_2, "nonexistent_task", -1),
                },
                {(DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1)},
                {(DAG_ID_2, DAG_RUN_ID_2, "nonexistent_task", -1)},
                id="wildcard_multiple_dags_mixed",
            ),
            pytest.param(
                [(DAG_ID_1, DAG_RUN_ID_1, [TASK_ID_1, TASK_ID_2]), (DAG_ID_2, DAG_RUN_ID_2, [TASK_ID_1])],
                "~",
                "~",
                {
                    (DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1),
                    (DAG_ID_1, DAG_RUN_ID_1, TASK_ID_2, -1),
                    (DAG_ID_2, DAG_RUN_ID_2, TASK_ID_1, -1),
                    (DAG_ID_2, DAG_RUN_ID_2, TASK_ID_2, -1),
                },
                {
                    (DAG_ID_1, DAG_RUN_ID_1, TASK_ID_1, -1),
                    (DAG_ID_1, DAG_RUN_ID_1, TASK_ID_2, -1),
                    (DAG_ID_2, DAG_RUN_ID_2, TASK_ID_1, -1),
                },
                {(DAG_ID_2, DAG_RUN_ID_2, TASK_ID_2, -1)},
                id="wildcard_partial_match_across_dags",
            ),
        ],
    )
    def test_categorize_task_instances(
        self,
        session,
        dag_maker,
        dags_to_create,
        dag_id,
        dag_run_id,
        task_keys,
        expected_matched_keys,
        expected_not_found_keys,
    ):
        """Test categorize_task_instances with various scenarios."""
        for dag_id_to_create, run_id_to_create, task_ids in dags_to_create:
            with dag_maker(dag_id=dag_id_to_create, session=session):
                for task_id in task_ids:
                    BashOperator(task_id=task_id, bash_command=f"echo {task_id}")
            dag_maker.create_dagrun(run_id=run_id_to_create)

        session.commit()

        user = self.MockUser()
        bulk_request = BulkBody(actions=[])
        service = BulkTaskInstanceService(
            session=session,
            request=bulk_request,
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            dag_bag=dag_maker.dagbag,
            user=user,
        )

        _, matched_task_keys, not_found_task_keys = service._categorize_task_instances(task_keys)

        assert matched_task_keys == expected_matched_keys
        assert not_found_task_keys == expected_not_found_keys


class TestExtractTaskIdentifiers(TestTaskInstanceEndpoint):
    """Tests for the _extract_task_identifiers method in BulkTaskInstanceService."""

    def setup_method(self):
        self.clear_db()

    def teardown_method(self):
        self.clear_db()

    class MockUser:
        username = "test_user"
        role = "admin"
        teams = ["team1"]

        def get_id(self) -> str:
            return "test_user"

        def get_name(self) -> str:
            return "test_user"

    @pytest.mark.parametrize(
        ("entity", "expected_dag_id", "expected_dag_run_id", "expected_task_id", "expected_map_index"),
        [
            pytest.param(
                BulkTaskInstanceBody(task_id="task_1", dag_id=None, dag_run_id=None, map_index=None),
                DAG_ID_1,
                DAG_RUN_ID_1,
                "task_1",
                None,
                id="object_entity_with_none_fields",
            ),
            pytest.param(
                BulkTaskInstanceBody(task_id="task_2", dag_id=DAG_ID_2, dag_run_id=DAG_RUN_ID_2, map_index=5),
                DAG_ID_2,
                DAG_RUN_ID_2,
                "task_2",
                5,
                id="object_entity_with_all_fields",
            ),
            pytest.param(
                BulkTaskInstanceBody(task_id="task_3", dag_id=None, dag_run_id=None, map_index=None),
                DAG_ID_1,
                DAG_RUN_ID_1,
                "task_3",
                None,
                id="object_entity_fallback_to_path_params",
            ),
        ],
    )
    def test_extract_task_identifiers(
        self,
        session,
        dag_maker,
        entity,
        expected_dag_id,
        expected_dag_run_id,
        expected_task_id,
        expected_map_index,
    ):
        """Test _extract_task_identifiers with different entity configurations."""

        user = self.MockUser()
        bulk_request = BulkBody(actions=[])
        service = BulkTaskInstanceService(
            session=session,
            request=bulk_request,
            dag_id=DAG_ID_1,
            dag_run_id=DAG_RUN_ID_1,
            dag_bag=dag_maker.dagbag,
            user=user,
        )

        dag_id, dag_run_id, task_id, map_index = service._extract_task_identifiers(entity)

        assert dag_id == expected_dag_id
        assert dag_run_id == expected_dag_run_id
        assert task_id == expected_task_id
        assert map_index == expected_map_index


class TestCategorizeEntities(TestTaskInstanceEndpoint):
    """Tests for the _categorize_entities method in BulkTaskInstanceService."""

    def setup_method(self):
        self.clear_db()

    def teardown_method(self):
        self.clear_db()

    class MockUser:
        username = "test_user"
        role = "admin"
        teams = ["team1"]

        def get_id(self) -> str:
            return "test_user"

        def get_name(self) -> str:
            return "test_user"

    @pytest.mark.parametrize(
        (
            "entities",
            "service_dag_id",
            "service_dag_run_id",
            "expected_specific_keys",
            "expected_all_keys",
            "expected_error_count",
        ),
        [
            pytest.param(
                [
                    BulkTaskInstanceBody(
                        task_id="task_1", dag_id=DAG_ID_1, dag_run_id=DAG_RUN_ID_1, map_index=5
                    )
                ],
                DAG_ID_1,
                DAG_RUN_ID_1,
                {(DAG_ID_1, DAG_RUN_ID_1, "task_1", 5)},
                set(),
                0,
                id="single_entity_with_map_index",
            ),
            pytest.param(
                [
                    BulkTaskInstanceBody(
                        task_id="task_1", dag_id=DAG_ID_1, dag_run_id=DAG_RUN_ID_1, map_index=None
                    )
                ],
                DAG_ID_1,
                DAG_RUN_ID_1,
                set(),
                {(DAG_ID_1, DAG_RUN_ID_1, "task_1")},
                0,
                id="single_entity_without_map_index",
            ),
            pytest.param(
                [
                    BulkTaskInstanceBody(
                        task_id="task_1", dag_id=DAG_ID_1, dag_run_id=DAG_RUN_ID_1, map_index=5
                    ),
                    BulkTaskInstanceBody(
                        task_id="task_2", dag_id=DAG_ID_1, dag_run_id=DAG_RUN_ID_1, map_index=None
                    ),
                ],
                DAG_ID_1,
                DAG_RUN_ID_1,
                {(DAG_ID_1, DAG_RUN_ID_1, "task_1", 5)},
                {(DAG_ID_1, DAG_RUN_ID_1, "task_2")},
                0,
                id="mixed_entities_with_and_without_map_index",
            ),
            pytest.param(
                [
                    BulkTaskInstanceBody(
                        task_id="task_1", dag_id=DAG_ID_1, dag_run_id=DAG_RUN_ID_1, map_index=5
                    ),
                    BulkTaskInstanceBody(
                        task_id="task_1", dag_id=DAG_ID_2, dag_run_id=DAG_RUN_ID_2, map_index=10
                    ),
                ],
                DAG_ID_1,
                DAG_RUN_ID_1,
                {(DAG_ID_1, DAG_RUN_ID_1, "task_1", 5), (DAG_ID_2, DAG_RUN_ID_2, "task_1", 10)},
                set(),
                0,
                id="multiple_entities_different_dags",
            ),
            pytest.param(
                [BulkTaskInstanceBody(task_id="task_1", dag_id="~", dag_run_id=DAG_RUN_ID_1, map_index=None)],
                "~",
                "~",
                set(),
                set(),
                1,
                id="wildcard_in_dag_id_with_none_fields",
            ),
            pytest.param(
                [BulkTaskInstanceBody(task_id="task_1", dag_id=DAG_ID_1, dag_run_id="~", map_index=None)],
                "~",
                "~",
                set(),
                set(),
                1,
                id="wildcard_in_dag_run_id_with_none_fields",
            ),
            pytest.param(
                [
                    BulkTaskInstanceBody(task_id="task_1", dag_id="~", dag_run_id="~", map_index=None),
                    BulkTaskInstanceBody(
                        task_id="task_2", dag_id=DAG_ID_1, dag_run_id=DAG_RUN_ID_1, map_index=5
                    ),
                ],
                "~",
                "~",
                {(DAG_ID_1, DAG_RUN_ID_1, "task_2", 5)},
                set(),
                1,
                id="wildcard_error_and_valid_entity",
            ),
        ],
    )
    def test_categorize_entities(
        self,
        session,
        dag_maker,
        entities,
        service_dag_id,
        service_dag_run_id,
        expected_specific_keys,
        expected_all_keys,
        expected_error_count,
    ):
        """Test _categorize_entities with different entity configurations and wildcard validation."""
        user = self.MockUser()
        bulk_request = BulkBody(actions=[])
        service = BulkTaskInstanceService(
            session=session,
            request=bulk_request,
            dag_id=service_dag_id,
            dag_run_id=service_dag_run_id,
            dag_bag=dag_maker.dagbag,
            user=user,
        )

        results = BulkActionResponse()
        with (
            mock.patch.object(DagModel, "get_team_name", return_value="team1"),
            mock.patch(
                "airflow.api_fastapi.core_api.services.public.task_instances.get_auth_manager"
            ) as mock_get_auth_manager,
        ):
            auth_manager = mock.create_autospec(BaseAuthManager, instance=True, spec_set=True)
            auth_manager.is_authorized_dag.return_value = True
            mock_get_auth_manager.return_value = auth_manager
            specific_map_index_task_keys, all_map_index_task_keys = service._categorize_entities(
                entities, results, method="PUT", action_name="update"
            )

        assert specific_map_index_task_keys == expected_specific_keys
        assert all_map_index_task_keys == expected_all_keys
        assert len(results.errors) == expected_error_count


CLEAR_DATE = timezone.datetime(2026, 10, 1)


def _record(ti, value, session, *, serialize: bool = True) -> None:
    """Store a target the way the execution API stores one a task reports with its success."""
    if isinstance(value, ExternalClearTarget):
        value = value.serialize()
    XComModel.set_for_attempt(
        task_instance_id=ti.id,
        key=EXTERNAL_CLEAR_TARGET_XCOM_KEY,
        value=value,
        serialize=serialize,
        session=session,
    )
    session.flush()


def _target(dag_id: str, task_id: str = "wait", **kwargs) -> ExternalClearTarget:
    return ExternalClearTarget(dag_id=dag_id, task_id=task_id, logical_date=CLEAR_DATE, **kwargs)


def _find(sources, session, *, can_clear=lambda dag_id: True, dag_bag=None, **kwargs):
    return find_dependent_clears(
        sources, dag_bag=dag_bag or DBDagBag(), can_clear=can_clear, session=session, **kwargs
    )


def _keys(clears) -> list[tuple[str, str, str]]:
    return sorted((ti.dag_id, ti.run_id, ti.task_id) for clear in clears for ti in clear.task_instances)


def _make_child(dag_maker, dag_id: str, *logical_dates):
    """A Dag ``prep >> wait >> load`` with one run per logical date."""
    with dag_maker(dag_id):
        EmptyOperator(task_id="prep") >> EmptyOperator(task_id="wait") >> EmptyOperator(task_id="load")
    return [dag_maker.create_dagrun(run_id=f"run_{d:%H}", logical_date=d) for d in logical_dates]


def _make_parent(dag_maker, dag_id: str, logical_date=CLEAR_DATE, task_ids=("mark",)):
    with dag_maker(dag_id):
        for task_id in task_ids:
            EmptyOperator(task_id=task_id)
    dag_run = dag_maker.create_dagrun(logical_date=logical_date)
    return [dag_run.get_task_instance(task_id) for task_id in task_ids]


def test_follows_target_into_downstream_of_target_task(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    (mark,) = _make_parent(dag_maker, "parent")
    _record(mark, _target("child"), session)

    assert _keys(_find([mark], session)) == [
        ("child", child_run.run_id, "load"),
        ("child", child_run.run_id, "wait"),
    ]


def test_uses_recorded_logical_date_not_the_parent_run_date(dag_maker, session):
    parent_date = CLEAR_DATE + timedelta(hours=5)
    midnight_run, _same_hour_run = _make_child(dag_maker, "child", CLEAR_DATE, parent_date)
    (mark,) = _make_parent(dag_maker, "parent", parent_date)
    _record(mark, _target("child"), session)

    assert [clear.run_id for clear in _find([mark], session)] == [midnight_run.run_id]


def test_follows_targets_recorded_by_the_target_dag(dag_maker, session):
    (grandchild_run,) = _make_child(dag_maker, "grandchild", CLEAR_DATE)
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    _record(child_run.get_task_instance("load"), _target("grandchild"), session)
    (mark,) = _make_parent(dag_maker, "parent")
    _record(mark, _target("child"), session)

    keys = _keys(_find([mark], session))

    assert ("grandchild", grandchild_run.run_id, "load") in keys
    assert len(keys) == 4


def test_merges_targets_into_one_clear_per_run(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    mark_wait, mark_load = _make_parent(dag_maker, "parent", task_ids=("mark_wait", "mark_load"))
    _record(mark_wait, _target("child"), session)
    _record(mark_load, _target("child", task_id="load"), session)

    clears = _find([mark_wait, mark_load], session)

    assert [(clear.dag.dag_id, clear.run_id, sorted(clear.task_ids)) for clear in clears] == [
        ("child", child_run.run_id, ["load", "wait"])
    ]
    assert _keys(clears) == [("child", child_run.run_id, "load"), ("child", child_run.run_id, "wait")]


def test_raises_when_targets_go_deeper_than_first_recursion_depth(dag_maker, session):
    _make_child(dag_maker, "grandchild", CLEAR_DATE)
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    _record(child_run.get_task_instance("load"), _target("grandchild"), session)
    (mark,) = _make_parent(dag_maker, "parent")
    _record(mark, _target("child", recursion_depth=1), session)

    with pytest.raises(ExternalClearDepthExceeded, match="more than 1 levels"):
        _find([mark], session)


def test_does_not_follow_targets_in_dags_the_caller_cannot_clear(dag_maker, session):
    _make_child(dag_maker, "grandchild", CLEAR_DATE)
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    _record(child_run.get_task_instance("load"), _target("grandchild"), session)
    (mark,) = _make_parent(dag_maker, "parent")
    # Following the forbidden Dag would also exceed this depth and abort the clear.
    _record(mark, _target("child", recursion_depth=1), session)

    clears = _find([mark], session, can_clear=lambda dag_id: dag_id != "grandchild")

    assert {key[0] for key in _keys(clears)} == {"child"}


def test_leaves_tasks_the_source_selection_clears_to_it(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    (mark,) = _make_parent(dag_maker, "parent")
    _record(mark, _target("child"), session)
    _record(child_run.get_task_instance("load"), _target("parent", task_id="mark"), session)

    assert _keys(_find([mark], session)) == [
        ("child", child_run.run_id, "load"),
        ("child", child_run.run_id, "wait"),
    ]


def test_task_without_a_recorded_target_pulls_in_nothing(dag_maker, session):
    _make_child(dag_maker, "child", CLEAR_DATE)
    (mark,) = _make_parent(dag_maker, "parent")

    assert _find([mark], session) == []


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(
            {"__classname__": "builtins.tuple", "__version__": 1, "__data__": [1, 2]}, id="envelope"
        ),
        pytest.param("child", id="not-a-dict"),
        pytest.param(
            {"dag_id": "child", "task_id": "wait", "logical_date": "2026-10-01T00:00:00+00:00"},
            id="missing-key",
        ),
        pytest.param(
            {
                "dag_id": "child",
                "task_id": "wait",
                "logical_date": "2026-10-01T00:00:00",
                "recursion_depth": 1,
            },
            id="naive-date",
        ),
    ],
)
def test_ignores_a_record_that_does_not_validate(dag_maker, session, value):
    _make_child(dag_maker, "child", CLEAR_DATE)
    (mark,) = _make_parent(dag_maker, "parent")
    # Stored as is, since the XCom encoder refuses the envelope's reserved key.
    _record(mark, value, session, serialize=False)

    assert _find([mark], session) == []


def test_skips_a_target_run_with_no_serialized_dag(dag_maker, session):
    _make_child(dag_maker, "child", CLEAR_DATE)
    (mark,) = _make_parent(dag_maker, "parent")
    _record(mark, _target("child"), session)
    dag_bag = mock.create_autospec(DBDagBag, instance=True)
    dag_bag.get_dag.return_value = None
    dag_bag.get_dag_for_run.return_value = None

    assert _find([mark], session, dag_bag=dag_bag) == []


def test_only_failed_keeps_successful_target_task_instances(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    child_run.get_task_instance("wait", session=session).state = TaskInstanceState.SUCCESS
    child_run.get_task_instance("load", session=session).state = TaskInstanceState.FAILED
    session.flush()
    (mark,) = _make_parent(dag_maker, "parent")
    _record(mark, _target("child"), session)

    assert _keys(_find([mark], session, only_failed=True)) == [("child", child_run.run_id, "load")]


def _make_loop_child(dag_maker, session):
    """A Dag ``wait >> loop(step)`` whose run holds three passes of the loop."""

    @task_group
    def body():
        EmptyOperator(task_id="step")

    with dag_maker("loop_child", serialized=True):
        loop = create_loop(body, max_iterations=3)
        EmptyOperator(task_id="wait") >> loop
    loop_run = dag_maker.create_dagrun(logical_date=CLEAR_DATE)
    root = session.scalar(select(DynamicRegion).where(DynamicRegion.node_id == loop.group_id))
    for index in (2, 3):
        for task in loop.iter_tasks():
            session.add(
                TaskInstance(
                    task=task,
                    run_id=loop_run.run_id,
                    dag_version_id=loop_run.created_dag_version_id,
                    region_id=root.id,
                    region_index=index,
                )
            )
    session.flush()
    return loop_run


def test_clearing_a_target_dag_archives_later_loop_passes(dag_maker, session):
    _make_loop_child(dag_maker, session)
    (mark,) = _make_parent(dag_maker, "parent")
    _record(mark, _target("loop_child"), session)

    clear_dependent_runs(
        _find([mark], session),
        body=ClearTaskInstancesBody(dry_run=False, only_failed=False),
        dag_run_state=DagRunState.QUEUED,
        session=session,
    )

    live_later_passes = session.scalars(
        select(TaskInstance.task_id).where(
            TaskInstance.dag_id == "loop_child",
            TaskInstance.working_set.is_(True),
            TaskInstance.region_index > 1,
        )
    ).all()
    assert live_later_passes == []


def test_clear_returns_only_the_executions_it_cleared(dag_maker, session):
    @task_group
    def body():
        MockOperator.partial(task_id="step").expand(arg1=["a", "b"])

    with dag_maker("loop_child", serialized=True):
        loop = create_loop(body, max_iterations=2)
        EmptyOperator(task_id="wait") >> loop
    loop_run = dag_maker.create_dagrun(logical_date=CLEAR_DATE)
    root = session.scalar(select(DynamicRegion).where(DynamicRegion.node_id == loop.group_id))
    # A second pass whose mapped instances reuse the map indexes of the first pass.
    second_pass = DynamicRegion(
        dag_id=loop_run.dag_id,
        run_id=loop_run.run_id,
        node_id="body.step",
        parent_region_id=root.id,
        parent_region_index=2,
    )
    session.add(second_pass)
    session.flush()
    for task in loop.iter_tasks():
        mapped = task.task_id == "body.step"
        for slot in range(2) if mapped else [2]:
            session.add(
                TaskInstance(
                    task=task,
                    run_id=loop_run.run_id,
                    dag_version_id=loop_run.created_dag_version_id,
                    region_id=second_pass.id if mapped else root.id,
                    region_index=slot,
                )
            )
    session.flush()
    for ti in loop_run.get_task_instances(session=session):
        failed = ti.region_id == second_pass.id and ti.region_index == 0
        ti.state = TaskInstanceState.FAILED if failed else TaskInstanceState.SUCCESS
    session.flush()
    (mark,) = _make_parent(dag_maker, "parent")
    _record(mark, _target("loop_child"), session)

    cleared = clear_dependent_runs(
        _find([mark], session, only_failed=True),
        body=ClearTaskInstancesBody(dry_run=False, only_failed=True),
        dag_run_state=DagRunState.QUEUED,
        session=session,
    )

    assert [(ti.task_id, ti.region_id, ti.region_index) for ti in cleared] == [
        ("body.step", second_pass.id, 0)
    ]


def test_cycle_back_into_a_partly_selected_mapped_task_clears_its_other_instances(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    with dag_maker("parent"):
        MockOperator.partial(task_id="mark").expand(arg1=["a", "b"])
    parent_run = dag_maker.create_dagrun(logical_date=CLEAR_DATE)
    mark_0, mark_1 = sorted(
        (ti for ti in parent_run.get_task_instances(session=session) if ti.task_id == "mark"),
        key=lambda ti: ti.map_index,
    )
    _record(mark_0, _target("child"), session)
    _record(child_run.get_task_instance("load"), _target("parent", task_id="mark"), session)

    found = {
        (ti.dag_id, ti.task_id, ti.map_index)
        for clear in _find([mark_0], session)
        for ti in clear.task_instances
    }

    assert ("parent", "mark", mark_1.map_index) in found
    assert ("parent", "mark", mark_0.map_index) not in found


def test_follows_a_target_stored_with_the_dag_version_without_a_report(dag_maker, session):
    (child_run,) = _make_child(dag_maker, "child", CLEAR_DATE)
    with dag_maker("parent"):
        ExternalTaskMarker(task_id="mark", external_dag_id="child", external_task_id="wait")
    mark = dag_maker.create_dagrun(logical_date=CLEAR_DATE).get_task_instance("mark")

    assert _keys(_find([mark], session)) == [
        ("child", child_run.run_id, "load"),
        ("child", child_run.run_id, "wait"),
    ]


def test_a_target_stored_with_the_dag_version_needs_a_logical_date(dag_maker, session):
    with dag_maker("child"):
        EmptyOperator(task_id="wait")
    # A child run without a logical date either, which a missing date must not match.
    dag_maker.create_dagrun(run_id="undated", logical_date=None)
    with dag_maker("parent"):
        ExternalTaskMarker(task_id="mark", external_dag_id="child", external_task_id="wait")
    mark = dag_maker.create_dagrun(logical_date=None).get_task_instance("mark")

    assert _find([mark], session) == []
