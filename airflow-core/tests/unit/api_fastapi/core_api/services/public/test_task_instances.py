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

import pytest

from airflow.api_fastapi.core_api.datamodels.common import BulkActionResponse, BulkBody
from airflow.api_fastapi.core_api.datamodels.task_instances import BulkTaskInstanceBody
from airflow.api_fastapi.core_api.services.public.task_instances import BulkTaskInstanceService
from airflow.providers.standard.operators.bash import BashOperator

from tests_common.test_utils.db import (
    clear_db_runs,
)

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
        def get_id(self) -> str:
            return "test_user"

        def get_name(self) -> str:
            return "test_user"

    @pytest.mark.parametrize(
        "dags_to_create, dag_id, dag_run_id, task_keys, expected_matched_keys, expected_not_found_keys",
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
        def get_id(self) -> str:
            return "test_user"

        def get_name(self) -> str:
            return "test_user"

    @pytest.mark.parametrize(
        "entity, expected_dag_id, expected_dag_run_id, expected_task_id, expected_map_index",
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
        def get_id(self) -> str:
            return "test_user"

        def get_name(self) -> str:
            return "test_user"

    @pytest.mark.parametrize(
        "entities, service_dag_id, service_dag_run_id, expected_specific_keys, expected_all_keys, expected_error_count",
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
        specific_map_index_task_keys, all_map_index_task_keys = service._categorize_entities(
            entities, results
        )

        assert specific_map_index_task_keys == expected_specific_keys
        assert all_map_index_task_keys == expected_all_keys
        assert len(results.errors) == expected_error_count
