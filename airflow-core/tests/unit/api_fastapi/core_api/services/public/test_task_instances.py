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

from airflow.api_fastapi.core_api.datamodels.common import BulkBody
from airflow.api_fastapi.core_api.services.public.task_instances import BulkTaskInstanceService
from airflow.providers.standard.operators.bash import BashOperator

from tests_common.test_utils.db import (
    clear_db_runs,
)

pytestmark = pytest.mark.db_test
DAG_ID = "TEST_DAG"
DAG_RUN_ID = "TEST_DAG_RUN"
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
        (
            "task_keys",
            "expected_matched_keys",
            "expected_not_found_keys",
            "expected_matched_count",
            "expected_not_found_count",
        ),
        [
            pytest.param(
                {(TASK_ID_1, -1), (TASK_ID_2, -1)},
                {(TASK_ID_1, -1), (TASK_ID_2, -1)},
                set(),
                2,
                0,
                id="all_found",
            ),
            pytest.param(
                {("nonexistent_task", -1), ("nonexistent_task", 0)},
                set(),
                {("nonexistent_task", -1), ("nonexistent_task", 0)},
                0,
                2,
                id="none_found",
            ),
            pytest.param(
                {(TASK_ID_1, -1), (TASK_ID_1, 0)},
                {(TASK_ID_1, -1)},
                {(TASK_ID_1, 0)},
                1,
                1,
                id="mixed_found_and_not_found",
            ),
            pytest.param(set(), set(), set(), 0, 0, id="empty_input"),
        ],
    )
    def test_categorize_task_instances(
        self,
        session,
        dag_maker,
        task_keys,
        expected_matched_keys,
        expected_not_found_keys,
        expected_matched_count,
        expected_not_found_count,
    ):
        """Test categorize_task_instances with various scenarios."""
        with dag_maker(dag_id=DAG_ID, session=session):
            BashOperator(task_id=TASK_ID_1, bash_command="echo 1")
            BashOperator(task_id=TASK_ID_2, bash_command="echo 2")

        dag_maker.create_dagrun(run_id=DAG_RUN_ID)

        session.commit()

        user = self.MockUser()
        bulk_request = BulkBody(actions=[])
        service = BulkTaskInstanceService(
            session=session,
            request=bulk_request,
            dag_id=DAG_ID,
            dag_run_id=DAG_RUN_ID,
            dag_bag=dag_maker.dagbag,
            user=user,
        )

        _, matched_task_keys, not_found_task_keys = service.categorize_task_instances(task_keys)

        assert len(matched_task_keys) == expected_matched_count
        assert len(not_found_task_keys) == expected_not_found_count
        assert matched_task_keys == expected_matched_keys
        assert not_found_task_keys == expected_not_found_keys
