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

from airflow.models import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


class TestDagState:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize(
        "state, expected",
        [
            (True, True),
            (False, False),
            (None, False),
        ],
    )
    def test_dag_is_paused(self, state, expected, client, session, dag_maker):
        """Test DagState is active or paused"""

        dag_id = "test_dag_is_paused"

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")

        session.query(DagModel).filter(DagModel.dag_id == dag_id).update({"is_paused": state})

        session.commit()

        response = client.get(
            f"/execution/dags/{dag_id}/state",
        )

        assert response.status_code == 200
        assert response.json() == {"is_paused": expected}

    def test_dag_not_found(self, client, session, dag_maker):
        """Test Dag not found"""

        dag_id = "test_dag_is_paused"

        response = client.get(
            f"/execution/dags/{dag_id}/state",
        )

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "The Dag with dag_id: `test_dag_is_paused` was not found",
                "reason": "not_found",
            }
        }
