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

from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator

pytestmark = pytest.mark.db_test


class TestXComsGetEndpoint:
    @pytest.mark.parametrize(
        ("offset", "expected_status", "expected_json"),
        [
            pytest.param(
                -4,
                404,
                {
                    "detail": {
                        "reason": "not_found",
                        "message": (
                            "XCom with key='xcom_1' offset=-4 not found "
                            "for task 'task' in DAG run 'runid' of 'dag'"
                        ),
                    },
                },
                id="-4",
            ),
            pytest.param(-3, 200, {"key": "xcom_1", "value": "f"}, id="-3"),
            pytest.param(-2, 200, {"key": "xcom_1", "value": "o"}, id="-2"),
            pytest.param(-1, 200, {"key": "xcom_1", "value": "b"}, id="-1"),
            pytest.param(0, 200, {"key": "xcom_1", "value": "f"}, id="0"),
            pytest.param(1, 200, {"key": "xcom_1", "value": "o"}, id="1"),
            pytest.param(2, 200, {"key": "xcom_1", "value": "b"}, id="2"),
            pytest.param(
                3,
                404,
                {
                    "detail": {
                        "reason": "not_found",
                        "message": (
                            "XCom with key='xcom_1' offset=3 not found "
                            "for task 'task' in DAG run 'runid' of 'dag'"
                        ),
                    },
                },
                id="3",
            ),
        ],
    )
    def test_xcom_get_with_offset(
        self,
        client,
        dag_maker,
        session,
        offset,
        expected_status,
        expected_json,
    ):
        xcom_values = ["f", None, "o", "b"]

        class MyOperator(EmptyOperator):
            def __init__(self, *, x, **kwargs):
                super().__init__(**kwargs)
                self.x = x

        with dag_maker(dag_id="dag"):
            MyOperator.partial(task_id="task").expand(x=xcom_values)

        dag_run = dag_maker.create_dagrun(run_id="runid")
        tis = {ti.map_index: ti for ti in dag_run.task_instances}
        for map_index, db_value in enumerate(xcom_values):
            if db_value is None:  # We don't put None to XCom.
                continue
            ti = tis[map_index]
            x = XComModel(
                key="xcom_1",
                value=db_value,
                dag_run_id=ti.dag_run.id,
                run_id=ti.run_id,
                task_id=ti.task_id,
                dag_id=ti.dag_id,
                map_index=map_index,
            )
            session.add(x)
        session.commit()

        response = client.get(f"/execution/xcoms/dag/runid/task/xcom_1?offset={offset}")
        assert response.status_code == expected_status
        assert response.json() == expected_json

    def test_xcom_batch_get(self, client, dag_maker, session):
        with dag_maker(dag_id="dag"):
            EmptyOperator(task_id="task_1")
            EmptyOperator(task_id="task_2")
            EmptyOperator(task_id="task_3")

        dag_run = dag_maker.create_dagrun(run_id="runid")
        tis = {ti.task_id: ti for ti in dag_run.task_instances}
        for task_id, value in {"task_1": "one", "task_2": "two"}.items():
            ti = tis[task_id]
            session.add(
                XComModel(
                    key="return_value",
                    value=value,
                    dag_run_id=ti.dag_run.id,
                    run_id=ti.run_id,
                    task_id=ti.task_id,
                    dag_id=ti.dag_id,
                    map_index=-1,
                )
            )
        session.commit()

        response = client.post(
            "/execution/xcoms/batch",
            json={
                "dag_id": "dag",
                "run_id": "runid",
                "key": "return_value",
                "task_ids": ["task_2", "task_3", "task_1"],
            },
        )

        assert response.status_code == 200
        assert response.json() == {
            "key": "return_value",
            "values": [
                {"task_id": "task_2", "value": "two"},
                {"task_id": "task_3", "value": None},
                {"task_id": "task_1", "value": "one"},
            ],
        }
