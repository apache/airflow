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
from sqlalchemy import select

from airflow.models.dynamic_region import DynamicRegion
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task_group
from airflow.sdk.definitions._internal.loop import create_loop

pytestmark = pytest.mark.db_test


def test_execution_requires_an_existing_dag_run(test_client):
    assert test_client.get("/dags/missing/dagRuns/missing/execution").status_code == 404


def test_execution_projects_public_indexes_and_pages_distinct_loop_passes(test_client, dag_maker, session):
    @task_group
    def body():
        EmptyOperator(task_id="work")

    with dag_maker(serialized=True) as dag:
        loop = create_loop(body, max_iterations=4)
        PythonOperator.partial(task_id="mapped", python_callable=list).expand(op_kwargs=[{}, {}])
    run = dag_maker.create_dagrun()
    region = session.scalar(select(DynamicRegion).where(DynamicRegion.node_id == loop.group_id))
    later = TaskInstance(
        task=dag.get_task("body.work"),
        run_id=run.run_id,
        dag_version_id=run.created_dag_version_id,
        region_id=region.id,
        region_index=2,
    )
    session.add(later)
    session.commit()
    url = f"/dags/{run.dag_id}/dagRuns/{run.run_id}/execution"

    response = test_client.get(url, params={"task_id": "body.work", "limit": 1, "offset": 1})
    assert response.status_code == 200, response.text
    assert response.json()["total_entries"] == 2
    assert len(response.json()["task_instances"]) == 1
    assert response.json()["task_instances"][0]["id"] == str(later.id)
    assert response.json()["task_instances"][0]["region_index"] == 2
    assert response.json()["task_instances"][0]["map_index"] == -1
    assert [r["id"] for r in response.json()["regions"]] == [str(region.id)]
    mapped = test_client.get(url, params={"task_id": "mapped"})
    assert {ti["map_index"] for ti in mapped.json()["task_instances"]} == {0, 1}
    assert test_client.get(url, params={"region_index": 2}).status_code == 400
    assert test_client.get(url, params={"offset": -1}).status_code == 422
