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
from sqlalchemy import select

from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task, task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.sdk.definitions.dag import _run_task
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.state import DagRunState, TaskInstanceState

pytestmark = pytest.mark.db_test


def test_execution_lists_live_work_and_selects_archived_tries(test_client, dag_maker, session):
    with dag_maker(serialized=True):
        EmptyOperator(task_id="task")
    run = dag_maker.create_dagrun()
    ti = run.task_instances[0]
    ti.state = TaskInstanceState.SUCCESS
    ti.try_number = 1
    archived_id = ti.id
    clear_task_instances([ti], session=session)
    session.commit()
    live = session.scalar(select(TaskInstance).where(TaskInstance.working_set.is_(True)))
    assert live.id != archived_id
    url = f"/dags/{run.dag_id}/dagRuns/{run.run_id}"

    response = test_client.get(f"{url}/execution")
    assert response.status_code == 200, response.text
    assert [item["id"] for item in response.json()["task_instances"]] == [str(live.id)]
    response = test_client.get(f"{url}/execution", params={"try_number": 1})
    assert [item["id"] for item in response.json()["task_instances"]] == [str(archived_id)]

    tries_url = f"{url}/taskInstances/task/tries"
    tries = test_client.get(tries_url)
    assert tries.status_code == 200, tries.text
    assert {item["id"] for item in tries.json()["task_instances"]} == {str(live.id), str(archived_id)}
    archived = test_client.get(f"{tries_url}/1")
    assert archived.status_code == 200, archived.text
    assert archived.json()["id"] == str(archived_id)

    with mock.patch("airflow.api_fastapi.core_api.routes.public.log.TaskLogReader", autospec=True) as reader:
        reader.return_value.supports_external_link = True
        reader.return_value.log_handler = mock.create_autospec(ExternalLoggingMixin, instance=True)
        reader.return_value.log_handler.get_external_log_url.return_value = "https://logs.example/archived"
        response = test_client.get(f"{url}/taskInstances/task/externalLogUrl/1")
        assert response.status_code == 200, response.text
        assert reader.return_value.log_handler.get_external_log_url.call_args.args[0].id == archived_id


@pytest.mark.parametrize("retain_later", [False, True])
def test_loop_repeated_selective_clears_archive_only_replaced_task_instances(
    test_client, dag_maker, session, retain_later
):
    @task
    def prepare():
        return "ready"

    @task
    def work(*, loop):
        return (loop.previous or 0) + 1

    @task_group
    def body():
        work()

    @task
    def consume():
        return "finished"

    with dag_maker(serialized=False) as dag:
        prepare() >> body.loop(max_iterations=3) >> consume()
    run = dag.test()
    run_url = f"/dags/{run.dag_id}/dagRuns/{run.run_id}"
    clear_url = f"/dags/{run.dag_id}/clearTaskInstances"

    def live_executions():
        response = test_client.get(f"{run_url}/execution")
        assert response.status_code == 200, response.text
        return response.json()["task_instances"]

    def finish_run():
        nonlocal run
        session.expire_all()
        run = session.get(DagRun, run.id, populate_existing=True)
        run.dag = DBDagBag().get_dag(run.created_dag_version_id, session=session)
        run.state = DagRunState.RUNNING
        session.commit()
        for _ in range(20):
            session.expire_all()
            runnable, _ = run.update_state(session=session)
            if run.state != DagRunState.RUNNING:
                assert run.state == DagRunState.SUCCESS
                return
            assert runnable, "Cleared loop stopped making progress"
            for ti in runnable:
                ti.try_number = max(ti.try_number, 1)
                ti.state = TaskInstanceState.SCHEDULED
            session.commit()
            for ti in runnable:
                _run_task(ti=ti, task=dag.get_task(ti.task_id))
        pytest.fail("Cleared loop did not finish")

    initial = live_executions()
    prepared = next(ti for ti in initial if ti["task_id"] == "prepare")
    archived_ids = set()
    for index, keep_future in [(1, retain_later), (0, False)]:
        before = live_executions()
        selected = next(ti for ti in before if ti["task_id"] == "body.work" and ti["region_index"] == index)
        response = test_client.post(
            clear_url,
            json={
                "dag_run_id": run.run_id,
                "task_instance_ids": [selected["id"]],
                "dry_run": False,
                "only_failed": False,
                "include_downstream": True,
                "include_later_loop_iterations": not keep_future,
            },
        )
        assert response.status_code == 200, response.text
        finish_run()
        after = live_executions()
        assert all(ti["state"] == "success" for ti in after)
        assert len(after) == len(initial)
        assert next(ti for ti in after if ti["task_id"] == "prepare")["id"] == prepared["id"]
        old_suffix = {ti["id"] for ti in before if ti["region_index"] > index}
        live_ids = {ti["id"] for ti in after}
        assert old_suffix <= live_ids if keep_future else old_suffix.isdisjoint(live_ids)
        assert selected["id"] not in live_ids
        archived_ids.add(selected["id"])
        for ti in after:
            if ti["task_id"] != "body.work":
                continue
            value = test_client.get(
                f"{run_url}/taskInstances/body.work/xcomEntries/return_value",
                params={"region_id": ti["region_id"], "region_index": ti["region_index"]},
            )
            assert value.status_code == 200, value.text
            assert value.json()["value"] == ti["region_index"] + 1

    archived = session.scalars(
        select(TaskInstance)
        .where(
            TaskInstance.dag_id == run.dag_id,
            TaskInstance.run_id == run.run_id,
            TaskInstance.working_set.is_(None),
        )
        .execution_options(include_all_attempts=True)
    ).all()
    assert archived_ids <= {str(ti.id) for ti in archived}


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
