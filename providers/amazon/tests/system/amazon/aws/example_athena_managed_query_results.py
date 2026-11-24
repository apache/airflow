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

from datetime import datetime

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_athena_managed_query_results"

QUERY = "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')) AS people (id, name) ORDER BY id"


@task
def create_workgroup(workgroup_name: str) -> str:
    AthenaHook().get_conn().create_work_group(
        Name=workgroup_name,
        Configuration={
            "ManagedQueryResultsConfiguration": {"Enabled": True},
            "EnforceWorkGroupConfiguration": True,
        },
    )
    return workgroup_name


# [START howto_athena_managed_query_results_read]
@task
def read_results(query_execution_id: str) -> list[list[str | None]]:
    pages = AthenaHook().get_query_results_paginator(query_execution_id=query_execution_id)
    if pages is None:
        raise ValueError(f"No results for query execution id {query_execution_id}")
    rows = pages.build_full_result()["ResultSet"]["Rows"]
    return [[column.get("VarCharValue") for column in row["Data"]] for row in rows]


# [END howto_athena_managed_query_results_read]


@task
def check_results(rows: list[list[str | None]]):
    expected = [["id", "name"], ["1", "Alice"], ["2", "Bob"], ["3", "Charlie"]]
    if rows != expected:
        raise ValueError(f"Unexpected query results: {rows}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_workgroup(workgroup_name: str | None):
    # The name is None when create_workgroup did not create the workgroup, for example because a
    # workgroup with that name already exists. That workgroup is not this test's to delete.
    if workgroup_name is None:
        return
    AthenaHook().get_conn().delete_work_group(WorkGroup=workgroup_name, RecursiveDeleteOption=True)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    workgroup_name = f"{env_id}-managed-results"
    created_workgroup = create_workgroup(workgroup_name)

    # [START howto_operator_athena_managed_query_results]
    run_query = AthenaOperator(
        task_id="run_query",
        query=QUERY,
        workgroup=workgroup_name,
    )
    # [END howto_operator_athena_managed_query_results]
    run_query.sleep_time = 1

    await_query = AthenaSensor(
        task_id="await_query",
        query_execution_id=run_query.output,
    )

    results = read_results(run_query.output)

    chain(
        # TEST SETUP
        test_context,
        created_workgroup,
        # TEST BODY
        run_query,
        await_query,
        results,
        check_results(results),
        # TEST TEARDOWN
        delete_workgroup(created_workgroup),
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the Dag
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
