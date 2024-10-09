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

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook
from airflow.providers.amazon.aws.operators.neptune import (
    NeptuneStartDbClusterOperator,
    NeptuneStopDbClusterOperator,
)

from providers.tests.system.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_neptune"

sys_test_context_task = SystemTestContextBuilder().build()


@task
def create_cluster(cluster_id):
    hook = NeptuneHook()
    hook.conn.create_db_cluster(DBClusterIdentifier=cluster_id, Engine="neptune", DeletionProtection=False)
    hook.wait_for_cluster_availability(cluster_id=cluster_id)


@task
def delete_cluster(cluster_id):
    hook = NeptuneHook()
    hook.conn.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()

    env_id = test_context["ENV_ID"]
    cluster_id = f"{env_id}-cluster"

    # [START howto_operator_start_neptune_cluster]
    start_cluster = NeptuneStartDbClusterOperator(task_id="start_task", db_cluster_id=cluster_id)
    # [END howto_operator_start_neptune_cluster]

    # [START howto_operator_stop_neptune_cluster]
    stop_cluster = NeptuneStopDbClusterOperator(task_id="stop_task", db_cluster_id=cluster_id)
    # [END howto_operator_stop_neptune_cluster]

    chain(
        # TEST SETUP
        test_context,
        create_cluster(cluster_id),
        # TEST BODY
        start_cluster,
        stop_cluster,
        # TEST TEARDOWN
        delete_cluster(cluster_id),
    )
    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
