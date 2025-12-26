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
"""
Example Airflow DAG that show how to use various Looker
operators to submit PDT materialization job and manage it.
"""

from __future__ import annotations

from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.looker import LookerStartPdtBuildOperator
from airflow.providers.google.cloud.sensors.looker import LookerCheckPdtBuildSensor

DAG_ID = "gcp_looker"
LOOKER_CONNECTION_ID = "your_airflow_connection_for_looker"
LOOKER_MODEL = "your_lookml_model"
LOOKER_VIEW = "your_lookml_view"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "looker"],
) as dag:
    # [START cloud_looker_async_start_pdt_sensor]
    start_pdt_task_async = LookerStartPdtBuildOperator(
        task_id="start_pdt_task_async",
        looker_conn_id=LOOKER_CONNECTION_ID,
        model=LOOKER_MODEL,
        view=LOOKER_VIEW,
        asynchronous=True,
    )

    check_pdt_task_async_sensor = LookerCheckPdtBuildSensor(
        task_id="check_pdt_task_async_sensor",
        looker_conn_id=LOOKER_CONNECTION_ID,
        materialization_id=start_pdt_task_async.output,
        poke_interval=10,
    )
    # [END cloud_looker_async_start_pdt_sensor]

    # [START how_to_cloud_looker_start_pdt_build_operator]
    build_pdt_task = LookerStartPdtBuildOperator(
        task_id="build_pdt_task",
        looker_conn_id=LOOKER_CONNECTION_ID,
        model=LOOKER_MODEL,
        view=LOOKER_VIEW,
    )
    # [END how_to_cloud_looker_start_pdt_build_operator]

    start_pdt_task_async >> check_pdt_task_async_sensor

    build_pdt_task

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
