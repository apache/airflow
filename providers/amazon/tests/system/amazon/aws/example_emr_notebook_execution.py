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

from __future__ import annotations

from datetime import datetime

from airflow.providers.amazon.aws.operators.emr import (
    EmrStartNotebookExecutionOperator,
    EmrStopNotebookExecutionOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrNotebookExecutionSensor
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_emr_notebook"
# Externally fetched variables:
EDITOR_ID_KEY = "EDITOR_ID"
CLUSTER_ID_KEY = "CLUSTER_ID"

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(EDITOR_ID_KEY).add_variable(CLUSTER_ID_KEY).build()
)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    editor_id = test_context[EDITOR_ID_KEY]
    cluster_id = test_context[CLUSTER_ID_KEY]

    # [START howto_operator_emr_start_notebook_execution]
    start_execution = EmrStartNotebookExecutionOperator(
        task_id="start_execution",
        editor_id=editor_id,
        cluster_id=cluster_id,
        relative_path="EMR-System-Test.ipynb",
        service_role="EMR_Notebooks_DefaultRole",
    )
    # [END howto_operator_emr_start_notebook_execution]

    notebook_execution_id_1 = start_execution.output

    # [START howto_sensor_emr_notebook_execution]
    wait_for_execution_start = EmrNotebookExecutionSensor(
        task_id="wait_for_execution_start",
        notebook_execution_id=notebook_execution_id_1,
        target_states={"RUNNING"},
        poke_interval=5,
    )
    # [END howto_sensor_emr_notebook_execution]

    # [START howto_operator_emr_stop_notebook_execution]
    stop_execution = EmrStopNotebookExecutionOperator(
        task_id="stop_execution",
        notebook_execution_id=notebook_execution_id_1,
    )
    # [END howto_operator_emr_stop_notebook_execution]

    wait_for_execution_stop = EmrNotebookExecutionSensor(
        task_id="wait_for_execution_stop",
        notebook_execution_id=notebook_execution_id_1,
        target_states={"STOPPED"},
        poke_interval=5,
    )
    finish_execution = EmrStartNotebookExecutionOperator(
        task_id="finish_execution",
        editor_id=editor_id,
        cluster_id=cluster_id,
        relative_path="EMR-System-Test.ipynb",
        service_role="EMR_Notebooks_DefaultRole",
    )
    notebook_execution_id_2 = finish_execution.output
    wait_for_execution_finish = EmrNotebookExecutionSensor(
        task_id="wait_for_execution_finish",
        notebook_execution_id=notebook_execution_id_2,
        poke_interval=5,
    )

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        start_execution,
        wait_for_execution_start,
        stop_execution,
        wait_for_execution_stop,
        finish_execution,
        # TEST TEARDOWN
        wait_for_execution_finish,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
