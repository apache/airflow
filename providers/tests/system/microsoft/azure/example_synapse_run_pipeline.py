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

import os
from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.synapse import AzureSynapseRunPipelineOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

with DAG(
    dag_id="example_synapse_run_pipeline",
    start_date=datetime(2021, 8, 13),
    schedule="@daily",
    catchup=False,
    tags=["synapse", "example"],
) as dag:
    begin = EmptyOperator(task_id="begin")
    # [START howto_operator_azure_synapse_run_pipeline]
    run_pipeline1 = AzureSynapseRunPipelineOperator(
        task_id="run_pipeline1",
        azure_synapse_conn_id="azure_synapse_connection",
        pipeline_name="Pipeline 1",
        azure_synapse_workspace_dev_endpoint="azure_synapse_workspace_dev_endpoint",
    )
    # [END howto_operator_azure_synapse_run_pipeline]
    begin >> run_pipeline1

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
