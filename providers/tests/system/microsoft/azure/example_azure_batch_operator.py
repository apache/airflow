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
Example Airflow DAG that trigger a task in Azure Batch.

This DAG relies on the following OS environment variables

* POOL_ID - The Pool ID in Batch accounts.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.azure.operators.batch import AzureBatchOperator

POOL_ID = os.environ.get("POOL_ID", "example-pool")

with DAG(
    dag_id="example_azure_batch",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    doc_md=__doc__,
    tags=["example"],
) as dag:
    # [START howto_azure_batch_operator]
    azure_batch_operator = AzureBatchOperator(
        task_id="azure_batch",
        batch_pool_id=POOL_ID,
        batch_pool_vm_size="standard_d2s_v3",
        batch_job_id="example-job",
        batch_task_command_line="/bin/bash -c 'set -e; set -o pipefail; echo hello world!; wait'",
        batch_task_id="example-task",
        vm_node_agent_sku_id="batch.node.ubuntu 22.04",
        vm_publisher="Canonical",
        vm_offer="0001-com-ubuntu-server-jammy",
        vm_sku="22_04-lts-gen2",
        target_dedicated_nodes=1,
    )
    # [END howto_azure_batch_operator]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
