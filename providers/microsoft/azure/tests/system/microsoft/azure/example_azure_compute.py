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

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.azure.operators.compute import (
    AzureVirtualMachineRestartOperator,
    AzureVirtualMachineStartOperator,
    AzureVirtualMachineStopOperator,
)
from airflow.providers.microsoft.azure.sensors.compute import (
    AzureVirtualMachineStateSensor,
)

DAG_ID = "example_azure_compute"
RESOURCE_GROUP = os.environ.get("AZURE_RESOURCE_GROUP", "airflow-test-rg")
VM_NAME = os.environ.get("AZURE_VM_NAME", "airflow-test-vm")

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "azure", "compute"],
) as dag:
    # [START howto_operator_azure_vm_start]
    start_vm = AzureVirtualMachineStartOperator(
        task_id="start_vm",
        resource_group_name=RESOURCE_GROUP,
        vm_name=VM_NAME,
        wait_for_completion=False,
    )
    # [END howto_operator_azure_vm_start]

    # [START howto_sensor_azure_vm_state]
    sense_running = AzureVirtualMachineStateSensor(
        task_id="sense_running",
        resource_group_name=RESOURCE_GROUP,
        vm_name=VM_NAME,
        target_state="running",
        deferrable=True,
        poke_interval=10,
        timeout=300,
    )
    # [END howto_sensor_azure_vm_state]

    # [START howto_operator_azure_vm_restart]
    restart_vm = AzureVirtualMachineRestartOperator(
        task_id="restart_vm",
        resource_group_name=RESOURCE_GROUP,
        vm_name=VM_NAME,
        wait_for_completion=True,
    )
    # [END howto_operator_azure_vm_restart]

    # [START howto_operator_azure_vm_stop]
    stop_vm = AzureVirtualMachineStopOperator(
        task_id="stop_vm",
        resource_group_name=RESOURCE_GROUP,
        vm_name=VM_NAME,
        wait_for_completion=False,
    )
    # [END howto_operator_azure_vm_stop]

    sense_deallocated = AzureVirtualMachineStateSensor(
        task_id="sense_deallocated",
        resource_group_name=RESOURCE_GROUP,
        vm_name=VM_NAME,
        target_state="deallocated",
        deferrable=True,
        poke_interval=10,
        timeout=300,
    )

    start_vm >> sense_running >> restart_vm >> stop_vm >> sense_deallocated

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
