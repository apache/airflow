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
Example Airflow DAG that starts, stops and sets the machine type of a Google Compute
Engine instance.

"""

import os
from datetime import datetime

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineSetMachineTypeOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.utils.trigger_rule import TriggerRule


# [START howto_operator_gce_args_common]
ENV_ID = os.environ.get('SYSTEM_TESTS_ENV_ID')
PROJECT_ID = os.environ.get('SYSTEM_TESTS_GCP_PROJECT')

GCE_INSTANCE = 'instance-1'
SHORT_MACHINE_TYPE_NAME = 'n1-standard-1'

DAG_ID = 'cloud_compute'
LOCATION = 'europe-west1-b'
# [END howto_operator_gce_args_common]


with models.DAG(
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_gce_start]
    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=PROJECT_ID,
        zone=LOCATION,
        resource_id=GCE_INSTANCE,
        task_id='gcp_compute_start_task'
    )
    # [END howto_operator_gce_start]

    # Duplicate start for idempotence testing
    # [START howto_operator_gce_start_no_project_id]
    gce_instance_start2 = ComputeEngineStartInstanceOperator(
        task_id='gcp_compute_start_task2',
        zone=LOCATION,
        resource_id=GCE_INSTANCE,
    )
    # [END howto_operator_gce_start_no_project_id]

    # [START howto_operator_gce_stop]
    gce_instance_stop = ComputeEngineStopInstanceOperator(
        task_id='gcp_compute_stop_task',
        project_id=PROJECT_ID,
        zone=LOCATION,
        resource_id=GCE_INSTANCE,
    )
    # [END howto_operator_gce_stop]
    gce_instance_stop.trigger_rule = TriggerRule.ALL_DONE

    # Duplicate stop for idempotence testing
    # [START howto_operator_gce_stop_no_project_id]
    gce_instance_stop2 = ComputeEngineStopInstanceOperator(
        task_id='gcp_compute_stop_task2',
        zone=LOCATION,
        resource_id=GCE_INSTANCE,
    )
    # [END howto_operator_gce_stop_no_project_id]
    gce_instance_stop2.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_gce_set_machine_type]
    gce_set_machine_type = ComputeEngineSetMachineTypeOperator(
        task_id='gcp_compute_set_machine_type',
        project_id=PROJECT_ID,
        zone=LOCATION,
        resource_id=GCE_INSTANCE,
        body={'machineType': f'zones/{LOCATION}/machineTypes/{SHORT_MACHINE_TYPE_NAME}'},
    )
    # [END howto_operator_gce_set_machine_type]

    # Duplicate set machine type for idempotence testing
    # [START howto_operator_gce_set_machine_type_no_project_id]
    gce_set_machine_type2 = ComputeEngineSetMachineTypeOperator(
        zone=LOCATION,
        resource_id=GCE_INSTANCE,
        body={'machineType': f'zones/{LOCATION}/machineTypes/{SHORT_MACHINE_TYPE_NAME}'},
        task_id='gcp_compute_set_machine_type2',
    )
    # [END howto_operator_gce_set_machine_type_no_project_id]

    chain(
        gce_instance_start,
        gce_instance_start2,
        gce_instance_stop,
        gce_instance_stop2,
        gce_set_machine_type,
        gce_set_machine_type2,
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)




