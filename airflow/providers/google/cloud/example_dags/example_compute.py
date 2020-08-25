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

This DAG relies on the following OS environment variables

* GCP_PROJECT_ID - Google Cloud Platform project where the Compute Engine instance exists.
* GCE_ZONE - Google Cloud Platform zone where the instance exists.
* GCE_INSTANCE - Name of the Compute Engine instance.
* GCE_SHORT_MACHINE_TYPE_NAME - Machine type resource name to set, e.g. 'n1-standard-1'.
    See https://cloud.google.com/compute/docs/machine-types
"""

import os

from airflow import models
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineSetMachineTypeOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.utils.dates import days_ago

# [START howto_operator_gce_args_common]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCE_ZONE = os.environ.get('GCE_ZONE', 'europe-west1-b')
GCE_INSTANCE = os.environ.get('GCE_INSTANCE', 'testinstance')
# [END howto_operator_gce_args_common]


GCE_SHORT_MACHINE_TYPE_NAME = os.environ.get('GCE_SHORT_MACHINE_TYPE_NAME', 'n1-standard-1')


with models.DAG(
    'example_gcp_compute',
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    # [START howto_operator_gce_start]
    gce_instance_start = ComputeEngineStartInstanceOperator(
        project_id=GCP_PROJECT_ID, zone=GCE_ZONE, resource_id=GCE_INSTANCE, task_id='gcp_compute_start_task'
    )
    # [END howto_operator_gce_start]
    # Duplicate start for idempotence testing
    # [START howto_operator_gce_start_no_project_id]
    gce_instance_start2 = ComputeEngineStartInstanceOperator(
        zone=GCE_ZONE, resource_id=GCE_INSTANCE, task_id='gcp_compute_start_task2'
    )
    # [END howto_operator_gce_start_no_project_id]
    # [START howto_operator_gce_stop]
    gce_instance_stop = ComputeEngineStopInstanceOperator(
        project_id=GCP_PROJECT_ID, zone=GCE_ZONE, resource_id=GCE_INSTANCE, task_id='gcp_compute_stop_task'
    )
    # [END howto_operator_gce_stop]
    # Duplicate stop for idempotence testing
    # [START howto_operator_gce_stop_no_project_id]
    gce_instance_stop2 = ComputeEngineStopInstanceOperator(
        zone=GCE_ZONE, resource_id=GCE_INSTANCE, task_id='gcp_compute_stop_task2'
    )
    # [END howto_operator_gce_stop_no_project_id]
    # [START howto_operator_gce_set_machine_type]
    gce_set_machine_type = ComputeEngineSetMachineTypeOperator(
        project_id=GCP_PROJECT_ID,
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        body={'machineType': 'zones/{}/machineTypes/{}'.format(GCE_ZONE, GCE_SHORT_MACHINE_TYPE_NAME)},
        task_id='gcp_compute_set_machine_type',
    )
    # [END howto_operator_gce_set_machine_type]
    # Duplicate set machine type for idempotence testing
    # [START howto_operator_gce_set_machine_type_no_project_id]
    gce_set_machine_type2 = ComputeEngineSetMachineTypeOperator(
        zone=GCE_ZONE,
        resource_id=GCE_INSTANCE,
        body={'machineType': 'zones/{}/machineTypes/{}'.format(GCE_ZONE, GCE_SHORT_MACHINE_TYPE_NAME)},
        task_id='gcp_compute_set_machine_type2',
    )
    # [END howto_operator_gce_set_machine_type_no_project_id]

    gce_instance_start >> gce_instance_start2 >> gce_instance_stop >> gce_instance_stop2
    gce_instance_stop2 >> gce_set_machine_type >> gce_set_machine_type2
