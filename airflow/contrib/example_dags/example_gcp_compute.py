# -*- coding: utf-8 -*-
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

* PROJECT_ID - Google Cloud Platform project where the Compute Engine instance exists.
* ZONE - Google Cloud Platform zone where the instance exists.
* INSTANCE - Name of the Compute Engine instance.
* SHORT_MACHINE_TYPE_NAME - Machine type resource name to set, e.g. 'n1-standard-1'.
    See https://cloud.google.com/compute/docs/machine-types
"""
import os
import datetime

import airflow
from airflow import models
from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator, \
    GceInstanceStopOperator, GceSetMachineTypeOperator

# [START howto_operator_gce_args_common]
PROJECT_ID = os.environ.get('PROJECT_ID', 'example-project')
ZONE = os.environ.get('ZONE', 'europe-west1-b')
INSTANCE = os.environ.get('INSTANCE', 'testinstance')
# [END howto_operator_gce_args_common]

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}

# [START howto_operator_gce_args_set_machine_type]
SHORT_MACHINE_TYPE_NAME = os.environ.get('SHORT_MACHINE_TYPE_NAME', 'n1-standard-1')
SET_MACHINE_TYPE_BODY = {
    'machineType': 'zones/{}/machineTypes/{}'.format(ZONE, SHORT_MACHINE_TYPE_NAME)
}
# [END howto_operator_gce_args_set_machine_type]


with models.DAG(
    'example_gcp_compute',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    # [START howto_operator_gce_start]
    gce_instance_start = GceInstanceStartOperator(
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=INSTANCE,
        task_id='gcp_compute_start_task'
    )
    # [END howto_operator_gce_start]
    # Duplicate start for idempotence testing
    gce_instance_start2 = GceInstanceStartOperator(
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=INSTANCE,
        task_id='gcp_compute_start_task2'
    )
    # [START howto_operator_gce_stop]
    gce_instance_stop = GceInstanceStopOperator(
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=INSTANCE,
        task_id='gcp_compute_stop_task'
    )
    # [END howto_operator_gce_stop]
    # Duplicate stop for idempotence testing
    gce_instance_stop2 = GceInstanceStopOperator(
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=INSTANCE,
        task_id='gcp_compute_stop_task2'
    )
    # [START howto_operator_gce_set_machine_type]
    gce_set_machine_type = GceSetMachineTypeOperator(
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=INSTANCE,
        body=SET_MACHINE_TYPE_BODY,
        task_id='gcp_compute_set_machine_type'
    )
    # [END howto_operator_gce_set_machine_type]
    # Duplicate set machine type for idempotence testing
    gce_set_machine_type2 = GceSetMachineTypeOperator(
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=INSTANCE,
        body=SET_MACHINE_TYPE_BODY,
        task_id='gcp_compute_set_machine_type2'
    )

    gce_instance_start >> gce_instance_start2 >> gce_instance_stop >> \
        gce_instance_stop2 >> gce_set_machine_type >> gce_set_machine_type2
