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
Example Airflow DAG for Google Cloud Memorystore Memcached service.
"""
from __future__ import annotations

import os
from datetime import datetime

from google.protobuf.field_mask_pb2 import FieldMask

from airflow import models
from airflow.providers.google.cloud.operators.cloud_memorystore import (
    CloudMemorystoreMemcachedApplyParametersOperator,
    CloudMemorystoreMemcachedCreateInstanceOperator,
    CloudMemorystoreMemcachedDeleteInstanceOperator,
    CloudMemorystoreMemcachedGetInstanceOperator,
    CloudMemorystoreMemcachedListInstancesOperator,
    CloudMemorystoreMemcachedUpdateInstanceOperator,
    CloudMemorystoreMemcachedUpdateParametersOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "google_project_id")

DAG_ID = "cloud_memorystore_memcached"

MEMORYSTORE_MEMCACHED_INSTANCE_NAME = f"memcached-{ENV_ID}-1"
LOCATION = "europe-north1"

# [START howto_operator_memcached_instance]
MEMCACHED_INSTANCE = {
    "name": "",
    "node_count": 1,
    "node_config": {"cpu_count": 1, "memory_size_mb": 1024},
    "zones": [LOCATION + "-a"],
}
# [END howto_operator_memcached_instance]


with models.DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_create_instance_memcached]
    create_memcached_instance = CloudMemorystoreMemcachedCreateInstanceOperator(
        task_id="create-instance",
        location=LOCATION,
        instance_id=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        instance=MEMCACHED_INSTANCE,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_create_instance_memcached]

    # [START howto_operator_delete_instance_memcached]
    delete_memcached_instance = CloudMemorystoreMemcachedDeleteInstanceOperator(
        task_id="delete-instance",
        location=LOCATION,
        instance=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_delete_instance_memcached]
    delete_memcached_instance.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_get_instance_memcached]
    get_memcached_instance = CloudMemorystoreMemcachedGetInstanceOperator(
        task_id="get-instance",
        location=LOCATION,
        instance=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_get_instance_memcached]

    # [START howto_operator_list_instances_memcached]
    list_memcached_instances = CloudMemorystoreMemcachedListInstancesOperator(
        task_id="list-instances", location="-", project_id=PROJECT_ID
    )
    # [END howto_operator_list_instances_memcached]

    # [START howto_operator_update_instance_memcached]
    update_memcached_instance = CloudMemorystoreMemcachedUpdateInstanceOperator(
        task_id="update-instance",
        location=LOCATION,
        instance_id=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
        update_mask=FieldMask(paths=["node_count"]),
        instance={"node_count": 2},  # 2
    )
    # [END howto_operator_update_instance_memcached]

    # [START howto_operator_update_and_apply_parameters_memcached]
    update_memcached_parameters = CloudMemorystoreMemcachedUpdateParametersOperator(
        task_id="update-parameters",
        location=LOCATION,
        instance_id=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
        update_mask={"paths": ["params"]},
        parameters={"params": {"protocol": "ascii", "hash_algorithm": "jenkins"}},
    )

    apply_memcached_parameters = CloudMemorystoreMemcachedApplyParametersOperator(
        task_id="apply-parameters",
        location=LOCATION,
        instance_id=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
        node_ids=["node-a-1"],
        apply_all=False,
    )
    # [END howto_operator_update_and_apply_parameters_memcached]

    (
        create_memcached_instance
        >> get_memcached_instance
        >> list_memcached_instances
        >> update_memcached_instance
        >> update_memcached_parameters
        >> apply_memcached_parameters
        >> delete_memcached_instance
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
