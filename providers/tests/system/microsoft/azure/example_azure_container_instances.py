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
This is an example dag for using the AzureContainerInstancesOperator.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.microsoft.azure.operators.container_instances import (
    AzureContainerInstancesOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "aci_example"
CONTAINER_REGISTRY_SERVER = os.environ.get("CONTAINER_REGISTRY_SERVER")
AZURE_VOLUME_SHARE_NAME = os.environ.get("AZURE_VOLUME_SHARE_NAME")
AZURE_STORAGE_ACOOUNT = os.environ.get("AZURE_STORAGE_ACOOUNT")

with DAG(
    dag_id=DAG_ID,
    default_args={"retries": 1},
    schedule=timedelta(days=1),
    start_date=datetime(2018, 11, 1),
    catchup=False,
    tags=["example"],
) as dag:
    t1 = AzureContainerInstancesOperator(
        ci_conn_id="azure_default",
        registry_conn_id=None,
        resource_group="resource-group",
        name="aci-test-{{ ds }}",
        image="hello-world",
        region="WestUS2",
        environment_variables={},
        volumes=[],
        memory_in_gb=4.0,
        cpu=1.0,
        task_id="start_container",
    )

    t2 = AzureContainerInstancesOperator(
        ci_conn_id="azure_default",
        registry_conn_id=None,
        resource_group="resource-group",
        name="aci-test-{{ ds }}",
        image=f"{CONTAINER_REGISTRY_SERVER}:hello-world",
        region="WestUS2",
        environment_variables={},
        volumes=[],
        memory_in_gb=4.0,
        cpu=1.0,
        task_id="start_container_with_custom_container_registry",
    )

    t3 = AzureContainerInstancesOperator(
        ci_conn_id="azure_default",
        registry_conn_id=None,
        resource_group="resource-group",
        name="aci-test-{{ ds }}",
        image="hello-world",
        region="WestUS2",
        environment_variables={},
        volumes=[
            (
                "azure_container_volume_default",
                AZURE_STORAGE_ACOOUNT,
                AZURE_VOLUME_SHARE_NAME,
                "/home",
                True,
            )
        ],
        memory_in_gb=4.0,
        cpu=1.0,
        task_id="start_container_with_azure_container_volume",
    )
from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
