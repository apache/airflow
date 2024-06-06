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
Example Airflow DAG that uses Google Cloud Run Service Operators.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.run_v2 import Service
from google.cloud.run_v2.types import k8s_min

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateServiceOperator,
)

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "example_cloud_run_serivce"

region = "us-central1"
service_name_prefix = "cloudrun-system-test-service"
service1_name = f"{service_name_prefix}1"
service2_name = f"{service_name_prefix}2"
service3_name = f"{service_name_prefix}3"

create1_task_name = "create-service1"


def _assert_created_services_xcom(ti):
    service1_dicts = ti.xcom_pull(task_ids=[create1_task_name], key="return_value")
    assert service1_name in service1_dicts[0]["name"]


# [START howto_operator_cloud_run_service_creation]
def _create_service():
    service = Service()
    container = k8s_min.Container()
    container.image = "us-docker.pkg.dev/cloudrun/container/service:latest"
    service.template.containers.append(container)
    return service


# [END howto_operator_cloud_run_job_creation]


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_cloud_run_create_service]
    create1 = CloudRunCreateServiceOperator(
        task_id=create1_task_name,
        project_id=PROJECT_ID,
        region=region,
        service_name=service1_name,
        service=_create_service(),
        dag=dag,
    )
    # [END howto_operator_cloud_run_create_service]

    create1

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
