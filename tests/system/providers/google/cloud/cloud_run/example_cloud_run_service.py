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
    CloudRunDeleteServiceOperator,
)

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")


# [START howto_operator_cloud_run_service_creation]
def _create_service():
    service = Service()
    container = k8s_min.Container()
    container.image = "us-docker.pkg.dev/cloudrun/container/placeholder:latest"
    service.template.containers.append(container)
    return service


# [END howto_operator_cloud_run_service_creation]


with DAG(
    "cloud_run_service",
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["cloud-run", "service", "example"],
) as dag:
    # [START howto_operator_cloud_run_create_service]
    create_cloud_run_service = CloudRunCreateServiceOperator(
        task_id="create-cloud-run-service",
        project_id=PROJECT_ID,
        region="us-central1",
        service=_create_service(),
        service_name="cloudrun-system-test-service",
    )
    # [END howto_operator_cloud_run_create_service]

    # [START howto_operator_cloud_run_delete_service]
    delete_cloud_run_service = CloudRunDeleteServiceOperator(
        task_id="delete-cloud-run-service",
        project_id=PROJECT_ID,
        region="us-central1",
        service_name="cloudrun-system-test-service",
        dag=dag,
    )
    # [END howto_operator_cloud_run_delete_service]

    (
        # TEST SETUP
        # TEST BODY
        create_cloud_run_service
        # TEST TEARDOWN
        >> delete_cloud_run_service
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
