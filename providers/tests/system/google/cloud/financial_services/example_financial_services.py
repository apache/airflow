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
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.financial_services import (
    FinancialServicesCreateInstanceOperator,
    FinancialServicesDeleteInstanceOperator,
    FinancialServicesGetInstanceOperator,
)

# from airflow.providers.google.cloud.sensors.financial_services import FinancialServicesOperationSensor
from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
LOCATION = os.environ.get("SYSTEM_TESTS_GCP_LOCATION", "us-central1")
KMS_KEY_RING = os.environ.get("SYSTEM_TESTS_GCP_KMS_KEY_RING")
KMS_KEY = os.environ.get("SYSTEM_TESTS_GCP_KMS_KEY")
assert KMS_KEY_RING is not None
assert KMS_KEY is not None

DAG_ID = "financial_services_instance"
INSTANCE_ID = f"instance_{DAG_ID}_{ENV_ID}"
RESOURCE_DIR_PATH = str(Path(__file__).parent / "resources" / "financial_services_discovery.json")

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_operator_financial_services_create_instance]
    create_instance_task = FinancialServicesCreateInstanceOperator(
        task_id="create_instance_task",
        project_id=PROJECT_ID,
        region=LOCATION,
        instance_id=INSTANCE_ID,
        kms_key_ring_id=KMS_KEY_RING,
        kms_key_id=KMS_KEY,
    )
    # [END howto_operator_financial_services_create_instance]

    # [START howto_operator_financial_services_get_instance]
    get_instance_task = FinancialServicesGetInstanceOperator(
        task_id="get_instance_task",
        project_id=PROJECT_ID,
        region=LOCATION,
        instance_id=INSTANCE_ID,
    )
    # [END howto_operator_financial_services_get_instance]

    # [START howto_operator_financial_services_delete_instance]
    delete_instance_task = FinancialServicesDeleteInstanceOperator(
        task_id="delete_instance_task",
        project_id=PROJECT_ID,
        region=LOCATION,
        instance_id=INSTANCE_ID,
    )
    # [END howto_operator_financial_services_delete_instance]

    (create_instance_task >> get_instance_task >> delete_instance_task)

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
