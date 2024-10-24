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

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.financial_services import (
    FinancialServicesCreateInstanceOperator,
    FinancialServicesDeleteInstanceOperator,
    FinancialServicesGetInstanceOperator,
)
from airflow.providers.google.cloud.sensors.financial_services import FinancialServicesOperationSensor

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
LOCATION = os.environ.get("SYSTEM_TESTS_GCP_LOCATION", "us-central1")
KMS_KEY = os.environ.get("SYSTEM_TESTS_GCP_KMS_KEY")

DAG_ID = "financial_services_instance"

INSTANCE_ID = f"instance_{DAG_ID}_{ENV_ID}"

RESOURCE_DIR_PATH = str(Path(__file__).parent / "resources" / "financial_services_discovery.json")

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    @task()
    def load_discovery_doc() -> str:
        with open(RESOURCE_DIR_PATH) as file:
            doc = json.load(file)
            return doc

    discovery_doc = load_discovery_doc()

    # [START howto_operator_financial_services_create_instance]
    create_instance_task = FinancialServicesCreateInstanceOperator(
        task_id="create_instance_task",
        discovery_doc=discovery_doc,
        instance_id=INSTANCE_ID,
        location_resource_uri=f"projects/{PROJECT_ID}/locations/{LOCATION}",
        kms_key_uri=KMS_KEY,
    )
    # [END howto_operator_financial_services_create_instance]

    # [START howto_sensor_financial_services_operation]
    create_instance_sensor = FinancialServicesOperationSensor(
        task_id="create_instance_sensor",
        discovery_doc=discovery_doc,
        operation_resource_uri="{{ task_instance.xcom_pull(task_ids='create_instance_task', key='return_value') }}",
        poke_interval=timedelta(seconds=5),
        timeout=timedelta(hours=1),
    )
    # [END howto_sensor_financial_services_operation]

    # [START howto_operator_financial_services_get_instance]
    get_instance_task = FinancialServicesGetInstanceOperator(
        task_id="get_instance_task",
        discovery_doc=discovery_doc,
        instance_resource_uri=f"projects/{PROJECT_ID}/locations/{LOCATION}/instances/{INSTANCE_ID}",
    )
    # [END howto_operator_financial_services_get_instance]

    # [START howto_operator_financial_services_delete_instance]
    delete_instance_task = FinancialServicesDeleteInstanceOperator(
        task_id="delete_instance_task",
        discovery_doc=discovery_doc,
        instance_resource_uri=f"projects/{PROJECT_ID}/locations/{LOCATION}/instances/{INSTANCE_ID}",
    )
    # [END howto_operator_financial_services_delete_instance]

    delete_instance_sensor = FinancialServicesOperationSensor(
        task_id="delete_instance_sensor",
        discovery_doc=discovery_doc,
        operation_resource_uri="{{ task_instance.xcom_pull(task_ids='delete_instance_task', key='return_value') }}",
        poke_interval=timedelta(seconds=5),
        timeout=timedelta(hours=1),
    )

    (
        discovery_doc
        >> create_instance_task
        >> create_instance_sensor
        >> get_instance_task
        >> delete_instance_task
        >> delete_instance_sensor
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
