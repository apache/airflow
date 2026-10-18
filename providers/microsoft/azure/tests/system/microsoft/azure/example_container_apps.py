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
"""Synthetic example: provision the job and an Azure connection before running this Dag.

For workload identity, configure azure_default without login/password and provision
Azure's federated token file and environment on workers and triggerers.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.compat.sdk import DAG
from airflow.providers.microsoft.azure.operators.container_apps import AzureContainerAppsJobOperator

DAG_ID = "example_container_apps_jobs"
RESOURCE_GROUP = os.environ.get("ACA_RESOURCE_GROUP", "example-resource-group")
JOB_NAME = os.environ.get("ACA_JOB_NAME", "example-job")
CONTAINER_NAME = os.environ.get("ACA_CONTAINER_NAME", "worker")

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START container_apps_jobs]
    defaults = AzureContainerAppsJobOperator(
        task_id="run_defaults",
        resource_group=RESOURCE_GROUP,
        job_name=JOB_NAME,
        deferrable=False,
        retries=0,
    )
    overridden = AzureContainerAppsJobOperator(
        task_id="run_named_container",
        azure_conn_id="azure_default",
        resource_group=RESOURCE_GROUP,
        job_name=JOB_NAME,
        container_name=CONTAINER_NAME,
        command=["python", "-c"],
        args=["print('Airflow run {{ run_id }}')"],
        env_vars={"RUN_DATE": "{{ ds }}"},
        cpu=0.5,
        memory="1Gi",
        deferrable=True,
        timeout_seconds=3600,
        retries=0,
    )
    defaults >> overridden
    # [END container_apps_jobs]

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
