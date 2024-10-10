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
Example Airflow DAG for Dataproc workflow operators.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateWorkflowTemplateOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
)

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataproc_workflow_def"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

REGION = "europe-west1"
CLUSTER_NAME_BASE = f"cluster-{DAG_ID}".replace("_", "-")
CLUSTER_NAME_FULL = CLUSTER_NAME_BASE + f"-{ENV_ID}".replace("_", "-")
CLUSTER_NAME = CLUSTER_NAME_BASE if len(CLUSTER_NAME_FULL) >= 33 else CLUSTER_NAME_FULL
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
}
PIG_JOB = {"query_list": {"queries": ["define sin HiveUDF('sin');"]}}
WORKFLOW_NAME = "airflow-dataproc-test-def"
WORKFLOW_TEMPLATE = {
    "id": WORKFLOW_NAME,
    "placement": {
        "managed_cluster": {
            "cluster_name": CLUSTER_NAME,
            "config": CLUSTER_CONFIG,
        }
    },
    "jobs": [{"step_id": "pig_job_1", "pig_job": PIG_JOB}],
}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc", "workflow", "deferrable"],
) as dag:
    create_workflow_template = DataprocCreateWorkflowTemplateOperator(
        task_id="create_workflow_template",
        template=WORKFLOW_TEMPLATE,
        project_id=PROJECT_ID,
        region=REGION,
    )

    # [START how_to_cloud_dataproc_trigger_workflow_template_async]
    trigger_workflow_async = DataprocInstantiateWorkflowTemplateOperator(
        task_id="trigger_workflow_async",
        region=REGION,
        project_id=PROJECT_ID,
        template_id=WORKFLOW_NAME,
        deferrable=True,
    )
    # [END how_to_cloud_dataproc_trigger_workflow_template_async]

    # [START how_to_cloud_dataproc_instantiate_inline_workflow_template_async]
    instantiate_inline_workflow_template_async = DataprocInstantiateInlineWorkflowTemplateOperator(
        task_id="instantiate_inline_workflow_template_async",
        template=WORKFLOW_TEMPLATE,
        region=REGION,
        deferrable=True,
    )
    # [END how_to_cloud_dataproc_instantiate_inline_workflow_template_async]

    (
        # TEST SETUP
        create_workflow_template
        # TEST BODY
        >> trigger_workflow_async
        # TEST TEARDOWN
        >> instantiate_inline_workflow_template_async
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
