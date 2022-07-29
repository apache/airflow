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

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateWorkflowTemplateOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "dataproc_workflow"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")

REGION = "europe-west1"
CLUSTER_NAME = f"cluster-dataproc-workflow-{ENV_ID}"
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}
PIG_JOB = {"query_list": {"queries": ["define sin HiveUDF('sin');"]}}
WORKFLOW_NAME = "airflow-dataproc-test"
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


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:
    # [START how_to_cloud_dataproc_create_workflow_template]
    create_workflow_template = DataprocCreateWorkflowTemplateOperator(
        task_id="create_workflow_template",
        template=WORKFLOW_TEMPLATE,
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_create_workflow_template]

    # [START how_to_cloud_dataproc_trigger_workflow_template]
    trigger_workflow = DataprocInstantiateWorkflowTemplateOperator(
        task_id="trigger_workflow", region=REGION, project_id=PROJECT_ID, template_id=WORKFLOW_NAME
    )
    # [END how_to_cloud_dataproc_trigger_workflow_template]

    # [START how_to_cloud_dataproc_instantiate_inline_workflow_template]
    instantiate_inline_workflow_template = DataprocInstantiateInlineWorkflowTemplateOperator(
        task_id='instantiate_inline_workflow_template', template=WORKFLOW_TEMPLATE, region=REGION
    )
    # [END how_to_cloud_dataproc_instantiate_inline_workflow_template]

    create_workflow_template >> trigger_workflow >> instantiate_inline_workflow_template

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
