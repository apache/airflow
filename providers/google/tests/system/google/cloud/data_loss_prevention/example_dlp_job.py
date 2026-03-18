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
Example Airflow DAG that creates, update, get and delete trigger for Data Loss Prevention actions.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.dlp_v2.types import InspectConfig, InspectJobConfig

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCancelDLPJobOperator,
    CloudDLPCreateDLPJobOperator,
    CloudDLPDeleteDLPJobOperator,
    CloudDLPGetDLPJobOperator,
    CloudDLPListDLPJobsOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dlp_job"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

JOB_ID = f"dlp_job_{ENV_ID}"

INSPECT_CONFIG = InspectConfig(info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}])
INSPECT_JOB = InspectJobConfig(
    inspect_config=INSPECT_CONFIG,
    storage_config={
        "datastore_options": {"partition_id": {"project_id": PROJECT_ID}, "kind": {"name": "test"}}
    },
)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["dlp", "example"],
) as dag:
    create_job = CloudDLPCreateDLPJobOperator(
        task_id="create_job", project_id=PROJECT_ID, inspect_job=INSPECT_JOB, job_id=JOB_ID
    )

    list_jobs = CloudDLPListDLPJobsOperator(
        task_id="list_jobs", project_id=PROJECT_ID, results_filter="state=DONE"
    )

    get_job = CloudDLPGetDLPJobOperator(
        task_id="get_job",
        project_id=PROJECT_ID,
        dlp_job_id="{{ task_instance.xcom_pull('create_job')['name'].split('/')[-1] }}",
    )

    cancel_job = CloudDLPCancelDLPJobOperator(
        task_id="cancel_job",
        project_id=PROJECT_ID,
        dlp_job_id="{{ task_instance.xcom_pull('create_job')['name'].split('/')[-1] }}",
    )

    delete_job = CloudDLPDeleteDLPJobOperator(
        task_id="delete_job",
        project_id=PROJECT_ID,
        dlp_job_id="{{ task_instance.xcom_pull('create_job')['name'].split('/')[-1] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (create_job >> list_jobs >> get_job >> cancel_job >> delete_job)

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
