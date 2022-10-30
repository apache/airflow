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

from airflow import models
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateJobTriggerOperator,
    CloudDLPDeleteJobTriggerOperator,
    CloudDLPGetDLPJobTriggerOperator,
    CloudDLPListJobTriggersOperator,
    CloudDLPUpdateJobTriggerOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "dlp_job_trigger"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

JOB_TRIGGER = {
    "inspect_job": {
        "storage_config": {
            "datastore_options": {"partition_id": {"project_id": PROJECT_ID}, "kind": {"name": "test"}}
        }
    },
    "triggers": [{"schedule": {"recurrence_period_duration": {"seconds": 60 * 60 * 24}}}],
    "status": "HEALTHY",
}

TRIGGER_ID = f"trigger_{ENV_ID}"

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["dlp", "example"],
) as dag:
    # [START howto_operator_dlp_create_job_trigger]
    create_trigger = CloudDLPCreateJobTriggerOperator(
        project_id=PROJECT_ID,
        job_trigger=JOB_TRIGGER,
        trigger_id=TRIGGER_ID,
        task_id="create_trigger",
    )
    # [END howto_operator_dlp_create_job_trigger]

    list_triggers = CloudDLPListJobTriggersOperator(task_id="list_triggers", project_id=PROJECT_ID)

    get_trigger = CloudDLPGetDLPJobTriggerOperator(
        task_id="get_trigger", project_id=PROJECT_ID, job_trigger_id=TRIGGER_ID
    )

    JOB_TRIGGER["triggers"] = [{"schedule": {"recurrence_period_duration": {"seconds": 2 * 60 * 60 * 24}}}]

    # [START howto_operator_dlp_update_job_trigger]
    update_trigger = CloudDLPUpdateJobTriggerOperator(
        project_id=PROJECT_ID,
        job_trigger_id=TRIGGER_ID,
        job_trigger=JOB_TRIGGER,
        task_id="update_info_type",
    )
    # [END howto_operator_dlp_update_job_trigger]

    # [START howto_operator_dlp_delete_job_trigger]
    delete_trigger = CloudDLPDeleteJobTriggerOperator(
        project_id=PROJECT_ID, job_trigger_id=TRIGGER_ID, task_id="delete_info_type"
    )
    # [END howto_operator_dlp_delete_job_trigger]
    delete_trigger.trigger_rule = TriggerRule.ALL_DONE

    (create_trigger >> list_triggers >> get_trigger >> update_trigger >> delete_trigger)

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
