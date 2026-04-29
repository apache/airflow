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
Example Airflow DAG that tests cancel_on_kill behavior for Dataproc triggers.

Test A (happy path): Submits a Spark job in deferrable mode with cancel_on_kill=True
and verifies it completes successfully.

Test B (cancel path): Submits a long-running Spark job asynchronously, cancels it
via DataprocHook.cancel_job() — the same call that trigger.on_kill() delegates to —
and verifies the job reaches CANCELLED state.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import pytest
from google.api_core.retry import Retry
from google.cloud.dataproc_v1 import JobStatus

from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

if not os.environ.get("RUN_MANUAL_DATAPROC_CANCEL_ON_KILL_TEST"):
    pytest.skip(
        "Manual-only system test: set RUN_MANUAL_DATAPROC_CANCEL_ON_KILL_TEST=1 to run.",
        allow_module_level=True,
    )

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataproc_cancel_on_kill"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

CLUSTER_NAME_BASE = f"cluster-{DAG_ID}".replace("_", "-")
CLUSTER_NAME_FULL = CLUSTER_NAME_BASE + f"-{ENV_ID}".replace("_", "-")
CLUSTER_NAME = CLUSTER_NAME_BASE if len(CLUSTER_NAME_FULL) >= 33 else CLUSTER_NAME_FULL

REGION = "europe-west1"

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

SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}

# [START how_to_cloud_dataproc_cancel_on_kill_config]
LONG_RUNNING_SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
        "args": ["1000000"],
    },
}
# [END how_to_cloud_dataproc_cancel_on_kill_config]


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataproc", "cancel_on_kill", "deferrable"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
        num_retries_if_resource_is_not_ready=3,
    )

    # Test A: deferrable submit with cancel_on_kill=True completes normally
    # [START how_to_cloud_dataproc_deferrable_cancel_on_kill]
    spark_task_deferrable = DataprocSubmitJobOperator(
        task_id="spark_task_deferrable",
        job=SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        deferrable=True,
        cancel_on_kill=True,
    )
    # [END how_to_cloud_dataproc_deferrable_cancel_on_kill]

    # Test B: submit a long-running job, cancel it, verify CANCELLED state
    submit_long_job = DataprocSubmitJobOperator(
        task_id="submit_long_job",
        job=LONG_RUNNING_SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        asynchronous=True,
    )

    @task(task_id="cancel_and_verify")
    def cancel_and_verify_job(job_id: str, project_id: str, region: str):
        """Cancel a running Dataproc job and verify it reaches CANCELLED state.

        Exercises the same DataprocHook.cancel_job() call that
        DataprocSubmitTrigger.on_kill() and DataprocSubmitJobDirectTrigger.on_kill()
        delegate to.
        """
        hook = DataprocHook(gcp_conn_id="google_cloud_default")

        hook.cancel_job(job_id=job_id, project_id=project_id, region=region)

        for _ in range(30):
            job = hook.get_job(job_id=job_id, project_id=project_id, region=region)
            state = job.status.state
            if state in (JobStatus.State.DONE, JobStatus.State.CANCELLED, JobStatus.State.ERROR):
                break
            time.sleep(5)
        else:
            raise RuntimeError(f"Job {job_id} did not reach terminal state within 150s")

        assert job.status.state == JobStatus.State.CANCELLED, (
            f"Expected CANCELLED, got {JobStatus.State(job.status.state).name}"
        )

    cancel_task = cancel_and_verify_job(
        job_id=submit_long_job.output,
        project_id=PROJECT_ID,
        region=REGION,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_cluster
        # TEST BODY
        >> spark_task_deferrable
        >> submit_long_job
        >> cancel_task
        # TEST TEARDOWN
        >> delete_cluster
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
