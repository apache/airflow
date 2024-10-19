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

"""Example Airflow DAG that uses Google Cloud Run Operators."""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.run_v2 import Job
from google.cloud.run_v2.types import k8s_min

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunDeleteJobOperator,
    CloudRunExecuteJobOperator,
    CloudRunListJobsOperator,
    CloudRunUpdateJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "cloud_run"

region = "us-central1"
job_name_prefix = "cloudrun-system-test-job"
job1_name = f"{job_name_prefix}1-{ENV_ID}"
job2_name = f"{job_name_prefix}2-{ENV_ID}"
job3_name = f"{job_name_prefix}3-{ENV_ID}"

create1_task_name = "create-job1"
create2_task_name = "create-job2"
create3_task_name = "create-job3"

execute1_task_name = "execute-job1"
execute2_task_name = "execute-job2"
execute3_task_name = "execute-job3"

update_job1_task_name = "update-job1"

delete1_task_name = "delete-job1"
delete2_task_name = "delete-job2"

list_jobs_limit_task_name = "list-jobs-limit"
list_jobs_task_name = "list-jobs"

clean1_task_name = "clean-job1"
clean2_task_name = "clean-job2"


def _assert_executed_jobs_xcom(ti):
    job1_dicts = ti.xcom_pull(task_ids=[execute1_task_name], key="return_value")
    assert job1_name in job1_dicts[0]["name"]

    job2_dicts = ti.xcom_pull(task_ids=[execute2_task_name], key="return_value")
    assert job2_name in job2_dicts[0]["name"]

    job3_dicts = ti.xcom_pull(task_ids=[execute3_task_name], key="return_value")
    assert job3_name in job3_dicts[0]["name"]


def _assert_created_jobs_xcom(ti):
    job1_dicts = ti.xcom_pull(task_ids=[create1_task_name], key="return_value")
    assert job1_name in job1_dicts[0]["name"]

    job2_dicts = ti.xcom_pull(task_ids=[create2_task_name], key="return_value")
    assert job2_name in job2_dicts[0]["name"]

    job3_dicts = ti.xcom_pull(task_ids=[create3_task_name], key="return_value")
    assert job3_name in job3_dicts[0]["name"]


def _assert_updated_job(ti):
    job_dicts = ti.xcom_pull(task_ids=[update_job1_task_name], key="return_value")
    job_dict = job_dicts[0]
    assert job_dict["labels"]["somelabel"] == "label1"


def _assert_jobs(ti):
    job_dicts = ti.xcom_pull(task_ids=[list_jobs_task_name], key="return_value")

    job1_exists = False
    job2_exists = False

    for job_dict in job_dicts[0]:
        if job1_exists and job2_exists:
            break

        if job1_name in job_dict["name"]:
            job1_exists = True

        if job2_name in job_dict["name"]:
            job2_exists = True

    assert job1_exists
    assert job2_exists


def _assert_one_job(ti):
    job_dicts = ti.xcom_pull(task_ids=[list_jobs_limit_task_name], key="return_value")
    assert len(job_dicts[0]) == 1


# [START howto_cloud_run_job_instance_creation]
def _create_job_instance() -> Job:
    """
    Create a Cloud Run job configuration with google.cloud.run_v2.Job object.

    As a minimum the configuration must contain a container image name in its template.
    The rest of the configuration parameters are optional and will be populated with default values if not set.
    """
    job = Job()
    container = k8s_min.Container()
    container.image = "us-docker.pkg.dev/cloudrun/container/job:latest"
    container.resources.limits = {"cpu": "2", "memory": "1Gi"}
    job.template.template.containers.append(container)
    return job


# [END howto_cloud_run_job_instance_creation]


# [START howto_cloud_run_job_dict_creation]
def _create_job_dict() -> dict:
    """
    Create a Cloud Run job configuration with a Python dict.

    As a minimum the configuration must contain a container image name in its template.
    """
    return {
        "template": {
            "template": {
                "containers": [
                    {
                        "image": "us-docker.pkg.dev/cloudrun/container/job:latest",
                        "resources": {
                            "limits": {"cpu": "1", "memory": "512Mi"},
                            "cpu_idle": False,
                            "startup_cpu_boost": False,
                        },
                        "name": "",
                        "command": [],
                        "args": [],
                        "env": [],
                        "ports": [],
                        "volume_mounts": [],
                        "working_dir": "",
                        "depends_on": [],
                    }
                ],
                "volumes": [],
                "execution_environment": 0,
                "encryption_key": "",
            },
            "labels": {},
            "annotations": {},
            "parallelism": 0,
            "task_count": 0,
        },
        "name": "",
        "uid": "",
        "generation": "0",
        "labels": {},
        "annotations": {},
        "creator": "",
        "last_modifier": "",
        "client": "",
        "client_version": "",
        "launch_stage": 0,
        "observed_generation": "0",
        "conditions": [],
        "execution_count": 0,
        "reconciling": False,
        "satisfies_pzs": False,
        "etag": "",
    }


# [END howto_cloud_run_job_dict_creation]


def _create_job_instance_with_label():
    job = _create_job_instance()
    job.labels = {"somelabel": "label1"}
    return job


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "cloud", "run"],
) as dag:
    # [START howto_operator_cloud_run_create_job]
    create1 = CloudRunCreateJobOperator(
        task_id=create1_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job1_name,
        job=_create_job_instance(),
        dag=dag,
    )
    # [END howto_operator_cloud_run_create_job]

    create2 = CloudRunCreateJobOperator(
        task_id=create2_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job2_name,
        job=_create_job_dict(),
        dag=dag,
    )

    create3 = CloudRunCreateJobOperator(
        task_id=create3_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job3_name,
        job=Job.to_dict(_create_job_instance()),
        dag=dag,
    )

    assert_created_jobs = PythonOperator(
        task_id="assert-created-jobs", python_callable=_assert_created_jobs_xcom, dag=dag
    )

    # [START howto_operator_cloud_run_execute_job]
    execute1 = CloudRunExecuteJobOperator(
        task_id=execute1_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job1_name,
        dag=dag,
        deferrable=False,
    )
    # [END howto_operator_cloud_run_execute_job]

    # [START howto_operator_cloud_run_execute_job_deferrable_mode]
    execute2 = CloudRunExecuteJobOperator(
        task_id=execute2_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job2_name,
        dag=dag,
        deferrable=True,
    )
    # [END howto_operator_cloud_run_execute_job_deferrable_mode]

    # [START howto_operator_cloud_run_execute_job_with_overrides]
    overrides = {
        "container_overrides": [
            {
                "name": "job",
                "args": ["python", "main.py"],
                "env": [{"name": "ENV_VAR", "value": "value"}],
                "clear_args": False,
            }
        ],
        "task_count": 1,
        "timeout": "60s",
    }

    execute3 = CloudRunExecuteJobOperator(
        task_id=execute3_task_name,
        project_id=PROJECT_ID,
        region=region,
        overrides=overrides,
        job_name=job3_name,
        dag=dag,
        deferrable=False,
    )
    # [END howto_operator_cloud_run_execute_job_with_overrides]

    assert_executed_jobs = PythonOperator(
        task_id="assert-executed-jobs", python_callable=_assert_executed_jobs_xcom, dag=dag
    )

    list_jobs_limit = CloudRunListJobsOperator(
        task_id=list_jobs_limit_task_name, project_id=PROJECT_ID, region=region, dag=dag, limit=1
    )

    assert_jobs_limit = PythonOperator(task_id="assert-jobs-limit", python_callable=_assert_one_job, dag=dag)

    # [START howto_operator_cloud_run_list_jobs]
    list_jobs = CloudRunListJobsOperator(
        task_id=list_jobs_task_name, project_id=PROJECT_ID, region=region, dag=dag
    )
    # [END howto_operator_cloud_run_list_jobs]

    assert_jobs = PythonOperator(task_id="assert-jobs", python_callable=_assert_jobs, dag=dag)

    # [START howto_operator_cloud_update_job]
    update_job1 = CloudRunUpdateJobOperator(
        task_id=update_job1_task_name,
        project_id=PROJECT_ID,
        region=region,
        job_name=job1_name,
        job=_create_job_instance_with_label(),
        dag=dag,
    )
    # [END howto_operator_cloud_update_job]

    assert_job_updated = PythonOperator(
        task_id="assert-job-updated", python_callable=_assert_updated_job, dag=dag
    )

    # [START howto_operator_cloud_delete_job]
    delete_job1 = CloudRunDeleteJobOperator(
        task_id="delete-job1",
        project_id=PROJECT_ID,
        region=region,
        job_name=job1_name,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_cloud_delete_job]

    delete_job2 = CloudRunDeleteJobOperator(
        task_id="delete-job2",
        project_id=PROJECT_ID,
        region=region,
        job_name=job2_name,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_job3 = CloudRunDeleteJobOperator(
        task_id="delete-job3",
        project_id=PROJECT_ID,
        region=region,
        job_name=job3_name,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        (create1, create2, create3)
        >> assert_created_jobs
        >> (execute1, execute2, execute3)
        >> assert_executed_jobs
        >> list_jobs_limit
        >> assert_jobs_limit
        >> list_jobs
        >> assert_jobs
        >> update_job1
        >> assert_job_updated
        >> (delete_job1, delete_job2, delete_job3)
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
