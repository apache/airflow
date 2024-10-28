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
Example Airflow DAG for Google Kubernetes Engine.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEDeleteJobOperator,
    GKEDescribeJobOperator,
    GKEListJobsOperator,
    GKEResumeJobOperator,
    GKEStartJobOperator,
    GKESuspendJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "kubernetes_engine_job"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_LOCATION = "europe-north1-a"
CLUSTER_NAME = f"gke-job-{ENV_ID}".replace("_", "-")
CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1}

JOB_NAME = "test-pi"
JOB_NAME_DEF = "test-pi-def"
JOB_NAMESPACE = "default"

with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=CLUSTER,
    )

    # [START howto_operator_gke_start_job]
    job_task = GKEStartJobOperator(
        task_id="job_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        namespace=JOB_NAMESPACE,
        image="perl:5.34.0",
        cmds=["perl", "-Mbignum=bpi", "-wle", "print bpi(2000)"],
        name=JOB_NAME,
    )
    # [END howto_operator_gke_start_job]

    # [START howto_operator_gke_start_job_def]
    job_task_def = GKEStartJobOperator(
        task_id="job_task_def",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        namespace=JOB_NAMESPACE,
        image="perl:5.34.0",
        cmds=["perl", "-Mbignum=bpi", "-wle", "print bpi(2000)"],
        name=JOB_NAME_DEF,
        wait_until_job_complete=True,
        deferrable=True,
    )
    # [END howto_operator_gke_start_job_def]

    # [START howto_operator_gke_list_jobs]
    list_job_task = GKEListJobsOperator(
        task_id="list_job_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
    )
    # [END howto_operator_gke_list_jobs]

    # [START howto_operator_gke_describe_job]
    describe_job_task = GKEDescribeJobOperator(
        task_id="describe_job_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        job_name=job_task.output["job_name"],
        namespace="default",
        cluster_name=CLUSTER_NAME,
    )
    # [END howto_operator_gke_describe_job]

    describe_job_task_def = GKEDescribeJobOperator(
        task_id="describe_job_task_def",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        job_name=job_task_def.output["job_name"],
        namespace="default",
        cluster_name=CLUSTER_NAME,
    )

    # [START howto_operator_gke_suspend_job]
    suspend_job = GKESuspendJobOperator(
        task_id="suspend_job",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        name=job_task.output["job_name"],
        namespace="default",
    )
    # [END howto_operator_gke_suspend_job]

    # [START howto_operator_gke_resume_job]
    resume_job = GKEResumeJobOperator(
        task_id="resume_job",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        name=job_task.output["job_name"],
        namespace="default",
    )
    # [END howto_operator_gke_resume_job]

    # [START howto_operator_gke_delete_job]
    delete_job = GKEDeleteJobOperator(
        task_id="delete_job",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        name=JOB_NAME,
        namespace=JOB_NAMESPACE,
    )
    # [END howto_operator_gke_delete_job]

    delete_job_def = GKEDeleteJobOperator(
        task_id="delete_job_def",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        name=JOB_NAME,
        namespace=JOB_NAMESPACE,
    )

    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        create_cluster,
        [job_task, job_task_def],
        list_job_task,
        [describe_job_task, describe_job_task_def],
        suspend_job,
        resume_job,
        [delete_job, delete_job_def],
        delete_cluster,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
