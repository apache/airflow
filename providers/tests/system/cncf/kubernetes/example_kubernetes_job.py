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
This is an example dag for using the KubernetesJobOperator.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.job import (
    KubernetesDeleteJobOperator,
    KubernetesJobOperator,
    KubernetesPatchJobOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_kubernetes_job_operator"

JOB_NAME = "test-pi"
JOB_NAMESPACE = "default"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example", "kubernetes"],
) as dag:
    # [START howto_operator_k8s_job]
    k8s_job = KubernetesJobOperator(
        task_id="job-task",
        namespace=JOB_NAMESPACE,
        image="perl:5.34.0",
        cmds=["perl", "-Mbignum=bpi", "-wle", "print bpi(2000)"],
        name=JOB_NAME,
    )
    # [END howto_operator_k8s_job]

    # [START howto_operator_update_job]
    update_job = KubernetesPatchJobOperator(
        task_id="update-job-task",
        namespace="default",
        name=k8s_job.output["job_name"],
        body={"spec": {"suspend": False}},
    )
    # [END howto_operator_update_job]

    # [START howto_operator_k8s_job_deferrable]
    k8s_job_def = KubernetesJobOperator(
        task_id="job-task-def",
        namespace="default",
        image="perl:5.34.0",
        cmds=["perl", "-Mbignum=bpi", "-wle", "print bpi(2000)"],
        name=JOB_NAME + "-def",
        wait_until_job_complete=True,
        deferrable=True,
    )
    # [END howto_operator_k8s_job_deferrable]

    # [START howto_operator_delete_k8s_job]
    delete_job_task = KubernetesDeleteJobOperator(
        task_id="delete_job_task",
        name=k8s_job.output["job_name"],
        namespace=JOB_NAMESPACE,
        wait_for_completion=True,
        delete_on_status="Complete",
        poll_interval=1.0,
    )
    # [END howto_operator_delete_k8s_job]

    delete_job_task_def = KubernetesDeleteJobOperator(
        task_id="delete_job_task_def",
        name=k8s_job_def.output["job_name"],
        namespace=JOB_NAMESPACE,
    )

    k8s_job >> update_job >> delete_job_task
    k8s_job_def >> delete_job_task_def

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
