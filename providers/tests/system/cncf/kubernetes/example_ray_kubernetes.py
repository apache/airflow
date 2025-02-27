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
This is an example DAG which uses RayKubernetesOperator and RayKubernetesSensor.
In this example, we create two tasks which execute sequentially.
The first task is to submit rayJob on Kubernetes cluster(the example uses ray-example application).
and the second task is to check the final state of the sparkApplication that submitted in the first state.

KubeRay operator is required to be already installed on Kubernetes
https://github.com/ray-project/kuberay
"""

from __future__ import annotations

import os
import pathlib
from datetime import datetime, timedelta
from os.path import join

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.ray_kubernetes import RayKubernetesOperator

# [END import_module]


# [START instantiate_dag]


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "ray_example"

with DAG(
    DAG_ID,
    default_args={"max_active_runs": 1},
    description="submit ray_example as rayJob on kubernetes",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START RayKubernetesOperator_DAG]
    pi_example_path = pathlib.Path(__file__).parent.resolve()
    t1 = RayKubernetesOperator(
        task_id="ray_example",
        namespace="default",
        application_file=join(pi_example_path, "example_ray_kubernetes.yaml"),
        do_xcom_push=True,
        dag=dag,
    )

    # t2 = SparkKubernetesSensor(
    #     task_id="spark_pi_monitor",
    #     namespace="default",
    #     application_name="{{ task_instance.xcom_pull(task_ids='ray_example')['metadata']['name'] }}",
    #     dag=dag,
    # )
    # t1 >> t2

    # [END RayKubernetesOperator_DAG]
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
