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
Example DAG using RayKubernetesOperator to submit a RayJob on Kubernetes.

KubeRay operator is required to be installed on the Kubernetes cluster.
https://docs.ray.io/en/latest/cluster/kubernetes/getting-started.html
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

# [START import_module]
from airflow.providers.cncf.kubernetes.operators.ray import RayKubernetesOperator

# [END import_module]

with DAG(
    dag_id="example_ray_kubernetes",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "ray", "kubernetes"],
) as dag:
    # [START howto_operator_ray_kubernetes_template_spec]
    ray_job_template_spec = RayKubernetesOperator(
        task_id="ray_job_template_spec",
        template_spec={
            "apiVersion": "ray.io/v1",
            "kind": "RayJob",
            "metadata": {"name": "ray-job-example"},
            "spec": {
                "entrypoint": "python -c 'import ray; ray.init(); print(ray.cluster_resources())'",
                "rayClusterSpec": {
                    "headGroupSpec": {
                        "rayStartParams": {"dashboard-host": "0.0.0.0"},
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": "ray-head",
                                        "image": "rayproject/ray:2.9.0",
                                        "resources": {
                                            "limits": {"cpu": "1", "memory": "2Gi"},
                                            "requests": {"cpu": "500m", "memory": "1Gi"},
                                        },
                                    }
                                ]
                            }
                        },
                    },
                },
            },
        },
        namespace="default",
        kubernetes_conn_id="kubernetes_default",
        startup_timeout_seconds=300,
        job_timeout_seconds=600,
        delete_on_termination=True,
    )
    # [END howto_operator_ray_kubernetes_template_spec]

    # [START howto_operator_ray_kubernetes_yaml]
    ray_job_yaml = RayKubernetesOperator(
        task_id="ray_job_yaml",
        application_file="example_ray_job.yaml",
        namespace="default",
        kubernetes_conn_id="kubernetes_default",
        delete_on_termination=True,
    )
    # [END howto_operator_ray_kubernetes_yaml]

    # [START howto_operator_ray_kubernetes_fire_and_forget]
    ray_job_fire_and_forget = RayKubernetesOperator(
        task_id="ray_job_fire_and_forget",
        template_spec={
            "apiVersion": "ray.io/v1",
            "kind": "RayJob",
            "metadata": {"name": "ray-job-fire-forget"},
            "spec": {
                "entrypoint": "python -c 'print(\"hello from ray\")'",
                "rayClusterSpec": {
                    "headGroupSpec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": "ray-head",
                                        "image": "rayproject/ray:2.9.0",
                                    }
                                ]
                            }
                        },
                    },
                },
            },
        },
        wait_for_completion=False,
        delete_on_termination=False,
    )
    # [END howto_operator_ray_kubernetes_fire_and_forget]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
