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
This is an example dag for using a Kubernetes Executor Configuration.
"""
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff
from airflow.settings import AIRFLOW_HOME

log = logging.getLogger(__name__)

try:
    from kubernetes.client import models as k8s

    with DAG(
        dag_id='example_kubernetes_executor_config',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example3'],
    ) as dag:
        # You can use annotations on your kubernetes pods!
        start_task_executor_config = {
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))
        }

        @task(executor_config=start_task_executor_config)
        def start_task():
            print_stuff()

        start_task = start_task()

        # [START task_with_volume]
        executor_config_volume_mount = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            volume_mounts=[
                                k8s.V1VolumeMount(mount_path="/foo/", name="example-kubernetes-test-volume")
                            ],
                        )
                    ],
                    volumes=[
                        k8s.V1Volume(
                            name="example-kubernetes-test-volume",
                            host_path=k8s.V1HostPathVolumeSource(path="/tmp/"),
                        )
                    ],
                )
            ),
        }

        @task(executor_config=executor_config_volume_mount)
        def test_volume_mount():
            """
            Tests whether the volume has been mounted.
            """
            with open('/foo/volume_mount_test.txt', 'w') as foo:
                foo.write('Hello')

            return_code = os.system("cat /foo/volume_mount_test.txt")
            if return_code != 0:
                raise ValueError(f"Error when checking volume mount. Return code {return_code}")

        volume_task = test_volume_mount()
        # [END task_with_volume]

        # [START task_with_template]
        executor_config_template = {
            "pod_template_file": os.path.join(AIRFLOW_HOME, "pod_templates/basic_template.yaml"),
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"release": "stable"})),
        }

        @task(executor_config=executor_config_template)
        def task_with_template():
            print_stuff()

        task_with_template = task_with_template()
        # [END task_with_template]

        # [START task_with_sidecar]
        executor_config_sidecar = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            volume_mounts=[k8s.V1VolumeMount(mount_path="/shared/", name="shared-empty-dir")],
                        ),
                        k8s.V1Container(
                            name="sidecar",
                            image="ubuntu",
                            args=["echo \"retrieved from mount\" > /shared/test.txt"],
                            command=["bash", "-cx"],
                            volume_mounts=[k8s.V1VolumeMount(mount_path="/shared/", name="shared-empty-dir")],
                        ),
                    ],
                    volumes=[
                        k8s.V1Volume(name="shared-empty-dir", empty_dir=k8s.V1EmptyDirVolumeSource()),
                    ],
                )
            ),
        }

        @task(executor_config=executor_config_sidecar)
        def test_sharedvolume_mount():
            """
            Tests whether the volume has been mounted.
            """
            for i in range(5):
                try:
                    return_code = os.system("cat /shared/test.txt")
                    if return_code != 0:
                        raise ValueError(f"Error when checking volume mount. Return code {return_code}")
                except ValueError as e:
                    if i > 4:
                        raise e

        sidecar_task = test_sharedvolume_mount()
        # [END task_with_sidecar]

        # Test that we can add labels to pods
        executor_config_non_root = {
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"release": "stable"}))
        }

        @task(executor_config=executor_config_non_root)
        def non_root_task():
            print_stuff()

        third_task = non_root_task()

        executor_config_other_ns = {
            "KubernetesExecutor": {"namespace": "test-namespace", "labels": {"release": "stable"}}
        }

        @task(executor_config=executor_config_other_ns)
        def other_namespace_task():
            print_stuff()

        other_ns_task = other_namespace_task()

        start_task >> volume_task >> third_task
        start_task >> other_ns_task
        start_task >> sidecar_task
        start_task >> task_with_template
except ImportError as e:
    log.warning("Could not import DAGs in example_kubernetes_executor_config.py: %s", str(e))
    log.warning("Install kubernetes dependencies with: pip install apache-airflow['cncf.kubernetes']")
