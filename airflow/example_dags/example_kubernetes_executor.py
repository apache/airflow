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

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

log = logging.getLogger(__name__)

worker_container_repository = conf.get('kubernetes', 'worker_container_repository')
worker_container_tag = conf.get('kubernetes', 'worker_container_tag')

try:
    from kubernetes.client import models as k8s
except ImportError:
    log.warning(
        "The example_kubernetes_executor example DAG requires the kubernetes provider."
        " Please install it with: pip install apache-airflow[cncf.kubernetes]"
    )
    k8s = None


if k8s:
    with DAG(
        dag_id='example_kubernetes_executor',
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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

        # You can add labels to pods
        executor_config_non_root = {
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"release": "stable"}))
        }

        @task(executor_config=executor_config_non_root)
        def non_root_task():
            print_stuff()

        third_task = non_root_task()

        executor_config_other_ns = {
            "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(namespace="test-namespace", labels={'release': 'stable'})
            )
        }

        @task(executor_config=executor_config_other_ns)
        def other_namespace_task():
            print_stuff()

        other_ns_task = other_namespace_task()

        # You can also change the base image, here we used the worker image for demonstration.
        # Note that the image must have the same configuration as the
        # worker image. Could be that you want to run this task in a special docker image that has a zip
        # library built-in. You build the special docker image on top your worker image.
        kube_exec_config_special = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base", image=f"{worker_container_repository}:{worker_container_tag}"
                        ),
                    ]
                )
            )
        }

        @task(executor_config=kube_exec_config_special)
        def base_image_override_task():
            print_stuff()

        base_image_task = base_image_override_task()

        # Use k8s_client.V1Affinity to define node affinity
        k8s_affinity = k8s.V1Affinity(
            pod_anti_affinity=k8s.V1PodAntiAffinity(
                required_during_scheduling_ignored_during_execution=[
                    k8s.V1PodAffinityTerm(
                        label_selector=k8s.V1LabelSelector(
                            match_expressions=[
                                k8s.V1LabelSelectorRequirement(key='app', operator='In', values=['airflow'])
                            ]
                        ),
                        topology_key='kubernetes.io/hostname',
                    )
                ]
            )
        )

        # Use k8s_client.V1Toleration to define node tolerations
        k8s_tolerations = [k8s.V1Toleration(key='dedicated', operator='Equal', value='airflow')]

        # Use k8s_client.V1ResourceRequirements to define resource limits
        k8s_resource_requirements = k8s.V1ResourceRequirements(
            requests={'memory': '512Mi'}, limits={'memory': '512Mi'}
        )

        kube_exec_config_resource_limits = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s_resource_requirements,
                        )
                    ],
                    affinity=k8s_affinity,
                    tolerations=k8s_tolerations,
                )
            )
        }

        @task(executor_config=kube_exec_config_resource_limits)
        def task_with_resource_limits():
            print_stuff()

        four_task = task_with_resource_limits()

        (
            start_task()
            >> [volume_task, other_ns_task, sidecar_task]
            >> third_task
            >> [base_image_task, four_task]
        )
