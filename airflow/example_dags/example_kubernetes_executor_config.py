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
import os

from kubernetes.client import models as k8s

from airflow import DAG
from airflow.example_dags.libs.helper import print_stuff
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='example_kubernetes_executor_config',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example3'],
) as dag:

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

    def test_volume_mount():
        """
        Tests whether the volume has been mounted.
        """
        with open('/foo/volume_mount_test.txt', 'w') as foo:
            foo.write('Hello')

        return_code = os.system("cat /foo/volume_mount_test.txt")
        if return_code != 0:
            raise ValueError(f"Error when checking volume mount. Return code {return_code}")

    # You can use annotations on your kubernetes pods!
    start_task = PythonOperator(
        task_id="start_task",
        python_callable=print_stuff,
        executor_config={
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))
        },
    )

    # [START task_with_volume]
    volume_task = PythonOperator(
        task_id="task_with_volume",
        python_callable=test_volume_mount,
        executor_config={
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
        },
    )
    # [END task_with_volume]

    # [START task_with_template]
    task_with_template = PythonOperator(
        task_id="task_with_template",
        python_callable=print_stuff,
        executor_config={
            "pod_template_file": "/usr/local/airflow/pod_templates/basic_template.yaml",
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"release": "stable"})),
        },
    )
    # [END task_with_template]

    # [START task_with_sidecar]
    sidecar_task = PythonOperator(
        task_id="task_with_sidecar",
        python_callable=test_sharedvolume_mount,
        executor_config={
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
        },
    )
    # [END task_with_sidecar]

    # Test that we can add labels to pods
    third_task = PythonOperator(
        task_id="non_root_task",
        python_callable=print_stuff,
        executor_config={"pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(labels={"release": "stable"}))},
    )

    other_ns_task = PythonOperator(
        task_id="other_namespace_task",
        python_callable=print_stuff,
        executor_config={
            "KubernetesExecutor": {"namespace": "test-namespace", "labels": {"release": "stable"}}
        },
    )

    start_task >> volume_task >> third_task
    start_task >> other_ns_task
    start_task >> sidecar_task
    start_task >> task_with_template
