# -*- coding: utf-8 -*-
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
from __future__ import print_function
import airflow
from airflow.operators.python_operator import PythonOperator
from libs.helper import print_stuff
from airflow.models import DAG
import os

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_kubernetes_executor_config', default_args=args,
    schedule_interval=None
)


def test_volume_mount():
    with open('/foo/volume_mount_test.txt', 'w') as foo:
        foo.write('Hello')

    rc = os.system("cat /foo/volume_mount_test.txt")
    assert rc == 0


# You can use annotations on your kubernetes pods!
start_task = PythonOperator(
    task_id="start_task", python_callable=print_stuff, dag=dag,
    executor_config={
        "KubernetesExecutor": {
            "annotations": {"test": "annotation"}
        }
    }
)

# You can mount volume or secret to the worker pod
second_task = PythonOperator(
    task_id="four_task", python_callable=test_volume_mount, dag=dag,
    executor_config={
        "KubernetesExecutor": {
            "volumes": [
                {
                    "name": "example-kubernetes-test-volume",
                    "hostPath": {"path": "/tmp/"},
                },
            ],
            "volume_mounts": [
                {
                    "mountPath": "/foo/",
                    "name": "example-kubernetes-test-volume",
                },
            ]
        }
    }
)

start_task.set_downstream(second_task)
