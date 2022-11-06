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
from __future__ import annotations

import unittest
from unittest import mock

import pendulum
from kubernetes.client.api_client import ApiClient

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils import timezone
from airflow.utils.types import DagRunType
from airflow.version import version as airflow_version

# noinspection DuplicatedCode

HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesHook"


def create_context(task):
    dag = DAG(dag_id="dag")
    tzinfo = pendulum.timezone("Europe/Amsterdam")
    execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


# noinspection DuplicatedCode,PyUnusedLocal
class TestKubernetesPodOperatorSystem(unittest.TestCase):
    def get_current_task_name(self):
        # reverse test name to make pod name unique (it has limited length)
        return "_" + unittest.TestCase.id(self).replace(".", "_")[::-1]

    def setUp(self):
        self.maxDiff = None
        self.api_client = ApiClient()
        self.expected_pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": mock.ANY,
                "annotations": {},
                "labels": {
                    "foo": "bar",
                    "kubernetes_pod_operator": "True",
                    "airflow_version": airflow_version.replace("+", "-"),
                    "airflow_kpo_in_cluster": "False",
                    "run_id": "manual__2016-01-01T0100000100-da4d1ce7b",
                    "dag_id": "dag",
                    "task_id": "task",
                    "try_number": "1",
                },
            },
            "spec": {
                "affinity": {},
                "containers": [
                    {
                        "image": "ubuntu:16.04",
                        "args": ["echo 10"],
                        "command": ["bash", "-cx"],
                        "env": [],
                        "envFrom": [],
                        "name": "base",
                        "ports": [],
                        "volumeMounts": [],
                    }
                ],
                "hostNetwork": False,
                "imagePullSecrets": [],
                "initContainers": [],
                "nodeSelector": {},
                "restartPolicy": "Never",
                "securityContext": {},
                "tolerations": [],
                "volumes": [],
            },
        }

    def tearDown(self):
        hook = KubernetesHook(conn_id=None, in_cluster=False)
        client = hook.core_v1_client
        client.delete_collection_namespaced_pod(namespace="default")
