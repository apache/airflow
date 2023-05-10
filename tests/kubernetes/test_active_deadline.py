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

import time
import pytest
from kubernetes import client, config
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


# Create a test class for the KubernetesPodOperator
class TestKubernetesPodOperator:
    @pytest.mark.parametrize("active_deadline_seconds", [10])
    def test_kubernetes_pod_operator_active_deadline_seconds(self, active_deadline_seconds):
        task_id = "test_task"
        image = "busybox"
        cmds = ["sh", "-c", "echo 'hello world' && sleep 20"]
        namespace = "default"

        operator = KubernetesPodOperator(
            task_id=task_id,
            active_deadline_seconds=active_deadline_seconds,
            image=image,
            cmds=cmds,
            namespace=namespace
        )

        operator.execute(context={})

        pod = operator.find_pod()
        pod_name = pod.metadata.name

        k8s_client = client.CoreV1Api()
        config.load_kube_config()

        time.sleep(active_deadline_seconds)

        pod_status = k8s_client.read_namespaced_pod_status(name=pod_name, namespace=namespace)
        phase = pod_status.status.phase

        assert phase == "Failed"

