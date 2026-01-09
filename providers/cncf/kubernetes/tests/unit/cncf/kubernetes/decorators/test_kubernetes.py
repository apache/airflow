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

import base64
import pickle

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from unit.cncf.kubernetes.decorators.test_kubernetes_commons import TestKubernetesDecoratorsBase

XCOM_IMAGE = "XCOM_IMAGE"


class TestKubernetesDecorator(TestKubernetesDecoratorsBase):
    def test_basic_kubernetes(self):
        """Test basic proper KubernetesPodOperator creation from @task.kubernetes decorator"""
        with self.dag_maker:

            @task.kubernetes(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def f():
                import random

                return [random.random() for _ in range(100)]

            k8s_task = f()

        self.execute_task(k8s_task)

        self.mock_hook.assert_called_once_with(
            conn_id="kubernetes_default",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        assert self.mock_create_pod.call_count == 1

        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers
        assert len(containers) == 1
        assert containers[0].command[0] == "bash"
        assert len(containers[0].args) == 0
        assert containers[0].env[0].name == "__PYTHON_SCRIPT"
        assert containers[0].env[0].value
        assert containers[0].env[1].name == "__PYTHON_INPUT"

        # Ensure we pass input through a b64 encoded env var
        decoded_input = pickle.loads(base64.b64decode(containers[0].env[1].value))
        assert decoded_input == {"args": [], "kwargs": {}}

    @pytest.mark.asyncio
    def test_kubernetes_with_input_output(self):
        """Verify @task.kubernetes will run XCom container if do_xcom_push is set."""
        with self.dag_maker:

            @task.kubernetes(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def f(arg1, arg2, kwarg1=None, kwarg2=None):
                return {"key1": "value1", "key2": "value2"}

            k8s_task = f.override(task_id="my_task_id", do_xcom_push=True)("arg1", "arg2", kwarg1="kwarg1")

        self.mock_hook.return_value.get_xcom_sidecar_container_image.return_value = XCOM_IMAGE
        self.mock_hook.return_value.get_xcom_sidecar_container_resources.return_value = {
            "requests": {"cpu": "1m", "memory": "10Mi"},
            "limits": {"cpu": "1m", "memory": "50Mi"},
        }

        self.execute_task(k8s_task)
        assert self.mock_create_pod.call_count == 1

        self.mock_hook.assert_called_once_with(
            conn_id="kubernetes_default",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        assert self.mock_hook.return_value.get_xcom_sidecar_container_image.call_count == 1
        assert self.mock_hook.return_value.get_xcom_sidecar_container_resources.call_count == 1

        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers

        # First container is Python script
        assert len(containers) == 2
        assert containers[0].command[0] == "bash"
        assert len(containers[0].args) == 0

        assert containers[0].env[0].name == "__PYTHON_SCRIPT"
        assert containers[0].env[0].value
        assert containers[0].env[1].name == "__PYTHON_INPUT"
        assert containers[0].env[1].value

        # Ensure we pass input through a b64 encoded env var
        decoded_input = pickle.loads(base64.b64decode(containers[0].env[1].value))
        assert decoded_input == {"args": ("arg1", "arg2"), "kwargs": {"kwarg1": "kwarg1"}}

        # Second container is xcom image
        assert containers[1].image == XCOM_IMAGE
        assert containers[1].volume_mounts[0].mount_path == "/airflow/xcom"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("do_xcom_push", "expected_output", "expects_mkdir"),
        [
            (False, "/dev/null", False),
            (True, "/airflow/xcom/return.json", True),
        ],
        ids=["without_xcom_push", "with_xcom_push"],
    )
    def test_generated_command_respects_do_xcom_push(
        self, do_xcom_push: bool, expected_output: str, expects_mkdir: bool
    ):
        with self.dag_maker:

            @task.kubernetes(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def f():
                return {"key": "value"}

            k8s_task = f.override(task_id="my_task_id", do_xcom_push=do_xcom_push)()

        if do_xcom_push:
            self.mock_hook.return_value.get_xcom_sidecar_container_image.return_value = XCOM_IMAGE
            self.mock_hook.return_value.get_xcom_sidecar_container_resources.return_value = {
                "requests": {"cpu": "1m", "memory": "10Mi"},
                "limits": {"cpu": "1m", "memory": "50Mi"},
            }

        self.execute_task(k8s_task)
        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers
        assert len(containers) == (2 if do_xcom_push else 1)

        generated_command = containers[0].command
        assert generated_command
        assert len(generated_command) >= 3

        bash_command = generated_command[-1]
        assert expected_output in bash_command
        assert ("/airflow/xcom" in bash_command) == expects_mkdir
        if not expects_mkdir:
            assert " && : && " in bash_command
