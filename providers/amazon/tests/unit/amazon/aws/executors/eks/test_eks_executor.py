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

import importlib
import os
import sys
import time
from contextlib import nullcontext
from unittest import mock

import pytest
from kubernetes.client import ApiClient, CoreV1Api, models as k8s
from kubernetes.client.rest import ApiException

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.eks import AwsEksExecutor, _client_factory
from airflow.providers.amazon.aws.executors.eks.eks_executor import (
    _ASYNC_CLIENT_FACTORY_PATH,
    _CLIENT_FACTORY_PATH,
)
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import AirflowKubernetesScheduler
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, conf
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

EKS_EXECUTOR_MODULE = "airflow.providers.amazon.aws.executors.eks.eks_executor"
EKS_EXECUTOR_PATH = f"{EKS_EXECUTOR_MODULE}.AwsEksExecutor"
LOCAL_KUBERNETES_EXECUTOR_PATH = (
    "airflow.providers.cncf.kubernetes.executors.local_kubernetes_executor.LocalKubernetesExecutor"
)
WORKER_IMAGE = "my-account.dkr.ecr.us-east-1.amazonaws.com/airflow:3"
CLIENT_FACTORY_ENV_VAR = "AIRFLOW__KUBERNETES_EXECUTOR__CLIENT_FACTORY"
REQUIRES_MULTI_TEAM = pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="Multi-team requires Airflow 3.2+")
TEAMS = [
    pytest.param(None, "AIRFLOW__", id="global"),
    pytest.param(
        "team_a",
        "AIRFLOW__TEAM_A___",
        id="team",
        marks=REQUIRES_MULTI_TEAM,
    ),
]


def _team_kwargs(team_name: str | None) -> dict:
    return {"team_name": team_name} if team_name else {}


@pytest.fixture(autouse=True)
def eks_config():
    with mock.patch.dict(os.environ), conf_vars({("aws_eks_executor", "cluster_name"): "test-eks-cluster"}):
        for prefix in ("AIRFLOW__", "AIRFLOW__TEAM_A___"):
            for key in ("CLIENT_FACTORY", "ASYNC_CLIENT_FACTORY"):
                os.environ.pop(f"{prefix}KUBERNETES_EXECUTOR__{key}", None)
        yield


class TestAwsEksExecutor:
    @conf_vars({("aws_eks_executor", "cluster_name"): None})
    def test_missing_cluster_name_raises(self):
        with pytest.raises(
            ValueError, match=r"\[aws_eks_executor\] cluster_name.*AIRFLOW__AWS_EKS_EXECUTOR__CLUSTER_NAME"
        ):
            AwsEksExecutor()

    @pytest.mark.parametrize(("team_name", "env_prefix"), TEAMS)
    def test_sets_client_factories_when_unset(self, team_name, env_prefix):
        AwsEksExecutor(**_team_kwargs(team_name))

        assert {k: v for k, v in os.environ.items() if k.endswith("CLIENT_FACTORY")} == {
            f"{env_prefix}KUBERNETES_EXECUTOR__CLIENT_FACTORY": _CLIENT_FACTORY_PATH,
            f"{env_prefix}KUBERNETES_EXECUTOR__ASYNC_CLIENT_FACTORY": _ASYNC_CLIENT_FACTORY_PATH,
        }
        team_kwargs = _team_kwargs(team_name)
        assert (
            conf.getimport("kubernetes_executor", "client_factory", **team_kwargs)
            is _client_factory._get_eks_kube_client
        )
        assert (
            conf.getimport("kubernetes_executor", "async_client_factory", **team_kwargs)
            is _client_factory._get_eks_async_kube_client
        )

    @conf_vars({("kubernetes_executor", "client_factory"): _CLIENT_FACTORY_PATH})
    def test_accepts_already_matching_client_factory(self):
        AwsEksExecutor()

        assert CLIENT_FACTORY_ENV_VAR not in os.environ

    @pytest.mark.parametrize("key", ["client_factory", "async_client_factory"])
    @pytest.mark.parametrize(("team_name", "env_prefix"), TEAMS)
    def test_conflicting_client_factory_raises(self, monkeypatch, team_name, env_prefix, key):
        monkeypatch.setenv(f"{env_prefix}KUBERNETES_EXECUTOR__{key.upper()}", "my_company.build_client")

        with pytest.raises(ValueError, match=rf"{key} itself.*my_company.build_client"):
            AwsEksExecutor(**_team_kwargs(team_name))

    @pytest.mark.parametrize(
        ("executors", "team_name", "conflict"),
        [
            pytest.param(
                f"KubernetesExecutor,{EKS_EXECUTOR_PATH}",
                None,
                "KubernetesExecutor in the global scope",
                id="both-global",
            ),
            pytest.param(
                f"{LOCAL_KUBERNETES_EXECUTOR_PATH},{EKS_EXECUTOR_PATH}",
                None,
                "LocalKubernetesExecutor in the global scope",
                id="hybrid-global",
            ),
            pytest.param(
                f"LocalExecutor;team_a=KubernetesExecutor,{EKS_EXECUTOR_PATH}",
                "team_a",
                "KubernetesExecutor in team team_a",
                id="both-same-team",
                marks=REQUIRES_MULTI_TEAM,
            ),
            pytest.param(
                f"KubernetesExecutor;team_a={EKS_EXECUTOR_PATH}",
                "team_a",
                None,
                id="global-kubernetes-team-eks",
                marks=REQUIRES_MULTI_TEAM,
            ),
            pytest.param(EKS_EXECUTOR_PATH, None, None, id="eks-alone"),
        ],
    )
    @pytest.mark.usefixtures("clean_executor_loader")
    def test_refuses_kubernetes_executor_in_same_scope(self, executors, team_name, conflict):
        expectation = (
            pytest.raises(ValueError, match=rf"alongside {conflict}\..*issues/74844")
            if conflict
            else nullcontext()
        )
        with conf_vars({("core", "executor"): executors, ("core", "multi_team"): "True"}), expectation:
            AwsEksExecutor(**_team_kwargs(team_name))

    @pytest.mark.parametrize("check_health", [True, False])
    @mock.patch.object(KubernetesExecutor, "start", autospec=True)
    def test_start_checks_cluster_access_unless_disabled(self, mock_start, check_health):
        executor = AwsEksExecutor()
        executor.kube_client = mock.MagicMock(spec=CoreV1Api)
        executor.kube_scheduler = mock.MagicMock(spec=AirflowKubernetesScheduler)

        with conf_vars({("aws_eks_executor", "check_health_on_startup"): str(check_health)}):
            executor.start()

        mock_start.assert_called_once_with(executor)
        assert executor.kube_client.list_namespaced_pod.call_args_list == (
            [mock.call(executor.kube_config.kube_namespace, limit=1)] if check_health else []
        )

    @mock.patch.object(KubernetesExecutor, "start", autospec=True)
    def test_start_fails_when_cluster_rejects_the_executor(self, mock_start):
        executor = AwsEksExecutor()
        executor.kube_client = mock.MagicMock(spec=CoreV1Api)
        executor.kube_scheduler = mock.MagicMock(spec=AirflowKubernetesScheduler)
        executor.kube_client.list_namespaced_pod.side_effect = ApiException(status=401, reason="Unauthorized")

        with pytest.raises(RuntimeError, match="401 Unauthorized"):
            executor.start()

    @pytest.mark.parametrize(
        "image_source", ["global-template", "task-template", "pod-override", "repo-tag", "none"]
    )
    @pytest.mark.parametrize(("team_name", "env_prefix"), TEAMS)
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client._get_executor_kube_client")
    def test_task_without_worker_image_fails_before_pod_creation(
        self, mock_get_kube_client, mock_watcher, monkeypatch, tmp_path, team_name, env_prefix, image_source
    ):
        template = tmp_path / "pod_template.yaml"
        template.write_text(
            f"kind: Pod\nspec:\n  containers:\n    - name: base\n      image: {WORKER_IMAGE}\n"
        )
        repository, tag = WORKER_IMAGE.rsplit(":", 1)
        env = {
            "global-template": {"POD_TEMPLATE_FILE": str(template)},
            "repo-tag": {"WORKER_CONTAINER_REPOSITORY": repository, "WORKER_CONTAINER_TAG": tag},
        }.get(image_source, {})
        for key, value in env.items():
            monkeypatch.setenv(f"{env_prefix}KUBERNETES_EXECUTOR__{key}", value)
        executor_config = {
            "task-template": {"pod_template_file": str(template)},
            "pod-override": {
                "pod_override": k8s.V1Pod(
                    spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base", image=WORKER_IMAGE)])
                )
            },
        }.get(image_source)
        kube_client = mock_get_kube_client.return_value
        kube_client.api_client = ApiClient()
        key = TaskInstanceKey("dag", "task", "run_id", 1)

        executor = AwsEksExecutor(**_team_kwargs(team_name))
        executor.job_id = 5
        executor._last_completed_pod_adoption = time.monotonic()
        with conf_vars({("aws_eks_executor", "check_health_on_startup"): "False"}):
            executor.start()
        try:
            executor.running.add(key)
            executor.execute_async(
                key=key, command=["airflow", "tasks", "run", "dag", "task"], executor_config=executor_config
            )
            executor.sync()
        finally:
            executor.end()

        if image_source != "none":
            pod = kube_client.create_namespaced_pod.call_args.kwargs["body"]
            assert pod["spec"]["containers"][0]["image"] == WORKER_IMAGE
            assert executor.event_buffer[key][0] == TaskInstanceState.QUEUED
        else:
            kube_client.create_namespaced_pod.assert_not_called()
            state, info = executor.event_buffer[key]
            assert state == TaskInstanceState.FAILED
            section = f"team {team_name}'s [kubernetes_executor]" if team_name else "[kubernetes_executor]"
            assert info.reason == (
                "Task dag.task has no worker image, so its pod was not created. Set an image in one of three "
                f"ways: on the base container of the pod template ({section} pod_template_file, or the "
                "task's executor_config pod_template_file), in the task's executor_config pod_override, or "
                f"with {section} worker_container_repository and worker_container_tag."
            )


class TestCncfKubernetesRequirement:
    def test_import_fails_without_cncf_kubernetes(self, monkeypatch):
        monkeypatch.delitem(sys.modules, EKS_EXECUTOR_MODULE)
        monkeypatch.setitem(
            sys.modules, "airflow.providers.cncf.kubernetes.executors.kubernetes_executor", None
        )

        with pytest.raises(AirflowOptionalProviderFeatureException, match=">=10.24.0"):
            importlib.import_module(EKS_EXECUTOR_MODULE)

    @pytest.mark.parametrize(
        "provider_info",
        [
            pytest.param({"config": {"kubernetes_executor": {"options": {}}}}, id="no-client-factory"),
            pytest.param({}, id="no-config"),
        ],
    )
    @mock.patch("airflow.providers.cncf.kubernetes.get_provider_info.get_provider_info", autospec=True)
    def test_import_fails_with_cncf_kubernetes_without_client_factory(
        self, mock_get_provider_info, monkeypatch, provider_info
    ):
        mock_get_provider_info.return_value = provider_info
        monkeypatch.delitem(sys.modules, EKS_EXECUTOR_MODULE)

        with pytest.raises(AirflowOptionalProviderFeatureException, match=">=10.24.0"):
            importlib.import_module(EKS_EXECUTOR_MODULE)
