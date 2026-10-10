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
from unittest import mock

import pytest
from kubernetes.client import CoreV1Api
from kubernetes.client.rest import ApiException

from airflow.providers.amazon.aws.executors.eks import AwsEksExecutor, _client_factory
from airflow.providers.amazon.aws.executors.eks.eks_executor import (
    _ASYNC_CLIENT_FACTORY_PATH,
    _CLIENT_FACTORY_PATH,
)
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, conf

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

EKS_EXECUTOR_MODULE = "airflow.providers.amazon.aws.executors.eks.eks_executor"
CLIENT_FACTORY_ENV_VAR = "AIRFLOW__KUBERNETES_EXECUTOR__CLIENT_FACTORY"
TEAMS = [
    pytest.param(None, "AIRFLOW__", id="global"),
    pytest.param(
        "team_a",
        "AIRFLOW__TEAM_A___",
        id="team",
        marks=pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="Multi-team requires Airflow 3.2+"),
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

    @pytest.mark.parametrize("check_health", [True, False])
    @mock.patch.object(KubernetesExecutor, "start", autospec=True)
    def test_start_checks_cluster_access_unless_disabled(self, mock_start, check_health):
        executor = AwsEksExecutor()
        executor.kube_client = mock.MagicMock(spec=CoreV1Api)

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
        executor.kube_client.list_namespaced_pod.side_effect = ApiException(status=401, reason="Unauthorized")

        with pytest.raises(RuntimeError, match="401 Unauthorized"):
            executor.start()


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
