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
from kubernetes.client.rest import ApiException

from airflow.providers.amazon.aws.executors.eks import AwsEksExecutor, _client_factory
from airflow.providers.amazon.aws.executors.eks.eks_executor import (
    _ASYNC_CLIENT_FACTORY_PATH,
    _CLIENT_FACTORY_PATH,
    MIN_CNCF_KUBERNETES_VERSION,
)
from airflow.providers.amazon.get_provider_info import get_provider_info
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, conf

from tests_common.test_utils.config import conf_vars

EKS_EXECUTOR_MODULE = "airflow.providers.amazon.aws.executors.eks.eks_executor"
CLIENT_FACTORY_ENV_VAR = "AIRFLOW__KUBERNETES_EXECUTOR__CLIENT_FACTORY"
ASYNC_CLIENT_FACTORY_ENV_VAR = "AIRFLOW__KUBERNETES_EXECUTOR__ASYNC_CLIENT_FACTORY"


@pytest.fixture(autouse=True)
def isolated_environ():
    with mock.patch.dict(os.environ):
        os.environ.pop(CLIENT_FACTORY_ENV_VAR, None)
        os.environ.pop(ASYNC_CLIENT_FACTORY_ENV_VAR, None)
        yield


class TestAwsEksExecutor:
    def test_is_a_kubernetes_executor(self):
        assert issubclass(AwsEksExecutor, KubernetesExecutor)

    def test_missing_cluster_name_raises(self):
        with conf_vars({("aws_eks_executor", "cluster_name"): None}):
            with pytest.raises(ValueError, match=r"\[aws_eks_executor\] cluster_name"):
                AwsEksExecutor()

    @conf_vars({("aws_eks_executor", "cluster_name"): "test-eks-cluster"})
    def test_sets_client_factories_when_unset(self):
        AwsEksExecutor()

        assert os.environ[CLIENT_FACTORY_ENV_VAR] == _CLIENT_FACTORY_PATH
        assert os.environ[ASYNC_CLIENT_FACTORY_ENV_VAR] == _ASYNC_CLIENT_FACTORY_PATH

    @conf_vars({("aws_eks_executor", "cluster_name"): "test-eks-cluster"})
    def test_client_factory_paths_resolve_to_the_factories(self):
        AwsEksExecutor()

        # cncf.kubernetes resolves the factories this way in every process, including the watcher.
        assert conf.getimport("kubernetes_executor", "client_factory") is _client_factory._get_eks_kube_client
        assert (
            conf.getimport("kubernetes_executor", "async_client_factory")
            is _client_factory._get_eks_async_kube_client
        )

    @conf_vars(
        {
            ("aws_eks_executor", "cluster_name"): "test-eks-cluster",
            ("kubernetes_executor", "client_factory"): _CLIENT_FACTORY_PATH,
        }
    )
    def test_accepts_already_matching_client_factory(self):
        AwsEksExecutor()

        assert CLIENT_FACTORY_ENV_VAR not in os.environ

    @pytest.mark.parametrize("key", ["client_factory", "async_client_factory"])
    def test_conflicting_client_factory_raises(self, key):
        with conf_vars(
            {
                ("aws_eks_executor", "cluster_name"): "test-eks-cluster",
                ("kubernetes_executor", key): "my_company.kubernetes.build_client",
            }
        ):
            with pytest.raises(ValueError, match=rf"{key} itself.*my_company.kubernetes.build_client"):
                AwsEksExecutor()

    def test_does_not_support_multi_team(self):
        assert AwsEksExecutor.supports_multi_team is False

    @conf_vars({("aws_eks_executor", "cluster_name"): "test-eks-cluster"})
    @mock.patch.object(KubernetesExecutor, "start", autospec=True)
    def test_start_checks_cluster_access(self, mock_start):
        executor = AwsEksExecutor()
        executor.kube_client = mock.MagicMock()

        executor.start()

        mock_start.assert_called_once_with(executor)
        executor.kube_client.list_namespaced_pod.assert_called_once_with(
            executor.kube_config.kube_namespace, limit=1
        )

    @conf_vars({("aws_eks_executor", "cluster_name"): "test-eks-cluster"})
    @mock.patch.object(KubernetesExecutor, "start", autospec=True)
    def test_start_fails_when_cluster_rejects_the_executor(self, mock_start):
        executor = AwsEksExecutor()
        executor.kube_client = mock.MagicMock()
        executor.kube_client.list_namespaced_pod.side_effect = ApiException(status=401, reason="Unauthorized")

        with pytest.raises(RuntimeError, match="401 Unauthorized"):
            executor.start()

    @conf_vars(
        {
            ("aws_eks_executor", "cluster_name"): "test-eks-cluster",
            ("aws_eks_executor", "check_health_on_startup"): "False",
        }
    )
    @mock.patch.object(KubernetesExecutor, "start", autospec=True)
    def test_start_skips_health_check_when_disabled(self, mock_start):
        executor = AwsEksExecutor()
        executor.kube_client = mock.MagicMock()

        executor.start()

        mock_start.assert_called_once_with(executor)
        executor.kube_client.list_namespaced_pod.assert_not_called()


class TestCncfKubernetesRequirement:
    def test_import_fails_without_cncf_kubernetes(self, monkeypatch):
        monkeypatch.delitem(sys.modules, EKS_EXECUTOR_MODULE)
        monkeypatch.setitem(
            sys.modules, "airflow.providers.cncf.kubernetes.executors.kubernetes_executor", None
        )

        with pytest.raises(AirflowOptionalProviderFeatureException, match=MIN_CNCF_KUBERNETES_VERSION):
            importlib.import_module(EKS_EXECUTOR_MODULE)

    @pytest.mark.parametrize(
        "provider_info",
        [
            pytest.param({"config": {"kubernetes_executor": {"options": {}}}}, id="no-client-factory"),
            pytest.param({}, id="no-config"),
        ],
    )
    def test_import_fails_with_cncf_kubernetes_without_client_factory(self, monkeypatch, provider_info):
        monkeypatch.delitem(sys.modules, EKS_EXECUTOR_MODULE)

        with mock.patch(
            "airflow.providers.cncf.kubernetes.get_provider_info.get_provider_info",
            return_value=provider_info,
        ):
            with pytest.raises(
                AirflowOptionalProviderFeatureException,
                match=rf">={MIN_CNCF_KUBERNETES_VERSION}.*The installed version is",
            ):
                importlib.import_module(EKS_EXECUTOR_MODULE)


def test_executor_is_declared_in_provider_info():
    assert (
        "airflow.providers.amazon.aws.executors.eks.eks_executor.AwsEksExecutor"
        in get_provider_info()["executors"]
    )
