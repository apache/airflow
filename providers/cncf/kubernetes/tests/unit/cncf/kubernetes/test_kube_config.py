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

from unittest import mock

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.providers.cncf.kubernetes.kube_config import KubeConfig

from tests_common.test_utils.config import conf_vars

BASE_K8S_CONFIG = {
    ("kubernetes_executor", "pod_template_file"): "",
    ("kubernetes_executor", "delete_worker_pods"): "True",
    ("kubernetes_executor", "delete_worker_pods_on_failure"): "False",
    ("kubernetes_executor", "worker_pods_creation_batch_size"): "1",
    ("kubernetes_executor", "worker_container_repository"): "my-repo",
    ("kubernetes_executor", "worker_container_tag"): "latest",
    ("kubernetes_executor", "namespace"): "default",
    ("kubernetes_executor", "multi_namespace_mode"): "False",
    ("kubernetes_executor", "kube_client_request_args"): "{}",
    ("kubernetes_executor", "delete_option_kwargs"): "{}",
    ("core", "parallelism"): "32",
}


class TestKubeConfigInit:
    @conf_vars(BASE_K8S_CONFIG)
    def test_basic_attributes_are_set(self):
        kube_config = KubeConfig()
        assert kube_config.delete_worker_pods is True
        assert kube_config.delete_worker_pods_on_failure is False
        assert kube_config.worker_pods_creation_batch_size == 1
        assert kube_config.parallelism == 32
        assert kube_config.kube_namespace == "default"

    @conf_vars(BASE_K8S_CONFIG)
    def test_kube_image_built_from_repo_and_tag(self):
        kube_config = KubeConfig()
        assert kube_config.kube_image == "my-repo:latest"

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "worker_container_repository"): "",
            ("kubernetes_executor", "worker_container_tag"): "",
        }
    )
    def test_kube_image_is_none_when_repo_or_tag_missing(self):
        kube_config = KubeConfig()
        assert kube_config.kube_image is None

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "worker_container_repository"): "repo",
            ("kubernetes_executor", "worker_container_tag"): "",
        }
    )
    def test_kube_image_is_none_when_tag_missing(self):
        kube_config = KubeConfig()
        assert kube_config.kube_image is None

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): "",
        }
    )
    def test_empty_fatal_reasons_list(self):
        kube_config = KubeConfig()
        assert kube_config.worker_pod_pending_fatal_container_state_reasons == []

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            (
                "kubernetes_executor",
                "worker_pod_pending_fatal_container_state_reasons",
            ): "ErrImagePull,OOMKilled",
        }
    )
    def test_fatal_reasons_are_parsed_into_list(self):
        kube_config = KubeConfig()
        assert kube_config.worker_pod_pending_fatal_container_state_reasons == ["ErrImagePull", "OOMKilled"]

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            (
                "kubernetes_executor",
                "worker_pod_pending_fatal_container_state_reasons",
            ): " ErrImagePull , OOMKilled ",
        }
    )
    def test_fatal_reasons_are_stripped(self):
        kube_config = KubeConfig()
        assert kube_config.worker_pod_pending_fatal_container_state_reasons == ["ErrImagePull", "OOMKilled"]


class TestKubeConfigMultiNamespaceMode:
    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "multi_namespace_mode"): "True",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): "",
        }
    )
    def test_multi_namespace_mode_without_list(self):
        kube_config = KubeConfig()
        assert kube_config.multi_namespace_mode is True
        assert kube_config.multi_namespace_mode_namespace_list is None

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "multi_namespace_mode"): "True",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): "ns1,ns2,ns3",
        }
    )
    def test_multi_namespace_mode_with_list(self):
        kube_config = KubeConfig()
        assert kube_config.multi_namespace_mode is True
        assert kube_config.multi_namespace_mode_namespace_list == ["ns1", "ns2", "ns3"]

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "multi_namespace_mode"): "False",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): "ns1,ns2",
        }
    )
    def test_namespace_list_ignored_when_multi_namespace_mode_off(self):
        kube_config = KubeConfig()
        assert kube_config.multi_namespace_mode is False
        assert kube_config.multi_namespace_mode_namespace_list is None


class TestKubeConfigClientRequestArgs:
    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "kube_client_request_args"): '{"_request_timeout": 60}',
        }
    )
    def test_valid_dict_is_accepted(self):
        kube_config = KubeConfig()
        assert kube_config.kube_client_request_args == {"_request_timeout": 60}

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "kube_client_request_args"): '{"_request_timeout": [30, 60]}',
        }
    )
    def test_list_timeout_is_converted_to_tuple(self):
        kube_config = KubeConfig()
        assert kube_config.kube_client_request_args["_request_timeout"] == (30, 60)

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "kube_client_request_args"): "[]",
        }
    )
    def test_non_dict_raises_airflow_config_exception(self):
        with pytest.raises(AirflowConfigException, match="expected a JSON dict"):
            KubeConfig()

    @conf_vars(BASE_K8S_CONFIG)
    def test_empty_dict_is_accepted(self):
        kube_config = KubeConfig()
        assert kube_config.kube_client_request_args == {}


class TestKubeConfigDeleteOptionKwargs:
    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "delete_option_kwargs"): '{"grace_period_seconds": 30}',
        }
    )
    def test_valid_dict_is_accepted(self):
        kube_config = KubeConfig()
        assert kube_config.delete_option_kwargs == {"grace_period_seconds": 30}

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "delete_option_kwargs"): '"not-a-dict"',
        }
    )
    def test_non_dict_raises_airflow_config_exception(self):
        with pytest.raises(AirflowConfigException, match="expected a JSON dict"):
            KubeConfig()

    @conf_vars(BASE_K8S_CONFIG)
    def test_empty_dict_is_accepted(self):
        kube_config = KubeConfig()
        assert kube_config.delete_option_kwargs == {}


class TestKubeConfigWithExecutorConf:
    @conf_vars(BASE_K8S_CONFIG)
    def test_executor_conf_is_used_when_provided(self):
        mock_conf = mock.MagicMock()
        mock_conf.get.side_effect = lambda section, key, **kwargs: {
            ("core", "dags_folder"): "/custom/dags",
            ("kubernetes_executor", "pod_template_file"): "",
            ("kubernetes_executor", "worker_container_repository"): "custom-repo",
            ("kubernetes_executor", "worker_container_tag"): "v1",
            ("kubernetes_executor", "namespace"): "custom-ns",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): "",
        }.get((section, key), kwargs.get("fallback", ""))
        mock_conf.getboolean.side_effect = lambda section, key, **kwargs: {
            ("kubernetes_executor", "delete_worker_pods"): True,
            ("kubernetes_executor", "delete_worker_pods_on_failure"): False,
            ("kubernetes_executor", "multi_namespace_mode"): False,
            ("kubernetes_executor", "async_pod_creation"): False,
        }.get((section, key), kwargs.get("fallback", False))
        mock_conf.getint.side_effect = lambda section, key, **kwargs: {
            ("core", "parallelism"): 16,
            ("kubernetes_executor", "worker_pods_creation_batch_size"): 2,
            ("kubernetes_executor", "pod_creation_max_concurrency"): 0,
        }.get((section, key), kwargs.get("fallback", 0))
        mock_conf.getjson.side_effect = lambda section, key, **kwargs: kwargs.get("fallback", {})
        mock_conf.get.return_value = mock_conf.get.side_effect

        kube_config = KubeConfig(executor_conf=mock_conf)

        # _conf should be the provided executor_conf, not the global one
        assert kube_config._conf is mock_conf

    @conf_vars(BASE_K8S_CONFIG)
    def test_global_conf_used_when_no_executor_conf(self):
        kube_config = KubeConfig()
        from airflow.providers.common.compat.sdk import conf

        assert kube_config._conf is conf


class TestKubeConfigExecutorNamespace:
    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "namespace"): "my-namespace",
        }
    )
    def test_executor_namespace_matches_kube_namespace(self):
        kube_config = KubeConfig()
        assert kube_config.executor_namespace == "my-namespace"

    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "namespace"): "",
        }
    )
    def test_executor_namespace_defaults_to_default(self):
        kube_config = KubeConfig()
        assert kube_config.executor_namespace == "default"


class TestKubeConfigAsyncPodCreation:
    @conf_vars(
        {
            **BASE_K8S_CONFIG,
            ("kubernetes_executor", "async_pod_creation"): "True",
            ("kubernetes_executor", "pod_creation_max_concurrency"): "10",
        }
    )
    def test_async_pod_creation_and_concurrency(self):
        kube_config = KubeConfig()
        assert kube_config.async_pod_creation is True
        assert kube_config.pod_creation_max_concurrency == 10

    @conf_vars(BASE_K8S_CONFIG)
    def test_async_pod_creation_defaults_to_false(self):
        kube_config = KubeConfig()
        assert kube_config.async_pod_creation is False

    @conf_vars(BASE_K8S_CONFIG)
    def test_pod_creation_max_concurrency_defaults_to_zero(self):
        kube_config = KubeConfig()
        assert kube_config.pod_creation_max_concurrency == 0
