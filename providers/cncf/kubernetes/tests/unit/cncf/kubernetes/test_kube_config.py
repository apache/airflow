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


class TestKubeConfigInit:
    @conf_vars(
        {
            ("kubernetes_executor", "delete_worker_pods"): "True",
            ("kubernetes_executor", "delete_worker_pods_on_failure"): "False",
            ("kubernetes_executor", "worker_pods_creation_batch_size"): "1",
            ("core", "parallelism"): "32",
            ("kubernetes_executor", "namespace"): "default",
        }
    )
    def test_basic_attributes_are_set(self):
        kube_config = KubeConfig()
        assert kube_config.delete_worker_pods is True
        assert kube_config.delete_worker_pods_on_failure is False
        assert kube_config.worker_pods_creation_batch_size == 1
        assert kube_config.parallelism == 32
        assert kube_config.kube_namespace == "default"


class TestKubeImage:
    @conf_vars(
        {
            ("kubernetes_executor", "worker_container_repository"): "apache/airflow",
            ("kubernetes_executor", "worker_container_tag"): "3.0.0",
        }
    )
    def test_kube_image_combines_repository_and_tag(self):
        assert KubeConfig().kube_image == "apache/airflow:3.0.0"

    @conf_vars(
        {
            ("kubernetes_executor", "worker_container_repository"): "",
            ("kubernetes_executor", "worker_container_tag"): "",
        }
    )
    def test_kube_image_is_none_without_repository_and_tag(self):
        assert KubeConfig().kube_image is None


class TestFatalContainerStateReasons:
    @conf_vars({("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): ""})
    def test_empty_config_means_no_fatal_reasons(self):
        assert KubeConfig().worker_pod_pending_fatal_container_state_reasons == []

    @conf_vars(
        {
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): (
                "CreateContainerConfigError, ErrImagePull ,InvalidImageName"
            )
        }
    )
    def test_reasons_are_split_and_stripped(self):
        assert KubeConfig().worker_pod_pending_fatal_container_state_reasons == [
            "CreateContainerConfigError",
            "ErrImagePull",
            "InvalidImageName",
        ]


class TestMultiNamespaceMode:
    @conf_vars(
        {
            ("kubernetes_executor", "multi_namespace_mode"): "True",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): "ns-a,ns-b",
        }
    )
    def test_namespace_list_is_parsed_when_enabled(self):
        assert KubeConfig().multi_namespace_mode_namespace_list == ["ns-a", "ns-b"]

    @conf_vars(
        {
            ("kubernetes_executor", "multi_namespace_mode"): "False",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): "ns-a,ns-b",
        }
    )
    def test_namespace_list_is_none_when_disabled(self):
        assert KubeConfig().multi_namespace_mode_namespace_list is None


class TestKubeClientRequestArgs:
    @conf_vars({("kubernetes_executor", "kube_client_request_args"): '{"_request_timeout": [60, 360]}'})
    def test_request_timeout_list_is_converted_to_tuple(self):
        assert KubeConfig().kube_client_request_args["_request_timeout"] == (60, 360)

    @conf_vars({("kubernetes_executor", "kube_client_request_args"): '["not", "a", "dict"]'})
    def test_non_dict_request_args_are_rejected(self):
        with pytest.raises(AirflowConfigException, match="kube_client_request_args"):
            KubeConfig()


class TestDeleteOptionKwargs:
    @conf_vars({("kubernetes_executor", "delete_option_kwargs"): '{"grace_period_seconds": 10}'})
    def test_delete_option_kwargs_dict_is_accepted(self):
        assert KubeConfig().delete_option_kwargs == {"grace_period_seconds": 10}

    @conf_vars({("kubernetes_executor", "delete_option_kwargs"): "[1, 2]"})
    def test_non_dict_delete_option_kwargs_are_rejected(self):
        with pytest.raises(AirflowConfigException, match="delete_option_kwargs"):
            KubeConfig()


class TestNamespaces:
    @conf_vars({("kubernetes_executor", "namespace"): "airflow-workers"})
    def test_executor_namespace_follows_configured_namespace(self):
        config = KubeConfig()

        assert config.kube_namespace == "airflow-workers"
        assert config.executor_namespace == "airflow-workers"

    @conf_vars({("kubernetes_executor", "namespace"): ""})
    def test_executor_namespace_defaults_to_default(self):
        config = KubeConfig()
        assert config.executor_namespace == "default"


class TestKubeConfigWithExecutorConf:
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
        assert kube_config._conf is mock_conf

    def test_global_conf_used_when_no_executor_conf(self):
        kube_config = KubeConfig()
        from airflow.providers.common.compat.sdk import conf

        assert kube_config._conf is conf


class TestKubeConfigAsyncPodCreation:
    @conf_vars(
        {
            ("kubernetes_executor", "async_pod_creation"): "True",
            ("kubernetes_executor", "pod_creation_max_concurrency"): "10",
        }
    )
    def test_async_pod_creation_and_concurrency(self):
        kube_config = KubeConfig()
        assert kube_config.async_pod_creation is True
        assert kube_config.pod_creation_max_concurrency == 10

    def test_async_pod_creation_defaults_to_false(self):
        kube_config = KubeConfig()
        assert kube_config.async_pod_creation is False

    def test_pod_creation_max_concurrency_defaults_to_zero(self):
        kube_config = KubeConfig()
        assert kube_config.pod_creation_max_concurrency == 0
