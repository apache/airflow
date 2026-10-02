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

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.providers.cncf.kubernetes.kube_config import KubeConfig

from tests_common.test_utils.config import conf_vars


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
