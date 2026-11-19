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

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.providers.cncf.kubernetes.kube_config import KubeConfig


class TestKubeConfig:
    @pytest.fixture
    def mock_conf(self):
        conf = MagicMock()
        conf.as_dict.return_value = {"core": {"dags_folder": "/dags", "parallelism": 32}}
        conf.get.side_effect = lambda section, key, **kwargs: {
            ("core", "dags_folder"): "/dags",
            ("kubernetes_executor", "pod_template_file"): "/templates/pod.yaml",
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): "CrashLoopBackOff, ErrImagePull",
            ("kubernetes_executor", "worker_container_repository"): "apache/airflow",
            ("kubernetes_executor", "worker_container_tag"): "3.0.0",
            ("kubernetes_executor", "namespace"): "airflow-workers",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): "ns1,ns2",
        }.get((section, key), kwargs.get("fallback", ""))

        conf.getint.side_effect = lambda section, key, **kwargs: {
            ("core", "parallelism"): 32,
            ("kubernetes_executor", "worker_pods_creation_batch_size"): 8,
            ("kubernetes_executor", "pod_creation_max_concurrency"): 16,
        }.get((section, key), kwargs.get("fallback", 0))

        conf.getboolean.side_effect = lambda section, key, **kwargs: {
            ("kubernetes_executor", "delete_worker_pods"): True,
            ("kubernetes_executor", "delete_worker_pods_on_failure"): False,
            ("kubernetes_executor", "async_pod_creation"): True,
            ("kubernetes_executor", "multi_namespace_mode"): True,
        }.get((section, key), kwargs.get("fallback", False))

        conf.getjson.side_effect = lambda section, key, **kwargs: {
            ("kubernetes_executor", "kube_client_request_args"): {
                "_request_timeout": [10, 30],
                "verify_ssl": True,
            },
            ("kubernetes_executor", "delete_option_kwargs"): {"grace_period_seconds": 15},
        }.get((section, key), kwargs.get("fallback", {}))

        return conf

    def test_kube_config_initialization_with_executor_conf(self, mock_conf):
        kube_cfg = KubeConfig(executor_conf=mock_conf)

        assert kube_cfg.dags_folder == "/dags"
        assert kube_cfg.parallelism == 32
        assert kube_cfg.pod_template_file == "/templates/pod.yaml"
        assert kube_cfg.delete_worker_pods is True
        assert kube_cfg.delete_worker_pods_on_failure is False
        assert kube_cfg.worker_pod_pending_fatal_container_state_reasons == [
            "CrashLoopBackOff",
            "ErrImagePull",
        ]
        assert kube_cfg.worker_pods_creation_batch_size == 8
        assert kube_cfg.async_pod_creation is True
        assert kube_cfg.pod_creation_max_concurrency == 16
        assert kube_cfg.kube_image == "apache/airflow:3.0.0"
        assert kube_cfg.kube_namespace == "airflow-workers"
        assert kube_cfg.executor_namespace == "airflow-workers"
        assert kube_cfg.multi_namespace_mode is True
        assert kube_cfg.multi_namespace_mode_namespace_list == ["ns1", "ns2"]
        # _request_timeout converted from list to tuple
        assert kube_cfg.kube_client_request_args["_request_timeout"] == (10, 30)
        assert kube_cfg.kube_client_request_args["verify_ssl"] is True
        assert kube_cfg.delete_option_kwargs == {"grace_period_seconds": 15}

    def test_kube_config_no_image_when_repo_or_tag_missing(self, mock_conf):
        mock_conf.get.side_effect = lambda section, key, **kwargs: {
            ("core", "dags_folder"): "/dags",
            ("kubernetes_executor", "worker_container_repository"): "",
            ("kubernetes_executor", "worker_container_tag"): "3.0.0",
        }.get((section, key), kwargs.get("fallback", ""))

        kube_cfg = KubeConfig(executor_conf=mock_conf)
        assert kube_cfg.kube_image is None

    def test_kube_config_empty_fatal_container_state_reasons(self, mock_conf):
        mock_conf.get.side_effect = lambda section, key, **kwargs: {
            ("core", "dags_folder"): "/dags",
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): "",
        }.get((section, key), kwargs.get("fallback", ""))

        kube_cfg = KubeConfig(executor_conf=mock_conf)
        assert kube_cfg.worker_pod_pending_fatal_container_state_reasons == []

    def test_kube_config_invalid_request_args_type_raises(self, mock_conf):
        mock_conf.getjson.side_effect = lambda section, key, **kwargs: (
            ["not", "a", "dict"] if key == "kube_client_request_args" else {}
        )
        with pytest.raises(AirflowConfigException, match="'kube_client_request_args' expected a JSON dict"):
            KubeConfig(executor_conf=mock_conf)

    def test_kube_config_invalid_delete_option_kwargs_type_raises(self, mock_conf):
        mock_conf.getjson.side_effect = lambda section, key, **kwargs: (
            "not-a-dict" if key == "delete_option_kwargs" else {}
        )
        with pytest.raises(AirflowConfigException, match="'delete_option_kwargs' expected a JSON dict"):
            KubeConfig(executor_conf=mock_conf)

    @patch("airflow.providers.cncf.kubernetes.kube_config.conf")
    def test_kube_config_fallback_to_global_conf(self, mock_global_conf):
        mock_global_conf.as_dict.return_value = {"core": {"dags_folder": "/dags_global"}}
        mock_global_conf.get.side_effect = lambda section, key, **kwargs: {
            ("core", "dags_folder"): "/dags_global",
            ("kubernetes_executor", "namespace"): "default",
        }.get((section, key), kwargs.get("fallback", ""))
        mock_global_conf.getint.side_effect = lambda section, key, **kwargs: {
            ("core", "parallelism"): 4,
            ("kubernetes_executor", "worker_pods_creation_batch_size"): 2,
        }.get((section, key), kwargs.get("fallback", 0))
        mock_global_conf.getboolean.side_effect = lambda section, key, **kwargs: False
        mock_global_conf.getjson.side_effect = lambda section, key, **kwargs: {}

        kube_cfg = KubeConfig()
        assert kube_cfg._conf == mock_global_conf
        assert kube_cfg.kube_namespace == "default"
        assert kube_cfg.executor_namespace == "default"
