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

import warnings
from unittest import mock

import pytest

from airflow.providers.cncf.kubernetes.kube_config import KubeConfig


class TestKubeConfig:
    """Test KubeConfig class."""

    @mock.patch("airflow.providers.cncf.kubernetes.kube_config.conf")
    def test_deprecated_worker_container_repository_warning(self, mock_conf):
        """Test that deprecation warning is issued for worker_container_repository."""
        # Set up mock configuration with deprecated field
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_container_repository"): "my-repo",
            ("kubernetes_executor", "worker_container_tag"): "",
            ("kubernetes_executor", "namespace"): "default",
            ("kubernetes_executor", "pod_template_file"): None,
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): "",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): None,
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
            ("core", "dags_folder"): "/tmp/dags",
        }.get((section, key), fallback)

        mock_conf.getboolean.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "delete_worker_pods"): True,
            ("kubernetes_executor", "delete_worker_pods_on_failure"): False,
            ("kubernetes_executor", "multi_namespace_mode"): False,
        }.get((section, key), fallback)

        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_pods_creation_batch_size"): 1,
            ("core", "parallelism"): 32,
        }.get((section, key), fallback)

        mock_conf.getjson.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
        }.get((section, key), fallback)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            KubeConfig()

            # Check that deprecation warning was issued
            deprecation_warnings = [warning for warning in w if issubclass(warning.category, DeprecationWarning)]
            assert len(deprecation_warnings) == 1
            assert "worker_container_repository" in str(deprecation_warnings[0].message)
            assert "deprecated" in str(deprecation_warnings[0].message)
            assert "pod_template_file" in str(deprecation_warnings[0].message)

    @mock.patch("airflow.providers.cncf.kubernetes.kube_config.conf")
    def test_deprecated_worker_container_tag_warning(self, mock_conf):
        """Test that deprecation warning is issued for worker_container_tag."""
        # Set up mock configuration with deprecated field
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_container_repository"): "",
            ("kubernetes_executor", "worker_container_tag"): "my-tag",
            ("kubernetes_executor", "namespace"): "default",
            ("kubernetes_executor", "pod_template_file"): None,
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): "",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): None,
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
            ("core", "dags_folder"): "/tmp/dags",
        }.get((section, key), fallback)

        mock_conf.getboolean.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "delete_worker_pods"): True,
            ("kubernetes_executor", "delete_worker_pods_on_failure"): False,
            ("kubernetes_executor", "multi_namespace_mode"): False,
        }.get((section, key), fallback)

        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_pods_creation_batch_size"): 1,
            ("core", "parallelism"): 32,
        }.get((section, key), fallback)

        mock_conf.getjson.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
        }.get((section, key), fallback)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            KubeConfig()

            # Check that deprecation warning was issued
            deprecation_warnings = [warning for warning in w if issubclass(warning.category, DeprecationWarning)]
            assert len(deprecation_warnings) == 1
            assert "worker_container_tag" in str(deprecation_warnings[0].message)
            assert "deprecated" in str(deprecation_warnings[0].message)
            assert "pod_template_file" in str(deprecation_warnings[0].message)

    @mock.patch("airflow.providers.cncf.kubernetes.kube_config.conf")
    def test_deprecated_namespace_warning(self, mock_conf):
        """Test that deprecation warning is issued for non-default namespace."""
        # Set up mock configuration with deprecated field
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_container_repository"): "",
            ("kubernetes_executor", "worker_container_tag"): "",
            ("kubernetes_executor", "namespace"): "my-namespace",
            ("kubernetes_executor", "pod_template_file"): None,
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): "",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): None,
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
            ("core", "dags_folder"): "/tmp/dags",
        }.get((section, key), fallback)

        mock_conf.getboolean.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "delete_worker_pods"): True,
            ("kubernetes_executor", "delete_worker_pods_on_failure"): False,
            ("kubernetes_executor", "multi_namespace_mode"): False,
        }.get((section, key), fallback)

        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_pods_creation_batch_size"): 1,
            ("core", "parallelism"): 32,
        }.get((section, key), fallback)

        mock_conf.getjson.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
        }.get((section, key), fallback)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            KubeConfig()

            # Check that deprecation warning was issued
            deprecation_warnings = [warning for warning in w if issubclass(warning.category, DeprecationWarning)]
            assert len(deprecation_warnings) == 1
            assert "namespace" in str(deprecation_warnings[0].message)
            assert "deprecated" in str(deprecation_warnings[0].message)
            assert "pod_template_file" in str(deprecation_warnings[0].message)

    @mock.patch("airflow.providers.cncf.kubernetes.kube_config.conf")
    def test_no_warning_for_default_namespace(self, mock_conf):
        """Test that no deprecation warning is issued for default namespace."""
        # Set up mock configuration with default namespace
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_container_repository"): "",
            ("kubernetes_executor", "worker_container_tag"): "",
            ("kubernetes_executor", "namespace"): "default",
            ("kubernetes_executor", "pod_template_file"): None,
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): "",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): None,
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
            ("core", "dags_folder"): "/tmp/dags",
        }.get((section, key), fallback)

        mock_conf.getboolean.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "delete_worker_pods"): True,
            ("kubernetes_executor", "delete_worker_pods_on_failure"): False,
            ("kubernetes_executor", "multi_namespace_mode"): False,
        }.get((section, key), fallback)

        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_pods_creation_batch_size"): 1,
            ("core", "parallelism"): 32,
        }.get((section, key), fallback)

        mock_conf.getjson.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
        }.get((section, key), fallback)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            KubeConfig()

            # Check that no deprecation warning was issued for default namespace
            deprecation_warnings = [warning for warning in w if issubclass(warning.category, DeprecationWarning)]
            assert len(deprecation_warnings) == 0

    @mock.patch("airflow.providers.cncf.kubernetes.kube_config.conf")
    def test_multiple_deprecated_fields_warnings(self, mock_conf):
        """Test that multiple deprecation warnings are issued when multiple deprecated fields are used."""
        # Set up mock configuration with multiple deprecated fields
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_container_repository"): "my-repo",
            ("kubernetes_executor", "worker_container_tag"): "my-tag",
            ("kubernetes_executor", "namespace"): "my-namespace",
            ("kubernetes_executor", "pod_template_file"): None,
            ("kubernetes_executor", "worker_pod_pending_fatal_container_state_reasons"): "",
            ("kubernetes_executor", "multi_namespace_mode_namespace_list"): None,
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
            ("core", "dags_folder"): "/tmp/dags",
        }.get((section, key), fallback)

        mock_conf.getboolean.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "delete_worker_pods"): True,
            ("kubernetes_executor", "delete_worker_pods_on_failure"): False,
            ("kubernetes_executor", "multi_namespace_mode"): False,
        }.get((section, key), fallback)

        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "worker_pods_creation_batch_size"): 1,
            ("core", "parallelism"): 32,
        }.get((section, key), fallback)

        mock_conf.getjson.side_effect = lambda section, key, fallback=None: {
            ("kubernetes_executor", "kube_client_request_args"): {},
            ("kubernetes_executor", "delete_option_kwargs"): {},
        }.get((section, key), fallback)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            KubeConfig()

            # Check that all three deprecation warnings were issued
            deprecation_warnings = [warning for warning in w if issubclass(warning.category, DeprecationWarning)]
            assert len(deprecation_warnings) == 3

            warning_messages = [str(warning.message) for warning in deprecation_warnings]
            assert any("worker_container_repository" in msg for msg in warning_messages)
            assert any("worker_container_tag" in msg for msg in warning_messages)
            assert any("namespace" in msg for msg in warning_messages)
