#
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

"""Tests for Kubernetes Engine links."""

from __future__ import annotations

from unittest import mock

import pytest
from google.cloud.container_v1.types import Cluster

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.kubernetes_engine import (
    KUBERNETES_CLUSTER_LINK,
    KUBERNETES_JOB_LINK,
    KUBERNETES_POD_LINK,
    KUBERNETES_WORKLOADS_LINK,
    KubernetesEngineClusterLink,
    KubernetesEngineJobLink,
    KubernetesEnginePodLink,
    KubernetesEngineWorkloadsLink,
)

TEST_CLUSTER_NAME = "test-cluster-name"
TEST_JOB_NAME = "test-job-name"
TEST_LOCATION = "test-location"
TEST_NAMESPACE = "test-namespace"
TEST_POD_NAME = "test-pod-name"
TEST_PROJECT_ID = "test-project-id"


class TestKubernetesEngineClusterLink:
    def test_class_attributes(self):
        assert KubernetesEngineClusterLink.key == "kubernetes_cluster_conf"
        assert KubernetesEngineClusterLink.name == "Kubernetes Cluster"
        assert KubernetesEngineClusterLink.format_str == KUBERNETES_CLUSTER_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        KubernetesEngineClusterLink.persist(context=mock_context, cluster=Cluster(name=TEST_CLUSTER_NAME))

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="kubernetes_cluster_conf",
            value={"cluster_name": TEST_CLUSTER_NAME},
        )

    def test_persist_with_cluster_given_as_dict(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        KubernetesEngineClusterLink.persist(context=mock_context, cluster={"name": TEST_CLUSTER_NAME})

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="kubernetes_cluster_conf",
            value={"cluster_name": TEST_CLUSTER_NAME},
        )

    def test_persist_without_cluster(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        with pytest.raises(ValueError, match="Cluster must be provided"):
            KubernetesEngineClusterLink.persist(context=mock_context)

        mock_context["ti"].xcom_push.assert_not_called()

    def test_format_link(self):
        link = KubernetesEngineClusterLink()

        result = link._format_link(
            cluster_name=TEST_CLUSTER_NAME, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )

        assert result == BASE_LINK + KUBERNETES_CLUSTER_LINK.format(
            cluster_name=TEST_CLUSTER_NAME, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )


class TestKubernetesEnginePodLink:
    def test_class_attributes(self):
        assert KubernetesEnginePodLink.key == "kubernetes_pod_conf"
        assert KubernetesEnginePodLink.name == "Kubernetes Pod"
        assert KubernetesEnginePodLink.format_str == KUBERNETES_POD_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        KubernetesEnginePodLink.persist(
            context=mock_context,
            cluster_name=TEST_CLUSTER_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            pod_name=TEST_POD_NAME,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="kubernetes_pod_conf",
            value={
                "cluster_name": TEST_CLUSTER_NAME,
                "location": TEST_LOCATION,
                "namespace": TEST_NAMESPACE,
                "pod_name": TEST_POD_NAME,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = KubernetesEnginePodLink()

        result = link._format_link(
            cluster_name=TEST_CLUSTER_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            pod_name=TEST_POD_NAME,
            project_id=TEST_PROJECT_ID,
        )

        assert result == BASE_LINK + KUBERNETES_POD_LINK.format(
            cluster_name=TEST_CLUSTER_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            pod_name=TEST_POD_NAME,
            project_id=TEST_PROJECT_ID,
        )


class TestKubernetesEngineJobLink:
    def test_class_attributes(self):
        assert KubernetesEngineJobLink.key == "kubernetes_job_conf"
        assert KubernetesEngineJobLink.name == "Kubernetes Job"
        assert KubernetesEngineJobLink.format_str == KUBERNETES_JOB_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        KubernetesEngineJobLink.persist(
            context=mock_context,
            cluster_name=TEST_CLUSTER_NAME,
            job_name=TEST_JOB_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="kubernetes_job_conf",
            value={
                "cluster_name": TEST_CLUSTER_NAME,
                "job_name": TEST_JOB_NAME,
                "location": TEST_LOCATION,
                "namespace": TEST_NAMESPACE,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = KubernetesEngineJobLink()

        result = link._format_link(
            cluster_name=TEST_CLUSTER_NAME,
            job_name=TEST_JOB_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            project_id=TEST_PROJECT_ID,
        )

        assert result == BASE_LINK + KUBERNETES_JOB_LINK.format(
            cluster_name=TEST_CLUSTER_NAME,
            job_name=TEST_JOB_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            project_id=TEST_PROJECT_ID,
        )


class TestKubernetesEngineWorkloadsLink:
    def test_class_attributes(self):
        assert KubernetesEngineWorkloadsLink.key == "kubernetes_workloads_conf"
        assert KubernetesEngineWorkloadsLink.name == "Kubernetes Workloads"
        assert KubernetesEngineWorkloadsLink.format_str == KUBERNETES_WORKLOADS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        KubernetesEngineWorkloadsLink.persist(
            context=mock_context,
            cluster_name=TEST_CLUSTER_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="kubernetes_workloads_conf",
            value={
                "cluster_name": TEST_CLUSTER_NAME,
                "location": TEST_LOCATION,
                "namespace": TEST_NAMESPACE,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = KubernetesEngineWorkloadsLink()

        result = link._format_link(
            cluster_name=TEST_CLUSTER_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            project_id=TEST_PROJECT_ID,
        )

        assert result == BASE_LINK + KUBERNETES_WORKLOADS_LINK.format(
            cluster_name=TEST_CLUSTER_NAME,
            location=TEST_LOCATION,
            namespace=TEST_NAMESPACE,
            project_id=TEST_PROJECT_ID,
        )
