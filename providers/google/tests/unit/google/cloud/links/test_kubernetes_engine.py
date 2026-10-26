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
"""Tests for Google Cloud Kubernetes Engine links.

Expected URLs are fully hard-coded: the path and query parts are never derived from the
constants under test, so a broken or hand-edited ``format_str`` always fails the test. The
``key`` values are asserted as literals too - they are stored in XComs and serialized DAGs,
so values that merely mirror the module constants would not catch a rename.
"""

from __future__ import annotations

from unittest import mock

import pytest
from google.cloud.container_v1.types import Cluster

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.kubernetes_engine import (
    KubernetesEngineClusterLink,
    KubernetesEngineJobLink,
    KubernetesEnginePodLink,
    KubernetesEngineWorkloadsLink,
)

TEST_PROJECT_ID = "test-project"
TEST_LOCATION = "us-central1"
TEST_CLUSTER_NAME = "test-cluster"
TEST_NAMESPACE = "test-namespace"


def _mock_context():
    mock_context = mock.MagicMock()
    mock_context["ti"] = mock.MagicMock()
    mock_context["task"] = mock.MagicMock(spec=[])
    return mock_context


class TestKubernetesEngineClusterLink:
    def test_class_attributes(self):
        assert KubernetesEngineClusterLink.key == "kubernetes_cluster_conf"
        assert KubernetesEngineClusterLink.name == "Kubernetes Cluster"

    def test_get_link(self):
        result = KubernetesEngineClusterLink()._format_link(
            location=TEST_LOCATION, cluster_name=TEST_CLUSTER_NAME, project_id=TEST_PROJECT_ID
        )
        assert result == (
            BASE_LINK + "/kubernetes/clusters/details/us-central1/test-cluster/details?project=test-project"
        )

    def test_persist_accepts_a_cluster_dict(self):
        mock_context = _mock_context()

        KubernetesEngineClusterLink.persist(mock_context, cluster={"name": TEST_CLUSTER_NAME})

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="kubernetes_cluster_conf",
            value={"cluster_name": TEST_CLUSTER_NAME},
        )

    def test_persist_accepts_a_cluster_object(self):
        mock_context = _mock_context()

        KubernetesEngineClusterLink.persist(mock_context, cluster=Cluster(name=TEST_CLUSTER_NAME))

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="kubernetes_cluster_conf",
            value={"cluster_name": TEST_CLUSTER_NAME},
        )

    def test_persist_without_cluster_raises(self):
        mock_context = _mock_context()

        with pytest.raises(ValueError, match="Cluster must be provided for KubernetesEngineClusterLink."):
            KubernetesEngineClusterLink.persist(mock_context)


class TestKubernetesEnginePodLink:
    def test_class_attributes(self):
        assert KubernetesEnginePodLink.key == "kubernetes_pod_conf"
        assert KubernetesEnginePodLink.name == "Kubernetes Pod"

    def test_get_link(self):
        result = KubernetesEnginePodLink()._format_link(
            location=TEST_LOCATION,
            cluster_name=TEST_CLUSTER_NAME,
            namespace=TEST_NAMESPACE,
            pod_name="test-pod-1",
            project_id=TEST_PROJECT_ID,
        )
        assert result == (
            BASE_LINK
            + "/kubernetes/pod/us-central1/test-cluster/test-namespace/test-pod-1/details?project=test-project"
        )

    def test_persist(self):
        mock_context = _mock_context()

        KubernetesEnginePodLink.persist(
            mock_context,
            location=TEST_LOCATION,
            cluster_name=TEST_CLUSTER_NAME,
            namespace=TEST_NAMESPACE,
            pod_name="test-pod-1",
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="kubernetes_pod_conf",
            value={
                "location": TEST_LOCATION,
                "cluster_name": TEST_CLUSTER_NAME,
                "namespace": TEST_NAMESPACE,
                "pod_name": "test-pod-1",
            },
        )


class TestKubernetesEngineJobLink:
    def test_class_attributes(self):
        assert KubernetesEngineJobLink.key == "kubernetes_job_conf"
        assert KubernetesEngineJobLink.name == "Kubernetes Job"

    def test_get_link(self):
        result = KubernetesEngineJobLink()._format_link(
            location=TEST_LOCATION,
            cluster_name=TEST_CLUSTER_NAME,
            namespace=TEST_NAMESPACE,
            job_name="test-job-1",
            project_id=TEST_PROJECT_ID,
        )
        assert result == (
            BASE_LINK
            + "/kubernetes/job/us-central1/test-cluster/test-namespace/test-job-1/details?project=test-project"
        )


class TestKubernetesEngineWorkloadsLink:
    def test_class_attributes(self):
        assert KubernetesEngineWorkloadsLink.key == "kubernetes_workloads_conf"
        assert KubernetesEngineWorkloadsLink.name == "Kubernetes Workloads"

    def test_get_link_builds_the_console_deep_link(self):
        result = KubernetesEngineWorkloadsLink()._format_link(
            location=TEST_LOCATION,
            cluster_name=TEST_CLUSTER_NAME,
            namespace=TEST_NAMESPACE,
            project_id=TEST_PROJECT_ID,
        )
        expected = (
            BASE_LINK
            + '/kubernetes/workload/overview?project=test-project&pageState=("savedViews":'
            + '("c":%5B"gke%2Fus-central1%2Ftest-cluster"%5D,"n":%5B"test-namespace"%5D))'
        )
        assert result == expected
