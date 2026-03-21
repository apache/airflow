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
from kubernetes.client import models as k8s
from kubernetes.client.exceptions import ApiException

from airflow.providers.cncf.kubernetes.utils.resource_quota import (
    PodResourceQuotaExceededException,
    check_pod_quota_compliance,
    get_namespace_quota_status,
    get_pod_resource_requests,
    parse_resource_quantity,
)


class TestParseResourceQuantity:
    """Test resource quantity parsing."""

    def test_parse_cpu_cores(self):
        """Test parsing CPU in cores."""
        assert parse_resource_quantity("1") == 1.0
        assert parse_resource_quantity("2.5") == 2.5
        assert parse_resource_quantity("0.5") == 0.5

    def test_parse_cpu_millicores(self):
        """Test parsing CPU in millicores."""
        assert parse_resource_quantity("100m") == 0.1
        assert parse_resource_quantity("500m") == 0.5
        assert parse_resource_quantity("1000m") == 1.0

    def test_parse_memory_binary(self):
        """Test parsing memory with binary suffixes."""
        assert parse_resource_quantity("1Ki") == 1024
        assert parse_resource_quantity("1Mi") == 1024**2
        assert parse_resource_quantity("1Gi") == 1024**3
        assert parse_resource_quantity("1Ti") == 1024**4
        assert parse_resource_quantity("2Gi") == 2 * 1024**3

    def test_parse_memory_decimal(self):
        """Test parsing memory with decimal suffixes."""
        assert parse_resource_quantity("1k") == 1000
        assert parse_resource_quantity("1M") == 1000**2
        assert parse_resource_quantity("1G") == 1000**3
        assert parse_resource_quantity("2G") == 2 * 1000**3

    def test_parse_empty_or_none(self):
        """Test parsing empty or None values."""
        assert parse_resource_quantity(None) == 0.0
        assert parse_resource_quantity("") == 0.0
        assert parse_resource_quantity("   ") == 0.0

    def test_parse_invalid(self):
        """Test parsing invalid values."""
        assert parse_resource_quantity("invalid") == 0.0
        assert parse_resource_quantity("abc123") == 0.0


class TestGetPodResourceRequests:
    """Test extracting resource requests from pods."""

    def test_pod_with_requests(self):
        """Test pod with resource requests."""
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="main",
                        image="test:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "100m", "memory": "256Mi"}),
                    )
                ]
            ),
        )

        requests = get_pod_resource_requests(pod)
        assert requests["cpu"] == 0.1
        assert requests["memory"] == 256 * 1024**2

    def test_pod_with_multiple_containers(self):
        """Test pod with multiple containers."""
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="main",
                        image="test:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "100m", "memory": "256Mi"}),
                    ),
                    k8s.V1Container(
                        name="sidecar",
                        image="sidecar:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "50m", "memory": "128Mi"}),
                    ),
                ]
            ),
        )

        requests = get_pod_resource_requests(pod)
        assert requests["cpu"] == pytest.approx(0.15)  # 100m + 50m
        assert requests["memory"] == (256 + 128) * 1024**2

    def test_pod_with_init_containers(self):
        """Test pod with init containers."""
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="main",
                        image="test:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "100m", "memory": "256Mi"}),
                    )
                ],
                init_containers=[
                    k8s.V1Container(
                        name="init",
                        image="init:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "50m", "memory": "128Mi"}),
                    )
                ],
            ),
        )

        requests = get_pod_resource_requests(pod)
        assert requests["cpu"] == pytest.approx(0.15)  # 100m + 50m
        assert requests["memory"] == (256 + 128) * 1024**2

    def test_pod_without_requests(self):
        """Test pod without resource requests."""
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="main", image="test:latest")]),
        )

        requests = get_pod_resource_requests(pod)
        assert requests["cpu"] == 0.0
        assert requests["memory"] == 0.0

    def test_pod_without_spec(self):
        """Test pod without spec."""
        pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"))

        requests = get_pod_resource_requests(pod)
        assert requests["cpu"] == 0.0
        assert requests["memory"] == 0.0


class TestGetNamespaceQuotaStatus:
    """Test getting namespace quota status."""

    def test_namespace_with_quota(self):
        """Test namespace with resource quota."""
        mock_client = mock.MagicMock()
        mock_quota = k8s.V1ResourceQuota(
            metadata=k8s.V1ObjectMeta(name="quota"),
            spec=k8s.V1ResourceQuotaSpec(hard={"requests.cpu": "4", "requests.memory": "8Gi"}),
            status=k8s.V1ResourceQuotaStatus(used={"requests.cpu": "2", "requests.memory": "4Gi"}),
        )
        mock_client.list_namespaced_resource_quota.return_value = k8s.V1ResourceQuotaList(items=[mock_quota])

        result = get_namespace_quota_status(mock_client, "default")

        assert result is not None
        used, hard = result
        assert used["cpu"] == 2.0
        assert used["memory"] == 4 * 1024**3
        assert hard["cpu"] == 4.0
        assert hard["memory"] == 8 * 1024**3

    def test_namespace_without_quota(self):
        """Test namespace without resource quota."""
        mock_client = mock.MagicMock()
        mock_client.list_namespaced_resource_quota.return_value = k8s.V1ResourceQuotaList(items=[])

        result = get_namespace_quota_status(mock_client, "default")

        assert result is None

    def test_namespace_quota_no_limits(self):
        """Test namespace quota with no actual limits defined."""
        mock_client = mock.MagicMock()
        mock_quota = k8s.V1ResourceQuota(
            metadata=k8s.V1ObjectMeta(name="quota"),
            spec=k8s.V1ResourceQuotaSpec(hard={}),
            status=k8s.V1ResourceQuotaStatus(used={}),
        )
        mock_client.list_namespaced_resource_quota.return_value = k8s.V1ResourceQuotaList(items=[mock_quota])

        result = get_namespace_quota_status(mock_client, "default")

        assert result is None

    def test_namespace_forbidden_error(self):
        """Test handling of 403 Forbidden error."""
        mock_client = mock.MagicMock()
        mock_client.list_namespaced_resource_quota.side_effect = ApiException(status=403)

        result = get_namespace_quota_status(mock_client, "default")

        assert result is None

    def test_namespace_not_found_error(self):
        """Test handling of 404 Not Found error."""
        mock_client = mock.MagicMock()
        mock_client.list_namespaced_resource_quota.side_effect = ApiException(status=404)

        result = get_namespace_quota_status(mock_client, "default")

        assert result is None

    def test_namespace_other_api_error(self):
        """Test handling of other API errors."""
        mock_client = mock.MagicMock()
        mock_client.list_namespaced_resource_quota.side_effect = ApiException(status=500)

        result = get_namespace_quota_status(mock_client, "default")

        assert result is None


class TestCheckPodQuotaCompliance:
    """Test checking pod quota compliance."""

    def test_pod_within_quota(self):
        """Test pod that fits within quota."""
        mock_client = mock.MagicMock()
        mock_quota = k8s.V1ResourceQuota(
            metadata=k8s.V1ObjectMeta(name="quota"),
            spec=k8s.V1ResourceQuotaSpec(hard={"requests.cpu": "4", "requests.memory": "8Gi"}),
            status=k8s.V1ResourceQuotaStatus(used={"requests.cpu": "2", "requests.memory": "4Gi"}),
        )
        mock_client.list_namespaced_resource_quota.return_value = k8s.V1ResourceQuotaList(items=[mock_quota])

        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="main",
                        image="test:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "1", "memory": "2Gi"}),
                    )
                ]
            ),
        )

        # Should not raise
        check_pod_quota_compliance(mock_client, pod, "default")

    def test_pod_exceeds_cpu_quota(self):
        """Test pod that exceeds CPU quota."""
        mock_client = mock.MagicMock()
        mock_quota = k8s.V1ResourceQuota(
            metadata=k8s.V1ObjectMeta(name="quota"),
            spec=k8s.V1ResourceQuotaSpec(hard={"requests.cpu": "4", "requests.memory": "8Gi"}),
            status=k8s.V1ResourceQuotaStatus(used={"requests.cpu": "3", "requests.memory": "2Gi"}),
        )
        mock_client.list_namespaced_resource_quota.return_value = k8s.V1ResourceQuotaList(items=[mock_quota])

        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="main",
                        image="test:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "2", "memory": "1Gi"}),
                    )
                ]
            ),
        )

        with pytest.raises(PodResourceQuotaExceededException) as exc_info:
            check_pod_quota_compliance(mock_client, pod, "default")

        assert "CPU" in str(exc_info.value)
        assert "test-pod" in str(exc_info.value)

    def test_pod_exceeds_memory_quota(self):
        """Test pod that exceeds memory quota."""
        mock_client = mock.MagicMock()
        mock_quota = k8s.V1ResourceQuota(
            metadata=k8s.V1ObjectMeta(name="quota"),
            spec=k8s.V1ResourceQuotaSpec(hard={"requests.cpu": "4", "requests.memory": "8Gi"}),
            status=k8s.V1ResourceQuotaStatus(used={"requests.cpu": "1", "requests.memory": "6Gi"}),
        )
        mock_client.list_namespaced_resource_quota.return_value = k8s.V1ResourceQuotaList(items=[mock_quota])

        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="main",
                        image="test:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "1", "memory": "3Gi"}),
                    )
                ]
            ),
        )

        with pytest.raises(PodResourceQuotaExceededException) as exc_info:
            check_pod_quota_compliance(mock_client, pod, "default")

        assert "Memory" in str(exc_info.value)
        assert "test-pod" in str(exc_info.value)

    def test_pod_exceeds_both_quotas(self):
        """Test pod that exceeds both CPU and memory quotas."""
        mock_client = mock.MagicMock()
        mock_quota = k8s.V1ResourceQuota(
            metadata=k8s.V1ObjectMeta(name="quota"),
            spec=k8s.V1ResourceQuotaSpec(hard={"requests.cpu": "4", "requests.memory": "8Gi"}),
            status=k8s.V1ResourceQuotaStatus(used={"requests.cpu": "3", "requests.memory": "6Gi"}),
        )
        mock_client.list_namespaced_resource_quota.return_value = k8s.V1ResourceQuotaList(items=[mock_quota])

        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="main",
                        image="test:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "2", "memory": "3Gi"}),
                    )
                ]
            ),
        )

        with pytest.raises(PodResourceQuotaExceededException) as exc_info:
            check_pod_quota_compliance(mock_client, pod, "default")

        error_message = str(exc_info.value)
        assert "CPU" in error_message
        assert "Memory" in error_message
        assert "test-pod" in error_message

    def test_pod_without_quota(self):
        """Test pod in namespace without quota."""
        mock_client = mock.MagicMock()
        mock_client.list_namespaced_resource_quota.return_value = k8s.V1ResourceQuotaList(items=[])

        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod", namespace="default"),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="main",
                        image="test:latest",
                        resources=k8s.V1ResourceRequirements(requests={"cpu": "100", "memory": "100Gi"}),
                    )
                ]
            ),
        )

        # Should not raise even with large requests
        check_pod_quota_compliance(mock_client, pod, "default")
