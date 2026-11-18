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
from kubernetes.client.rest import ApiException

from airflow.providers.cncf.kubernetes.utils.delete_from import (
    FailToDeleteError,
    _delete_from_yaml_single_item,
    delete_from_dict,
    delete_from_yaml,
)


def _make_mock_api(kind_snake, namespaced=True):
    """Build a mock API instance that supports delete_namespaced_<kind> or delete_<kind>."""
    api = mock.MagicMock()
    if namespaced:
        setattr(
            api,
            f"delete_namespaced_{kind_snake}",
            mock.MagicMock(return_value=mock.MagicMock(status="Success")),
        )
        # Also ensure the hasattr check works
    else:
        # Remove namespaced variant so hasattr returns False
        if hasattr(api, f"delete_namespaced_{kind_snake}"):
            delattr(type(api), f"delete_namespaced_{kind_snake}")
    return api


class TestDeleteFromYamlSingleItemApiClassDerivation:
    """Tests for API class name derivation from apiVersion."""

    def test_core_group(self):
        """apiVersion='v1' → CoreV1Api."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        mock_api_instance.delete_namespaced_pod.return_value = mock.MagicMock(status="Success")

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.CoreV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "test-pod", "namespace": "default"},
                },
            )
            mock_k8s_client.CoreV1Api.assert_called_once_with(mock_client)
            mock_api_instance.delete_namespaced_pod.assert_called_once()

    def test_named_group(self):
        """apiVersion='apps/v1' → AppsV1Api."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        mock_api_instance.delete_namespaced_deployment.return_value = mock.MagicMock(status="Success")

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.AppsV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {"name": "test-deploy", "namespace": "default"},
                },
            )
            mock_k8s_client.AppsV1Api.assert_called_once_with(mock_client)

    def test_k8s_io_stripping(self):
        """apiVersion='apiextensions.k8s.io/v1' → ApiextensionsV1Api
        (the '.k8s.io' suffix is stripped)."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        mock_api_instance.delete_namespaced_custom_resource_definition.return_value = mock.MagicMock(
            status="Success"
        )

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.ApiextensionsV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "apiextensions.k8s.io/v1",
                    "kind": "CustomResourceDefinition",
                    "metadata": {"name": "test-crd"},
                },
            )
            mock_k8s_client.ApiextensionsV1Api.assert_called_once_with(mock_client)

    def test_dns_subdomain_to_camelcase(self):
        """apiVersion='rbac.authorization.k8s.io/v1' → RbacAuthorizationV1Api
        (dots split and capitalised, then .k8s.io stripped)."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        mock_api_instance.delete_namespaced_role.return_value = mock.MagicMock(status="Success")

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.RbacAuthorizationV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "Role",
                    "metadata": {"name": "test-role", "namespace": "default"},
                },
            )
            mock_k8s_client.RbacAuthorizationV1Api.assert_called_once_with(mock_client)


class TestDeleteFromYamlSingleItemKindConversion:
    """Tests for CamelCase → snake_case kind conversion."""

    @pytest.mark.parametrize(
        ("kind", "expected_snake"),
        [
            ("Pod", "pod"),
            ("Deployment", "deployment"),
            ("ConfigMap", "config_map"),
            ("CustomResourceDefinition", "custom_resource_definition"),
            ("HorizontalPodAutoscaler", "horizontal_pod_autoscaler"),
        ],
    )
    def test_kind_to_snake_case(self, kind, expected_snake):
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        # Set up a mock for the expected delete method
        setattr(
            mock_api_instance,
            f"delete_namespaced_{expected_snake}",
            mock.MagicMock(return_value=mock.MagicMock(status="Success")),
        )

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.CoreV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "v1",
                    "kind": kind,
                    "metadata": {"name": "test", "namespace": "default"},
                },
            )
            getattr(mock_api_instance, f"delete_namespaced_{expected_snake}").assert_called_once()


class TestDeleteFromYamlSingleItemNamespaceHandling:
    """Tests for namespace dispatch and precedence."""

    def test_namespaced_dispatch(self):
        """When the API has delete_namespaced_<kind>, it is used."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        mock_api_instance.delete_namespaced_pod.return_value = mock.MagicMock(status="Success")

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.CoreV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "test-pod", "namespace": "kube-system"},
                },
                namespace="default",
            )
            mock_api_instance.delete_namespaced_pod.assert_called_once()
            call_kwargs = mock_api_instance.delete_namespaced_pod.call_args
            assert call_kwargs.kwargs["namespace"] == "kube-system"
            assert call_kwargs.kwargs["name"] == "test-pod"

    def test_non_namespaced_dispatch(self):
        """When the API lacks delete_namespaced_<kind>, delete_<kind> is used."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock(spec=[])
        mock_api_instance.delete_node = mock.MagicMock(return_value=mock.MagicMock(status="Success"))

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.CoreV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "test-node"},
                },
            )
            mock_api_instance.delete_node.assert_called_once()
            call_kwargs = mock_api_instance.delete_node.call_args
            assert call_kwargs.kwargs["name"] == "test-node"

    def test_document_namespace_takes_precedence(self):
        """namespace in the document metadata overrides the namespace argument."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        mock_api_instance.delete_namespaced_pod.return_value = mock.MagicMock(status="Success")

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.CoreV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "test-pod", "namespace": "doc-ns"},
                },
                namespace="arg-ns",
            )
            call_kwargs = mock_api_instance.delete_namespaced_pod.call_args
            assert call_kwargs.kwargs["namespace"] == "doc-ns"

    def test_argument_namespace_used_when_not_in_document(self):
        """When document metadata has no namespace, the argument namespace is used."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        mock_api_instance.delete_namespaced_pod.return_value = mock.MagicMock(status="Success")

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.CoreV1Api.return_value = mock_api_instance
            mock_k8s_client.V1DeleteOptions = mock.MagicMock()

            _delete_from_yaml_single_item(
                k8s_client=mock_client,
                yml_document={
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "test-pod"},
                },
                namespace="arg-ns",
            )
            call_kwargs = mock_api_instance.delete_namespaced_pod.call_args
            assert call_kwargs.kwargs["namespace"] == "arg-ns"


class TestDeleteFromYamlSingleItemDefaultBody:
    """Tests for default deletion body."""

    def test_default_body_used_when_none(self):
        """When body=None, DEFAULT_DELETION_BODY is used."""
        mock_client = mock.MagicMock()
        mock_api_instance = mock.MagicMock()
        mock_api_instance.delete_namespaced_pod.return_value = mock.MagicMock(status="Success")

        with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as mock_k8s_client:
            mock_k8s_client.CoreV1Api.return_value = mock_api_instance
            default_body = mock.MagicMock()
            mock_k8s_client.V1DeleteOptions.return_value = default_body

            # Re-import to pick up the mocked default
            with mock.patch(
                "airflow.providers.cncf.kubernetes.utils.delete_from.DEFAULT_DELETION_BODY",
                default_body,
            ):
                _delete_from_yaml_single_item(
                    k8s_client=mock_client,
                    yml_document={
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"name": "test-pod", "namespace": "default"},
                    },
                    body=None,
                )
                call_kwargs = mock_api_instance.delete_namespaced_pod.call_args
                assert call_kwargs.kwargs["body"] is default_body


class TestDeleteFromDict:
    """Tests for delete_from_dict."""

    def test_single_item(self):
        """Non-list kind delegates to _delete_from_yaml_single_item once."""
        mock_client = mock.MagicMock()

        with mock.patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from._delete_from_yaml_single_item"
        ) as mock_delete:
            data = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "test-pod", "namespace": "default"},
            }
            delete_from_dict(mock_client, data, body=None, namespace="default")
            mock_delete.assert_called_once()

    def test_list_kind_fans_out_over_items(self):
        """A 'PodList' kind fans out over items, each inheriting apiVersion and kind."""
        mock_client = mock.MagicMock()

        with mock.patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from._delete_from_yaml_single_item"
        ) as mock_delete:
            data = {
                "apiVersion": "v1",
                "kind": "PodList",
                "items": [
                    {"metadata": {"name": "pod-1", "namespace": "default"}},
                    {"metadata": {"name": "pod-2", "namespace": "default"}},
                ],
            }
            delete_from_dict(mock_client, data, body=None, namespace="default")
            assert mock_delete.call_count == 2
            # Each item should have inherited apiVersion and kind from parent
            for call in mock_delete.call_args_list:
                doc = call.kwargs["yml_document"]
                assert doc["apiVersion"] == "v1"
                assert doc["kind"] == "Pod"

    def test_list_kind_empty_kind_after_strip(self):
        """A 'List' kind (kind becomes '' after strip) does not set apiVersion/kind on items."""
        mock_client = mock.MagicMock()

        with mock.patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from._delete_from_yaml_single_item"
        ) as mock_delete:
            data = {
                "apiVersion": "v1",
                "kind": "List",
                "items": [
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"name": "pod-1", "namespace": "default"},
                    },
                ],
            }
            delete_from_dict(mock_client, data, body=None, namespace="default")
            mock_delete.assert_called_once()
            doc = mock_delete.call_args.kwargs["yml_document"]
            # kind=="" so the items keep their own apiVersion/kind
            assert doc["apiVersion"] == "v1"
            assert doc["kind"] == "Pod"

    def test_api_exceptions_collected_and_raised_as_fail_to_delete_error(self):
        """ApiExceptions from items are collected and raised as FailToDeleteError."""
        mock_client = mock.MagicMock()

        with mock.patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from._delete_from_yaml_single_item"
        ) as mock_delete:
            mock_delete.side_effect = ApiException(status=404, reason="Not Found")
            data = {
                "apiVersion": "v1",
                "kind": "PodList",
                "items": [
                    {"metadata": {"name": "pod-1", "namespace": "default"}},
                    {"metadata": {"name": "pod-2", "namespace": "default"}},
                ],
            }
            with pytest.raises(FailToDeleteError) as exc_info:
                delete_from_dict(mock_client, data, body=None, namespace="default")
            assert len(exc_info.value.api_exceptions) == 2

    def test_single_item_api_exception_raises_fail_to_delete_error(self):
        """ApiException from a single (non-list) item also raises FailToDeleteError."""
        mock_client = mock.MagicMock()

        with mock.patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from._delete_from_yaml_single_item"
        ) as mock_delete:
            mock_delete.side_effect = ApiException(status=404, reason="Not Found")
            data = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "test-pod", "namespace": "default"},
            }
            with pytest.raises(FailToDeleteError) as exc_info:
                delete_from_dict(mock_client, data, body=None, namespace="default")
            assert len(exc_info.value.api_exceptions) == 1


class TestDeleteFromYaml:
    """Tests for delete_from_yaml."""

    def test_iterates_over_yaml_objects(self):
        mock_client = mock.MagicMock()
        docs = [
            {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p1", "namespace": "default"}},
            {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p2", "namespace": "default"}},
        ]

        with mock.patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict"
        ) as mock_delete_dict:
            delete_from_yaml(k8s_client=mock_client, yaml_objects=docs)
            assert mock_delete_dict.call_count == 2

    def test_skips_none_documents(self):
        mock_client = mock.MagicMock()
        docs = [
            None,
            {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p1", "namespace": "default"}},
            None,
        ]

        with mock.patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict"
        ) as mock_delete_dict:
            delete_from_yaml(k8s_client=mock_client, yaml_objects=docs)
            mock_delete_dict.assert_called_once()


class TestFailToDeleteError:
    """Tests for FailToDeleteError."""

    def test_str_formatting(self):
        exc1 = ApiException(status=404, reason="Not Found")
        exc1.body = '{"message": "pod not found"}'
        exc2 = ApiException(status=409, reason="Conflict")
        exc2.body = '{"message": "conflict"}'

        error = FailToDeleteError([exc1, exc2])
        msg = str(error)
        assert "Not Found" in msg
        assert "Conflict" in msg
        assert "pod not found" in msg
        assert "conflict" in msg

    def test_str_single_exception(self):
        exc = ApiException(status=500, reason="Internal Server Error")
        exc.body = '{"message": "server error"}'

        error = FailToDeleteError([exc])
        msg = str(error)
        assert "Internal Server Error" in msg
        assert "server error" in msg

    def test_api_exceptions_attribute(self):
        exc = ApiException(status=404, reason="Not Found")
        error = FailToDeleteError([exc])
        assert error.api_exceptions == [exc]
