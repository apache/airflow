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
from kubernetes import client

from airflow.providers.cncf.kubernetes.utils.delete_from import (
    DEFAULT_DELETION_BODY,
    FailToDeleteError,
    _delete_from_yaml_single_item,
    delete_from_dict,
    delete_from_yaml,
)


def test_fail_to_delete_error_formatting():
    exc1 = client.rest.ApiException(status=404, reason="Not Found")
    exc1.body = '{"message": "pod not found"}'
    exc2 = client.rest.ApiException(status=403, reason="Forbidden")
    exc2.body = '{"message": "access denied"}'

    err = FailToDeleteError([exc1, exc2])
    msg = str(err)
    assert 'Error from server (Not Found):{"message": "pod not found"}\n' in msg
    assert 'Error from server (Forbidden):{"message": "access denied"}\n' in msg


@patch("airflow.providers.cncf.kubernetes.utils.delete_from.client")
def test_delete_from_yaml_single_item_namespaced(mock_client, capsys):
    mock_k8s_client = MagicMock()
    mock_core_v1_api = MagicMock()
    mock_client.CoreV1Api.return_value = mock_core_v1_api
    mock_core_v1_api.delete_namespaced_pod.return_value = MagicMock(status="Success")

    yml_document = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "my-pod", "namespace": "custom-ns"},
    }

    resp = _delete_from_yaml_single_item(
        k8s_client=mock_k8s_client,
        yml_document=yml_document,
        namespace="default",
        verbose=True,
    )

    mock_client.CoreV1Api.assert_called_once_with(mock_k8s_client)
    mock_core_v1_api.delete_namespaced_pod.assert_called_once_with(
        name="my-pod", namespace="custom-ns", body=DEFAULT_DELETION_BODY
    )
    assert resp.status == "Success"
    captured = capsys.readouterr()
    assert "pod deleted. status='Success'" in captured.out


@patch("airflow.providers.cncf.kubernetes.utils.delete_from.client")
def test_delete_from_yaml_single_item_non_namespaced(mock_client):
    mock_k8s_client = MagicMock()
    mock_rbac_api = MagicMock(spec=["delete_cluster_role"])
    mock_client.RbacAuthorizationV1Api.return_value = mock_rbac_api
    mock_rbac_api.delete_cluster_role.return_value = MagicMock(status="Success")

    yml_document = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "cluster-admin"},
    }

    _delete_from_yaml_single_item(
        k8s_client=mock_k8s_client,
        yml_document=yml_document,
    )

    mock_client.RbacAuthorizationV1Api.assert_called_once_with(mock_k8s_client)
    mock_rbac_api.delete_cluster_role.assert_called_once_with(
        name="cluster-admin", body=DEFAULT_DELETION_BODY
    )


@patch("airflow.providers.cncf.kubernetes.utils.delete_from.client")
def test_delete_from_yaml_api_class_derivation(mock_client):
    mock_k8s_client = MagicMock()
    test_cases = [
        ("v1", "CoreV1Api"),
        ("apps/v1", "AppsV1Api"),
        ("batch/v1", "BatchV1Api"),
        ("apiextensions.k8s.io/v1", "ApiextensionsV1Api"),
        ("storage.k8s.io/v1", "StorageV1Api"),
    ]

    for api_version, expected_class in test_cases:
        mock_api = MagicMock(spec=["delete_namespaced_custom_resource_definition"])
        setattr(mock_client, expected_class, MagicMock(return_value=mock_api))

        yml_doc = {
            "apiVersion": api_version,
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "crds.test.com"},
        }
        _delete_from_yaml_single_item(k8s_client=mock_k8s_client, yml_document=yml_doc)
        getattr(mock_client, expected_class).assert_called_with(mock_k8s_client)


@patch("airflow.providers.cncf.kubernetes.utils.delete_from._delete_from_yaml_single_item")
def test_delete_from_dict_list_kind(mock_single_item):
    mock_k8s_client = MagicMock()
    data = {
        "apiVersion": "v1",
        "kind": "PodList",
        "items": [
            {"metadata": {"name": "pod-1"}},
            {"metadata": {"name": "pod-2"}},
        ],
    }

    delete_from_dict(
        k8s_client=mock_k8s_client,
        data=data,
        body=None,
        namespace="default",
    )

    assert mock_single_item.call_count == 2
    first_call_args = mock_single_item.call_args_list[0].kwargs
    assert first_call_args["yml_document"]["apiVersion"] == "v1"
    assert first_call_args["yml_document"]["kind"] == "Pod"
    assert first_call_args["yml_document"]["metadata"]["name"] == "pod-1"


@patch("airflow.providers.cncf.kubernetes.utils.delete_from._delete_from_yaml_single_item")
def test_delete_from_dict_collects_exceptions(mock_single_item):
    mock_k8s_client = MagicMock()
    exc = client.rest.ApiException(status=404, reason="Not Found")
    mock_single_item.side_effect = exc

    data = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "missing-pod"},
    }

    with pytest.raises(FailToDeleteError) as exc_info:
        delete_from_dict(
            k8s_client=mock_k8s_client,
            data=data,
            body=None,
            namespace="default",
        )

    assert len(exc_info.value.api_exceptions) == 1
    assert exc_info.value.api_exceptions[0] == exc


@patch("airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict")
def test_delete_from_yaml_multiple_objects(mock_delete_dict):
    mock_k8s_client = MagicMock()
    doc1 = {"kind": "Pod", "metadata": {"name": "pod1"}}
    doc2 = {"kind": "Service", "metadata": {"name": "svc1"}}

    delete_from_yaml(
        k8s_client=mock_k8s_client,
        yaml_objects=[doc1, None, doc2],
        namespace="test-ns",
    )

    assert mock_delete_dict.call_count == 2
    mock_delete_dict.assert_any_call(
        k8s_client=mock_k8s_client,
        data=doc1,
        body=None,
        namespace="test-ns",
        verbose=False,
    )
    mock_delete_dict.assert_any_call(
        k8s_client=mock_k8s_client,
        data=doc2,
        body=None,
        namespace="test-ns",
        verbose=False,
    )
