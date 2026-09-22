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

from airflow.providers.cncf.kubernetes.utils import delete_from


@pytest.mark.parametrize(
    ("api_version", "kind", "method_name", "expected_kwargs"),
    [
        (
            "v1",
            "ConfigMap",
            "delete_namespaced_config_map",
            {"name": "resource", "namespace": "default", "body": delete_from.DEFAULT_DELETION_BODY},
        ),
        (
            "apps/v1",
            "Deployment",
            "delete_namespaced_deployment",
            {"name": "resource", "namespace": "default", "body": delete_from.DEFAULT_DELETION_BODY},
        ),
    ],
)
@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client.AppsV1Api", autospec=True)
@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client.CoreV1Api", autospec=True)
def test_delete_from_yaml_derives_namespaced_api_class_and_kind(
    mock_core_v1_api, mock_apps_v1_api, api_version, kind, method_name, expected_kwargs
):
    api_constructor = mock_core_v1_api if api_version == "v1" else mock_apps_v1_api
    api = api_constructor.return_value
    getattr(api, method_name).return_value = mock.Mock(status="Success")

    delete_from._delete_from_yaml_single_item(
        k8s_client=mock.sentinel.api_client,
        yml_document={"apiVersion": api_version, "kind": kind, "metadata": {"name": "resource"}},
    )

    api_constructor.assert_called_once_with(mock.sentinel.api_client)
    getattr(api, method_name).assert_called_once_with(**expected_kwargs)


@pytest.mark.parametrize(
    ("api_version", "kind", "method_name"),
    [
        ("apiextensions.k8s.io/v1", "CustomResourceDefinition", "delete_custom_resource_definition"),
        ("flowcontrol.apiserver.k8s.io/v1", "FlowSchema", "delete_flow_schema"),
    ],
)
@mock.patch(
    "airflow.providers.cncf.kubernetes.utils.delete_from.client.FlowcontrolApiserverV1Api", autospec=True
)
@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client.ApiextensionsV1Api", autospec=True)
def test_delete_from_yaml_derives_cluster_scoped_api_class_and_kind(
    mock_apiextensions_v1_api, mock_flowcontrol_apiserver_v1_api, api_version, kind, method_name
):
    api_constructor = (
        mock_apiextensions_v1_api
        if api_version == "apiextensions.k8s.io/v1"
        else mock_flowcontrol_apiserver_v1_api
    )
    api = api_constructor.return_value
    getattr(api, method_name).return_value = mock.Mock(status="Success")

    delete_from._delete_from_yaml_single_item(
        k8s_client=mock.sentinel.api_client,
        yml_document={"apiVersion": api_version, "kind": kind, "metadata": {"name": "resource"}},
    )

    api_constructor.assert_called_once_with(mock.sentinel.api_client)
    getattr(api, method_name).assert_called_once_with(name="resource", body=delete_from.DEFAULT_DELETION_BODY)


@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client.CoreV1Api", autospec=True)
def test_delete_from_yaml_document_namespace_takes_precedence(mock_core_v1_api):
    api = mock_core_v1_api.return_value
    api.delete_namespaced_config_map.return_value = mock.Mock(status="Success")

    delete_from._delete_from_yaml_single_item(
        k8s_client=mock.sentinel.api_client,
        yml_document={
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "resource", "namespace": "document-namespace"},
        },
        namespace="argument-namespace",
    )

    api.delete_namespaced_config_map.assert_called_once_with(
        name="resource", namespace="document-namespace", body=delete_from.DEFAULT_DELETION_BODY
    )


@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client.CoreV1Api", autospec=True)
def test_delete_from_dict_expands_lists_and_inherits_api_version(mock_core_v1_api):
    api = mock_core_v1_api.return_value
    api.delete_namespaced_config_map.return_value = mock.Mock(status="Success")
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMapList",
        "items": [
            {"metadata": {"name": "first"}},
            {"metadata": {"name": "second", "namespace": "item-namespace"}},
        ],
    }

    delete_from.delete_from_dict(
        k8s_client=mock.sentinel.api_client,
        data=document,
        body=None,
        namespace="argument-namespace",
    )

    assert document["items"] == [
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "first"}},
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "second", "namespace": "item-namespace"},
        },
    ]
    api.delete_namespaced_config_map.assert_has_calls(
        [
            mock.call(name="first", namespace="argument-namespace", body=delete_from.DEFAULT_DELETION_BODY),
            mock.call(name="second", namespace="item-namespace", body=delete_from.DEFAULT_DELETION_BODY),
        ]
    )


@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client.CoreV1Api", autospec=True)
def test_delete_from_dict_collects_api_exceptions(mock_core_v1_api):
    api = mock_core_v1_api.return_value
    first_exception = ApiException(reason="first")
    first_exception.body = "first body"
    second_exception = ApiException(reason="second")
    second_exception.body = "second body"
    api.delete_namespaced_config_map.side_effect = [first_exception, second_exception]

    with pytest.raises(delete_from.FailToDeleteError) as error:
        delete_from.delete_from_dict(
            k8s_client=mock.sentinel.api_client,
            data={
                "apiVersion": "v1",
                "kind": "ConfigMapList",
                "items": [{"metadata": {"name": "first"}}, {"metadata": {"name": "second"}}],
            },
            body=None,
            namespace="default",
        )

    assert error.value.api_exceptions == [first_exception, second_exception]
    assert (
        str(error.value) == "Error from server (first):first body\nError from server (second):second body\n"
    )


@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict", autospec=True)
def test_delete_from_yaml_skips_empty_documents_and_forwards_verbose(mock_delete_from_dict):
    document = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "resource"}}

    delete_from.delete_from_yaml(
        k8s_client=mock.sentinel.api_client,
        yaml_objects=[None, document],
        verbose=True,
        namespace="configured-namespace",
        body=mock.sentinel.body,
        grace_period_seconds=0,
    )

    mock_delete_from_dict.assert_called_once_with(
        k8s_client=mock.sentinel.api_client,
        data=document,
        body=mock.sentinel.body,
        namespace="configured-namespace",
        verbose=True,
        grace_period_seconds=0,
    )


@mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client.CoreV1Api", autospec=True)
def test_delete_from_yaml_prints_status_when_verbose(mock_core_v1_api, capsys):
    api = mock_core_v1_api.return_value
    api.delete_namespaced_config_map.return_value = mock.Mock(status="Success")

    delete_from._delete_from_yaml_single_item(
        k8s_client=mock.sentinel.api_client,
        yml_document={"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "resource"}},
        verbose=True,
    )

    assert capsys.readouterr().out == "config_map deleted. status='Success'\n"
