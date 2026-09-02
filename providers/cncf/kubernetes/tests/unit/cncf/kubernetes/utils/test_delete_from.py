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


def create_api_constructor(monkeypatch, api_class_name):
    api_constructor = mock.create_autospec(getattr(delete_from.client, api_class_name))
    monkeypatch.setattr(delete_from.client, api_class_name, api_constructor)
    return api_constructor


@pytest.mark.parametrize(
    ("api_version", "kind", "api_class_name", "method_name"),
    [
        ("v1", "ConfigMap", "CoreV1Api", "delete_namespaced_config_map"),
        ("apps/v1", "Deployment", "AppsV1Api", "delete_namespaced_deployment"),
        (
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            "ApiextensionsV1Api",
            "delete_custom_resource_definition",
        ),
        (
            "flowcontrol.apiserver.k8s.io/v1",
            "FlowSchema",
            "FlowcontrolApiserverV1Api",
            "delete_flow_schema",
        ),
    ],
)
def test_delete_from_yaml_derives_api_class_and_kind(
    monkeypatch, api_version, kind, api_class_name, method_name
):
    api_constructor = create_api_constructor(monkeypatch, api_class_name)
    api_client = mock.sentinel.api_client
    api = api_constructor.return_value
    getattr(api, method_name).return_value = mock.Mock(status="Success")

    delete_from._delete_from_yaml_single_item(
        k8s_client=api_client,
        yml_document={"apiVersion": api_version, "kind": kind, "metadata": {"name": "resource"}},
    )

    api_constructor.assert_called_once_with(api_client)
    if method_name.startswith("delete_namespaced"):
        getattr(api, method_name).assert_called_once_with(
            name="resource", namespace="default", body=delete_from.DEFAULT_DELETION_BODY
        )
    else:
        getattr(api, method_name).assert_called_once_with(
            name="resource", body=delete_from.DEFAULT_DELETION_BODY
        )


def test_delete_from_yaml_document_namespace_takes_precedence(monkeypatch):
    api_constructor = create_api_constructor(monkeypatch, "CoreV1Api")
    api = api_constructor.return_value
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


def test_delete_from_dict_expands_lists_and_inherits_api_version(monkeypatch):
    api_constructor = create_api_constructor(monkeypatch, "CoreV1Api")
    api = api_constructor.return_value
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


def test_delete_from_dict_collects_api_exceptions(monkeypatch):
    api_constructor = create_api_constructor(monkeypatch, "CoreV1Api")
    api = api_constructor.return_value
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
