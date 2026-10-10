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
    DEFAULT_DELETION_BODY,
    FailToDeleteError,
    _delete_from_yaml_single_item,
    delete_from_dict,
    delete_from_yaml,
)


@pytest.fixture
def mock_client():
    with mock.patch("airflow.providers.cncf.kubernetes.utils.delete_from.client") as m:
        m.rest.ApiException = ApiException
        yield m


def _mock_api(mock_client, api_class_name, method_names):
    """Point client.<api_class_name> at a mock exposing exactly method_names."""
    api_class_mock = mock.MagicMock()
    api_mock = mock.MagicMock(spec=method_names)
    api_class_mock.return_value = api_mock
    setattr(mock_client, api_class_name, api_class_mock)
    return api_mock


def _doc(api_version="apps/v1", kind="Deployment", name="my-dep", namespace=None):
    metadata = {"name": name}
    if namespace is not None:
        metadata["namespace"] = namespace
    return {"apiVersion": api_version, "kind": kind, "metadata": metadata}


class TestDeleteFromYamlSingleItem:
    @pytest.mark.parametrize(
        ("api_version", "kind", "api_class", "method"),
        [
            pytest.param("v1", "Pod", "CoreV1Api", "delete_namespaced_pod", id="core-group"),
            pytest.param(
                "apps/v1", "Deployment", "AppsV1Api", "delete_namespaced_deployment", id="named-group"
            ),
            pytest.param(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                "ApiextensionsV1Api",
                "delete_custom_resource_definition",
                id="k8s-io-suffix-stripped",
            ),
            pytest.param(
                "networking.k8s.io/v1",
                "Ingress",
                "NetworkingV1Api",
                "delete_namespaced_ingress",
                id="dns-subdomain-group",
            ),
        ],
    )
    def test_api_class_derivation_and_dispatch(self, mock_client, api_version, kind, api_class, method):
        api_mock = _mock_api(mock_client, api_class, [method])

        resp = _delete_from_yaml_single_item(
            k8s_client=mock.sentinel.k8s_client,
            yml_document=_doc(api_version, kind),
            namespace="default",
        )

        getattr(mock_client, api_class).assert_called_once_with(mock.sentinel.k8s_client)
        getattr(api_mock, method).assert_called_once()
        assert resp is getattr(api_mock, method).return_value

    def test_document_namespace_takes_precedence(self, mock_client):
        api_mock = _mock_api(mock_client, "AppsV1Api", ["delete_namespaced_deployment"])

        _delete_from_yaml_single_item(
            k8s_client=mock.sentinel.k8s_client,
            yml_document=_doc(namespace="doc-namespace"),
            namespace="arg-namespace",
        )

        api_mock.delete_namespaced_deployment.assert_called_once_with(
            name="my-dep", namespace="doc-namespace", body=DEFAULT_DELETION_BODY
        )

    def test_argument_namespace_used_when_document_has_none(self, mock_client):
        api_mock = _mock_api(mock_client, "AppsV1Api", ["delete_namespaced_deployment"])

        _delete_from_yaml_single_item(
            k8s_client=mock.sentinel.k8s_client,
            yml_document=_doc(),
            namespace="arg-namespace",
        )

        api_mock.delete_namespaced_deployment.assert_called_once_with(
            name="my-dep", namespace="arg-namespace", body=DEFAULT_DELETION_BODY
        )

    def test_explicit_body_passed_through(self, mock_client):
        api_mock = _mock_api(mock_client, "AppsV1Api", ["delete_namespaced_deployment"])
        body = mock.sentinel.body

        _delete_from_yaml_single_item(
            k8s_client=mock.sentinel.k8s_client,
            yml_document=_doc(),
            namespace="default",
            body=body,
        )

        api_mock.delete_namespaced_deployment.assert_called_once_with(
            name="my-dep", namespace="default", body=body
        )

    def test_verbose_prints_deletion_status(self, mock_client, capsys):
        api_mock = _mock_api(mock_client, "CoreV1Api", ["delete_namespaced_pod"])
        api_mock.delete_namespaced_pod.return_value.status = "Success"

        _delete_from_yaml_single_item(
            k8s_client=mock.sentinel.k8s_client,
            yml_document=_doc("v1", "Pod"),
            namespace="default",
            verbose=True,
        )

        assert "pod deleted. status='Success'" in capsys.readouterr().out


class TestDeleteFromDict:
    def test_list_kind_fans_out_over_items(self, mock_client):
        api_mock = _mock_api(mock_client, "AppsV1Api", ["delete_namespaced_deployment"])
        data = {
            "apiVersion": "apps/v1",
            "kind": "DeploymentList",
            "items": [{"metadata": {"name": "dep-1"}}, {"metadata": {"name": "dep-2"}}],
        }

        delete_from_dict(k8s_client=mock.sentinel.k8s_client, data=data, body=None, namespace="default")

        assert api_mock.delete_namespaced_deployment.call_count == 2
        for item in data["items"]:
            assert item["apiVersion"] == "apps/v1"
            assert item["kind"] == "Deployment"

    def test_api_exceptions_collected_into_fail_to_delete_error(self, mock_client):
        api_mock = _mock_api(mock_client, "AppsV1Api", ["delete_namespaced_deployment"])
        error = ApiException(status=404, reason="Not Found")
        api_mock.delete_namespaced_deployment.side_effect = error
        data = {
            "apiVersion": "apps/v1",
            "kind": "DeploymentList",
            "items": [{"metadata": {"name": "dep-1"}}, {"metadata": {"name": "dep-2"}}],
        }

        with pytest.raises(FailToDeleteError) as exc_info:
            delete_from_dict(k8s_client=mock.sentinel.k8s_client, data=data, body=None, namespace="default")

        assert exc_info.value.api_exceptions == [error, error]

    def test_single_document_api_exception_raises_fail_to_delete_error(self, mock_client):
        api_mock = _mock_api(mock_client, "AppsV1Api", ["delete_namespaced_deployment"])
        error = ApiException(status=409, reason="Conflict")
        api_mock.delete_namespaced_deployment.side_effect = error

        with pytest.raises(FailToDeleteError) as exc_info:
            delete_from_dict(
                k8s_client=mock.sentinel.k8s_client,
                data=_doc(),
                body=None,
                namespace="default",
            )

        assert exc_info.value.api_exceptions == [error]


class TestDeleteFromYaml:
    def test_skips_none_documents(self, mock_client):
        with mock.patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict"
        ) as mock_delete_from_dict:
            delete_from_yaml(
                k8s_client=mock.sentinel.k8s_client,
                yaml_objects=[None, {"kind": "Pod"}],
                namespace="custom",
            )

        mock_delete_from_dict.assert_called_once_with(
            k8s_client=mock.sentinel.k8s_client,
            data={"kind": "Pod"},
            body=None,
            namespace="custom",
            verbose=False,
        )


class TestFailToDeleteError:
    def test_str_formats_all_exceptions(self):
        errors = [
            mock.MagicMock(reason="Not Found", body="no such deployment"),
            mock.MagicMock(reason="Conflict", body="already deleting"),
        ]

        assert str(FailToDeleteError(errors)) == (
            "Error from server (Not Found):no such deployment\n"
            "Error from server (Conflict):already deleting\n"
        )
