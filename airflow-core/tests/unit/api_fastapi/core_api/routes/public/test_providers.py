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

from airflow.providers_manager import ProviderInfo

from tests_common.test_utils.asserts import assert_queries_count

pytestmark = pytest.mark.db_test

MOCK_PROVIDERS = {
    "apache-airflow-providers-amazon": ProviderInfo(
        "1.0.0",
        {
            "package-name": "apache-airflow-providers-amazon",
            "name": "Amazon",
            "description": "`Amazon Web Services (AWS) <https://aws.amazon.com/>`__.\n",
            "versions": ["1.0.0"],
            "documentation-url": "https://airflow.apache.org/docs/apache-airflow-providers-amazon/1.0.0/",
            "connection-types": [
                {
                    "connection-type": "aws",
                    "hook-class-name": "airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook",
                    "hook-name": "Amazon Web Services",
                }
            ],
            "extra-links": ["airflow.providers.amazon.aws.links.emr.EmrClusterLink"],
        },
    ),
    "apache-airflow-providers-apache-cassandra": ProviderInfo(
        "1.0.0",
        {
            "package-name": "apache-airflow-providers-apache-cassandra",
            "name": "Apache Cassandra",
            "description": "`Apache Cassandra <http://cassandra.apache.org/>`__.\n",
            "versions": ["1.0.0"],
            "documentation-url": "https://airflow.apache.org/docs/apache-airflow-providers-apache-cassandra/1.0.0/",
        },
    ),
}

# A provider whose metadata is made up entirely of the keys shared with the summary response
# plus ``email-backends`` — nothing here may be dropped from ``provider_info``.
MOCK_SENDGRID_PROVIDER = {
    "apache-airflow-providers-sendgrid": ProviderInfo(
        "4.2.5",
        {
            "package-name": "apache-airflow-providers-sendgrid",
            "name": "Sendgrid",
            "description": "`Sendgrid <https://sendgrid.com/>`__\n",
            "email-backends": ["airflow.providers.sendgrid.utils.emailer.send_email"],
            "documentation-url": "https://airflow.apache.org/docs/apache-airflow-providers-sendgrid/4.2.5",
        },
    ),
}


class TestGetProviders:
    @pytest.mark.parametrize(
        ("query_params", "expected_total_entries", "expected_package_name"),
        [
            # Filters
            ({}, 2, ["apache-airflow-providers-amazon", "apache-airflow-providers-apache-cassandra"]),
            ({"limit": 1}, 2, ["apache-airflow-providers-amazon"]),
            ({"offset": 1}, 2, ["apache-airflow-providers-apache-cassandra"]),
        ],
    )
    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value=MOCK_PROVIDERS,
    )
    def test_should_respond_200(
        self, mock_provider, test_client, query_params, expected_total_entries, expected_package_name
    ):
        with assert_queries_count(0):
            response = test_client.get("/providers", params=query_params)

        assert response.status_code == 200
        body = response.json()

        assert body["total_entries"] == expected_total_entries
        assert [provider["package_name"] for provider in body["providers"]] == expected_package_name

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/providers")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/providers")
        assert response.status_code == 403


class TestGetProvider:
    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value=MOCK_PROVIDERS,
    )
    def test_should_respond_200(self, mock_provider, test_client):
        with assert_queries_count(0):
            response = test_client.get("/providers/apache-airflow-providers-amazon")

        assert response.status_code == 200
        body = response.json()
        assert body["package_name"] == "apache-airflow-providers-amazon"
        assert body["version"] == "1.0.0"
        assert body["description"] == "Amazon Web Services (AWS) https://aws.amazon.com/"
        provider_info = body["provider_info"]
        assert provider_info["name"] == "Amazon"
        assert provider_info["connection-types"] == [
            {
                "connection-type": "aws",
                "hook-class-name": "airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook",
                "hook-name": "Amazon Web Services",
            }
        ]
        assert provider_info["extra-links"] == ["airflow.providers.amazon.aws.links.emr.EmrClusterLink"]
        assert provider_info["package-name"] == "apache-airflow-providers-amazon"
        assert provider_info["description"] == "Amazon Web Services (AWS) https://aws.amazon.com/"
        assert (
            provider_info["documentation-url"]
            == "https://airflow.apache.org/docs/apache-airflow-providers-amazon/1.0.0/"
        )
        # Only keys explicitly declared in ProviderInfoResponse are serialized
        assert "versions" not in provider_info

    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value=MOCK_SENDGRID_PROVIDER,
    )
    def test_should_respond_200_with_all_provider_metadata(self, mock_provider, test_client):
        response = test_client.get("/providers/apache-airflow-providers-sendgrid")

        assert response.status_code == 200
        provider_info = response.json()["provider_info"]
        # The CLI renders the metadata with ``exclude_none``, so only the populated keys matter
        assert {key: value for key, value in provider_info.items() if value is not None} == {
            "package-name": "apache-airflow-providers-sendgrid",
            "name": "Sendgrid",
            "description": "Sendgrid https://sendgrid.com/",
            "email-backends": ["airflow.providers.sendgrid.utils.emailer.send_email"],
            "documentation-url": "https://airflow.apache.org/docs/apache-airflow-providers-sendgrid/4.2.5",
        }

    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value=MOCK_PROVIDERS,
    )
    def test_should_respond_404(self, mock_provider, test_client):
        response = test_client.get("/providers/missing-provider")

        assert response.status_code == 404
        assert response.json() == {"detail": "Provider 'missing-provider' was not found"}

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/providers/apache-airflow-providers-amazon")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/providers/apache-airflow-providers-amazon")
        assert response.status_code == 403
