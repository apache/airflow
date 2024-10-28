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

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

MOCK_PROVIDERS = {
    "apache-airflow-providers-amazon": ProviderInfo(
        "1.0.0",
        {
            "package-name": "apache-airflow-providers-amazon",
            "name": "Amazon",
            "description": "`Amazon Web Services (AWS) <https://aws.amazon.com/>`__.\n",
            "versions": ["1.0.0"],
        },
        "package",
    ),
    "apache-airflow-providers-apache-cassandra": ProviderInfo(
        "1.0.0",
        {
            "package-name": "apache-airflow-providers-apache-cassandra",
            "name": "Apache Cassandra",
            "description": "`Apache Cassandra <http://cassandra.apache.org/>`__.\n",
            "versions": ["1.0.0"],
        },
        "package",
    ),
}


class TestGetProviders:
    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_package_name",
        [
            # Filters
            (
                {},
                2,
                [
                    "apache-airflow-providers-amazon",
                    "apache-airflow-providers-apache-cassandra",
                ],
            ),
            ({"limit": 1}, 2, ["apache-airflow-providers-amazon"]),
            ({"offset": 1}, 2, ["apache-airflow-providers-apache-cassandra"]),
        ],
    )
    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value=MOCK_PROVIDERS,
    )
    def test_get_dags(
        self,
        mock_provider,
        test_client,
        query_params,
        expected_total_entries,
        expected_package_name,
    ):
        response = test_client.get("/public/providers", params=query_params)

        assert response.status_code == 200
        body = response.json()

        assert body["total_entries"] == expected_total_entries
        assert [
            provider["package_name"] for provider in body["providers"]
        ] == expected_package_name
