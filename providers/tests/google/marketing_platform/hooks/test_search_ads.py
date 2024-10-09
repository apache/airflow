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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.providers.google.marketing_platform.hooks.search_ads import (
    GoogleSearchAdsHook,
    GoogleSearchAdsReportingHook,
)

from providers.tests.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

GCP_CONN_ID = "google_cloud_default"
API_VERSION = "v0"
CUSTOMER_ID = "customer_id"
QUERY = "SELECT * FROM campaigns WHERE segments.date DURING LAST_30_DAYS"


class TestGoogleSearchAdsReportingHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.marketing_platform.hooks.search_ads.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleSearchAdsReportingHook(gcp_conn_id=GCP_CONN_ID)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.search_ads.GoogleSearchAdsReportingHook.get_credentials"
    )
    @mock.patch("airflow.providers.google.marketing_platform.hooks.search_ads.build")
    def test_gen_conn(self, mock_build, mock_get_credentials):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "searchads360",
            API_VERSION,
            credentials=mock_get_credentials.return_value,
            cache_discovery=False,
        )
        assert mock_build.return_value == result

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.search_ads.GoogleSearchAdsReportingHook.customer_service"
    )
    @pytest.mark.parametrize(
        "given_args, expected_args_extras",
        [
            ({"page_token": None}, {}),
            ({"page_token": "next_page_token"}, {"pageToken": "next_page_token"}),
            ({"summary_row_setting": "summary line content"}, {"summaryRowSetting": "summary line content"}),
            ({"page_size": 10, "validate_only": True}, {"pageSize": 10, "validateOnly": True}),
        ],
    )
    def test_search(self, customer_service_mock, given_args, expected_args_extras):
        return_value = {"results": [{"x": 1}]}
        (
            customer_service_mock.searchAds360.return_value.search.return_value.execute
        ).return_value = return_value

        result = self.hook.search(customer_id=CUSTOMER_ID, query=QUERY, **given_args)

        expected_args = {
            "customerId": CUSTOMER_ID,
            "body": {
                "query": QUERY,
                "pageSize": 10000,
                "returnTotalResultsCount": False,
                "validateOnly": False,
                **expected_args_extras,
            },
        }
        customer_service_mock.searchAds360.return_value.search.assert_called_once_with(**expected_args)

        assert return_value == result

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.search_ads.GoogleSearchAdsReportingHook.customer_service"
    )
    def test_get_custom_column(self, customer_service_mock):
        custom_column_id = "custom_column_id"
        return_value = {"resourceName": 1}
        (
            customer_service_mock.customColumns.return_value.get.return_value.execute
        ).return_value = return_value

        result = self.hook.get_custom_column(customer_id=CUSTOMER_ID, custom_column_id=custom_column_id)

        customer_service_mock.customColumns.return_value.get.assert_called_once_with(
            resourceName=f"customers/{CUSTOMER_ID}/customColumns/{custom_column_id}"
        )

        assert return_value == result

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.search_ads.GoogleSearchAdsReportingHook.customer_service"
    )
    def test_list_custom_columns(self, customer_service_mock):
        return_value = {
            "results": [
                {"resourceName": f"customers/{CUSTOMER_ID}/customColumns/col1"},
                {"resourceName": f"customers/{CUSTOMER_ID}/customColumns/col2"},
            ]
        }
        (
            customer_service_mock.customColumns.return_value.list.return_value.execute
        ).return_value = return_value

        result = self.hook.list_custom_columns(customer_id=CUSTOMER_ID)

        customer_service_mock.customColumns.return_value.list.assert_called_once_with(customerId=CUSTOMER_ID)

        assert return_value == result

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.search_ads.GoogleSearchAdsReportingHook.fields_service"
    )
    def test_get_field(self, fields_service_mock):
        field_name = "field_name"
        return_value = {
            "name": "Field 1",
            "resourceName": f"customers/{CUSTOMER_ID}/searchAds360Fields/field1",
        }
        fields_service_mock.get.return_value.execute.return_value = return_value

        result = self.hook.get_field(field_name=field_name)

        fields_service_mock.get.assert_called_once_with(resourceName=f"searchAds360Fields/{field_name}")

        assert return_value == result

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.search_ads.GoogleSearchAdsReportingHook.fields_service"
    )
    @pytest.mark.parametrize(
        "given_args, expected_args_extras",
        [
            ({"page_token": None}, {}),
            ({"page_token": "next_page_token"}, {"pageToken": "next_page_token"}),
            ({"page_size": 10}, {"pageSize": 10}),
        ],
    )
    def test_search_fields(self, fields_service_mock, given_args, expected_args_extras):
        query = "SELECT field1, field2 FROM campaigns;"
        return_value = {
            "results": [
                {"name": "Field 1", "resourceName": f"customers/{CUSTOMER_ID}/searchAds360Fields/field1"},
                {"name": "Field 2", "resourceName": f"customers/{CUSTOMER_ID}/searchAds360Fields/field2"},
            ]
        }
        fields_service_mock.search.return_value.execute.return_value = return_value

        result = self.hook.search_fields(query=query, **given_args)

        expected_args = {"query": query, "pageSize": 10000, **expected_args_extras}
        fields_service_mock.search.assert_called_once_with(body=expected_args)

        assert return_value == result


class TestSearchAdsHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.marketing_platform.hooks.search_ads.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleSearchAdsHook(gcp_conn_id=GCP_CONN_ID)

    @mock.patch("airflow.providers.google.marketing_platform.hooks.search_ads.GoogleSearchAdsHook._authorize")
    @mock.patch("airflow.providers.google.marketing_platform.hooks.search_ads.build")
    def test_gen_conn(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "doubleclicksearch",
            "v2",
            http=mock_authorize.return_value,
            cache_discovery=False,
        )
        assert mock_build.return_value == result
