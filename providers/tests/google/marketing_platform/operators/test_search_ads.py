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

from airflow.providers.google.marketing_platform.operators.search_ads import (
    GoogleSearchAdsGetCustomColumnOperator,
    GoogleSearchAdsGetFieldOperator,
    GoogleSearchAdsListCustomColumnsOperator,
    GoogleSearchAdsSearchFieldsOperator,
    GoogleSearchAdsSearchOperator,
)

GCP_CONN_ID = "google_search_ads_default"
API_VERSION = "v0"
CUSTOMER_ID = "customer_id"


class TestGoogleSearchAdsSearchOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsReportingHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.BaseOperator"
    )
    def test_execute(self, mock_base_op, hook_mock):
        query = "SELECT * FROM campaigns WHERE segments.date DURING LAST_30_DAYS"
        hook_mock.return_value.search.return_value = {"results": []}
        op = GoogleSearchAdsSearchOperator(
            customer_id=CUSTOMER_ID,
            query=query,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v0",
        )
        hook_mock.return_value.search.assert_called_once_with(
            customer_id=CUSTOMER_ID,
            query=query,
            page_size=10000,
            page_token=None,
            return_total_results_count=False,
            summary_row_setting=None,
            validate_only=False,
        )


class TestGoogleSearchAdsGetFieldOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsReportingHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.BaseOperator"
    )
    def test_execute(self, mock_base_op, hook_mock):
        field_name = "the_field"
        hook_mock.return_value.get_field.return_value = {
            "name": field_name,
            "resourceName": f"searchAds360Fields/{field_name}",
        }
        op = GoogleSearchAdsGetFieldOperator(
            field_name=field_name,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v0",
        )
        hook_mock.return_value.get_field.assert_called_once_with(
            field_name=field_name,
        )


class TestGoogleSearchAdsSearchFieldsOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsReportingHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.BaseOperator"
    )
    def test_execute(self, mock_base_op, hook_mock):
        field_name = "the_field"
        query = (
            "SELECT "
            "  name, category, selectable, filterable, sortable, selectable_with, data_type, "
            "  is_repeated "
            "WHERE "
            "  name LIKE 'ad_group.%'"
        )
        hook_mock.return_value.search_fields.return_value = {
            "results": [
                {"name": field_name, "resource_name": f"searchAds360Fields/{field_name}"}
            ]
        }
        op = GoogleSearchAdsSearchFieldsOperator(
            query=query,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v0",
        )
        hook_mock.return_value.search_fields.assert_called_once_with(
            query=query,
            page_token=None,
            page_size=10000,
        )


class TestGoogleSearchAdsGetCustomColumnOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsReportingHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.BaseOperator"
    )
    def test_execute(self, mock_base_op, hook_mock):
        custom_column_id = "custom_column_id"
        hook_mock.return_value.get_custom_column.return_value = {"id": custom_column_id}
        op = GoogleSearchAdsGetCustomColumnOperator(
            customer_id=CUSTOMER_ID,
            custom_column_id=custom_column_id,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v0",
        )
        hook_mock.return_value.get_custom_column.assert_called_once_with(
            customer_id=CUSTOMER_ID,
            custom_column_id=custom_column_id,
        )


class TestGoogleSearchAdsListCustomColumnsOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.GoogleSearchAdsReportingHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.search_ads.BaseOperator"
    )
    def test_execute(self, mock_base_op, hook_mock):
        customs_columns = [
            {"id": "custom_column_id_1"},
            {"id": "custom_column_id_2"},
            {"id": "custom_column_id_3"},
        ]
        hook_mock.return_value.list_custom_columns.return_value = {
            "customColumns": customs_columns
        }
        op = GoogleSearchAdsListCustomColumnsOperator(
            customer_id=CUSTOMER_ID,
            api_version=API_VERSION,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version="v0",
        )
        hook_mock.return_value.list_custom_columns.assert_called_once_with(
            customer_id=CUSTOMER_ID,
        )
