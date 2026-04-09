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

from airflow.providers.google.marketing_platform.hooks.bid_manager import GoogleBidManagerHook

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = "v2"
GCP_CONN_ID = "google_cloud_default"


class TestGoogleBidManagerHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleBidManagerHook(api_version=API_VERSION, gcp_conn_id=GCP_CONN_ID)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.bid_manager.GoogleBidManagerHook._authorize"
    )
    @mock.patch("airflow.providers.google.marketing_platform.hooks.bid_manager.build")
    def test_gen_conn(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "doubleclickbidmanager",
            API_VERSION,
            http=mock_authorize.return_value,
            cache_discovery=False,
        )
        assert mock_build.return_value == result

    @mock.patch("airflow.providers.google.marketing_platform.hooks.bid_manager.GoogleBidManagerHook.get_conn")
    def test_create_query(self, get_conn_mock):
        body = {"body": "test"}

        return_value = "TEST"
        get_conn_mock.return_value.queries.return_value.create.return_value.execute.return_value = (
            return_value
        )
        result = self.hook.create_query(query=body)

        get_conn_mock.return_value.queries.return_value.create.assert_called_once_with(body=body)

        assert return_value == result

    @mock.patch("airflow.providers.google.marketing_platform.hooks.bid_manager.GoogleBidManagerHook.get_conn")
    def test_delete_query(self, get_conn_mock):
        query_id = "QUERY_ID"

        return_value = "TEST"
        get_conn_mock.return_value.queries.return_value.delete.return_value.execute.return_value = (
            return_value
        )
        self.hook.delete_query(query_id=query_id)

        get_conn_mock.return_value.queries.return_value.delete.assert_called_once_with(queryId=query_id)

    @mock.patch("airflow.providers.google.marketing_platform.hooks.bid_manager.GoogleBidManagerHook.get_conn")
    def test_get_query(self, get_conn_mock):
        query_id = "QUERY_ID"

        return_value = "TEST"
        get_conn_mock.return_value.queries.return_value.get.return_value.execute.return_value = return_value
        result = self.hook.get_query(query_id=query_id)

        get_conn_mock.return_value.queries.return_value.get.assert_called_once_with(queryId=query_id)

        assert return_value == result

    @mock.patch("airflow.providers.google.marketing_platform.hooks.bid_manager.GoogleBidManagerHook.get_conn")
    def test_list_queries(self, get_conn_mock):
        queries = ["test"]
        return_value = {"queries": queries}
        get_conn_mock.return_value.queries.return_value.list.return_value.execute.return_value = return_value
        result = self.hook.list_queries()

        get_conn_mock.return_value.queries.return_value.list.assert_called_once_with()

        assert queries == result

    @mock.patch("airflow.providers.google.marketing_platform.hooks.bid_manager.GoogleBidManagerHook.get_conn")
    def test_run_query(self, get_conn_mock):
        query_id = "QUERY_ID"
        params = {"params": "test"}
        return_value = "TEST"
        get_conn_mock.return_value.queries.return_value.run.return_value.execute.return_value = return_value

        result = self.hook.run_query(query_id=query_id, params=params)

        get_conn_mock.return_value.queries.return_value.run.assert_called_once_with(
            queryId=query_id, body=params
        )
        assert return_value == result

    @mock.patch("airflow.providers.google.marketing_platform.hooks.bid_manager.GoogleBidManagerHook.get_conn")
    def test_get_report(self, get_conn_mock):
        query_id = "QUERY_ID"
        report_id = "REPORT_ID"
        return_value = "TEST_REPORT"
        (
            get_conn_mock.return_value.queries.return_value.reports.return_value.get.return_value.execute.return_value
        ) = return_value

        result = self.hook.get_report(query_id=query_id, report_id=report_id)

        get_conn_mock.return_value.queries.return_value.reports.return_value.get.assert_called_once_with(
            queryId=query_id, reportId=report_id
        )
        assert return_value == result

    @mock.patch("airflow.providers.google.marketing_platform.hooks.bid_manager.GoogleBidManagerHook.get_conn")
    def test_list_reports(self, get_conn_mock):
        query_id = "QUERY_ID"
        reports = ["report1", "report2"]
        return_value = {"reports": reports}
        (
            get_conn_mock.return_value.queries.return_value.reports.return_value.list.return_value.execute.return_value
        ) = return_value

        result = self.hook.list_reports(query_id=query_id)

        get_conn_mock.return_value.queries.return_value.reports.return_value.list.assert_called_once_with(
            queryId=query_id
        )
        assert reports == result["reports"]
