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
from unittest import TestCase, mock

from airflow.providers.google.marketing_platform.hooks.campaign_manager import GoogleCampaignManagerHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = "v3.3"
GCP_CONN_ID = "google_cloud_default"


class TestGoogleCampaignManagerHook(TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.base.CloudBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleCampaignManagerHook(
                gcp_conn_id=GCP_CONN_ID, api_version=API_VERSION
            )

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook._authorize"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.build"
    )
    def test_gen_conn(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "dfareporting",
            API_VERSION,
            http=mock_authorize.return_value,
            cache_discovery=False,
        )
        self.assertEqual(mock_build.return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.CloudBaseHook.__init__"
    )
    def test_delete_report(self, mock_base_hook, get_conn_mock):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.delete.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.delete_report(profile_id=profile_id, report_id=report_id)

        get_conn_mock.return_value.reports.return_value.delete.assert_called_once_with(
            profileId=profile_id, reportId=report_id
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.CloudBaseHook.__init__"
    )
    def test_get_report(self, mock_base_hook, get_conn_mock):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"
        file_id = "FILE_ID"

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.files.return_value.get.\
            return_value.execute.return_value = return_value

        result = self.hook.get_report(
            profile_id=profile_id, report_id=report_id, file_id=file_id
        )

        get_conn_mock.return_value.reports.return_value.files.return_value.get.assert_called_once_with(
            profileId=profile_id, reportId=report_id, fileId=file_id
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.CloudBaseHook.__init__"
    )
    def test_get_report_file(self, mock_base_hook, get_conn_mock):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"
        file_id = "FILE_ID"

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.files.return_value.get_media.return_value = (
            return_value
        )

        result = self.hook.get_report_file(
            profile_id=profile_id, report_id=report_id, file_id=file_id
        )

        get_conn_mock.return_value.reports.return_value.files.return_value.get_media.assert_called_once_with(
            profileId=profile_id, reportId=report_id, fileId=file_id
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.CloudBaseHook.__init__"
    )
    def test_insert_report(self, mock_base_hook, get_conn_mock):
        profile_id = "PROFILE_ID"
        report = {"body": "test"}

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.insert.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.insert_report(profile_id=profile_id, report=report)

        get_conn_mock.return_value.reports.return_value.insert.assert_called_once_with(
            profileId=profile_id, body=report
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.CloudBaseHook.__init__"
    )
    def test_list_reports(self, mock_base_hook, get_conn_mock):
        profile_id = "PROFILE_ID"
        max_results = 42
        scope = "SCOPE"
        sort_field = "SORT_FIELD"
        sort_order = "SORT_ORDER"
        items = ['item']

        return_value = {"nextPageToken": None, "items": items}
        get_conn_mock.return_value.reports.return_value.list.return_value.\
            execute.return_value = return_value

        request_mock = mock.MagicMock()
        request_mock.execute.return_value = {"nextPageToken": None, "items": items}
        get_conn_mock.return_value.reports.return_value.list_next.side_effect = [
            request_mock,
            request_mock,
            request_mock,
            None
        ]

        result = self.hook.list_reports(
            profile_id=profile_id,
            max_results=max_results,
            scope=scope,
            sort_field=sort_field,
            sort_order=sort_order,
        )

        get_conn_mock.return_value.reports.return_value.list.assert_called_once_with(
            profileId=profile_id,
            maxResults=max_results,
            scope=scope,
            sortField=sort_field,
            sortOrder=sort_order,
        )

        self.assertEqual(items * 4, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.CloudBaseHook.__init__"
    )
    def test_patch_report(self, mock_base_hook, get_conn_mock):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"
        update_mask = {"test": "test"}

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.patch.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.patch_report(
            profile_id=profile_id, report_id=report_id, update_mask=update_mask
        )

        get_conn_mock.return_value.reports.return_value.patch.assert_called_once_with(
            profileId=profile_id, reportId=report_id, body=update_mask
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.CloudBaseHook.__init__"
    )
    def test_run_report(self, mock_base_hook, get_conn_mock):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"
        synchronous = True

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.run.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.run_report(
            profile_id=profile_id, report_id=report_id, synchronous=synchronous
        )

        get_conn_mock.return_value.reports.return_value.run.assert_called_once_with(
            profileId=profile_id, reportId=report_id, synchronous=synchronous
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.CloudBaseHook.__init__"
    )
    def test_update_report(self, mock_base_hook, get_conn_mock):
        profile_id = "PROFILE_ID"
        report_id = "REPORT_ID"

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.update.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.update_report(profile_id=profile_id, report_id=report_id)

        get_conn_mock.return_value.reports.return_value.update.assert_called_once_with(
            profileId=profile_id, reportId=report_id
        )

        self.assertEqual(return_value, result)
