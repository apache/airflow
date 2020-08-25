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

REPORT_ID = "REPORT_ID"
PROFILE_ID = "PROFILE_ID"
ENCRYPTION_SOURCE = "encryption_source"
ENCRYPTION_ENTITY_TYPE = "encryption_entity_type"
ENCRYPTION_ENTITY_ID = 1234567


class TestGoogleCampaignManagerHook(TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleCampaignManagerHook(gcp_conn_id=GCP_CONN_ID, api_version=API_VERSION)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook._authorize"
    )
    @mock.patch("airflow.providers.google.marketing_platform.hooks.campaign_manager.build")
    def test_gen_conn(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "dfareporting", API_VERSION, http=mock_authorize.return_value, cache_discovery=False,
        )
        self.assertEqual(mock_build.return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    def test_delete_report(self, get_conn_mock):
        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.delete.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.delete_report(profile_id=PROFILE_ID, report_id=REPORT_ID)

        get_conn_mock.return_value.reports.return_value.delete.assert_called_once_with(
            profileId=PROFILE_ID, reportId=REPORT_ID
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    def test_get_report(self, get_conn_mock):
        file_id = "FILE_ID"

        return_value = "TEST"
        # fmt: off
        get_conn_mock.return_value.reports.return_value.files.return_value. \
            get.return_value.execute.return_value = return_value
        # fmt: on

        result = self.hook.get_report(profile_id=PROFILE_ID, report_id=REPORT_ID, file_id=file_id)

        get_conn_mock.return_value.reports.return_value.files.return_value.get.assert_called_once_with(
            profileId=PROFILE_ID, reportId=REPORT_ID, fileId=file_id
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    def test_get_report_file(self, get_conn_mock):
        file_id = "FILE_ID"

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.files.return_value.get_media.return_value = (
            return_value
        )

        result = self.hook.get_report_file(profile_id=PROFILE_ID, report_id=REPORT_ID, file_id=file_id)

        get_conn_mock.return_value.reports.return_value.files.return_value.get_media.assert_called_once_with(
            profileId=PROFILE_ID, reportId=REPORT_ID, fileId=file_id
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    def test_insert_report(self, get_conn_mock):
        report = {"body": "test"}

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.insert.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.insert_report(profile_id=PROFILE_ID, report=report)

        get_conn_mock.return_value.reports.return_value.insert.assert_called_once_with(
            profileId=PROFILE_ID, body=report
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    def test_list_reports(self, get_conn_mock):
        max_results = 42
        scope = "SCOPE"
        sort_field = "SORT_FIELD"
        sort_order = "SORT_ORDER"
        items = ["item"]

        return_value = {"nextPageToken": None, "items": items}
        get_conn_mock.return_value.reports.return_value.list.return_value.execute.return_value = return_value

        request_mock = mock.MagicMock()
        request_mock.execute.return_value = {"nextPageToken": None, "items": items}
        get_conn_mock.return_value.reports.return_value.list_next.side_effect = [
            request_mock,
            request_mock,
            request_mock,
            None,
        ]

        result = self.hook.list_reports(
            profile_id=PROFILE_ID,
            max_results=max_results,
            scope=scope,
            sort_field=sort_field,
            sort_order=sort_order,
        )

        get_conn_mock.return_value.reports.return_value.list.assert_called_once_with(
            profileId=PROFILE_ID,
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
    def test_patch_report(self, get_conn_mock):
        update_mask = {"test": "test"}

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.patch.return_value.execute.return_value = return_value

        result = self.hook.patch_report(profile_id=PROFILE_ID, report_id=REPORT_ID, update_mask=update_mask)

        get_conn_mock.return_value.reports.return_value.patch.assert_called_once_with(
            profileId=PROFILE_ID, reportId=REPORT_ID, body=update_mask
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    def test_run_report(self, get_conn_mock):
        synchronous = True

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.run.return_value.execute.return_value = return_value

        result = self.hook.run_report(profile_id=PROFILE_ID, report_id=REPORT_ID, synchronous=synchronous)

        get_conn_mock.return_value.reports.return_value.run.assert_called_once_with(
            profileId=PROFILE_ID, reportId=REPORT_ID, synchronous=synchronous
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    def test_update_report(self, get_conn_mock):
        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.update.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.update_report(profile_id=PROFILE_ID, report_id=REPORT_ID)

        get_conn_mock.return_value.reports.return_value.update.assert_called_once_with(
            profileId=PROFILE_ID, reportId=REPORT_ID
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform."
        "hooks.campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.campaign_manager.GoogleCampaignManagerHook"
        "._conversions_batch_request"
    )
    def test_conversion_batch_insert(self, batch_request_mock, get_conn_mock):
        conversions = [{"conversions1": "value"}, {"conversions2": "value"}]

        return_value = {'hasFailures': False}
        get_conn_mock.return_value.conversions.return_value.batchinsert.return_value.execute.return_value = (
            return_value
        )

        batch_request_mock.return_value = "batch_request_mock"

        result = self.hook.conversions_batch_insert(
            profile_id=PROFILE_ID,
            conversions=conversions,
            encryption_entity_id=ENCRYPTION_ENTITY_ID,
            encryption_entity_type=ENCRYPTION_ENTITY_TYPE,
            encryption_source=ENCRYPTION_SOURCE,
        )

        batch_request_mock.assert_called_once_with(
            conversions=conversions,
            encryption_entity_id=ENCRYPTION_ENTITY_ID,
            encryption_entity_type=ENCRYPTION_ENTITY_TYPE,
            encryption_source=ENCRYPTION_SOURCE,
            kind="dfareporting#conversionsBatchInsertRequest",
        )
        get_conn_mock.return_value.conversions.return_value.batchinsert.assert_called_once_with(
            profileId=PROFILE_ID, body=batch_request_mock.return_value
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "campaign_manager.GoogleCampaignManagerHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.campaign_manager.GoogleCampaignManagerHook"
        "._conversions_batch_request"
    )
    def test_conversions_batch_update(self, batch_request_mock, get_conn_mock):
        conversions = [{"conversions1": "value"}, {"conversions2": "value"}]

        return_value = {'hasFailures': False}
        get_conn_mock.return_value.conversions.return_value.batchupdate.return_value.execute.return_value = (
            return_value
        )

        batch_request_mock.return_value = "batch_request_mock"

        result = self.hook.conversions_batch_update(
            profile_id=PROFILE_ID,
            conversions=conversions,
            encryption_entity_id=ENCRYPTION_ENTITY_ID,
            encryption_entity_type=ENCRYPTION_ENTITY_TYPE,
            encryption_source=ENCRYPTION_SOURCE,
        )

        batch_request_mock.assert_called_once_with(
            conversions=conversions,
            encryption_entity_id=ENCRYPTION_ENTITY_ID,
            encryption_entity_type=ENCRYPTION_ENTITY_TYPE,
            encryption_source=ENCRYPTION_SOURCE,
            kind="dfareporting#conversionsBatchUpdateRequest",
        )
        get_conn_mock.return_value.conversions.return_value.batchupdate.assert_called_once_with(
            profileId=PROFILE_ID, body=batch_request_mock.return_value
        )

        self.assertEqual(return_value, result)
