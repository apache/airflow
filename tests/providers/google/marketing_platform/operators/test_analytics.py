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

import unittest
from tempfile import NamedTemporaryFile
from unittest import mock

from airflow.providers.google.marketing_platform.operators.analytics import (
    GoogleAnalyticsDataImportUploadOperator, GoogleAnalyticsDeletePreviousDataUploadsOperator,
    GoogleAnalyticsGetAdsLinkOperator, GoogleAnalyticsListAccountsOperator,
    GoogleAnalyticsModifyFileHeadersDataImportOperator, GoogleAnalyticsRetrieveAdsLinksListOperator,
)

WEB_PROPERTY_AD_WORDS_LINK_ID = "AAIIRRFFLLOOWW"
WEB_PROPERTY_ID = "web_property_id"
ACCOUNT_ID = "the_knight_who_says_ni!"
DATA_SOURCE = "Monthy Python"
API_VERSION = "v3"
GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
BUCKET = "gs://bucket"
BUCKET_OBJECT_NAME = "file.csv"


class TestGoogleAnalyticsListAccountsOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, hook_mock):
        op = GoogleAnalyticsListAccountsOperator(
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
        hook_mock.return_value.list_accounts.assert_called_once()


class TestGoogleAnalyticsRetrieveAdsLinksListOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, hook_mock):
        op = GoogleAnalyticsRetrieveAdsLinksListOperator(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
        hook_mock.return_value.list_ad_words_links.assert_called_once()
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.list_ad_words_links.assert_called_once_with(
            account_id=ACCOUNT_ID, web_property_id=WEB_PROPERTY_ID
        )


class TestGoogleAnalyticsGetAdsLinkOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, hook_mock):
        op = GoogleAnalyticsGetAdsLinkOperator(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            web_property_ad_words_link_id=WEB_PROPERTY_AD_WORDS_LINK_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
        hook_mock.return_value.get_ad_words_link.assert_called_once()
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hook_mock.return_value.get_ad_words_link.assert_called_once_with(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            web_property_ad_words_link_id=WEB_PROPERTY_AD_WORDS_LINK_ID,
        )


class TestGoogleAnalyticsDataImportUploadOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators." "analytics.GCSHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.analytics.NamedTemporaryFile"
    )
    def test_execute(self, mock_tempfile, gcs_hook_mock, ga_hook_mock):
        filename = "file/"
        mock_tempfile.return_value.__enter__.return_value.name = filename

        op = GoogleAnalyticsDataImportUploadOperator(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            storage_bucket=BUCKET,
            storage_name_object=BUCKET_OBJECT_NAME,
            custom_data_source_id=DATA_SOURCE,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=None)

        gcs_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        ga_hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version=API_VERSION,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        gcs_hook_mock.return_value.download.assert_called_once_with(
            bucket_name=BUCKET, object_name=BUCKET_OBJECT_NAME, filename=filename
        )

        ga_hook_mock.return_value.upload_data.assert_called_once_with(
            filename, ACCOUNT_ID, WEB_PROPERTY_ID, DATA_SOURCE, False
        )


class TestGoogleAnalyticsDeletePreviousDataUploadsOperator:
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, mock_hook):
        mock_hook.return_value.list_uploads.return_value = [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]

        op = GoogleAnalyticsDeletePreviousDataUploadsOperator(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            custom_data_source_id=DATA_SOURCE,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
        )
        op.execute(context=None)
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version=API_VERSION,
            impersonation_chain=None,
        )

        mock_hook.return_value.list_uploads.assert_called_once_with(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            custom_data_source_id=DATA_SOURCE,
        )

        mock_hook.return_value.delete_upload_data.assert_called_once_with(
            ACCOUNT_ID,
            WEB_PROPERTY_ID,
            DATA_SOURCE,
            {"customDataImportUids": [1, 2, 3]},
        )


class TestGoogleAnalyticsModifyFileHeadersDataImportOperator:
    def test_modify_column_headers(self):
        op = GoogleAnalyticsModifyFileHeadersDataImportOperator(
            storage_bucket=BUCKET,
            storage_name_object=BUCKET_OBJECT_NAME,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
        )

        data = """pagePath,dimension1
how_to_make_pizza,1
how_to_make_doughnuts,2
"""

        # No modification
        expected_data = """ga:pagePath,ga:dimension1
how_to_make_pizza,1
how_to_make_doughnuts,2
"""

        with NamedTemporaryFile("w+") as tmp:
            tmp.write(data)
            tmp.flush()

            op._modify_column_headers(tmp.name, {})

            with open(tmp.name) as f:
                assert expected_data == f.read()

        # with modification
        expected_data = """ga:brrr,ga:dimension1
how_to_make_pizza,1
how_to_make_doughnuts,2
"""
        with NamedTemporaryFile("w+") as tmp:
            tmp.write(data)
            tmp.flush()

            op._modify_column_headers(tmp.name, {"pagePath": "brrr"})

            with open(tmp.name) as f:
                assert expected_data == f.read()

    @mock.patch(
        "airflow.providers.google.marketing_platform.operators." "analytics.GCSHook"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsModifyFileHeadersDataImportOperator._modify_column_headers"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators.analytics.NamedTemporaryFile"
    )
    def test_execute(self, mock_tempfile, mock_modify, mock_hook):
        mapping = {"a": "b"}
        filename = "file/"
        mock_tempfile.return_value.__enter__.return_value.name = filename

        op = GoogleAnalyticsModifyFileHeadersDataImportOperator(
            storage_bucket=BUCKET,
            storage_name_object=BUCKET_OBJECT_NAME,
            custom_dimension_header_mapping=mapping,
            gcp_conn_id=GCP_CONN_ID,
            task_id="test_task",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=None)

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.download.assert_called_once_with(
            bucket_name=BUCKET, object_name=BUCKET_OBJECT_NAME, filename=filename
        )

        mock_modify.assert_called_once_with(
            tmp_file_location=filename, custom_dimension_header_mapping=mapping
        )

        mock_hook.return_value.upload(
            bucket_name=BUCKET, object_name=BUCKET_OBJECT_NAME, filename=filename
        )
