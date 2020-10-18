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
from unittest import mock

from airflow.providers.google.cloud.transfers.facebook_ads_to_gcs import FacebookAdsReportToGcsOperator

GCS_BUCKET = "airflow_bucket_fb"
GCS_OBJ_PATH = "Temp/this_is_my_report_json.json"
GCS_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
FACEBOOK_ADS_CONN_ID = "facebook_default"
API_VERSION = "v6.0"
FIELDS = [
    "campaign_name",
    "campaign_id",
    "ad_id",
    "clicks",
    "impressions",
]
PARAMS = {"level": "ad", "date_preset": "yesterday"}
FACEBOOK_RETURN_VALUE = [
    {
        "campaign_name": "abcd",
        "campaign_id": "abcd",
        "ad_id": "abcd",
        "clicks": "2",
        "impressions": "2",
    }
]


class TestFacebookAdsReportToGcsOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.facebook_ads_to_gcs.FacebookAdsReportingHook")
    @mock.patch("airflow.providers.google.cloud.transfers.facebook_ads_to_gcs.GCSHook")
    def test_execute(self, mock_gcs_hook, mock_ads_hook):
        mock_ads_hook.return_value.bulk_facebook_report.return_value = FACEBOOK_RETURN_VALUE
        op = FacebookAdsReportToGcsOperator(
            facebook_conn_id=FACEBOOK_ADS_CONN_ID,
            fields=FIELDS,
            params=PARAMS,
            object_name=GCS_OBJ_PATH,
            bucket_name=GCS_BUCKET,
            task_id="run_operator",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute({})
        mock_ads_hook.assert_called_once_with(facebook_conn_id=FACEBOOK_ADS_CONN_ID, api_version=API_VERSION)
        mock_ads_hook.return_value.bulk_facebook_report.assert_called_once_with(params=PARAMS, fields=FIELDS)
        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_gcs_hook.return_value.upload.assert_called_once_with(
            bucket_name=GCS_BUCKET, object_name=GCS_OBJ_PATH, filename=mock.ANY, gzip=False
        )
