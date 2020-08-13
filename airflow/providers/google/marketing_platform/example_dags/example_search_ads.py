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
"""
Example Airflow DAG that shows how to use SearchAds.
"""
import os

from airflow import models
from airflow.providers.google.marketing_platform.operators.search_ads import (
    GoogleSearchAdsDownloadReportOperator, GoogleSearchAdsInsertReportOperator,
)
from airflow.providers.google.marketing_platform.sensors.search_ads import GoogleSearchAdsReportSensor
from airflow.utils import dates

# [START howto_search_ads_env_variables]
AGENCY_ID = os.environ.get("GMP_AGENCY_ID")
ADVERTISER_ID = os.environ.get("GMP_ADVERTISER_ID")
GCS_BUCKET = os.environ.get("GMP_GCS_BUCKET", "test-cm-bucket")

REPORT = {
    "reportScope": {"agencyId": AGENCY_ID, "advertiserId": ADVERTISER_ID},
    "reportType": "account",
    "columns": [{"columnName": "agency"}, {"columnName": "lastModifiedTimestamp"}],
    "includeRemovedEntities": False,
    "statisticsCurrency": "usd",
    "maxRowsPerFile": 1000000,
    "downloadFormat": "csv",
}
# [END howto_search_ads_env_variables]

with models.DAG(
    "example_search_ads",
    schedule_interval=None,  # Override to match your needs,
    start_date=dates.days_ago(1)
) as dag:
    # [START howto_search_ads_generate_report_operator]
    generate_report = GoogleSearchAdsInsertReportOperator(
        report=REPORT, task_id="generate_report"
    )
    # [END howto_search_ads_generate_report_operator]

    # [START howto_search_ads_get_report_id]
    report_id = "{{ task_instance.xcom_pull('generate_report', key='report_id') }}"
    # [END howto_search_ads_get_report_id]

    # [START howto_search_ads_get_report_operator]
    wait_for_report = GoogleSearchAdsReportSensor(
        report_id=report_id, task_id="wait_for_report"
    )
    # [END howto_search_ads_get_report_operator]

    # [START howto_search_ads_getfile_report_operator]
    download_report = GoogleSearchAdsDownloadReportOperator(
        report_id=report_id, bucket_name=GCS_BUCKET, task_id="download_report"
    )
    # [END howto_search_ads_getfile_report_operator]

    generate_report >> wait_for_report >> download_report
