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
"""This module contains Facebook Ads Reporting hooks"""
from __future__ import annotations

import time
from enum import Enum
from typing import Any

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class JobStatus(Enum):
    """Available options for facebook async task status"""

    COMPLETED = "Job Completed"
    STARTED = "Job Started"
    RUNNING = "Job Running"
    FAILED = "Job Failed"
    SKIPPED = "Job Skipped"


class FacebookAdsReportingHook(BaseHook):
    """
    Hook for the Facebook Ads API

    .. seealso::
        For more information on the Facebook Ads API, take a look at the API docs:
        https://developers.facebook.com/docs/marketing-apis/

    :param facebook_conn_id: Airflow Facebook Ads connection ID
    :param api_version: The version of Facebook API. Default to None. If it is None,
        it will use the Facebook business SDK default version.

    """

    conn_name_attr = "facebook_conn_id"
    default_conn_name = "facebook_default"
    conn_type = "facebook_social"
    hook_name = "Facebook Ads"

    def __init__(
        self,
        facebook_conn_id: str = default_conn_name,
        api_version: str | None = None,
    ) -> None:
        super().__init__()
        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version
        self.client_required_fields = ["app_id", "app_secret", "access_token", "account_id"]

    def _get_service(self) -> FacebookAdsApi:
        """Returns Facebook Ads Client using a service account"""
        config = self.facebook_ads_config
        return FacebookAdsApi.init(
            app_id=config["app_id"],
            app_secret=config["app_secret"],
            access_token=config["access_token"],
            api_version=self.api_version,
        )

    @cached_property
    def multiple_accounts(self) -> bool:
        """Checks whether provided account_id in the Facebook Ads Connection is provided as a list"""
        return isinstance(self.facebook_ads_config["account_id"], list)

    @cached_property
    def facebook_ads_config(self) -> dict:
        """
        Gets Facebook ads connection from meta db and sets
        facebook_ads_config attribute with returned config file
        """
        self.log.info("Fetching fb connection: %s", self.facebook_conn_id)
        conn = self.get_connection(self.facebook_conn_id)
        config = conn.extra_dejson
        missing_keys = self.client_required_fields - config.keys()
        if missing_keys:
            message = f"{missing_keys} fields are missing"
            raise AirflowException(message)
        return config

    def bulk_facebook_report(
        self,
        params: dict[str, Any] | None,
        fields: list[str],
        sleep_time: int = 5,
    ) -> list[AdsInsights] | dict[str, list[AdsInsights]]:
        """Pulls data from the Facebook Ads API regarding Account ID with matching return type.

        The return type and value depends on the ``account_id`` configuration. If the
        configuration is a str representing a single Account ID, the return value is the
        list of reports for that ID. If the configuration is a list of str representing
        multiple Account IDs, the return value is a dict of Account IDs and their
        respective list of reports.

        :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :param sleep_time: Time to sleep when async call is happening

        :return: Facebook Ads API response,
            converted to Facebook Ads Row objects regarding given Account ID type
        :rtype: List[AdsInsights] or Dict[str, List[AdsInsights]]
        """
        api = self._get_service()
        if self.multiple_accounts:
            all_insights = {}
            for account_id in self.facebook_ads_config["account_id"]:
                all_insights[account_id] = self._facebook_report(
                    account_id=account_id, api=api, params=params, fields=fields, sleep_time=sleep_time
                )
                self.log.info(
                    "%s Account Id used to extract data from Facebook Ads Iterators successfully", account_id
                )
            return all_insights
        else:
            return self._facebook_report(
                account_id=self.facebook_ads_config["account_id"],
                api=api,
                params=params,
                fields=fields,
                sleep_time=sleep_time,
            )

    def _facebook_report(
        self,
        account_id: str,
        api: FacebookAdsApi,
        params: dict[str, Any] | None,
        fields: list[str],
        sleep_time: int = 5,
    ) -> list[AdsInsights]:
        """
        Pulls data from the Facebook Ads API with given account_id

        :param account_id: Facebook Account ID that holds ads information
                https://developers.facebook.com/docs/marketing-api/reference/ads-insights/
        :param api: FacebookAdsApi created in the hook
        :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :param params: Parameters that determine the query for Facebook
            https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
        :param sleep_time: Time to sleep when async call is happening
        """
        ad_account = AdAccount(account_id, api=api)
        _async = ad_account.get_insights(params=params, fields=fields, is_async=True)
        while True:
            request = _async.api_get()
            async_status = request[AdReportRun.Field.async_status]
            percent = request[AdReportRun.Field.async_percent_completion]
            self.log.info("%s %s completed, async_status: %s", percent, "%", async_status)
            if async_status == JobStatus.COMPLETED.value:
                self.log.info("Job run completed")
                break
            if async_status in [JobStatus.SKIPPED.value, JobStatus.FAILED.value]:
                message = f"{async_status}. Please retry."
                raise AirflowException(message)
            time.sleep(sleep_time)
        report_run_id = _async.api_get()["report_run_id"]
        report_object = AdReportRun(report_run_id, api=api)
        self.log.info("Extracting data from returned Facebook Ads Iterators")
        insights = report_object.get_insights()
        return list(insights)
