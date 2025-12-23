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
"""This module contains Facebook Ad Reporting to GCS operators."""

from __future__ import annotations

import csv
import tempfile
from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.facebook.ads.hooks.ads import FacebookAdsReportingHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.version_compat import BaseOperator

if TYPE_CHECKING:
    from facebook_business.adobjects.adsinsights import AdsInsights

    from airflow.providers.common.compat.sdk import Context


class FlushAction(Enum):
    """Facebook Ads Export Options."""

    EXPORT_ONCE = "ExportAtOnce"
    EXPORT_EVERY_ACCOUNT = "ExportEveryAccount"


class FacebookAdsReportToGcsOperator(BaseOperator):
    """
    Fetch from Facebook Ads API.

    This converts and saves the data as a temporary JSON file, and uploads the
    JSON to Google Cloud Storage.

    .. seealso::
        For more information on the Facebook Ads API, take a look at the API docs:
        https://developers.facebook.com/docs/marketing-apis/

    .. seealso::
        For more information on the Facebook Ads Python SDK, take a look at the docs:
        https://github.com/facebook/facebook-python-business-sdk

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FacebookAdsReportToGcsOperator`

    :param bucket_name: The GCS bucket to upload to
    :param object_name: GCS path to save the object. Must be the full file path (ex. `path/to/file.txt`)
    :param gcp_conn_id: Airflow Google Cloud connection ID
    :param facebook_conn_id: Airflow Facebook Ads connection ID
    :param api_version: The version of Facebook API. Default to None. If it is None,
        it will use the Facebook business SDK default version.
    :param fields: List of fields that is obtained from Facebook. Found in AdsInsights.Field class.
        https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
    :param parameters: Parameters that determine the query for Facebook
        https://developers.facebook.com/docs/marketing-api/insights/parameters/v6.0
    :param gzip: Option to compress local file or file data for upload
    :param upload_as_account: Option to export file with account_id
        This parameter only works if Account Id sets as array in Facebook Connection
        If set as True, each file will be exported in a separate file that has a prefix of account_id
        If set as False, a single file will be exported for all account_id
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "facebook_conn_id",
        "bucket_name",
        "object_name",
        "impersonation_chain",
        "parameters",
    )

    def __init__(
        self,
        *,
        bucket_name: str,
        object_name: str,
        fields: list[str],
        parameters: dict[str, Any] | None = None,
        gzip: bool = False,
        upload_as_account: bool = False,
        api_version: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        facebook_conn_id: str = "facebook_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gcp_conn_id = gcp_conn_id
        self.facebook_conn_id = facebook_conn_id
        self.api_version = api_version
        self.fields = fields
        self.parameters = parameters
        self.gzip = gzip
        self.upload_as_account = upload_as_account
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        service = FacebookAdsReportingHook(
            facebook_conn_id=self.facebook_conn_id, api_version=self.api_version
        )
        bulk_report = service.bulk_facebook_report(params=self.parameters, fields=self.fields)

        if isinstance(bulk_report, list):
            converted_rows_with_action = self._generate_rows_with_action(False)
            converted_rows_with_action = self._prepare_rows_for_upload(
                rows=bulk_report, converted_rows_with_action=converted_rows_with_action, account_id=None
            )
        elif isinstance(bulk_report, dict):
            converted_rows_with_action = self._generate_rows_with_action(True)
            for account_id in bulk_report.keys():
                rows = bulk_report.get(account_id, [])
                if rows:
                    converted_rows_with_action = self._prepare_rows_for_upload(
                        rows=rows,
                        converted_rows_with_action=converted_rows_with_action,
                        account_id=account_id,
                    )
                else:
                    self.log.warning("account_id: %s returned empty report", account_id)
        else:
            message = (
                "Facebook Ads Hook returned different type than expected. Expected return types should be "
                f"List or Dict. Actual return type of the Hook: {type(bulk_report)}"
            )
            raise AirflowException(message)
        total_row_count = self._decide_and_flush(converted_rows_with_action=converted_rows_with_action)
        self.log.info("Facebook Returned %s data points in total: ", total_row_count)

    def _generate_rows_with_action(self, type_check: bool):
        if type_check and self.upload_as_account:
            return {FlushAction.EXPORT_EVERY_ACCOUNT: []}
        return {FlushAction.EXPORT_ONCE: []}

    def _prepare_rows_for_upload(
        self,
        rows: list[AdsInsights],
        converted_rows_with_action: dict[FlushAction, list],
        account_id: str | None,
    ):
        converted_rows = [dict(row) for row in rows]
        if account_id is not None and self.upload_as_account:
            converted_rows_with_action[FlushAction.EXPORT_EVERY_ACCOUNT].append(
                {"account_id": account_id, "converted_rows": converted_rows}
            )
            self.log.info(
                "Facebook Returned %s data points for account_id: %s", len(converted_rows), account_id
            )
        else:
            converted_rows_with_action[FlushAction.EXPORT_ONCE].extend(converted_rows)
            self.log.info("Facebook Returned %s data points ", len(converted_rows))
        return converted_rows_with_action

    def _decide_and_flush(self, converted_rows_with_action: dict[FlushAction, list]):
        total_data_count = 0
        once_action = converted_rows_with_action.get(FlushAction.EXPORT_ONCE)
        if once_action is not None:
            self._flush_rows(
                converted_rows=once_action,
                object_name=self.object_name,
            )
            total_data_count += len(once_action)
        else:
            every_account_action = converted_rows_with_action.get(FlushAction.EXPORT_EVERY_ACCOUNT)
            if every_account_action:
                for converted_rows in every_account_action:
                    self._flush_rows(
                        converted_rows=converted_rows.get("converted_rows"),
                        object_name=self._transform_object_name_with_account_id(
                            account_id=converted_rows.get("account_id")
                        ),
                    )
                    total_data_count += len(converted_rows.get("converted_rows"))
            else:
                message = (
                    "FlushAction not found in the data. Please check the FlushAction in "
                    f"the operator. Converted Rows with Action: {converted_rows_with_action}"
                )
                raise AirflowException(message)
        return total_data_count

    def _flush_rows(self, converted_rows: list[Any] | None, object_name: str):
        if converted_rows:
            headers = self.fields
            with tempfile.NamedTemporaryFile("w", suffix=".csv") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(converted_rows)
                csvfile.flush()
                hook = GCSHook(
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                )
                hook.upload(
                    bucket_name=self.bucket_name,
                    object_name=object_name,
                    filename=csvfile.name,
                    gzip=self.gzip,
                )
                self.log.info("%s uploaded to GCS", csvfile.name)

    def _transform_object_name_with_account_id(self, account_id: str):
        directory_parts = self.object_name.split("/")
        directory_parts[-1] = f"{account_id}_{directory_parts[-1]}"
        return "/".join(directory_parts)
