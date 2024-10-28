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
"""This module contains Google Analytics 360 operators."""

from __future__ import annotations

import csv
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.marketing_platform.hooks.analytics import (
    GoogleAnalyticsHook,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


@deprecated(
    planned_removal_date="November 01, 2024",
    use_instead="GoogleAnalyticsAdminListAccountsOperator",
    reason="The Google Analytics API v3 has sunset and is no longer available as of July 1, 2024.",
    category=AirflowProviderDeprecationWarning,
)
class GoogleAnalyticsListAccountsOperator(BaseOperator):
    """
    Lists all accounts to which the user has access.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminListAccountsOperator`:

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/accounts/list
        and for python client
        http://googleapis.github.io/google-api-python-client/docs/dyn/analytics_v3.management.accounts.html#list

    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "api_version",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list[dict[str, Any]]:
        hook = GoogleAnalyticsHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.list_accounts()
        return result


@deprecated(
    planned_removal_date="November 01, 2024",
    use_instead="GoogleAnalyticsAdminGetGoogleAdsLinkOperator",
    reason="The Google Analytics API v3 has sunset and is no longer available as of July 1, 2024.",
    category=AirflowProviderDeprecationWarning,
)
class GoogleAnalyticsGetAdsLinkOperator(BaseOperator):
    """
    Returns a web property-Google Ads link to which the user has access.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminGetGoogleAdsLinkOperator`:

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webPropertyAdWordsLinks/get


    :param account_id: ID of the account which the given web property belongs to.
    :param web_property_ad_words_link_id: Web property-Google Ads link ID.
    :param web_property_id: Web property ID to retrieve the Google Ads link for.
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
        "api_version",
        "gcp_conn_id",
        "account_id",
        "web_property_ad_words_link_id",
        "web_property_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        account_id: str,
        web_property_ad_words_link_id: str,
        web_property_id: str,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.account_id = account_id
        self.web_property_ad_words_link_id = web_property_ad_words_link_id
        self.web_property_id = web_property_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        hook = GoogleAnalyticsHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.get_ad_words_link(
            account_id=self.account_id,
            web_property_id=self.web_property_id,
            web_property_ad_words_link_id=self.web_property_ad_words_link_id,
        )
        return result


@deprecated(
    planned_removal_date="November 01, 2024",
    use_instead="GoogleAnalyticsAdminListGoogleAdsLinksOperator",
    reason="The Google Analytics API v3 has sunset and is no longer available as of July 1, 2024.",
    category=AirflowProviderDeprecationWarning,
)
class GoogleAnalyticsRetrieveAdsLinksListOperator(BaseOperator):
    """
    Lists webProperty-Google Ads links for a given web property.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminListGoogleAdsLinksOperator`:

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webPropertyAdWordsLinks/list#http-request

    :param account_id: ID of the account which the given web property belongs to.
    :param web_property_id: Web property UA-string to retrieve the Google Ads links for.
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
        "api_version",
        "gcp_conn_id",
        "account_id",
        "web_property_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        account_id: str,
        web_property_id: str,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.account_id = account_id
        self.web_property_id = web_property_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list[dict[str, Any]]:
        hook = GoogleAnalyticsHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.list_ad_words_links(
            account_id=self.account_id,
            web_property_id=self.web_property_id,
        )
        return result


@deprecated(
    planned_removal_date="November 01, 2024",
    use_instead="GoogleAnalyticsAdminCreateDataStreamOperator",
    reason="The Google Analytics API v3 has sunset and is no longer available as of July 1, 2024.",
    category=AirflowProviderDeprecationWarning,
)
class GoogleAnalyticsDataImportUploadOperator(BaseOperator):
    """
    Take a file from Cloud Storage and uploads it to GA via data import API.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminCreateDataStreamOperator`:

    :param storage_bucket: The Google cloud storage bucket where the file is stored.
    :param storage_name_object: The name of the object in the desired Google cloud
          storage bucket. (templated) If the destination points to an existing
          folder, the file will be taken from the specified folder.
    :param account_id: The GA account Id (long) to which the data upload belongs.
    :param web_property_id: The web property UA-string associated with the upload.
    :param custom_data_source_id: The id to which the data import belongs
    :param resumable_upload: flag to upload the file in a resumable fashion, using a
        series of at least two requests.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param api_version: The version of the api that will be requested for example 'v3'.
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
        "storage_bucket",
        "storage_name_object",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        storage_bucket: str,
        storage_name_object: str,
        account_id: str,
        web_property_id: str,
        custom_data_source_id: str,
        resumable_upload: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        api_version: str = "v3",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.storage_bucket = storage_bucket
        self.storage_name_object = storage_name_object
        self.account_id = account_id
        self.web_property_id = web_property_id
        self.custom_data_source_id = custom_data_source_id
        self.resumable_upload = resumable_upload
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        ga_hook = GoogleAnalyticsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        with NamedTemporaryFile("w+") as tmp_file:
            self.log.info(
                "Downloading file from GCS: %s/%s ",
                self.storage_bucket,
                self.storage_name_object,
            )
            gcs_hook.download(
                bucket_name=self.storage_bucket,
                object_name=self.storage_name_object,
                filename=tmp_file.name,
            )

            ga_hook.upload_data(
                tmp_file.name,
                self.account_id,
                self.web_property_id,
                self.custom_data_source_id,
                self.resumable_upload,
            )


@deprecated(
    planned_removal_date="November 01, 2024",
    use_instead="GoogleAnalyticsAdminDeleteDataStreamOperator",
    reason="The Google Analytics API v3 has sunset and is no longer available as of July 1, 2024.",
    category=AirflowProviderDeprecationWarning,
)
class GoogleAnalyticsDeletePreviousDataUploadsOperator(BaseOperator):
    """
    Deletes previous GA uploads to leave the latest file to control the size of the Data Set Quota.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.marketing_platform.operators.analytics_admin.GoogleAnalyticsAdminDeleteDataStreamOperator`:

    :param account_id: The GA account Id (long) to which the data upload belongs.
    :param web_property_id: The web property UA-string associated with the upload.
    :param custom_data_source_id: The id to which the data import belongs.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("impersonation_chain",)

    def __init__(
        self,
        account_id: str,
        web_property_id: str,
        custom_data_source_id: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        api_version: str = "v3",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.account_id = account_id
        self.web_property_id = web_property_id
        self.custom_data_source_id = custom_data_source_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        ga_hook = GoogleAnalyticsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        uploads = ga_hook.list_uploads(
            account_id=self.account_id,
            web_property_id=self.web_property_id,
            custom_data_source_id=self.custom_data_source_id,
        )

        cids = [upload["id"] for upload in uploads]
        delete_request_body = {"customDataImportUids": cids}

        ga_hook.delete_upload_data(
            self.account_id,
            self.web_property_id,
            self.custom_data_source_id,
            delete_request_body,
        )


class GoogleAnalyticsModifyFileHeadersDataImportOperator(BaseOperator):
    """
    GA has a very particular naming convention for Data Import.

    Ability to prefix "ga:" to all column headers and also a dict to rename columns to
    match the custom dimension ID in GA i.e clientId : dimensionX.

    :param storage_bucket: The Google cloud storage bucket where the file is stored.
    :param storage_name_object: The name of the object in the desired Google cloud
          storage bucket. (templated) If the destination points to an existing
          folder, the file will be taken from the specified folder.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param custom_dimension_header_mapping: Dictionary to handle when uploading
          custom dimensions which have generic IDs ie. 'dimensionX' which are
          set by GA. Dictionary maps the current CSV header to GA ID which will
          be the new header for the CSV to upload to GA eg clientId : dimension1.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        "storage_bucket",
        "storage_name_object",
        "impersonation_chain",
    )

    def __init__(
        self,
        storage_bucket: str,
        storage_name_object: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        custom_dimension_header_mapping: dict[str, str] | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.storage_bucket = storage_bucket
        self.storage_name_object = storage_name_object
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.custom_dimension_header_mapping = custom_dimension_header_mapping or {}
        self.impersonation_chain = impersonation_chain

    def _modify_column_headers(
        self, tmp_file_location: str, custom_dimension_header_mapping: dict[str, str]
    ) -> None:
        # Check headers
        self.log.info("Checking if file contains headers")
        with open(tmp_file_location) as check_header_file:
            has_header = csv.Sniffer().has_header(check_header_file.read(1024))
            if not has_header:
                raise NameError(
                    "CSV does not contain headers, please add them "
                    "to use the modify column headers functionality"
                )

        # Transform
        self.log.info("Modifying column headers to be compatible for data upload")
        with open(tmp_file_location) as read_file:
            reader = csv.reader(read_file)
            headers = next(reader)
            new_headers = []
            for header in headers:
                if header in custom_dimension_header_mapping:
                    header = custom_dimension_header_mapping.get(header)  # type: ignore
                new_header = f"ga:{header}"
                new_headers.append(new_header)
            all_data = read_file.readlines()
            final_headers = ",".join(new_headers) + "\n"
            all_data.insert(0, final_headers)

        # Save result
        self.log.info("Saving transformed file")
        with open(tmp_file_location, "w") as write_file:
            write_file.writelines(all_data)

    def execute(self, context: Context) -> None:
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        with NamedTemporaryFile("w+") as tmp_file:
            # Download file from GCS
            self.log.info(
                "Downloading file from GCS: %s/%s ",
                self.storage_bucket,
                self.storage_name_object,
            )

            gcs_hook.download(
                bucket_name=self.storage_bucket,
                object_name=self.storage_name_object,
                filename=tmp_file.name,
            )

            # Modify file
            self.log.info("Modifying temporary file %s", tmp_file.name)
            self._modify_column_headers(
                tmp_file_location=tmp_file.name,
                custom_dimension_header_mapping=self.custom_dimension_header_mapping,
            )

            # Upload newly formatted file to cloud storage
            self.log.info(
                "Uploading file to GCS: %s/%s ",
                self.storage_bucket,
                self.storage_name_object,
            )
            gcs_hook.upload(
                bucket_name=self.storage_bucket,
                object_name=self.storage_name_object,
                filename=tmp_file.name,
            )
