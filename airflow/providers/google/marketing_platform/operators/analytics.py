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
This module contains Google Analytics 360 operators.
"""
import csv
from tempfile import NamedTemporaryFile
from typing import Dict, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.marketing_platform.hooks.analytics import GoogleAnalyticsHook
from airflow.utils.decorators import apply_defaults


class GoogleAnalyticsListAccountsOperator(BaseOperator):
    """
    Lists all accounts to which the user has access.

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/accounts/list
        and for python client
        http://googleapis.github.io/google-api-python-client/docs/dyn/analytics_v3.management.accounts.html#list

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsListAccountsOperator`

    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "api_version",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = GoogleAnalyticsHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.list_accounts()
        return result


class GoogleAnalyticsGetAdsLinkOperator(BaseOperator):
    """
    Returns a web property-Google Ads link to which the user has access.

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webPropertyAdWordsLinks/get

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsGetAdsLinkOperator`

    :param account_id: ID of the account which the given web property belongs to.
    :type account_id: str
    :param web_property_ad_words_link_id: Web property-Google Ads link ID.
    :type web_property_ad_words_link_id: str
    :param web_property_id: Web property ID to retrieve the Google Ads link for.
    :type web_property_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "api_version",
        "gcp_conn_id",
        "account_id",
        "web_property_ad_words_link_id",
        "web_property_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        account_id: str,
        web_property_ad_words_link_id: str,
        web_property_id: str,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.account_id = account_id
        self.web_property_ad_words_link_id = web_property_ad_words_link_id
        self.web_property_id = web_property_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
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


class GoogleAnalyticsRetrieveAdsLinksListOperator(BaseOperator):
    """
    Lists webProperty-Google Ads links for a given web property

    .. seealso::
        Check official API docs:
        https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webPropertyAdWordsLinks/list#http-request

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsRetrieveAdsLinksListOperator`

    :param account_id: ID of the account which the given web property belongs to.
    :type account_id: str
    :param web_property_id: Web property UA-string to retrieve the Google Ads links for.
    :type web_property_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "api_version",
        "gcp_conn_id",
        "account_id",
        "web_property_id",
        "impersonation_chain",
    )

    @apply_defaults
    def __init__(
        self, *,
        account_id: str,
        web_property_id: str,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.account_id = account_id
        self.web_property_id = web_property_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        hook = GoogleAnalyticsHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.list_ad_words_links(
            account_id=self.account_id, web_property_id=self.web_property_id,
        )
        return result


class GoogleAnalyticsDataImportUploadOperator(BaseOperator):
    """
    Take a file from Cloud Storage and uploads it to GA via data import API.

    :param storage_bucket: The Google cloud storage bucket where the file is stored.
    :type storage_bucket: str
    :param storage_name_object: The name of the object in the desired Google cloud
          storage bucket. (templated) If the destination points to an existing
          folder, the file will be taken from the specified folder.
    :type storage_name_object: str
    :param account_id: The GA account Id (long) to which the data upload belongs.
    :type account_id: str
    :param web_property_id: The web property UA-string associated with the upload.
    :type web_property_id: str
    :param custom_data_source_id: The id to which the data import belongs
    :type custom_data_source_id: str
    :param resumable_upload: flag to upload the file in a resumable fashion, using a
        series of at least two requests.
    :type resumable_upload: bool
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("storage_bucket", "storage_name_object", "impersonation_chain",)

    @apply_defaults
    def __init__(
        self, *,
        storage_bucket: str,
        storage_name_object: str,
        account_id: str,
        web_property_id: str,
        custom_data_source_id: str,
        resumable_upload: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        api_version: str = "v3",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ):
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

    def execute(self, context):
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


class GoogleAnalyticsDeletePreviousDataUploadsOperator(BaseOperator):
    """
    Deletes previous GA uploads to leave the latest file to control the size of the Data Set Quota.

    :param account_id: The GA account Id (long) to which the data upload belongs.
    :type account_id: str
    :param web_property_id: The web property UA-string associated with the upload.
    :type web_property_id: str
    :param custom_data_source_id: The id to which the data import belongs.
    :type custom_data_source_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("impersonation_chain",)

    def __init__(
        self,
        account_id: str,
        web_property_id: str,
        custom_data_source_id: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        api_version: str = "v3",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.account_id = account_id
        self.web_property_id = web_property_id
        self.custom_data_source_id = custom_data_source_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
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
    GA has a very particular naming convention for Data Import. Ability to
    prefix "ga:" to all column headers and also a dict to rename columns to
    match the custom dimension ID in GA i.e clientId : dimensionX.

    :param storage_bucket: The Google cloud storage bucket where the file is stored.
    :type storage_bucket: str
    :param storage_name_object: The name of the object in the desired Google cloud
          storage bucket. (templated) If the destination points to an existing
          folder, the file will be taken from the specified folder.
    :type storage_name_object: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param custom_dimension_header_mapping: Dictionary to handle when uploading
          custom dimensions which have generic IDs ie. 'dimensionX' which are
          set by GA. Dictionary maps the current CSV header to GA ID which will
          be the new header for the CSV to upload to GA eg clientId : dimension1.
    :type custom_dimension_header_mapping: dict
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("storage_bucket", "storage_name_object", "impersonation_chain",)

    def __init__(
        self,
        storage_bucket: str,
        storage_name_object: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        custom_dimension_header_mapping: Optional[Dict[str, str]] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
    ):
        super(GoogleAnalyticsModifyFileHeadersDataImportOperator, self).__init__(
            **kwargs
        )
        self.storage_bucket = storage_bucket
        self.storage_name_object = storage_name_object
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.custom_dimension_header_mapping = custom_dimension_header_mapping or {}
        self.impersonation_chain = impersonation_chain

    def _modify_column_headers(
        self, tmp_file_location: str, custom_dimension_header_mapping: Dict[str, str]
    ) -> None:
        # Check headers
        self.log.info("Checking if file contains headers")
        with open(tmp_file_location, "r") as check_header_file:
            has_header = csv.Sniffer().has_header(check_header_file.read(1024))
            if not has_header:
                raise NameError(
                    "CSV does not contain headers, please add them "
                    "to use the modify column headers functionality"
                )

        # Transform
        self.log.info("Modifying column headers to be compatible for data upload")
        with open(tmp_file_location, "r") as read_file:
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

    def execute(self, context):
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
