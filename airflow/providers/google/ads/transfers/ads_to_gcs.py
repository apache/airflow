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

import csv
from operator import attrgetter
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.ads.hooks.ads import GoogleAdsHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GoogleAdsToGcsOperator(BaseOperator):
    """Fetch daily results from the Google Ads API for 1-n clients.

    Converts and saves the data as a temporary CSV file Uploads the CSV to
    Google Cloud Storage.

    .. seealso::
        For more information on the Google Ads API, take a look at the API docs:
        https://developers.google.com/google-ads/api/docs/start

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAdsToGcsOperator`

    :param client_ids: Google Ads client IDs to query
    :param query: Google Ads Query Language API query
    :param attributes: List of Google Ads Row attributes to extract
    :param bucket: The GCS bucket to upload to
    :param obj: GCS path to save the object. Must be the full file path (ex. `path/to/file.txt`)
    :param gcp_conn_id: Airflow Google Cloud connection ID
    :param google_ads_conn_id: Airflow Google Ads connection ID
    :param page_size: The number of results per API page request. Max 10,000
    :param gzip: Option to compress local file or file data for upload
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param api_version: Optional Google Ads API version to use.
    """

    template_fields: Sequence[str] = (
        "client_ids",
        "query",
        "attributes",
        "bucket",
        "obj",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        client_ids: list[str],
        query: str,
        attributes: list[str],
        bucket: str,
        obj: str,
        gcp_conn_id: str = "google_cloud_default",
        google_ads_conn_id: str = "google_ads_default",
        page_size: int = 10000,
        gzip: bool = False,
        impersonation_chain: str | Sequence[str] | None = None,
        api_version: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.client_ids = client_ids
        self.query = query
        self.attributes = attributes
        self.bucket = bucket
        self.obj = obj
        self.gcp_conn_id = gcp_conn_id
        self.google_ads_conn_id = google_ads_conn_id
        self.page_size = page_size
        self.gzip = gzip
        self.impersonation_chain = impersonation_chain
        self.api_version = api_version

    def execute(self, context: Context) -> None:
        service = GoogleAdsHook(
            gcp_conn_id=self.gcp_conn_id,
            google_ads_conn_id=self.google_ads_conn_id,
            api_version=self.api_version,
        )
        rows = service.search(client_ids=self.client_ids, query=self.query, page_size=self.page_size)

        try:
            getter = attrgetter(*self.attributes)
            converted_rows = [getter(row) for row in rows]
        except Exception as e:
            self.log.error("An error occurred in converting the Google Ad Rows. \n Error %s", e)
            raise

        with NamedTemporaryFile("w", suffix=".csv") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(converted_rows)
            csvfile.flush()

            hook = GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
            hook.upload(
                bucket_name=self.bucket,
                object_name=self.obj,
                filename=csvfile.name,
                gzip=self.gzip,
            )
            self.log.info("%s uploaded to GCS", self.obj)
