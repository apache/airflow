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

import csv
from tempfile import NamedTemporaryFile
from typing import Any, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.utils.decorators import apply_defaults


class GCSToGoogleSheetsOperator(BaseOperator):
    """
    Uploads .csv file from Google Cloud Storage to provided Google Spreadsheet.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSToGoogleSheets`

    :param spreadsheet_id: The Google Sheet ID to interact with.
    :type spreadsheet_id: str
    :param bucket_name: Name of GCS bucket.:
    :type bucket_name: str
    :param object_name: Path to the .csv file on the GCS bucket.
    :type object_name: str
    :param spreadsheet_range: The A1 notation of the values to retrieve.
    :type spreadsheet_range: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
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

    template_fields = [
        "spreadsheet_id",
        "bucket_name",
        "object_name",
        "spreadsheet_range",
        "impersonation_chain",
    ]

    @apply_defaults
    def __init__(
        self,
        *,
        spreadsheet_id: str,
        bucket_name: str,
        object_name: str,
        spreadsheet_range: str = "Sheet1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Any) -> None:
        sheet_hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        with NamedTemporaryFile("w+") as temp_file:
            # Download data
            gcs_hook.download(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=temp_file.name,
            )

            # Upload data
            values = list(csv.reader(temp_file))
            sheet_hook.update_values(
                spreadsheet_id=self.spreadsheet_id,
                range_=self.spreadsheet_range,
                values=values,
            )
