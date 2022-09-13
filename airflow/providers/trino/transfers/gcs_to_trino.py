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
"""This module contains Google Cloud Storage to Trino operator."""
from __future__ import annotations

import csv
import json
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Iterable, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.trino.hooks.trino import TrinoHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GCSToTrinoOperator(BaseOperator):
    """
    Loads a csv file from Google Cloud Storage into a Trino table.
    Assumptions:
    1. CSV file should not have headers
    2. Trino table with requisite columns is already created
    3. Optionally, a separate JSON file with headers can be provided

    :param source_bucket: Source GCS bucket that contains the csv
    :param source_object: csv file including the path
    :param trino_table: trino table to upload the data
    :param trino_conn_id: destination trino connection
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud and
        interact with the Google Cloud Storage service.
    :param schema_fields: The names of the columns to fill in the table. If schema_fields is
        provided, any path provided in the schema object will be
    :param schema_object: JSON file with schema fields
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
        account from the list granting this role to the originating account.
    """

    template_fields: Sequence[str] = (
        'source_bucket',
        'source_object',
        'trino_table',
    )

    def __init__(
        self,
        *,
        source_bucket: str,
        source_object: str,
        trino_table: str,
        trino_conn_id: str = "trino_default",
        gcp_conn_id: str = "google_cloud_default",
        schema_fields: Iterable[str] | None = None,
        schema_object: str | None = None,
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.trino_table = trino_table
        self.trino_conn_id = trino_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.schema_fields = schema_fields
        self.schema_object = schema_object
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        trino_hook = TrinoHook(trino_conn_id=self.trino_conn_id)

        with NamedTemporaryFile("w+") as temp_file:
            self.log.info("Downloading data from %s", self.source_object)
            gcs_hook.download(
                bucket_name=self.source_bucket,
                object_name=self.source_object,
                filename=temp_file.name,
            )

            data = csv.reader(temp_file)
            rows = (tuple(row) for row in data)
            self.log.info("Inserting data into %s", self.trino_table)
            if self.schema_fields:
                trino_hook.insert_rows(table=self.trino_table, rows=rows, target_fields=self.schema_fields)
            elif self.schema_object:
                blob = gcs_hook.download(
                    bucket_name=self.source_bucket,
                    object_name=self.schema_object,
                )
                schema_fields = json.loads(blob.decode("utf-8"))
                trino_hook.insert_rows(table=self.trino_table, rows=rows, target_fields=schema_fields)
            else:
                trino_hook.insert_rows(table=self.trino_table, rows=rows)
