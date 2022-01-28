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
"""This module contains Google BigQuery to Google Cloud Storage operator."""
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

from google.cloud.bigquery.table import TableReference

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BigQueryToGCSOperator(BaseOperator):
    """
    Transfers a BigQuery table to a Google Cloud Storage bucket.

    .. seealso::
        For more details about these parameters:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    :param source_project_dataset_table: The dotted
        ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to use as the
        source data. If ``<project>`` is not included, project will be the project
        defined in the connection json. (templated)
    :param destination_cloud_storage_uris: The destination Google Cloud
        Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
        convention defined here:
        https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
    :param compression: Type of compression to use.
    :param export_format: File format to export.
    :param field_delimiter: The delimiter to use when extracting to a CSV.
    :param print_header: Whether to print a header for a CSV file extract.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :param location: The location used for the operation.
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
        'source_project_dataset_table',
        'destination_cloud_storage_uris',
        'labels',
        'impersonation_chain',
    )
    template_ext: Sequence[str] = ()
    ui_color = '#e4e6f0'

    def __init__(
        self,
        *,
        source_project_dataset_table: str,
        destination_cloud_storage_uris: List[str],
        compression: str = 'NONE',
        export_format: str = 'CSV',
        field_delimiter: str = ',',
        print_header: bool = True,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        labels: Optional[Dict] = None,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = bigquery_conn_id

        self.source_project_dataset_table = source_project_dataset_table
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.compression = compression
        self.export_format = export_format
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.labels = labels
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context'):
        self.log.info(
            'Executing extract of %s into: %s',
            self.source_project_dataset_table,
            self.destination_cloud_storage_uris,
        )
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        table_ref = TableReference.from_string(self.source_project_dataset_table, hook.project_id)

        configuration: Dict[str, Any] = {
            'extract': {
                'sourceTable': table_ref.to_api_repr(),
                'compression': self.compression,
                'destinationUris': self.destination_cloud_storage_uris,
                'destinationFormat': self.export_format,
            }
        }

        if self.labels:
            configuration['labels'] = self.labels

        if self.export_format == 'CSV':
            # Only set fieldDelimiter and printHeader fields if using CSV.
            # Google does not like it if you set these fields for other export
            # formats.
            configuration['extract']['fieldDelimiter'] = self.field_delimiter
            configuration['extract']['printHeader'] = self.print_header

        hook.insert_job(configuration=configuration)
