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
This module contains Google BigQuery to Google Cloud Storage operator.
"""
import warnings
from typing import Dict, List, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.decorators import apply_defaults


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
    :type source_project_dataset_table: str
    :param destination_cloud_storage_uris: The destination Google Cloud
        Storage URI (e.g. gs://some-bucket/some-file.txt). (templated) Follows
        convention defined here:
        https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
    :type destination_cloud_storage_uris: List[str]
    :param compression: Type of compression to use.
    :type compression: str
    :param export_format: File format to export.
    :type export_format: str
    :param field_delimiter: The delimiter to use when extracting to a CSV.
    :type field_delimiter: str
    :param print_header: Whether to print a header for a CSV file extract.
    :type print_header: bool
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param location: The location used for the operation.
    :type location: str
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
        'source_project_dataset_table',
        'destination_cloud_storage_uris',
        'labels',
        'impersonation_chain',
    )
    template_ext = ()
    ui_color = '#e4e6f0'

    @apply_defaults
    def __init__(
        self,
        *,  # pylint: disable=too-many-arguments
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

    def execute(self, context):
        self.log.info(
            'Executing extract of %s into: %s',
            self.source_project_dataset_table,
            self.destination_cloud_storage_uris,
        )
        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_extract(
            source_project_dataset_table=self.source_project_dataset_table,
            destination_cloud_storage_uris=self.destination_cloud_storage_uris,
            compression=self.compression,
            export_format=self.export_format,
            field_delimiter=self.field_delimiter,
            print_header=self.print_header,
            labels=self.labels,
        )
