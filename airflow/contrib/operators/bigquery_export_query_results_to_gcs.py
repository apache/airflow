# -*- coding: utf-8 -*-
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
This module contains BigQuery Export Query Results to GCS operator.
"""
import logging
from uuid import uuid4
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryBaseCursor, BigQueryHook
from airflow.exceptions import AirflowException


class BigQueryExportQueryResultsToGCS(BaseOperator):
    """
    Executes a BigQuery Standard SQL query and exports query results to GCS.
    For large query results, a GCS * pattern must be present
    Grant necessary Service Account permission to your output GCS location table

    :param project_id: The GCP project where the query gets executed, temp table is stored and extracted from
    :type project_id: string
    :param query: The BigQuery SQL to execute.
    :type query: string
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
    :param bigquery_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param location: The location used for the operation.
    :type location: str
    """
    template_fields = ('query',
                       'destination_cloud_storage_uris',
                       'field_delimiter',
                       'project_id',
                       'bigquery_conn_id',
                       'compression',
                       'print_header',
                       'export_format')
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
        self,
        query,
        destination_cloud_storage_uris,
        project_id,
        field_delimiter='\t',
        bigquery_conn_id='bigquery_default',
        compression='NONE',
        print_header=False,
        export_format='CSV',
        dataset_id='BigQueryExportQueryResultsToGCS',
        persist_temp_table=False,
        delegate_to=None,
        labels=None,
        location=None,
        default_table_expiry_in_ms='3600000',
        *args,
        **kwargs):
        super(BigQueryExportQueryResultsToGCS, self).__init__(*args, **kwargs)

        self.query = query
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.field_delimiter = field_delimiter
        self.project_id = project_id
        self.bigquery_conn_id = bigquery_conn_id
        self.compression = compression
        self.print_header = print_header
        self.export_format = export_format
        self.dataset_id = dataset_id + str(uuid4()).replace('-', '')
        self.temp_table_name = self.project_id + '.%s.%s' % (self.dataset_id, str(uuid4()).replace('-', ''))
        self.persist_temp_table = persist_temp_table
        self.delegate_to = delegate_to
        self.labels = labels
        self.location = location
        self.default_table_expiry_in_ms = default_table_expiry_in_ms

    def execute(self, context):
        result_export_success = True
        dataset_creation_success = False
        query_execution_success = False
        err_msg = ""
        try:
            hook = BigQueryHook(use_legacy_sql=False,
                                bigquery_conn_id=self.bigquery_conn_id,
                                delegate_to=self.delegate_to,
                                location=self.location)
            service = hook.get_service()
            cursor = BigQueryBaseCursor(project_id=self.project_id,
                                        service=service)
            cursor.create_empty_dataset(dataset_id=self.dataset_id,
                                        project_id=self.project_id,
                                        dataset_reference=
                                        {'defaultTableExpirationMs': self.default_table_expiry_in_ms})
            dataset_creation_success = True
            cursor.run_query(destination_dataset_table=self.temp_table_name,
                             write_disposition='WRITE_TRUNCATE',
                             allow_large_results=True,
                             sql=self.query,
                             use_legacy_sql=False)
            query_execution_success = True
            cursor.run_extract(source_project_dataset_table=self.temp_table_name,
                               destination_cloud_storage_uris=self.destination_cloud_storage_uris,
                               compression=self.compression,
                               export_format=self.export_format,
                               field_delimiter=self.field_delimiter,
                               print_header=self.print_header)
        except Exception as e:
            err_msg = e
            logging.error(e)
            result_export_success = False
        finally:
            if query_execution_success:
                cursor.run_table_delete(deletion_dataset_table=self.temp_table_name)
            if dataset_creation_success:
                cursor.delete_dataset(dataset_id=self.dataset_id, project_id=self.project_id)
            if result_export_success is False:
                raise AirflowException(
                    "Query export failed. Error: {}".format(err_msg))
