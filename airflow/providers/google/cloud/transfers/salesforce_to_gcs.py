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

import os
import tempfile
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SalesforceToGcsOperator(BaseOperator):
    """
    Submits Salesforce query and uploads results to Google Cloud Storage

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SalesforceToGcsOperator`

    :param query: The query to make to Salesforce.
    :param bucket_name: The bucket to upload to.
    :param object_name: The object name to set when uploading the file.
    :param salesforce_conn_id: the name of the connection that has the parameters
        we need to connect to Salesforce.
    :param include_deleted: True if the query should include deleted records.
    :param query_params: Additional optional arguments
    :param export_format: Desired format of files to be exported.
    :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
        False if you want them to be left in the same format as they were in Salesforce.
        Leaving the value as False will result in datetimes being strings. Default: False
    :param record_time_added: True if you want to add a Unix timestamp field
        to the resulting data that marks when the data was fetched from Salesforce. Default: False
    :param gzip: Option to compress local file or file data for upload
    :param gcp_conn_id: the name of the connection that has the parameters we need to connect to GCS.
    """

    template_fields: Sequence[str] = (
        'query',
        'bucket_name',
        'object_name',
    )
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}

    def __init__(
        self,
        *,
        query: str,
        bucket_name: str,
        object_name: str,
        salesforce_conn_id: str,
        include_deleted: bool = False,
        query_params: dict | None = None,
        export_format: str = "csv",
        coerce_to_timestamp: bool = False,
        record_time_added: bool = False,
        gzip: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.query = query
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.salesforce_conn_id = salesforce_conn_id
        self.export_format = export_format
        self.coerce_to_timestamp = coerce_to_timestamp
        self.record_time_added = record_time_added
        self.gzip = gzip
        self.gcp_conn_id = gcp_conn_id
        self.include_deleted = include_deleted
        self.query_params = query_params

    def execute(self, context: Context):
        salesforce = SalesforceHook(salesforce_conn_id=self.salesforce_conn_id)
        response = salesforce.make_query(
            query=self.query, include_deleted=self.include_deleted, query_params=self.query_params
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "salesforce_temp_file")
            salesforce.write_object_to_file(
                query_results=response["records"],
                filename=path,
                fmt=self.export_format,
                coerce_to_timestamp=self.coerce_to_timestamp,
                record_time_added=self.record_time_added,
            )

            hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            hook.upload(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=path,
                gzip=self.gzip,
            )

            gcs_uri = f"gs://{self.bucket_name}/{self.object_name}"
            self.log.info("%s uploaded to GCS", gcs_uri)
            return gcs_uri
