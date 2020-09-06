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
import json
import tempfile
from typing import Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook


class SalesforceToGcsOperator(BaseOperator):
    """
    Submits Salesforce query and uploads results to Google Cloud Storage

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SalesforceToGcsOperator`

    :param query: The query to make to Salesforce.
    :type query: str
    :param bucket_name: The bucket to upload to.
    :type bucket_name: str
    :param object_name: The object name to set when uploading the file.
    :type object_name: str
    :param salesforce_conn_id: the name of the connection that has the parameters
        we need to connect to Salesforce.
    :type conn_id: str
    :param include_deleted: True if the query should include deleted records.
    :type include_deleted: bool
    :param query_params: Additional optional arguments
    :type query_params: dict
    :param gzip: Option to compress local file or file data for upload
    :type gzip: bool
    :param gcp_conn_id: the name of the connection that has the parameters we need to connect to GCS.
    :type conn_id: str
    """

    def __init__(
        self,
        *,
        query: str,
        bucket_name: str,
        object_name: str,
        salesforce_conn_id: str,
        include_deleted: bool = False,
        query_params: Optional[dict] = None,
        gzip: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.query = query
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.salesforce_conn_id = salesforce_conn_id
        self.gzip = gzip
        self.gcp_conn_id = gcp_conn_id
        self.include_deleted = include_deleted
        self.query_params = query_params

    def execute(self, context: Dict):
        salesforce = SalesforceHook(conn_id=self.salesforce_conn_id)
        response = salesforce.make_query(
            query=self.query, include_deleted=self.include_deleted, query_params=self.query_params
        )

        def jsonify_attributes(row):
            if "attributes" in row:
                row["attributes"] = json.dumps(row["attributes"])
            return row

        rows = [jsonify_attributes(row) for row in response["records"]]
        self.log.info("Salesforce Returned %s data points", len(rows))

        if rows:
            headers = rows[0].keys()
            with tempfile.NamedTemporaryFile("w", suffix=".csv") as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
                file.flush()
                hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
                hook.upload(
                    bucket_name=self.bucket_name,
                    object_name=self.object_name,
                    filename=file.name,
                    gzip=self.gzip,
                )
                self.log.info("%s uploaded to GCS", file.name)
