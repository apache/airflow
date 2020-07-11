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
Example Airflow DAG export Cloud SQL instance to GCS, in Google Cloud Platform.

This DAG relies on the following OS environment variables
https://airflow.apache.org/concepts.html#variables
* GCP_PROJECT_ID - Google Cloud Platform project for the Cloud SQL instance.
* INSTANCE_NAME - Name of the Cloud SQL instance.
* EXPORT_URI - Name of the GCS path.
"""

import os

from airflow import models
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceExportOperator

from six.moves.urllib.parse import urlsplit
from airflow.utils.dates import days_ago

default_args = {"start_date": days_ago(1)}
# [START example_mysql_to_gcs_arguments]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
INSTANCE_NAME = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME', 'test-mysql')
EXPORT_URI = os.environ.get('GCSQL_MYSQL_EXPORT_URI', 'gs://bucketName/fileName')
# [END example_mysql_to_gcs_arguments]

with models.DAG(
    "example_mysql_to_gcs", default_args=default_args, schedule_interval=None, tags=['example']
) as dag:
    # [START example_mysql_to_gcs_body]
    export_body = {
        "exportContext": {
            "fileType": "sql",
            "uri": EXPORT_URI,
            "sqlExportOptions": {
                "schemaOnly": False
            }
        }
    }
    # [END example_mysql_to_gcs_body]

    # ############################################## #
    # ############ EXPORTING SQL  ################## #
    # ############################################## #
    export_url_split = urlsplit(EXPORT_URI)

    # [START example_mysql_to_gcs]
    sql_export_task = CloudSqlInstanceExportOperator(
        project_id=GCP_PROJECT_ID,
        body=export_body,
        instance=INSTANCE_NAME,
        task_id='sql_export_task'
    )
    # [END example_mysql_to_gcs]
