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

PROVIDER_NAME = "google"
PROVIDER_VERSION = "2020.09.10"
PROVIDER_URL = "https://cloud.google.com/"
PROVIDER_DOCS = "https://airflow.readthedocs.io/en/latest/operators-and-hooks-ref.html#google"

CONN_TYPE_TO_HOOK = {
    "gcpcloudsql": (
        "airflow.providers.google.cloud.hooks.cloud_sql.CloudSQLDatabaseHook",
        "gcp_cloudsql_conn_id",
    ),
    "google_cloud_platform": (
        "airflow.providers.google.cloud.hooks.bigquery.BigQueryHook",
        "bigquery_conn_id",
    ),
    "dataprep": ("airflow.providers.google.cloud.hooks.dataprep.GoogleDataprepHook", "dataprep_default"),
}

connection_types = [
    ('google_cloud_platform', 'Google Cloud'),
]
