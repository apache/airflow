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
import pytest

from airflow.providers.google.cloud.example_dags.example_bigquery_dts import (
    BUCKET_URI,
    GCP_DTS_BQ_DATASET,
    GCP_DTS_BQ_TABLE,
    GCP_PROJECT_ID,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_BIGQUERY_KEY)
class GcpBigqueryDtsSystemTest(GoogleSystemTest):
    def create_dataset(self, project_id: str, dataset: str, table: str):
        dataset_name = f"{project_id}:{dataset}"
        table_name = f"{dataset_name}.{table}"

        self.execute_with_ctx(
            ["bq", "--location", "us", "mk", "--dataset", dataset_name], key=GCP_BIGQUERY_KEY
        )
        self.execute_with_ctx(["bq", "mk", "--table", table_name, ""], key=GCP_BIGQUERY_KEY)

    def upload_data(self, dataset: str, table: str, gcs_file: str):
        table_name = f"{dataset}.{table}"
        self.execute_with_ctx(
            [
                "bq",
                "--location",
                "us",
                "load",
                "--autodetect",
                "--source_format",
                "CSV",
                table_name,
                gcs_file,
            ],
            key=GCP_BIGQUERY_KEY,
        )

    def delete_dataset(self, project_id: str, dataset: str):
        dataset_name = f"{project_id}:{dataset}"
        self.execute_with_ctx(["bq", "rm", "-r", "-f", "-d", dataset_name], key=GCP_BIGQUERY_KEY)

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def setUp(self):
        super().setUp()
        self.create_dataset(
            project_id=GCP_PROJECT_ID,
            dataset=GCP_DTS_BQ_DATASET,
            table=GCP_DTS_BQ_TABLE,
        )
        self.upload_data(dataset=GCP_DTS_BQ_DATASET, table=GCP_DTS_BQ_TABLE, gcs_file=BUCKET_URI)

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def tearDown(self):
        self.delete_dataset(project_id=GCP_PROJECT_ID, dataset=GCP_DTS_BQ_DATASET)
        super().tearDown()

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def test_run_example_dag_function(self):
        self.run_dag('example_gcp_bigquery_dts', CLOUD_DAG_FOLDER)
