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

from airflow.providers.google.cloud.example_dags.example_bigquery_dts import (
    BUCKET_URI, GCP_DTS_BQ_DATASET, GCP_DTS_BQ_TABLE, GCP_PROJECT_ID,
)
from tests.providers.google.cloud.operators.test_bigquery_dts_system_helper import GcpBigqueryDtsTestHelper
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, provide_gcp_context, skip_gcp_system
from tests.test_utils.system_tests_class import SystemTest


@skip_gcp_system(GCP_BIGQUERY_KEY, require_local_executor=True)
class GcpBigqueryDtsSystemTest(SystemTest):
    helper = GcpBigqueryDtsTestHelper()

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def setUp(self):
        super().setUp()
        self.helper.create_dataset(
            project_id=GCP_PROJECT_ID,
            dataset=GCP_DTS_BQ_DATASET,
            table=GCP_DTS_BQ_TABLE,
        )
        self.helper.upload_data(dataset=GCP_DTS_BQ_DATASET, table=GCP_DTS_BQ_TABLE, gcs_file=BUCKET_URI)

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def tearDown(self):
        self.helper.delete_dataset(
            project_id=GCP_PROJECT_ID, dataset=GCP_DTS_BQ_DATASET
        )
        super().tearDown()

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def test_run_example_dag_function(self):
        self.run_dag('example_gcp_bigquery_dts', CLOUD_DAG_FOLDER)
