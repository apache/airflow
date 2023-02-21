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
from __future__ import annotations

import pytest

from tests.providers.google.cloud.utils.gcp_authenticator import GCP_VERTEX_AI_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_VERTEX_AI_KEY)
class TestVertexAIExampleDagsSystem(GoogleSystemTest):
    @provide_gcp_context(GCP_VERTEX_AI_KEY)
    def test_run_custom_jobs_example_dag(self):
        self.run_dag(dag_id="example_gcp_vertex_ai_custom_jobs", dag_folder=CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_VERTEX_AI_KEY)
    def test_run_dataset_example_dag(self):
        self.run_dag(dag_id="example_gcp_vertex_ai_dataset", dag_folder=CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_VERTEX_AI_KEY)
    def test_run_auto_ml_example_dag(self):
        self.run_dag(dag_id="example_gcp_vertex_ai_auto_ml", dag_folder=CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_VERTEX_AI_KEY)
    def test_run_batch_prediction_job_example_dag(self):
        self.run_dag(dag_id="example_gcp_vertex_ai_batch_prediction_job", dag_folder=CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_VERTEX_AI_KEY)
    def test_run_endpoint_example_dag(self):
        self.run_dag(dag_id="example_gcp_vertex_ai_endpoint", dag_folder=CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_VERTEX_AI_KEY)
    def test_run_hyperparameter_tuning_job_example_dag(self):
        self.run_dag(dag_id="example_gcp_vertex_ai_hyperparameter_tuning_job", dag_folder=CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_VERTEX_AI_KEY)
    def test_run_model_service_example_dag(self):
        self.run_dag(dag_id="example_gcp_vertex_ai_model_service", dag_folder=CLOUD_DAG_FOLDER)
