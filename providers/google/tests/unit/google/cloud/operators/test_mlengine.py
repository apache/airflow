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

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.operators.mlengine import MLEngineCreateModelOperator

TEST_PROJECT_ID = "test-project-id"
TEST_MODEL_NAME = "test-model-name"
TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TEST_MODEL = {
    "name": TEST_MODEL_NAME,
}
MLENGINE_AI_PATH = "airflow.providers.google.cloud.operators.mlengine.{}"


class TestMLEngineCreateModelOperator:
    @patch(MLENGINE_AI_PATH.format("MLEngineHook"))
    def test_success_create_model(self, mock_hook):
        with pytest.warns(AirflowProviderDeprecationWarning):
            task = MLEngineCreateModelOperator(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                model=TEST_MODEL,
                gcp_conn_id=TEST_GCP_CONN_ID,
                impersonation_chain=TEST_IMPERSONATION_CHAIN,
            )

        task.execute(context=MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_model.assert_called_once_with(
            project_id=TEST_PROJECT_ID, model=TEST_MODEL
        )

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
        with pytest.warns(AirflowProviderDeprecationWarning):
            ti = create_task_instance_of_operator(
                MLEngineCreateModelOperator,
                # Templated fields
                project_id="{{ 'project_id' }}",
                model="{{ 'model' }}",
                impersonation_chain="{{ 'impersonation_chain' }}",
                # Other parameters
                dag_id="test_template_body_templating_dag",
                task_id="test_template_body_templating_task",
            )
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: MLEngineCreateModelOperator = ti.task
        assert task.project_id == "project_id"
        assert task.model == "model"
        assert task.impersonation_chain == "impersonation_chain"
