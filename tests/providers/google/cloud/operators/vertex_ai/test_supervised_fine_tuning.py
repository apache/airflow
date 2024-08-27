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

from unittest import mock

import pytest

from airflow.providers.google.cloud.operators.vertex_ai.supervised_fine_tuning import (
    SupervisedFineTuningTrainOperator,
)

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")
vertexai = pytest.importorskip("vertexai.preview.tuning.sft")


VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class TestVertexAISupervisedFineTuningTrainOperator:
    @mock.patch(VERTEX_AI_PATH.format("supervised_fine_tuning.SupervisedFineTuningHook"))
    def test_execute(self, mock_hook):
        source_model = "gemini-1.0-pro-002"
        train_dataset = "gs://cloud-samples-data/ai-platform/generative_ai/sft_train_data.jsonl"

        op = SupervisedFineTuningTrainOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=source_model,
            train_dataset=train_dataset,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.train.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=source_model,
            train_dataset=train_dataset,
            adapter_size=None,
            epochs=None,
            learning_rate_multiplier=None,
            tuned_model_display_name=None,
            validation_dataset=None,
        )
