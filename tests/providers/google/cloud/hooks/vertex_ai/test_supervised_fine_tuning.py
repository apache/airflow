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

from unittest import mock

import pytest

from airflow.providers.google.cloud.hooks.vertex_ai.supervised_fine_tuning import (
    SupervisedFineTuningHook,
)
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
)

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")
vertexai = pytest.importorskip("vertexai.preview.tuning.sft")


TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"

SOURCE_MODEL = "gemini-1.0-pro-002"
TRAIN_DATASET = "gs://cloud-samples-data/ai-platform/generative_ai/sft_train_data.jsonl"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
SUPERVISED_FINE_TUNING_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.supervised_fine_tuning.{}"


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class TestSupervisedFineTuningWithDefaultProjectIdHook:
    def dummy_get_credentials(self):
        pass

    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = SupervisedFineTuningHook(gcp_conn_id=TEST_GCP_CONN_ID)
            self.hook.get_credentials = self.dummy_get_credentials

    @mock.patch("vertexai.preview.tuning.sft.train")
    def test_train(self, mock_train) -> None:
        self.hook.train(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=SOURCE_MODEL,
            train_dataset=TRAIN_DATASET,
        )

        # Assertions
        mock_train.assert_called_once_with(
            source_model=SOURCE_MODEL,
            train_dataset=TRAIN_DATASET,
            validation_dataset=None,
            epochs=None,
            adapter_size=None,
            learning_rate_multiplier=None,
            tuned_model_display_name=None,
        )
