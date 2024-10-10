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

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.vertex_ai.prediction_service import (
    PredictionServiceHook,
)

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_ENDPOINT_ID: str = "test-endpoint-id"
TEST_OUTPUT_CONFIG: dict = {}

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
PREDICTION_SERVICE_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.prediction_service.{}"


class TestPredictionServiceWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = PredictionServiceHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(PREDICTION_SERVICE_STRING.format("PredictionServiceHook.get_prediction_service_client"))
    def test_predict(self, mock_client):
        self.hook.predict(
            endpoint_id=TEST_ENDPOINT_ID,
            instances=["instance1", "instance2"],
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.predict.assert_called_once_with(
            request=dict(
                endpoint=f"projects/{TEST_PROJECT_ID}/locations/{TEST_REGION}/endpoints/{TEST_ENDPOINT_ID}",
                instances=["instance1", "instance2"],
                parameters=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )


class TestPredictionServiceWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = PredictionServiceHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(PREDICTION_SERVICE_STRING.format("PredictionServiceHook.get_prediction_service_client"))
    def test_predict(self, mock_client):
        self.hook.predict(
            endpoint_id=TEST_ENDPOINT_ID,
            instances=["instance1", "instance2"],
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.predict.assert_called_once_with(
            request=dict(
                endpoint=f"projects/{TEST_PROJECT_ID}/locations/{TEST_REGION}/endpoints/{TEST_ENDPOINT_ID}",
                instances=["instance1", "instance2"],
                parameters=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
