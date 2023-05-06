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
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.videointelligence_v1 import enums

from airflow.providers.google.cloud.hooks.video_intelligence import CloudVideoIntelligenceHook
from airflow.providers.google.common.consts import CLIENT_INFO
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

INPUT_URI = "gs://bucket-name/input-file"
OUTPUT_URI = "gs://bucket-name/output-file"

FEATURES = [enums.Feature.LABEL_DETECTION]

ANNOTATE_VIDEO_RESPONSE = {"test": "test"}


class TestCloudVideoIntelligenceHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            CloudVideoIntelligenceHook(gcp_conn_id="GCP_CONN_ID", delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.video_intelligence.CloudVideoIntelligenceHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = CloudVideoIntelligenceHook(gcp_conn_id="test")

    @mock.patch(
        "airflow.providers.google.cloud.hooks.video_intelligence.CloudVideoIntelligenceHook.get_credentials"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.video_intelligence.VideoIntelligenceServiceClient")
    def test_video_intelligence_service_client_creation(self, mock_client, mock_get_creds):
        result = self.hook.get_conn()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.hook._conn == result

    @mock.patch("airflow.providers.google.cloud.hooks.video_intelligence.CloudVideoIntelligenceHook.get_conn")
    def test_annotate_video(self, get_conn):
        # Given
        annotate_video_method = get_conn.return_value.annotate_video
        get_conn.return_value.annotate_video.return_value = ANNOTATE_VIDEO_RESPONSE

        # When
        result = self.hook.annotate_video(input_uri=INPUT_URI, features=FEATURES)

        # Then
        assert result is ANNOTATE_VIDEO_RESPONSE
        annotate_video_method.assert_called_once_with(
            input_uri=INPUT_URI,
            input_content=None,
            features=FEATURES,
            video_context=None,
            output_uri=None,
            location_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.video_intelligence.CloudVideoIntelligenceHook.get_conn")
    def test_annotate_video_with_output_uri(self, get_conn):
        # Given
        annotate_video_method = get_conn.return_value.annotate_video
        get_conn.return_value.annotate_video.return_value = ANNOTATE_VIDEO_RESPONSE

        # When
        result = self.hook.annotate_video(input_uri=INPUT_URI, output_uri=OUTPUT_URI, features=FEATURES)

        # Then
        assert result is ANNOTATE_VIDEO_RESPONSE
        annotate_video_method.assert_called_once_with(
            input_uri=INPUT_URI,
            output_uri=OUTPUT_URI,
            input_content=None,
            features=FEATURES,
            video_context=None,
            location_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
