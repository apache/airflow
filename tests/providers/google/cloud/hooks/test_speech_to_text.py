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

from unittest.mock import patch

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.speech_to_text import CloudSpeechToTextHook
from airflow.providers.google.common.consts import CLIENT_INFO
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

PROJECT_ID = "project-id"
CONFIG = {"encryption": "LINEAR16"}
AUDIO = {"uri": "gs://bucket/object"}


class TestTextToSpeechOperator:
    def setup_method(self):
        with patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gcp_speech_to_text_hook = CloudSpeechToTextHook(gcp_conn_id="test")

    @patch("airflow.providers.google.cloud.hooks.speech_to_text.CloudSpeechToTextHook.get_credentials")
    @patch("airflow.providers.google.cloud.hooks.speech_to_text.SpeechClient")
    def test_speech_client_creation(self, mock_client, mock_get_creds):
        result = self.gcp_speech_to_text_hook.get_conn()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.gcp_speech_to_text_hook._client == result

    @patch("airflow.providers.google.cloud.hooks.speech_to_text.CloudSpeechToTextHook.get_conn")
    def test_synthesize_speech(self, get_conn):
        recognize_method = get_conn.return_value.recognize
        recognize_method.return_value = None
        self.gcp_speech_to_text_hook.recognize_speech(config=CONFIG, audio=AUDIO)
        recognize_method.assert_called_once_with(config=CONFIG, audio=AUDIO, retry=DEFAULT, timeout=None)
