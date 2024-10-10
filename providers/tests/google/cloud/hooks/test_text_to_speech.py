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

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.texttospeech_v1.types import (
    AudioConfig,
    SynthesisInput,
    VoiceSelectionParams,
)

from airflow.providers.google.cloud.hooks.text_to_speech import CloudTextToSpeechHook
from airflow.providers.google.common.consts import CLIENT_INFO

from providers.tests.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

INPUT = {"text": "test text"}
VOICE = {"language_code": "en-US", "ssml_gender": "FEMALE"}
AUDIO_CONFIG = {"audio_encoding": "MP3"}


class TestTextToSpeechHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            CloudTextToSpeechHook(gcp_conn_id="GCP_CONN_ID", delegate_to="delegate_to")

    def setup_method(self):
        with patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gcp_text_to_speech_hook = CloudTextToSpeechHook(gcp_conn_id="test")

    @patch("airflow.providers.google.cloud.hooks.text_to_speech.CloudTextToSpeechHook.get_credentials")
    @patch("airflow.providers.google.cloud.hooks.text_to_speech.TextToSpeechClient")
    def test_text_to_speech_client_creation(self, mock_client, mock_get_creds):
        result = self.gcp_text_to_speech_hook.get_conn()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.gcp_text_to_speech_hook._client == result

    @patch("airflow.providers.google.cloud.hooks.text_to_speech.CloudTextToSpeechHook.get_conn")
    def test_synthesize_speech(self, get_conn):
        synthesize_method = get_conn.return_value.synthesize_speech
        synthesize_method.return_value = None
        self.gcp_text_to_speech_hook.synthesize_speech(
            input_data=INPUT, voice=VOICE, audio_config=AUDIO_CONFIG
        )
        synthesize_method.assert_called_once_with(
            input=SynthesisInput(INPUT),
            voice=VoiceSelectionParams(VOICE),
            audio_config=AudioConfig(AUDIO_CONFIG),
            retry=DEFAULT,
            timeout=None,
        )
