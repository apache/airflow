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

from unittest.mock import ANY, MagicMock, Mock, PropertyMock, patch

import pytest
from google.api_core.gapic_v1.method import DEFAULT

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.text_to_speech import CloudTextToSpeechSynthesizeOperator

PROJECT_ID = "project-id"
GCP_CONN_ID = "gcp-conn-id"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
INPUT = {"text": "text"}
VOICE = {"language_code": "en-US"}
AUDIO_CONFIG = {"audio_encoding": "MP3"}
TARGET_BUCKET_NAME = "target_bucket_name"
TARGET_FILENAME = "target_filename"


class TestGcpTextToSpeech:
    @patch("airflow.providers.google.cloud.operators.text_to_speech.GCSHook")
    @patch("airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechHook")
    def test_synthesize_text_green_path(self, mock_text_to_speech_hook, mock_gcp_hook):
        mocked_response = Mock()
        type(mocked_response).audio_content = PropertyMock(return_value=b"audio")
        mocked_context = MagicMock()

        mock_text_to_speech_hook.return_value.synthesize_speech.return_value = mocked_response
        mock_gcp_hook.return_value.upload.return_value = True

        CloudTextToSpeechSynthesizeOperator(
            project_id=PROJECT_ID,
            gcp_conn_id=GCP_CONN_ID,
            input_data=INPUT,
            voice=VOICE,
            audio_config=AUDIO_CONFIG,
            target_bucket_name=TARGET_BUCKET_NAME,
            target_filename=TARGET_FILENAME,
            task_id="id",
            impersonation_chain=IMPERSONATION_CHAIN,
        ).execute(context=mocked_context)

        mock_text_to_speech_hook.assert_called_once_with(
            gcp_conn_id="gcp-conn-id",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_gcp_hook.assert_called_once_with(
            gcp_conn_id="gcp-conn-id",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_text_to_speech_hook.return_value.synthesize_speech.assert_called_once_with(
            input_data=INPUT, voice=VOICE, audio_config=AUDIO_CONFIG, retry=DEFAULT, timeout=None
        )
        mock_gcp_hook.return_value.upload.assert_called_once_with(
            bucket_name=TARGET_BUCKET_NAME, object_name=TARGET_FILENAME, filename=ANY
        )

    @pytest.mark.parametrize(
        "missing_arg, input_data, voice, audio_config, target_bucket_name, target_filename",
        [
            ("input_data", "", VOICE, AUDIO_CONFIG, TARGET_BUCKET_NAME, TARGET_FILENAME),
            ("voice", INPUT, "", AUDIO_CONFIG, TARGET_BUCKET_NAME, TARGET_FILENAME),
            ("audio_config", INPUT, VOICE, "", TARGET_BUCKET_NAME, TARGET_FILENAME),
            ("target_bucket_name", INPUT, VOICE, AUDIO_CONFIG, "", TARGET_FILENAME),
            ("target_filename", INPUT, VOICE, AUDIO_CONFIG, TARGET_BUCKET_NAME, ""),
        ],
    )
    @patch("airflow.providers.google.cloud.operators.text_to_speech.GCSHook")
    @patch("airflow.providers.google.cloud.operators.text_to_speech.CloudTextToSpeechHook")
    def test_missing_arguments(
        self,
        mock_text_to_speech_hook,
        mock_gcp_hook,
        missing_arg,
        input_data,
        voice,
        audio_config,
        target_bucket_name,
        target_filename,
    ):
        mocked_context = Mock()

        with pytest.raises(AirflowException) as ctx:
            CloudTextToSpeechSynthesizeOperator(
                project_id="project-id",
                input_data=input_data,
                voice=voice,
                audio_config=audio_config,
                target_bucket_name=target_bucket_name,
                target_filename=target_filename,
                task_id="id",
            ).execute(context=mocked_context)

        err = ctx.value
        assert missing_arg in str(err)
        mock_text_to_speech_hook.assert_not_called()
        mock_gcp_hook.assert_not_called()
