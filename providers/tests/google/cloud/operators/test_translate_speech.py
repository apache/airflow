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
from google.cloud.speech_v1 import (
    RecognitionAudio,
    RecognitionConfig,
    RecognizeResponse,
    SpeechRecognitionAlternative,
    SpeechRecognitionResult,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.translate_speech import (
    CloudTranslateSpeechOperator,
)

GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestCloudTranslateSpeech:
    @mock.patch(
        "airflow.providers.google.cloud.operators.translate_speech.CloudSpeechToTextHook"
    )
    @mock.patch(
        "airflow.providers.google.cloud.operators.translate_speech.CloudTranslateHook"
    )
    def test_minimal_green_path(self, mock_translate_hook, mock_speech_hook):
        mock_speech_hook.return_value.recognize_speech.return_value = RecognizeResponse(
            results=[
                SpeechRecognitionResult(
                    alternatives=[
                        SpeechRecognitionAlternative(
                            transcript="test speech recognition result"
                        )
                    ]
                )
            ]
        )
        mock_translate_hook.return_value.translate.return_value = [
            {
                "translatedText": "sprawdzić wynik rozpoznawania mowy",
                "detectedSourceLanguage": "en",
                "model": "base",
                "input": "test speech recognition result",
            }
        ]

        op = CloudTranslateSpeechOperator(
            audio=RecognitionAudio({"uri": "gs://bucket/object"}),
            config=RecognitionConfig({"encoding": "LINEAR16"}),
            target_language="pl",
            format_="text",
            source_language=None,
            model="base",
            gcp_conn_id=GCP_CONN_ID,
            task_id="id",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        context = mock.MagicMock()
        return_value = op.execute(context=context)

        mock_speech_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_translate_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_speech_hook.return_value.recognize_speech.assert_called_once_with(
            audio=RecognitionAudio({"uri": "gs://bucket/object"}),
            config=RecognitionConfig({"encoding": "LINEAR16"}),
        )

        mock_translate_hook.return_value.translate.assert_called_once_with(
            values="test speech recognition result",
            target_language="pl",
            format_="text",
            source_language=None,
            model="base",
        )
        assert [
            {
                "translatedText": "sprawdzić wynik rozpoznawania mowy",
                "detectedSourceLanguage": "en",
                "model": "base",
                "input": "test speech recognition result",
            }
        ] == return_value

    @mock.patch(
        "airflow.providers.google.cloud.operators.translate_speech.CloudSpeechToTextHook"
    )
    @mock.patch(
        "airflow.providers.google.cloud.operators.translate_speech.CloudTranslateHook"
    )
    def test_bad_recognition_response(self, mock_translate_hook, mock_speech_hook):
        mock_speech_hook.return_value.recognize_speech.return_value = RecognizeResponse(
            results=[SpeechRecognitionResult()]
        )
        op = CloudTranslateSpeechOperator(
            audio=RecognitionAudio({"uri": "gs://bucket/object"}),
            config=RecognitionConfig({"encoding": "LINEAR16"}),
            target_language="pl",
            format_="text",
            source_language=None,
            model="base",
            gcp_conn_id=GCP_CONN_ID,
            task_id="id",
        )
        with pytest.raises(AirflowException) as ctx:
            op.execute(context=None)
        err = ctx.value
        assert "it should contain 'alternatives' field" in str(err)

        mock_speech_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_translate_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )

        mock_speech_hook.return_value.recognize_speech.assert_called_once_with(
            audio=RecognitionAudio({"uri": "gs://bucket/object"}),
            config=RecognitionConfig({"encoding": "LINEAR16"}),
        )

        mock_translate_hook.return_value.translate.assert_not_called()

    @mock.patch(
        "airflow.providers.google.cloud.operators.translate_speech.FileDetailsLink.persist"
    )
    @mock.patch(
        "airflow.providers.google.cloud.operators.translate_speech.CloudSpeechToTextHook"
    )
    @mock.patch(
        "airflow.providers.google.cloud.operators.translate_speech.CloudTranslateHook"
    )
    def test_no_audio_uri(self, mock_translate_hook, mock_speech_hook, file_link_mock):
        mock_speech_hook.return_value.recognize_speech.return_value = RecognizeResponse(
            results=[
                SpeechRecognitionResult(
                    alternatives=[
                        SpeechRecognitionAlternative(
                            transcript="test speech recognition result"
                        )
                    ]
                )
            ]
        )
        mock_translate_hook.return_value.translate.return_value = [
            {
                "translatedText": "sprawdzić wynik rozpoznawania mowy",
                "detectedSourceLanguage": "en",
                "model": "base",
                "input": "test speech recognition result",
            }
        ]
        op = CloudTranslateSpeechOperator(
            audio=RecognitionAudio({"content": b"set content data instead of uri"}),
            config=RecognitionConfig({"encoding": "LINEAR16"}),
            target_language="pl",
            format_="text",
            source_language=None,
            model="base",
            gcp_conn_id=GCP_CONN_ID,
            task_id="id",
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context=mock.MagicMock())

        mock_speech_hook.return_value.recognize_speech.assert_called_once_with(
            audio=RecognitionAudio({"content": b"set content data instead of uri"}),
            config=RecognitionConfig({"encoding": "LINEAR16"}),
        )
        assert op.audio.uri == ""
        file_link_mock.assert_not_called()
