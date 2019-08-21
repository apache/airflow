# -*- coding: utf-8 -*-
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

import unittest
from google.cloud.speech_v1.proto.cloud_speech_pb2 import RecognizeResponse, SpeechRecognitionResult, \
    SpeechRecognitionAlternative

from airflow import AirflowException
from airflow.gcp.operators.translate_speech import GcpTranslateSpeechOperator
from tests.compat import mock

GCP_CONN_ID = 'google_cloud_default'


class CloudTranslateSpeechTest(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.translate_speech.GCPSpeechToTextHook')
    @mock.patch('airflow.gcp.operators.translate_speech.CloudTranslateHook')
    def test_minimal_green_path(self, mock_translate_hook, mock_speech_hook):
        mock_speech_hook.return_value.recognize_speech.return_value = RecognizeResponse(
            results=[SpeechRecognitionResult(
                alternatives=[SpeechRecognitionAlternative(
                    transcript='test speech recognition result'
                )]
            )]
        )
        mock_translate_hook.return_value.translate.return_value = [
            {
                'translatedText': 'sprawdzić wynik rozpoznawania mowy',
                'detectedSourceLanguage': 'en',
                'model': 'base',
                'input': 'test speech recognition result',
            }
        ]

        op = GcpTranslateSpeechOperator(
            audio={"uri": "gs://bucket/object"},
            config={"encoding": "LINEAR16"},
            target_language='pl',
            format_='text',
            source_language=None,
            model='base',
            gcp_conn_id=GCP_CONN_ID,
            task_id='id',
        )
        return_value = op.execute(context=None)

        mock_speech_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_translate_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

        mock_speech_hook.return_value.recognize_speech.assert_called_once_with(
            audio={"uri": "gs://bucket/object"},
            config={"encoding": "LINEAR16"},
        )

        mock_translate_hook.return_value.translate.assert_called_once_with(
            values='test speech recognition result',
            target_language='pl',
            format_='text',
            source_language=None,
            model='base',
        )
        self.assertEqual(
            [
                {
                    'translatedText': 'sprawdzić wynik rozpoznawania mowy',
                    'detectedSourceLanguage': 'en',
                    'model': 'base',
                    'input': 'test speech recognition result',
                }
            ],
            return_value,
        )

    @mock.patch('airflow.gcp.operators.translate_speech.GCPSpeechToTextHook')
    @mock.patch('airflow.gcp.operators.translate_speech.CloudTranslateHook')
    def test_bad_recognition_response(self, mock_translate_hook, mock_speech_hook):
        mock_speech_hook.return_value.recognize_speech.return_value = RecognizeResponse(
            results=[SpeechRecognitionResult()]
        )
        op = GcpTranslateSpeechOperator(
            audio={"uri": "gs://bucket/object"},
            config={"encoding": "LINEAR16"},
            target_language='pl',
            format_='text',
            source_language=None,
            model='base',
            gcp_conn_id=GCP_CONN_ID,
            task_id='id',
        )
        with self.assertRaises(AirflowException) as cm:
            op.execute(context=None)
        err = cm.exception
        self.assertIn("it should contain 'alternatives' field", str(err))

        mock_speech_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_translate_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)

        mock_speech_hook.return_value.recognize_speech.assert_called_once_with(
            audio={"uri": "gs://bucket/object"},
            config={"encoding": "LINEAR16"},
        )

        mock_translate_hook.return_value.translate.assert_not_called()
