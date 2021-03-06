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
from unittest.mock import MagicMock, Mock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.speech_to_text import CloudSpeechToTextRecognizeSpeechOperator

PROJECT_ID = "project-id"
GCP_CONN_ID = "gcp-conn-id"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
CONFIG = {"encoding": "LINEAR16"}
AUDIO = {"uri": "gs://bucket/object"}


class TestCloudSql(unittest.TestCase):
    @patch("airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextHook")
    def test_recognize_speech_green_path(self, mock_hook):
        mock_hook.return_value.recognize_speech.return_value = MagicMock()

        CloudSpeechToTextRecognizeSpeechOperator(  # pylint: disable=no-value-for-parameter
            project_id=PROJECT_ID,
            gcp_conn_id=GCP_CONN_ID,
            config=CONFIG,
            audio=AUDIO,
            task_id="id",
            impersonation_chain=IMPERSONATION_CHAIN,
        ).execute(context={"task_instance": Mock()})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.recognize_speech.assert_called_once_with(
            config=CONFIG, audio=AUDIO, retry=None, timeout=None
        )

    @patch("airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextHook")
    def test_missing_config(self, mock_hook):
        mock_hook.return_value.recognize_speech.return_value = True

        with pytest.raises(AirflowException) as ctx:
            CloudSpeechToTextRecognizeSpeechOperator(  # pylint: disable=missing-kwoa
                project_id=PROJECT_ID, gcp_conn_id=GCP_CONN_ID, audio=AUDIO, task_id="id"
            ).execute(context={"task_instance": Mock()})

        err = ctx.value
        assert "config" in str(err)
        mock_hook.assert_not_called()

    @patch("airflow.providers.google.cloud.operators.speech_to_text.CloudSpeechToTextHook")
    def test_missing_audio(self, mock_hook):
        mock_hook.return_value.recognize_speech.return_value = True

        with pytest.raises(AirflowException) as ctx:
            CloudSpeechToTextRecognizeSpeechOperator(  # pylint: disable=missing-kwoa
                project_id=PROJECT_ID, gcp_conn_id=GCP_CONN_ID, config=CONFIG, task_id="id"
            ).execute(context={"task_instance": Mock()})

        err = ctx.value
        assert "audio" in str(err)
        mock_hook.assert_not_called()
