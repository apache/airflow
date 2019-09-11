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
#

import unittest

from airflow.contrib.hooks.gcp_text_to_speech_hook import GCPTextToSpeechHook
from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

from tests.compat import mock

INPUT = {"text": "test text"}
VOICE = {"language_code": "en-US", "ssml_gender": "FEMALE"}
AUDIO_CONFIG = {"audio_encoding": "MP3"}


class TestTextToSpeechHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.gcp_text_to_speech_hook = GCPTextToSpeechHook(gcp_conn_id="test")

    @mock.patch("airflow.contrib.hooks.gcp_text_to_speech_hook.GCPTextToSpeechHook.get_conn")
    def test_synthesize_speech(self, get_conn):
        synthesize_method = get_conn.return_value.synthesize_speech
        synthesize_method.return_value = None
        self.gcp_text_to_speech_hook.synthesize_speech(
            input_data=INPUT, voice=VOICE, audio_config=AUDIO_CONFIG
        )
        synthesize_method.assert_called_once_with(
            input_=INPUT, voice=VOICE, audio_config=AUDIO_CONFIG, retry=None, timeout=None
        )
