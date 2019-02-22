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

from google.cloud.speech_v1 import SpeechClient

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class GCPSpeechToTextHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Speech API.
    """

    _client = None

    def __init__(self, gcp_conn_id="google_cloud_default", delegate_to=None):
        super(GCPSpeechToTextHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Retrieves connection to Cloud Speech.

        :return: Google Cloud Speech client object.
        :rtype: google.cloud.speech_v1.SpeechClient
        """
        if not self._client:
            self._client = SpeechClient(credentials=self._get_credentials())
        return self._client

    def recognize_speech(self, config, audio):
        """
        Recognizes audio input

        :param config: information to the recognizer that specifies how to process the request.
            https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionConfig
        :type config: dict or RecognitionConfig object
        :param audio: audio data to be recognized
            https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionAudio
        :type audio: dict or RecognitionAudio object
        """
        client = self.get_conn()
        response = client.recognize(config=config, audio=audio)
        self.log.info("Recognised speech: %s" % response)
        return response
