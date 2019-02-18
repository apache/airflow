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

from google.cloud.texttospeech_v1 import TextToSpeechClient

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class GCPTextToSpeechHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Text to Speech APIs.
    """

    _client = None

    def __init__(self, gcp_conn_id="google_cloud_default", delegate_to=None):
        super(GCPTextToSpeechHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Retrieves connection to Cloud Text to Speech.

        :return: Google Cloud Text to Speech client object.
        :rtype: google.cloud.texttospeech_v1.TextToSpeechClient
        """
        if not self._client:
            self._client = TextToSpeechClient(credentials=self._get_credentials())
        return self._client

    def synthesize_speech(self, input_, voice, audio_config):
        """
        Synthesizes text input

        :param input_: text input to be synthesized. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesisInput
        :type input_: dict or google.cloud.texttospeech_v1.types.SynthesisInput
        :param voice: configuration of voice to be used in synthesis. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.VoiceSelectionParams
        :type voice: dict or google.cloud.texttospeech_v1.types.VoiceSelectionParams
        :param audio_config: configuration of the synthesized audio. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.AudioConfig
        :type audio_config: dict or google.cloud.texttospeech_v1.types.AudioConfig
        :return: SynthesizeSpeechResponse See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesizeSpeechResponse
        :rtype: object
        """
        client = self.get_conn()
        self.log.info("Synthesizing input: %s" % input_)
        return client.synthesize_speech(input_=input_, voice=voice, audio_config=audio_config)
