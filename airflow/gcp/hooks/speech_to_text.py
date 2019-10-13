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
"""
This module contains a Google Cloud Speech Hook.
"""
from typing import Dict, Optional, Union

from google.api_core.retry import Retry
from google.protobuf.json_format import MessageToDict
from google.cloud.speech_v1 import SpeechClient
from google.cloud.speech_v1.types import RecognitionAudio, RecognitionConfig

from airflow.gcp.hooks.base import GoogleCloudBaseHook


class GCPSpeechToTextHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Speech API.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(self, gcp_conn_id: str = "google_cloud_default", delegate_to: Optional[str] = None) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self._client = None

    def get_conn(self) -> SpeechClient:
        """
        Retrieves connection to Cloud Speech.

        :return: Google Cloud Speech client object.
        :rtype: google.cloud.speech_v1.SpeechClient
        """
        if not self._client:
            self._client = SpeechClient(credentials=self._get_credentials(), client_info=self.client_info)
        return self._client

    def recognize_speech(
        self,
        config: Union[Dict, RecognitionConfig],
        audio: Union[Dict, RecognitionAudio],
        retry: Retry = None,
        timeout: Optional[float] = None
    ):
        """
        Recognizes audio input

        :param config: information to the recognizer that specifies how to process the request.
            https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionConfig
        :type config: dict or google.cloud.speech_v1.types.RecognitionConfig
        :param audio: audio data to be recognized
            https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionAudio
        :type audio: dict or google.cloud.speech_v1.types.RecognitionAudio
        :param retry: (Optional) A retry object used to retry requests. If None is specified,
            requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
            Note that if retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        """
        client = self.get_conn()
        response = client.recognize(config=config, audio=audio, retry=retry, timeout=timeout)
        self.log.info("Recognised speech: %s" % response)
        return response


    @GoogleCloudBaseHook.catch_http_exception
    def long_running_recognize_speech(
        self,
        config: Union[Dict, RecognitionConfig],
        audio: Union[Dict, RecognitionAudio],
        retry: Retry = None,
        timeout: Optional[float] = None,
        wait_until_finished: bool = True
    ):
        """
        Recognizes long audio input

        :param config: information to the recognizer that specifies how to process the request.
            https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionConfig
        :type config: dict or google.cloud.speech_v1.types.RecognitionConfig
        :param audio: audio data uri on GCS to be recognized.
            https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionAudio
        :type audio: dict or google.cloud.speech_v1.types.RecognitionAudio
        :param retry: (Optional) A retry object used to retry requests. If None is specified,
            requests will not be retried.
        :type retry: google.api_core.retry.Retry
            Note that if retry is specified, the timeout applies to each individual attempt.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the operation to complete.
        :type timeout: float
        :param wait_until_finished: (Optional) If true, it will wait until the operation to complete.
        :type wait_until_finished: bool
        """
        client = self.get_conn()
        operation = client.long_running_recognize(config=config, audio=audio, retry=retry)

        if not wait_until_finished:
            return operation

        # Waiting for operation to complete...
        response = operation.result(timeout=timeout)

        return MessageToDict(response)
