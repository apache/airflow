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
This module contains a Google Cloud Text to Speech Hook.
"""
from typing import Dict, Optional, Sequence, Union

from google.api_core.retry import Retry
from google.cloud.texttospeech_v1 import TextToSpeechClient
from google.cloud.texttospeech_v1.types import (
    AudioConfig,
    SynthesisInput,
    SynthesizeSpeechResponse,
    VoiceSelectionParams,
)

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CloudTextToSpeechHook(GoogleBaseHook):
    """
    Hook for Google Cloud Text to Speech API.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to, impersonation_chain=impersonation_chain,
        )
        self._client = None  # type: Optional[TextToSpeechClient]

    def get_conn(self) -> TextToSpeechClient:
        """
        Retrieves connection to Cloud Text to Speech.

        :return: Google Cloud Text to Speech client object.
        :rtype: google.cloud.texttospeech_v1.TextToSpeechClient
        """
        if not self._client:
            # pylint: disable=unexpected-keyword-arg
            self._client = TextToSpeechClient(
                credentials=self._get_credentials(), client_info=self.client_info
            )
            # pylint: enable=unexpected-keyword-arg

        return self._client

    @GoogleBaseHook.quota_retry()
    def synthesize_speech(
        self,
        input_data: Union[Dict, SynthesisInput],
        voice: Union[Dict, VoiceSelectionParams],
        audio_config: Union[Dict, AudioConfig],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
    ) -> SynthesizeSpeechResponse:
        """
        Synthesizes text input

        :param input_data: text input to be synthesized. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesisInput
        :type input_data: dict or google.cloud.texttospeech_v1.types.SynthesisInput
        :param voice: configuration of voice to be used in synthesis. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.VoiceSelectionParams
        :type voice: dict or google.cloud.texttospeech_v1.types.VoiceSelectionParams
        :param audio_config: configuration of the synthesized audio. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.AudioConfig
        :type audio_config: dict or google.cloud.texttospeech_v1.types.AudioConfig
        :param retry: (Optional) A retry object used to retry requests. If None is specified,
                requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
            Note that if retry is specified, the timeout applies to each individual attempt.
        :type timeout: float
        :return: SynthesizeSpeechResponse See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesizeSpeechResponse
        :rtype: object
        """
        client = self.get_conn()
        self.log.info("Synthesizing input: %s", input_data)
        # pylint: disable=unexpected-keyword-arg
        return client.synthesize_speech(
            input_=input_data, voice=voice, audio_config=audio_config, retry=retry, timeout=timeout
        )
        # pylint: enable=unexpected-keyword-arg
