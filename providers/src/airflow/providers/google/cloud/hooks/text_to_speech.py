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
"""This module contains a Google Cloud Text to Speech Hook."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.texttospeech_v1 import TextToSpeechClient
from google.cloud.texttospeech_v1.types import (
    AudioConfig,
    SynthesisInput,
    SynthesizeSpeechResponse,
    VoiceSelectionParams,
)

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.retry import Retry


class CloudTextToSpeechHook(GoogleBaseHook):
    """
    Hook for Google Cloud Text to Speech API.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._client: TextToSpeechClient | None = None

    def get_conn(self) -> TextToSpeechClient:
        """
        Retrieve connection to Cloud Text to Speech.

        :return: Google Cloud Text to Speech client object.
        """
        if not self._client:
            self._client = TextToSpeechClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )

        return self._client

    @GoogleBaseHook.quota_retry()
    def synthesize_speech(
        self,
        input_data: dict | SynthesisInput,
        voice: dict | VoiceSelectionParams,
        audio_config: dict | AudioConfig,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
    ) -> SynthesizeSpeechResponse:
        """
        Synthesizes text input.

        :param input_data: text input to be synthesized. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesisInput
        :param voice: configuration of voice to be used in synthesis. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.VoiceSelectionParams
        :param audio_config: configuration of the synthesized audio. See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.AudioConfig
        :param retry: (Optional) A retry object used to retry requests. If None is specified,
                requests will not be retried.
        :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
            Note that if retry is specified, the timeout applies to each individual attempt.
        :return: SynthesizeSpeechResponse See more:
            https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesizeSpeechResponse
        """
        client = self.get_conn()

        if isinstance(input_data, dict):
            input_data = SynthesisInput(input_data)
        if isinstance(voice, dict):
            voice = VoiceSelectionParams(voice)
        if isinstance(audio_config, dict):
            audio_config = AudioConfig(audio_config)
        self.log.info("Synthesizing input: %s", input_data)

        return client.synthesize_speech(
            input=input_data,
            voice=voice,
            audio_config=audio_config,
            retry=retry,
            timeout=timeout,
        )
