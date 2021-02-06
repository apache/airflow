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
"""This module contains a Google Text to Speech operator."""

from tempfile import NamedTemporaryFile
from typing import Dict, Optional, Sequence, Union

from google.api_core.retry import Retry
from google.cloud.texttospeech_v1.types import AudioConfig, SynthesisInput, VoiceSelectionParams

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.text_to_speech import CloudTextToSpeechHook
from airflow.utils.decorators import apply_defaults


class CloudTextToSpeechSynthesizeOperator(BaseOperator):
    """
    Synthesizes text to speech and stores it in Google Cloud Storage

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTextToSpeechSynthesizeOperator`

    :param input_data: text input to be synthesized. See more:
        https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.SynthesisInput
    :type input_data: dict or google.cloud.texttospeech_v1.types.SynthesisInput
    :param voice: configuration of voice to be used in synthesis. See more:
        https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.VoiceSelectionParams
    :type voice: dict or google.cloud.texttospeech_v1.types.VoiceSelectionParams
    :param audio_config: configuration of the synthesized audio. See more:
        https://googleapis.github.io/google-cloud-python/latest/texttospeech/gapic/v1/types.html#google.cloud.texttospeech_v1.types.AudioConfig
    :type audio_config: dict or google.cloud.texttospeech_v1.types.AudioConfig
    :param target_bucket_name: name of the GCS bucket in which output file should be stored
    :type target_bucket_name: str
    :param target_filename: filename of the output file.
    :type target_filename: str
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param retry: (Optional) A retry object used to retry requests. If None is specified,
            requests will not be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
        Note that if retry is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    # [START gcp_text_to_speech_synthesize_template_fields]
    template_fields = (
        "input_data",
        "voice",
        "audio_config",
        "project_id",
        "gcp_conn_id",
        "target_bucket_name",
        "target_filename",
        "impersonation_chain",
    )
    # [END gcp_text_to_speech_synthesize_template_fields]

    @apply_defaults
    def __init__(
        self,
        *,
        input_data: Union[Dict, SynthesisInput],
        voice: Union[Dict, VoiceSelectionParams],
        audio_config: Union[Dict, AudioConfig],
        target_bucket_name: str,
        target_filename: str,
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        self.input_data = input_data
        self.voice = voice
        self.audio_config = audio_config
        self.target_bucket_name = target_bucket_name
        self.target_filename = target_filename
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.retry = retry
        self.timeout = timeout
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        for parameter in [
            "input_data",
            "voice",
            "audio_config",
            "target_bucket_name",
            "target_filename",
        ]:
            if getattr(self, parameter) == "":
                raise AirflowException(f"The required parameter '{parameter}' is empty")

    def execute(self, context) -> None:
        hook = CloudTextToSpeechHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        result = hook.synthesize_speech(
            input_data=self.input_data,
            voice=self.voice,
            audio_config=self.audio_config,
            retry=self.retry,
            timeout=self.timeout,
        )
        with NamedTemporaryFile() as temp_file:
            temp_file.write(result.audio_content)
            cloud_storage_hook = GCSHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
            cloud_storage_hook.upload(
                bucket_name=self.target_bucket_name, object_name=self.target_filename, filename=temp_file.name
            )
