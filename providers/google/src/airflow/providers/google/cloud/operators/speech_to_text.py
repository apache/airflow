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
"""This module contains a Google Speech to Text operator."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.protobuf.json_format import MessageToDict

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.speech_to_text import CloudSpeechToTextHook, RecognitionAudio
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.google.common.links.storage import FileDetailsLink

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.speech_v1.types import RecognitionConfig

    from airflow.providers.common.compat.sdk import Context


class CloudSpeechToTextRecognizeSpeechOperator(GoogleCloudBaseOperator):
    """
    Recognizes speech from audio file and returns it as text.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudSpeechToTextRecognizeSpeechOperator`

    :param config: information to the recognizer that specifies how to process the request. See more:
        https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionConfig
    :param audio: audio data to be recognized. See more:
        https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionAudio
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param retry: (Optional) A retry object used to retry requests. If None is specified,
            requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request to complete.
        Note that if retry is specified, the timeout applies to each individual attempt.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_speech_to_text_synthesize_template_fields]
    template_fields: Sequence[str] = (
        "audio",
        "config",
        "project_id",
        "gcp_conn_id",
        "timeout",
        "impersonation_chain",
    )
    # [END gcp_speech_to_text_synthesize_template_fields]
    operator_extra_links = (FileDetailsLink(),)

    def __init__(
        self,
        *,
        audio: RecognitionAudio,
        config: RecognitionConfig,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.audio = audio
        self.config = config
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.retry = retry
        self.timeout = timeout
        self._validate_inputs()
        self.impersonation_chain = impersonation_chain
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.audio == "":
            raise AirflowException("The required parameter 'audio' is empty")
        if self.config == "":
            raise AirflowException("The required parameter 'config' is empty")

    def execute(self, context: Context):
        hook = CloudSpeechToTextHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        if self.audio.uri:
            FileDetailsLink.persist(
                context=context,
                # Slice from: "gs://{BUCKET_NAME}/{FILE_NAME}" to: "{BUCKET_NAME}/{FILE_NAME}"
                uri=self.audio.uri[5:],
                project_id=self.project_id or hook.project_id,
            )
        response = hook.recognize_speech(
            config=self.config, audio=self.audio, retry=self.retry, timeout=self.timeout
        )
        return MessageToDict(response._pb)
