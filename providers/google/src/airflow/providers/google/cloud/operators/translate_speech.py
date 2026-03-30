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
"""This module contains a Google Cloud Translate Speech operator."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.protobuf.json_format import MessageToDict

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.speech_to_text import CloudSpeechToTextHook
from airflow.providers.google.cloud.hooks.translate import CloudTranslateHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.google.common.links.storage import FileDetailsLink

if TYPE_CHECKING:
    from google.cloud.speech_v1.types import RecognitionAudio, RecognitionConfig

    from airflow.providers.common.compat.sdk import Context


class CloudTranslateSpeechOperator(GoogleCloudBaseOperator):
    """
    Recognizes speech in audio input and translates it.

    Note that it uses the first result from the recognition api response - the one with the highest confidence
    In order to see other possible results please use
    :ref:`howto/operator:CloudSpeechToTextRecognizeSpeechOperator`
    and
    :ref:`howto/operator:CloudTranslateTextOperator`
    separately

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTranslateSpeechOperator`

    See https://cloud.google.com/translate/docs/translating-text

    Execute method returns string object with the translation

    This is a list of dictionaries queried value.
    Dictionary typically contains three keys (though not
    all will be present in all cases).

    * ``detectedSourceLanguage``: The detected language (as an
      ISO 639-1 language code) of the text.
    * ``translatedText``: The translation of the text into the
      target language.
    * ``input``: The corresponding input value.
    * ``model``: The model used to translate the text.

    Dictionary is set as XCom return value.

    :param audio: audio data to be recognized. See more:
        https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionAudio

    :param config: information to the recognizer that specifies how to process the request. See more:
        https://googleapis.github.io/google-cloud-python/latest/speech/gapic/v1/types.html#google.cloud.speech_v1.types.RecognitionConfig

    :param target_language: The language to translate results into. This is required by the API and defaults
        to the target language of the current instance.
        Check the list of available languages here: https://cloud.google.com/translate/docs/languages

    :param format_: (Optional) One of ``text`` or ``html``, to specify
        if the input text is plain text or HTML.

    :param source_language: (Optional) The language of the text to
        be translated.

    :param model: (Optional) The model used to translate the text, such
        as ``'base'`` or ``'nmt'``.

    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.

    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.

    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    # [START translate_speech_template_fields]
    template_fields: Sequence[str] = (
        "target_language",
        "format_",
        "source_language",
        "model",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (FileDetailsLink(),)
    # [END translate_speech_template_fields]

    def __init__(
        self,
        *,
        audio: RecognitionAudio,
        config: RecognitionConfig,
        target_language: str,
        format_: str,
        source_language: str | None,
        model: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.audio = audio
        self.config = config
        self.target_language = target_language
        self.format_ = format_
        self.source_language = source_language
        self.model = model
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        speech_to_text_hook = CloudSpeechToTextHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        translate_hook = CloudTranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        recognize_result = speech_to_text_hook.recognize_speech(config=self.config, audio=self.audio)
        recognize_dict = MessageToDict(recognize_result._pb)

        self.log.info("Recognition operation finished")

        if not recognize_dict["results"]:
            self.log.info("No recognition results")
            return {}
        self.log.debug("Recognition result: %s", recognize_dict)

        try:
            transcript = recognize_dict["results"][0]["alternatives"][0]["transcript"]
        except KeyError as key:
            raise AirflowException(
                f"Wrong response '{recognize_dict}' returned - it should contain {key} field"
            )
        if self.audio.uri:
            FileDetailsLink.persist(
                context=context,
                # Slice from: "gs://{BUCKET_NAME}/{FILE_NAME}" to: "{BUCKET_NAME}/{FILE_NAME}"
                uri=self.audio.uri[5:],
                project_id=self.project_id or translate_hook.project_id,
            )
        try:
            translation = translate_hook.translate(
                values=transcript,
                target_language=self.target_language,
                format_=self.format_,
                source_language=self.source_language,
                model=self.model,
            )
            self.log.info("Translated output: %s", translation)
            return translation
        except ValueError as e:
            self.log.error("An error has been thrown from translate speech method:")
            self.log.error(e)
            raise AirflowException(e)
