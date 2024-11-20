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
"""This module contains a Google Cloud Translate Hook."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    MutableMapping,
    MutableSequence,
    Sequence,
    cast,
)

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.translate_v2 import Client
from google.cloud.translate_v3 import TranslationServiceClient

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.cloud.translate_v3.types import (
        InputConfig,
        OutputConfig,
        TranslateTextGlossaryConfig,
        TransliterationConfig,
    )


class CloudTranslateHook(GoogleBaseHook):
    """
    Hook for Google Cloud translate APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        self._client: Client | None = None

    def get_conn(self) -> Client:
        """
        Retrieve connection to Cloud Translate.

        :return: Google Cloud Translate client object.
        """
        if not self._client:
            self._client = Client(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    @GoogleBaseHook.quota_retry()
    def translate(
        self,
        values: str | list[str],
        target_language: str,
        format_: str | None = None,
        source_language: str | None = None,
        model: str | list[str] | None = None,
    ) -> dict:
        """
        Translate a string or list of strings.

        See https://cloud.google.com/translate/docs/translating-text

        :param values: String or list of strings to translate.
        :param target_language: The language to translate results into. This
                                is required by the API and defaults to
                                the target language of the current instance.
        :param format_: (Optional) One of ``text`` or ``html``, to specify
                        if the input text is plain text or HTML.
        :param source_language: (Optional) The language of the text to
                                be translated.
        :param model: (Optional) The model used to translate the text, such
                      as ``'base'`` or ``'NMT'``.
        :returns: A list of dictionaries for each queried value. Each
                  dictionary typically contains three keys (though not
                  all will be present in all cases)

                  * ``detectedSourceLanguage``: The detected language (as an
                    ISO 639-1 language code) of the text.

                  * ``translatedText``: The translation of the text into the
                    target language.

                  * ``input``: The corresponding input value.

                  * ``model``: The model used to translate the text.

                  If only a single value is passed, then only a single
                  dictionary will be returned.
        :raises: :class:`~exceptions.ValueError` if the number of
                 values and translations differ.
        """
        client = self.get_conn()
        return client.translate(
            values=values,
            target_language=target_language,
            format_=format_,
            source_language=source_language,
            model=model,
        )


class TranslateHook(GoogleBaseHook):
    """
    Hook for Google Cloud translation (Advanced) using client version V3.

    See related docs https://cloud.google.com/translate/docs/editions#advanced.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._client: TranslationServiceClient | None = None

    def get_client(self) -> TranslationServiceClient:
        """
        Retrieve TranslationService client.

        :return: Google Cloud Translation Service client object.
        """
        if self._client is None:
            self._client = TranslationServiceClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
        return self._client

    @staticmethod
    def wait_for_operation(operation: Operation, timeout: int | None = None):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except GoogleAPICallError:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def translate_text(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        contents: Sequence[str],
        target_language_code: str,
        source_language_code: str | None = None,
        mime_type: str | None = None,
        location: str | None = None,
        model: str | None = None,
        transliteration_config: TransliterationConfig | None = None,
        glossary_config: TranslateTextGlossaryConfig | None = None,
        labels: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        retry: Retry | _MethodDefault | None = DEFAULT,
    ) -> dict:
        """
        Translate text content provided.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param contents: Required. The content of the input in string
            format. Max length 1024 items with 30_000 codepoints recommended.
        :param  mime_type: Optional. The format of the source text, If left
            blank, the MIME type defaults to "text/html".
        :param source_language_code: Optional. The ISO-639 language code of the
            input text if known. If the source language
            isn't specified, the API attempts to identify
            the source language automatically and returns
            the source language within the response.
        :param target_language_code: Required. The ISO-639 language code to use
            for translation of the input text
        :param location: Optional. Project or location to make a call. Must refer to
            a caller's project.
            If not specified, 'global' is used.
            Non-global location is required for requests using AutoML
            models or custom glossaries.

            Models and glossaries must be within the same region (have
            the same location-id).
        :param model: Optional. The ``model`` type requested for this translation.
            If not provided, the default Google model (NMT) will be used.

            The format depends on model type:

            - AutoML Translation models:
              ``projects/{project-number-or-id}/locations/{location-id}/models/{model-id}``
            - General (built-in) models:
              ``projects/{project-number-or-id}/locations/{location-id}/models/general/nmt``
            - Translation LLM models:
              ``projects/{project-number-or-id}/locations/{location-id}/models/general/translation-llm``

            For global (no region) requests, use ``location-id`` ``global``.
            For example, ``projects/{project-number-or-id}/locations/global/models/general/nmt``.
        :param glossary_config: Optional. Glossary to be applied. The glossary must be
            within the same region (have the same location-id) as the
            model.
        :param transliteration_config: Optional. Transliteration to be applied.
        :param labels: Optional. The labels with user-defined
            metadata for the request.
            See https://cloud.google.com/translate/docs/advanced/labels for more information.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.

        :return: Translate text result from the API response.
        """
        client = self.get_client()
        location_id = "global" if not location else location
        parent = f"projects/{project_id or self.project_id}/locations/{location_id}"

        result = client.translate_text(
            request={
                "parent": parent,
                "source_language_code": source_language_code,
                "target_language_code": target_language_code,
                "contents": contents,
                "mime_type": mime_type,
                "glossary_config": glossary_config,
                "transliteration_config": transliteration_config,
                "model": model,
                "labels": labels,
            },
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )
        return cast(dict, type(result).to_dict(result))

    def batch_translate_text(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        source_language_code: str,
        target_language_codes: MutableSequence[str],
        input_configs: MutableSequence[InputConfig | dict],
        output_config: OutputConfig | dict,
        models: str | None = None,
        glossaries: MutableMapping[str, TranslateTextGlossaryConfig] | None = None,
        labels: MutableMapping[str, str] | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        retry: Retry | _MethodDefault | None = DEFAULT,
    ) -> Operation:
        """
        Translate large volumes of text data.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Optional. Project or location to make a call. Must refer to
            a caller's project. Must be non-global.
        :param source_language_code: Required. Source language code.
        :param target_language_codes: Required. Specify up to 10 language codes here.
        :param models: Optional. The models to use for translation. Map's key is
            target language code. Map's value is model name. Value can
            be a built-in general model, or an AutoML Translation model.
            The value format depends on model type:

            - AutoML Translation models:
              ``projects/{project-number-or-id}/locations/{location-id}/models/{model-id}``
            - General (built-in) models:
              ``projects/{project-number-or-id}/locations/{location-id}/models/general/nmt``

            If the map is empty or a specific model is not requested for
            a language pair, then the default Google model (NMT) is used.
        :param input_configs: Required. Input configurations.
            The total number of files matched should be <= 100. The total content size should be <= 100M
            Unicode codepoints. The files must use UTF-8 encoding.
        :param output_config: Required. Output configuration.
        :param glossaries: Optional. Glossaries to be applied for
            translation. It's keyed by target language code.
        :param labels: Optional. The labels with user-defined metadata for the request.
            See https://cloud.google.com/translate/docs/advanced/labels for more information.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.

        :returns: Operation object with the batch text translate results,
         that are returned by batches as they are ready.
        """
        client = self.get_client()
        if location == "global":
            raise AirflowException(
                "Global location is not allowed for the batch text translation, "
                "please provide the correct value!"
            )
        parent = f"projects/{project_id or self.project_id}/locations/{location}"
        result = client.batch_translate_text(
            request={
                "parent": parent,
                "source_language_code": source_language_code,
                "target_language_codes": target_language_codes,
                "input_configs": input_configs,
                "output_config": output_config,
                "glossaries": glossaries,
                "models": models,
                "labels": labels,
            },
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )
        return result
