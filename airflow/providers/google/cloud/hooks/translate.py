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

from google.api_core.operation import Operation

from google.api_core.exceptions import GoogleAPICallError

from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)

from google.cloud.translate_v2 import Client

from google.cloud.translate_v3 import TranslationServiceClient

from google.cloud.translate_v3.types import (
    TranslateTextRequest,
    TransliterationConfig,
    TranslateTextGlossaryConfig,
    TranslateTextGlossaryConfig,
    InputConfig,
    OutputConfig
)

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook



from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.retry import Retry


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
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
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
                      as ``'base'`` or ``'nmt'``.
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
    Translate V3 hook
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
        self._client: TranslationServiceClient | None = None

    def get_client(self) -> TranslationServiceClient:
        """
        Retrieve connection to Translation service.

        :return: Google Cloud Translation Service client object.
        """
        if self._client is None:
            self._client = TranslationServiceClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
        return self._client

    def wait_for_operation(self, operation: Operation, timeout: int | None = None):
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
        client = self.get_client()
        location_id = 'global' if not location else location
        parent = f"projects/{project_id or self.project_id}/locations/{location_id}"

        result = client.translate_text(
            request={
                'parent': parent,
                'source_language_code': source_language_code,
                'target_language_code': target_language_code,
                'contents': contents,
                'mime_type': mime_type,
                'glossary_config': glossary_config,
                'transliteration_config': transliteration_config,
                'model': model,
                'labels': labels,
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
            target_language_codes: MutableSequence[str],
            source_language_code: str,
            input_configs: MutableSequence[InputConfig | dict],
            output_config: OutputConfig | dict,
            models: str | None = None,
            glossaries: MutableMapping[str, TranslateTextGlossaryConfig] | None = None,
            labels: MutableMapping[str, str] | None = None,
            timeout: float | _MethodDefault = DEFAULT,
            metadata: Sequence[tuple[str, str]] = (),
            retry: Retry | _MethodDefault | None = DEFAULT,
    ) -> Operation:
        client = self.get_client()
        if location == 'global':
            raise AirflowException(
                'Global location is not allowed for the batch text translation, '
                'please provide the correct value!'
            )
        parent = f"projects/{project_id or self.project_id}/locations/{location}"
        result = client.batch_translate_text(
            request={
                'parent': parent,
                'source_language_code': source_language_code,
                'target_language_codes': target_language_codes,
                'input_configs': input_configs,
                'output_config': output_config,
                'glossaries': glossaries,
                'models': models,
                'labels': labels,
            },
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )
        return result
