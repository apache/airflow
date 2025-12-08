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

from collections.abc import MutableMapping, MutableSequence, Sequence
from typing import (
    TYPE_CHECKING,
    cast,
)

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.translate_v2 import Client
from google.cloud.translate_v3 import TranslationServiceClient
from google.cloud.translate_v3.types.translation_service import GlossaryInputConfig

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook
from airflow.providers.google.common.hooks.operation_helpers import OperationHelper

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.cloud.translate_v3.services.translation_service import pagers
    from google.cloud.translate_v3.types import (
        BatchDocumentInputConfig,
        BatchDocumentOutputConfig,
        DatasetInputConfig,
        DocumentInputConfig,
        DocumentOutputConfig,
        InputConfig,
        OutputConfig,
        TranslateDocumentResponse,
        TranslateTextGlossaryConfig,
        TransliterationConfig,
        automl_translation,
    )
    from google.cloud.translate_v3.types.translation_service import Glossary


class WaitOperationNotDoneYetError(Exception):
    """Wait operation not done yet error."""

    pass


def _if_exc_is_wait_failed_error(exc: Exception):
    return isinstance(exc, WaitOperationNotDoneYetError)


def _check_if_operation_done(operation: Operation):
    if not operation.done():
        raise WaitOperationNotDoneYetError("Operation is not done yet.")


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


class TranslateHook(GoogleBaseHook, OperationHelper):
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
    def wait_for_operation_done(
        *,
        operation: Operation,
        timeout: float | None = None,
        initial: float = 3,
        multiplier: float = 2,
        maximum: float = 3600,
    ) -> None:
        """
        Wait for long-running operation to be done.

        Calls operation.done() until success or timeout exhaustion, following the back-off retry strategy.
        See `google.api_core.retry.Retry`.
        It's intended use on `Operation` instances that have empty result
        (:class `google.protobuf.empty_pb2.Empty`) by design.
        Thus calling operation.result() for such operation triggers the exception
        ``GoogleAPICallError("Unexpected state: Long-running operation had neither response nor error set.")``
        even though operation itself is totally fine.
        """
        wait_op_for_done = Retry(
            predicate=_if_exc_is_wait_failed_error,
            initial=initial,
            timeout=timeout,
            multiplier=multiplier,
            maximum=maximum,
        )(_check_if_operation_done)
        try:
            wait_op_for_done(operation=operation)
        except GoogleAPICallError:
            if timeout:
                timeout = int(timeout)
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @staticmethod
    def extract_object_id(obj: dict) -> str:
        """Return unique id of the object."""
        return obj["name"].rpartition("/")[-1]

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
        return cast("dict", type(result).to_dict(result))

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

        :return: Operation object with the batch text translate results,
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

    def create_dataset(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        dataset: dict | automl_translation.Dataset,
        timeout: float | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        retry: Retry | _MethodDefault | None = DEFAULT,
    ) -> Operation:
        """
        Create the translation dataset.

        :param dataset: The dataset to create. If a dict is provided, it must correspond to
         the automl_translation.Dataset type.
        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `Operation` object for the dataset to be created.
        """
        client = self.get_client()
        parent = f"projects/{project_id or self.project_id}/locations/{location}"
        return client.create_dataset(
            request={"parent": parent, "dataset": dataset},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def get_dataset(
        self,
        dataset_id: str,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> automl_translation.Dataset:
        """
        Retrieve the dataset for the given dataset_id.

        :param dataset_id: ID of translation dataset to be retrieved.
        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `automl_translation.Dataset` instance.
        """
        client = self.get_client()
        name = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}"
        return client.get_dataset(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def import_dataset_data(
        self,
        dataset_id: str,
        location: str,
        input_config: dict | DatasetInputConfig,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Import data into the translation dataset.

        :param dataset_id: ID of the translation dataset.
        :param input_config: The desired input location and its domain specific semantics, if any.
            If a dict is provided, it must be of the same form as the protobuf message InputConfig.
        :param project_id: ID of the Google Cloud project where dataset is located if None then
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `Operation` object for the import data.
        """
        client = self.get_client()
        name = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}"
        result = client.import_data(
            request={"dataset": name, "input_config": input_config},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def list_datasets(
        self,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> pagers.ListDatasetsPager:
        """
        List translation datasets in a project.

        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: ``pagers.ListDatasetsPager`` instance, iterable object to retrieve the datasets list.
        """
        client = self.get_client()
        parent = f"projects/{project_id}/locations/{location}"
        result = client.list_datasets(
            request={"parent": parent},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def delete_dataset(
        self,
        dataset_id: str,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete the translation dataset and all of its contents.

        :param dataset_id: ID of dataset to be deleted.
        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `Operation` object with dataset deletion results, when finished.
        """
        client = self.get_client()
        name = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}"
        result = client.delete_dataset(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def create_model(
        self,
        dataset_id: str,
        display_name: str,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create the native model by training on translation dataset provided.

        :param dataset_id: ID of dataset to be used for model training.
        :param display_name: Display name of the model trained.
            A-Z and a-z, underscores (_), and ASCII digits 0-9.
        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `Operation` object with the model creation results, when finished.
        """
        client = self.get_client()
        project_id = project_id or self.project_id
        parent = f"projects/{project_id}/locations/{location}"
        dataset = f"projects/{project_id}/locations/{location}/datasets/{dataset_id}"
        result = client.create_model(
            request={
                "parent": parent,
                "model": {
                    "display_name": display_name,
                    "dataset": dataset,
                },
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def get_model(
        self,
        model_id: str,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> automl_translation.Model:
        """
        Retrieve the dataset for the given model_id.

        :param model_id: ID of translation model to be retrieved.
        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `automl_translation.Model` instance.
        """
        client = self.get_client()
        name = f"projects/{project_id}/locations/{location}/models/{model_id}"
        return client.get_model(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def list_models(
        self,
        project_id: str,
        location: str,
        filter_str: str | None = None,
        page_size: int | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> pagers.ListModelsPager:
        """
        List translation models in a project.

        :param project_id: ID of the Google Cloud project where models are located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param filter_str: An optional expression for filtering the models that will
            be returned. Supported filter: ``dataset_id=${dataset_id}``.
        :param page_size: Optional custom page size value. The server can
            return fewer results than requested.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: ``pagers.ListDatasetsPager`` instance, iterable object to retrieve the datasets list.
        """
        client = self.get_client()
        parent = f"projects/{project_id}/locations/{location}"
        result = client.list_models(
            request={
                "parent": parent,
                "filter": filter_str,
                "page_size": page_size,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def delete_model(
        self,
        model_id: str,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete the translation model and all of its contents.

        :param model_id: ID of model to be deleted.
        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `Operation` object with dataset deletion results, when finished.
        """
        client = self.get_client()
        name = f"projects/{project_id}/locations/{location}/models/{model_id}"
        result = client.delete_model(
            request={"name": name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def translate_document(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        source_language_code: str | None = None,
        target_language_code: str,
        location: str | None = None,
        document_input_config: DocumentInputConfig | dict,
        document_output_config: DocumentOutputConfig | dict | None,
        customized_attribution: str | None = None,
        is_translate_native_pdf_only: bool = False,
        enable_shadow_removal_native_pdf: bool = False,
        enable_rotation_correction: bool = False,
        model: str | None = None,
        glossary_config: TranslateTextGlossaryConfig | None = None,
        labels: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        retry: Retry | _MethodDefault | None = DEFAULT,
    ) -> TranslateDocumentResponse:
        """
        Translate the document provided.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param source_language_code: Optional. The ISO-639 language code of the
            input document text if known. If the source language isn't specified,
            the API attempts to identify the source language automatically and returns
            the source language within the response.
        :param target_language_code: Required. The ISO-639 language code to use
            for translation of the input document text.
        :param location: Optional. Project or location to make a call. Must refer to
            a caller's project.
            If not specified, 'global' is used.
            Non-global location is required for requests using AutoML models or custom glossaries.
            Models and glossaries must be within the same region (have the same location-id).
        :param document_input_config: A document translation request input config.
        :param document_output_config: Optional. A document translation request output config.
            If not provided the translated file will only be returned through a byte-stream
            and its output mime type will be the same as the input file's mime type.
        :param customized_attribution: Optional. This flag is to support user customized
            attribution. If not provided, the default is ``Machine Translated by Google``.
            Customized attribution should follow rules in
            https://cloud.google.com/translate/attribution#attribution_and_logos
        :param is_translate_native_pdf_only: Optional. Param for external
            customers. If true, the page limit of online native PDF
            translation is 300 and only native PDF pages will be
            translated.
        :param enable_shadow_removal_native_pdf: Optional. If true, use the text removal server to remove the
            shadow text on background image for native PDF translation.
            Shadow removal feature can only be enabled when both ``is_translate_native_pdf_only``,
            ``pdf_native_only`` are False.
        :param enable_rotation_correction: Optional. If true, enable auto rotation
            correction in DVS.
        :param model: Optional. The ``model`` type requested for this translation.
            If not provided, the default Google model (NMT) will be used.
            The format depends on model type:

            -  AutoML Translation models:
               ``projects/{project-number-or-id}/locations/{location-id}/models/{model-id}``
            -  General (built-in) models:
               ``projects/{project-number-or-id}/locations/{location-id}/models/general/nmt``,

            If not provided, the default Google model (NMT) will be used
            for translation.
        :param glossary_config: Optional. Glossary to be applied. The glossary must be
            within the same region (have the same location-id) as the
            model.
        :param labels: Optional. The labels with user-defined
            metadata for the request.
            See https://cloud.google.com/translate/docs/advanced/labels for more information.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.

        :return: Translate document result from the API response.
        """
        client = self.get_client()
        location_id = "global" if not location else location
        parent = f"projects/{project_id or self.project_id}/locations/{location_id}"
        return client.translate_document(
            request={
                "parent": parent,
                "source_language_code": source_language_code,
                "target_language_code": target_language_code,
                "document_input_config": document_input_config,
                "document_output_config": document_output_config,
                "customized_attribution": customized_attribution,
                "is_translate_native_pdf_only": is_translate_native_pdf_only,
                "enable_shadow_removal_native_pdf": enable_shadow_removal_native_pdf,
                "enable_rotation_correction": enable_rotation_correction,
                "model": model,
                "glossary_config": glossary_config,
                "labels": labels,
            },
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )

    def batch_translate_document(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        source_language_code: str,
        target_language_codes: MutableSequence[str] | None = None,
        location: str | None = None,
        input_configs: MutableSequence[BatchDocumentInputConfig | dict],
        output_config: BatchDocumentOutputConfig | dict,
        customized_attribution: str | None = None,
        format_conversions: MutableMapping[str, str] | None = None,
        enable_shadow_removal_native_pdf: bool = False,
        enable_rotation_correction: bool = False,
        models: MutableMapping[str, str] | None = None,
        glossaries: MutableMapping[str, TranslateTextGlossaryConfig] | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        retry: Retry | _MethodDefault | None = DEFAULT,
    ) -> Operation:
        """
        Translate documents batch by configs provided.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param source_language_code: Optional. The ISO-639 language code of the
            input text if known. If the source language isn't specified, the API attempts to identify
            the source language automatically and returns the source language within the response.
        :param target_language_codes: Required. The ISO-639 language code to use
            for translation of the input document. Specify up to 10 language codes here.
        :param location: Optional. Project or location to make a call. Must refer to
            a caller's project. If not specified, 'global' is used.
            Non-global location is required for requests using AutoML models or custom glossaries.
            Models and glossaries must be within the same region (have the same location-id).
        :param input_configs: Input configurations. The total number of files matched should be <=
            100. The total content size to translate should be <= 100M Unicode codepoints.
            The files must use UTF-8 encoding.
        :param output_config: Output configuration. If 2 input configs match to the same file (that
            is, same input path), no output for duplicate inputs will be generated.
        :param format_conversions: Optional. The file format conversion map that is applied to
            all input files. The map key is the original mime_type.
            The map value is the target mime_type of translated documents.
            Supported file format conversion includes:

            -  ``application/pdf`` to
               ``application/vnd.openxmlformats-officedocument.wordprocessingml.document``

            If nothing specified, output files will be in the same format as the original file.
        :param customized_attribution: Optional. This flag is to support user customized
            attribution. If not provided, the default is ``Machine Translated by Google``.
            Customized attribution should follow rules in
            https://cloud.google.com/translate/attribution#attribution_and_logos
        :param enable_shadow_removal_native_pdf: Optional. If true, use the text removal server to remove the
            shadow text on background image for native PDF translation.
            Shadow removal feature can only be enabled when both ``is_translate_native_pdf_only``,
            ``pdf_native_only`` are False.
        :param enable_rotation_correction: Optional. If true, enable auto rotation
            correction in DVS.
        :param models: Optional. The models to use for translation. Map's key is
            target language code. Map's value is the model name. Value
            can be a built-in general model, or an AutoML Translation model.
            The value format depends on model type:

            -  AutoML Translation models:
               ``projects/{project-number-or-id}/locations/{location-id}/models/{model-id}``
            -  General (built-in) models:
               ``projects/{project-number-or-id}/locations/{location-id}/models/general/nmt``,

            If the map is empty or a specific model is not requested for
            a language pair, then default google model (NMT) is used.
        :param glossaries: Glossaries to be applied. It's keyed by target language code.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.

        :return: Batch translate document result from the API response.
        """
        client = self.get_client()
        location_id = "global" if not location else location
        parent = f"projects/{project_id or self.project_id}/locations/{location_id}"
        return client.batch_translate_document(
            request={
                "parent": parent,
                "source_language_code": source_language_code,
                "target_language_codes": target_language_codes,
                "input_configs": input_configs,
                "output_config": output_config,
                "format_conversions": format_conversions,
                "customized_attribution": customized_attribution,
                "enable_shadow_removal_native_pdf": enable_shadow_removal_native_pdf,
                "enable_rotation_correction": enable_rotation_correction,
                "models": models,
                "glossaries": glossaries,
            },
            timeout=timeout,
            retry=retry,
            metadata=metadata,
        )

    def create_glossary(
        self,
        project_id: str,
        location: str,
        glossary_id: str,
        input_config: GlossaryInputConfig | dict,
        language_pair: Glossary.LanguageCodePair | dict | None = None,
        language_codes_set: Glossary.LanguageCodesSet | MutableSequence[str] | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create the glossary resource from the input source file.

        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param glossary_id: User-specified id to built glossary resource name.
        :param input_config: The input configuration of examples to built glossary from.
            Total glossary must not exceed 10M Unicode codepoints.
            The headers should not be included into the input file table, as languages specified with the
            ``language_pair`` or ``language_codes_set`` params.
        :param language_pair: Pair of language codes to be used for glossary creation.
            Used to built unidirectional glossary. If specified, the ``language_codes_set`` should be empty.
        :param language_codes_set: Set of language codes to create the equivalent term sets glossary.
            Meant multiple languages mapping. If specified, the ``language_pair`` should be empty.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `Operation` object with the glossary creation results.
        """
        client = self.get_client()
        parent = f"projects/{project_id}/locations/{location}"
        name = f"projects/{project_id}/locations/{location}/glossaries/{glossary_id}"

        result = client.create_glossary(
            request={
                "parent": parent,
                "glossary": {
                    "name": name,
                    "input_config": input_config,
                    "language_pair": language_pair,
                    "language_codes_set": language_codes_set,
                },
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def get_glossary(
        self,
        project_id: str,
        location: str,
        glossary_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Glossary:
        """
        Fetch glossary item data by the given id.

        The glossary_id is a substring of glossary name, following the format:
        ``projects/{project-number-or-id}/locations/{location-id}/glossaries/{glossary-id}``

        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param glossary_id: User-specified id to built glossary resource name.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: Fetched glossary item.
        """
        client = self.get_client()
        name = f"projects/{project_id}/locations/{location}/glossaries/{glossary_id}"
        result = client.get_glossary(
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if not result:
            raise AirflowException(f"Fail to get glossary {name}! Please check if it exists.")
        return result

    def update_glossary(
        self,
        glossary: Glossary,
        new_display_name: str | None = None,
        new_input_config: GlossaryInputConfig | dict | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Update glossary item with values provided.

        Only ``display_name`` and ``input_config`` fields are allowed for update.

        :param glossary: Glossary item to update.
        :param new_display_name: New value of the ``display_name`` to be updated.
        :param new_input_config: New value of the ``input_config`` to be updated.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `Operation` with glossary update results.
        """
        client = self.get_client()
        updated_fields = []
        if new_display_name:
            glossary.display_name = new_display_name
            updated_fields.append("display_name")
        if new_input_config is not None:
            if isinstance(new_input_config, dict):
                new_input_config = GlossaryInputConfig(**new_input_config)
            glossary.input_config = new_input_config
            updated_fields.append("input_config")
        result = client.update_glossary(
            request={"glossary": glossary, "update_mask": {"paths": updated_fields}},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def list_glossaries(
        self,
        project_id: str,
        location: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter_str: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> pagers.ListGlossariesPager:
        """
        Get the list of glossaries available.

        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param page_size: Page size requested, if not set server use appropriate default.
        :param page_token: A token identifying a page of results the server should return.
            The first page is returned if ``page_token`` is empty or missing.
        :param filter_str: Filter specifying constraints of a list operation. Specify the constraint by the
            format of "key=value", where key must be ``src`` or ``tgt``, and the value must be a valid
            language code.
            For multiple restrictions, concatenate them by "AND" (uppercase only), such as:
            ``src=en-US AND tgt=zh-CN``. Notice that the exact match is used here, which means using 'en-US'
            and 'en' can lead to different results, which depends on the language code you used when you
            create the glossary.
            For the unidirectional glossaries, the ``src`` and ``tgt`` add restrictions
            on the source and target language code separately.
            For the equivalent term set glossaries, the ``src`` and/or ``tgt`` add restrictions on the term set.
            For example: ``src=en-US AND tgt=zh-CN`` will only pick the unidirectional glossaries which exactly
            match the source language code as ``en-US`` and the target language code ``zh-CN``, but all
            equivalent term set glossaries which contain ``en-US`` and ``zh-CN`` in their language set will
            be picked.
            If missing, no filtering is performed.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: Glossaries list pager object.
        """
        client = self.get_client()
        parent = f"projects/{project_id}/locations/{location}"
        result = client.list_glossaries(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter_str,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def delete_glossary(
        self,
        project_id: str,
        location: str,
        glossary_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete the glossary item by the given id.

        :param project_id: ID of the Google Cloud project where dataset is located. If not provided
            default project_id is used.
        :param location: The location of the project.
        :param glossary_id: Glossary id to be deleted.
        :param retry: A retry object used to retry requests. If `None` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            `retry` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.

        :return: `Operation` with glossary deletion results.
        """
        client = self.get_client()
        name = f"projects/{project_id}/locations/{location}/glossaries/{glossary_id}"
        result = client.delete_glossary(
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
