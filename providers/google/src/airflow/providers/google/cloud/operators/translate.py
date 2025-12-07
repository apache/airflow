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
"""This module contains Google Translate operators."""

from __future__ import annotations

from collections.abc import MutableMapping, MutableSequence, Sequence
from typing import TYPE_CHECKING, cast

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.translate import CloudTranslateHook, TranslateHook
from airflow.providers.google.cloud.links.translate import (
    TranslateResultByOutputConfigLink,
    TranslateTextBatchLink,
    TranslationDatasetsListLink,
    TranslationGlossariesListLink,
    TranslationModelLink,
    TranslationModelsListLink,
    TranslationNativeDatasetLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.operators.vertex_ai.dataset import DatasetImportDataResultsCheckHelper
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.translate_v3.types import (
        BatchDocumentInputConfig,
        BatchDocumentOutputConfig,
        DatasetInputConfig,
        DocumentInputConfig,
        DocumentOutputConfig,
        InputConfig,
        OutputConfig,
        TranslateTextGlossaryConfig,
        TransliterationConfig,
        automl_translation,
    )
    from google.cloud.translate_v3.types.translation_service import Glossary, GlossaryInputConfig

    from airflow.providers.common.compat.sdk import Context


class CloudTranslateTextOperator(GoogleCloudBaseOperator):
    """
    Translate a string or list of strings.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudTranslateTextOperator`

    See https://cloud.google.com/translate/docs/translating-text

    Execute method returns str or list.

    This is a list of dictionaries for each queried value. Each
    dictionary typically contains three keys (though not all will be present in all cases):

    * ``detectedSourceLanguage``: The detected language (as an ISO 639-1 language code) of the text.
    * ``translatedText``: The translation of the text into the target language.
    * ``input``: The corresponding input value.
    * ``model``: The model used to translate the text.

    If only a single value is passed, then only a single
    dictionary is set as the XCom return value.

    :param values: String or list of strings to translate.
    :param target_language: The language to translate results into. This is required by the API.
    :param format_: (Optional) One of ``text`` or ``html``, to specify if the input text is plain text or HTML.
    :param source_language: (Optional) The language of the text to be translated.
    :param model: (Optional) The model used to translate the text, such as ``'base'`` or ``'nmt'``.
    :param impersonation_chain: Optional service account to impersonate using short-term credentials, or
        chained list of accounts required to get the access_token of the last account in the list, which
        will be impersonated in the request. If set as a string, the account must grant the originating
        account the Service Account Token Creator IAM role. If set as a sequence, the identities from
        the list must grant Service Account Token Creator IAM role to the directly preceding identity,
        with the first account from the list granting this role to the originating account (templated).
    """

    # [START translate_template_fields]
    template_fields: Sequence[str] = (
        "values",
        "target_language",
        "format_",
        "source_language",
        "model",
        "gcp_conn_id",
        "impersonation_chain",
    )
    # [END translate_template_fields]

    def __init__(
        self,
        *,
        values: list[str] | str,
        target_language: str,
        format_: str,
        source_language: str | None,
        model: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.values = values
        self.target_language = target_language
        self.format_ = format_
        self.source_language = source_language
        self.model = model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = CloudTranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            translation = hook.translate(
                values=self.values,
                target_language=self.target_language,
                format_=self.format_,
                source_language=self.source_language,
                model=self.model,
            )
            self.log.debug("Translation %s", translation)
            return translation
        except ValueError as e:
            self.log.error("An error has been thrown from translate method:")
            self.log.error(e)
            raise AirflowException(e)


class TranslateTextOperator(GoogleCloudBaseOperator):
    """
    Translate text content of moderate amount, for larger volumes of text please use the TranslateTextBatchOperator.

    Wraps the Google cloud Translate Text (Advanced) functionality.
    See https://cloud.google.com/translate/docs/advanced/translating-text-v3

    For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TranslateTextOperator`.

    :param project_id: Optional. The ID of the Google Cloud project that the
        service belongs to.
        If not provided default project_id is used.
    :param location: optional. The ID of the Google Cloud location that the
        service belongs to. if not specified, 'global' is used.
        Non-global location is required for requests using AutoML models or custom glossaries.
    :param contents: Required. The sequence of content strings to be translated.
        Limited to 1024 items  with 30_000 codepoints total recommended.
    :param  mime_type: Optional. The format of the source text, If left blank,
        the MIME type defaults to "text/html".
    :param source_language_code: Optional. The ISO-639 language code of the
        input text if known. If not specified, attempted to recognize automatically.
    :param target_language_code: Required. The ISO-639 language code to use
        for translation of the input text.
    :param model: Optional. The ``model`` type requested for this translation.
        If not provided, the default Google model (NMT) will be used.
        The format depends on model type:

        - AutoML Translation models:
          ``projects/{project-number-or-id}/locations/{location-id}/models/{model-id}``
        - General (built-in) models:
          ``projects/{project-number-or-id}/locations/{location-id}/models/general/nmt``
        - Translation LLM models:
          ``projects/{project-number-or-id}/locations/{location-id}/models/general/translation-llm``

        For global (non-region) requests, use 'global' ``location-id``.
    :param glossary_config: Optional. Glossary to be applied.
    :param transliteration_config: Optional. Transliteration to be applied.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "contents",
        "target_language_code",
        "mime_type",
        "source_language_code",
        "model",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        contents: Sequence[str],
        source_language_code: str | None = None,
        target_language_code: str,
        mime_type: str | None = None,
        location: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        model: str | None = None,
        transliteration_config: TransliterationConfig | None = None,
        glossary_config: TranslateTextGlossaryConfig | None = None,
        labels: str | None = None,
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.contents = contents
        self.source_language_code = source_language_code
        self.target_language_code = target_language_code
        self.mime_type = mime_type
        self.location = location
        self.labels = labels
        self.model = model
        self.transliteration_config = transliteration_config
        self.glossary_config = glossary_config
        self.metadate = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            self.log.info("Starting the text translation run")
            translation_result = hook.translate_text(
                contents=self.contents,
                source_language_code=self.source_language_code,
                target_language_code=self.target_language_code,
                mime_type=self.mime_type,
                location=self.location,
                labels=self.labels,
                model=self.model,
                transliteration_config=self.transliteration_config,
                glossary_config=self.glossary_config,
                timeout=self.timeout,
                retry=self.retry,
                metadata=self.metadate,
            )
            self.log.info("Text translation run complete")
            return translation_result
        except GoogleAPICallError as e:
            self.log.error("An error occurred executing translate_text method: \n%s", e)
            raise AirflowException(e)


class TranslateTextBatchOperator(GoogleCloudBaseOperator):
    """
    Translate large volumes of text content, by the inputs provided.

    Wraps the Google cloud Translate Text (Advanced) functionality.
    See https://cloud.google.com/translate/docs/advanced/batch-translation

    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateTextBatchOperator`.

    :param project_id: Optional. The ID of the Google Cloud project that the
        service belongs to. If not specified the hook project_id will be used.
    :param location: required. The ID of the Google Cloud location, (non-global) that the
        service belongs to.
    :param source_language_code: Required. Source language code.
    :param target_language_codes: Required. Up to 10 language codes allowed here.
    :param input_configs: Required. Input configurations.
        The total number of files matched should be <=100. The total content size should be <= 100M Unicode codepoints.
        The files must use UTF-8 encoding.
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
    :param output_config: Required. Output configuration.
    :param glossaries: Optional. Glossaries to be applied for translation. It's keyed by target language code.
    :param labels: Optional. The labels with user-defined metadata.
        See https://cloud.google.com/translate/docs/advanced/labels for more information.

    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (TranslateTextBatchLink(),)

    template_fields: Sequence[str] = (
        "input_configs",
        "target_language_codes",
        "source_language_code",
        "models",
        "glossaries",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
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
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.target_language_codes = target_language_codes
        self.source_language_code = source_language_code
        self.input_configs = input_configs
        self.output_config = output_config
        self.models = models
        self.glossaries = glossaries
        self.labels = labels
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        translate_operation = hook.batch_translate_text(
            project_id=self.project_id,
            location=self.location,
            target_language_codes=self.target_language_codes,
            source_language_code=self.source_language_code,
            input_configs=self.input_configs,
            output_config=self.output_config,
            models=self.models,
            glossaries=self.glossaries,
            labels=self.labels,
            metadata=self.metadata,
            timeout=self.timeout,
            retry=self.retry,
        )
        self.log.info("Translate text batch job started.")
        TranslateTextBatchLink.persist(
            context=context,
            project_id=self.project_id or hook.project_id,
            output_config=self.output_config,
        )
        hook.wait_for_operation_result(translate_operation)
        self.log.info("Translate text batch job finished")
        return {"batch_text_translate_results": self.output_config["gcs_destination"]}


class TranslateCreateDatasetOperator(GoogleCloudBaseOperator):
    """
    Create a Google Cloud Translate dataset.

    Creates a `native` translation dataset, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateCreateDatasetOperator`.

    :param dataset: The dataset to create. If a dict is provided, it must correspond to
        the automl_translation.Dataset type.
    :param project_id: ID of the Google Cloud project where dataset is located.
        If not provided default project_id is used.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "dataset",
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    operator_extra_links = (TranslationNativeDatasetLink(),)

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        dataset: dict | automl_translation.Dataset,
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.dataset = dataset
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Dataset creation started %s...", self.dataset)
        result_operation = hook.create_dataset(
            dataset=self.dataset,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = hook.wait_for_operation_result(result_operation)
        result = type(result).to_dict(result)
        dataset_id = hook.extract_object_id(result)
        context["ti"].xcom_push(key="dataset_id", value=dataset_id)
        self.log.info("Dataset creation complete. The dataset_id: %s.", dataset_id)

        project_id = self.project_id or hook.project_id
        TranslationNativeDatasetLink.persist(
            context=context,
            dataset_id=dataset_id,
            project_id=project_id,
            location=self.location,
        )
        return result


class TranslateDatasetsListOperator(GoogleCloudBaseOperator):
    """
    Get a list of native Google Cloud Translation datasets in a project.

    Get project's list of `native` translation datasets, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateDatasetsListOperator`.

    :param project_id: ID of the Google Cloud project where dataset is located.
        If not provided default project_id is used.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    operator_extra_links = (TranslationDatasetsListLink(),)

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        project_id = self.project_id or hook.project_id
        TranslationDatasetsListLink.persist(
            context=context,
            project_id=project_id,
        )
        self.log.info("Requesting datasets list")
        results_pager = hook.list_datasets(
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result_ids = []
        for ds_item in results_pager:
            ds_data = type(ds_item).to_dict(ds_item)
            ds_id = hook.extract_object_id(ds_data)
            result_ids.append(ds_id)

        self.log.info("Fetching the datasets list complete.")
        return result_ids


class TranslateImportDataOperator(GoogleCloudBaseOperator, DatasetImportDataResultsCheckHelper):
    """
    Import data to the translation dataset.

    Loads data to the translation dataset, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateImportDataOperator`.

    :param dataset_id: The dataset_id of target native dataset to import data to.
    :param input_config: The desired input location of translations language pairs file. If a dict provided,
        must follow the structure of DatasetInputConfig.
        If a dict is provided, it must be of the same form as the protobuf message InputConfig.
    :param project_id: ID of the Google Cloud project where dataset is located. If not provided
        default project_id is used.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param raise_for_empty_result: Raise an error if no additional data has been populated after the import.
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "input_config",
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    operator_extra_links = (TranslationNativeDatasetLink(),)

    def __init__(
        self,
        *,
        dataset_id: str,
        location: str,
        input_config: dict | DatasetInputConfig,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        raise_for_empty_result: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.input_config = input_config
        self.project_id = project_id
        self.location = location
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.raise_for_empty_result = raise_for_empty_result

    def execute(self, context: Context):
        hook = TranslateHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        initial_dataset_size = self._get_number_of_ds_items(
            dataset=hook.get_dataset(
                dataset_id=self.dataset_id,
                project_id=self.project_id,
                location=self.location,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            ),
            total_key_name="example_count",
        )
        self.log.info("Importing data to dataset...")
        operation = hook.import_dataset_data(
            dataset_id=self.dataset_id,
            input_config=self.input_config,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        project_id = self.project_id or hook.project_id
        TranslationNativeDatasetLink.persist(
            context=context,
            dataset_id=self.dataset_id,
            project_id=project_id,
            location=self.location,
        )
        hook.wait_for_operation_done(operation=operation, timeout=self.timeout)

        result_dataset_size = self._get_number_of_ds_items(
            dataset=hook.get_dataset(
                dataset_id=self.dataset_id,
                project_id=self.project_id,
                location=self.location,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            ),
            total_key_name="example_count",
        )
        if self.raise_for_empty_result:
            self._raise_for_empty_import_result(self.dataset_id, initial_dataset_size, result_dataset_size)
        self.log.info("Importing data finished!")
        return {"total_imported": int(result_dataset_size) - int(initial_dataset_size)}


class TranslateDeleteDatasetOperator(GoogleCloudBaseOperator):
    """
    Delete translation dataset and all of its contents.

    Deletes the translation dataset and it's data, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateDeleteDatasetOperator`.

    :param dataset_id: The dataset_id of target native dataset to be deleted.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        dataset_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.location = location
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = TranslateHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Deleting the dataset %s...", self.dataset_id)
        operation = hook.delete_dataset(
            dataset_id=self.dataset_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation_done(operation=operation, timeout=self.timeout)
        self.log.info("Dataset deletion complete!")


class TranslateCreateModelOperator(GoogleCloudBaseOperator):
    """
    Creates a Google Cloud Translate model.

    Creates a `native` translation model, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateCreateModelOperator`.

    :param dataset_id: The dataset id used for model training.
    :param project_id: ID of the Google Cloud project where dataset is located.
        If not provided default project_id is used.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "dataset_id",
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    operator_extra_links = (TranslationModelLink(),)

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        dataset_id: str,
        display_name: str,
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.dataset_id = dataset_id
        self.display_name = display_name
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Model creation started, dataset_id %s...", self.dataset_id)
        try:
            result_operation = hook.create_model(
                dataset_id=self.dataset_id,
                display_name=self.display_name,
                location=self.location,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except GoogleAPICallError as e:
            self.log.error("Error submitting create_model operation ")
            raise AirflowException(e)

        self.log.info("Training has started")
        hook.wait_for_operation_done(operation=result_operation)
        result = hook.wait_for_operation_result(operation=result_operation)
        result = type(result).to_dict(result)
        model_id = hook.extract_object_id(result)
        context["ti"].xcom_push(key="model_id", value=model_id)
        self.log.info("Model creation complete. The model_id: %s.", model_id)

        project_id = self.project_id or hook.project_id
        TranslationModelLink.persist(
            context=context,
            dataset_id=self.dataset_id,
            model_id=model_id,
            project_id=project_id,
            location=self.location,
        )
        return result


class TranslateModelsListOperator(GoogleCloudBaseOperator):
    """
    Get a list of native Google Cloud Translation models in a project.

    Get project's list of `native` translation models, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateModelsListOperator`.

    :param project_id: ID of the Google Cloud project where dataset is located.
        If not provided default project_id is used.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    operator_extra_links = (TranslationModelsListLink(),)

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        project_id = self.project_id or hook.project_id
        TranslationModelsListLink.persist(
            context=context,
            project_id=project_id,
        )
        self.log.info("Requesting models list")
        results_pager = hook.list_models(
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result_ids = []
        for model_item in results_pager:
            model_data = type(model_item).to_dict(model_item)
            model_id = hook.extract_object_id(model_data)
            result_ids.append(model_id)
        self.log.info("Fetching the models list complete. Model id-s: %s", result_ids)
        return result_ids


class TranslateDeleteModelOperator(GoogleCloudBaseOperator):
    """
    Delete translation model and all of its contents.

    Deletes the translation model and it's data, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateDeleteModelOperator`.

    :param model_id: The model_id of target native model to be deleted.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "model_id",
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        model_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.model_id = model_id
        self.project_id = project_id
        self.location = location
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = TranslateHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Deleting the model %s...", self.model_id)
        operation = hook.delete_model(
            model_id=self.model_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation_done(operation=operation, timeout=self.timeout)
        self.log.info("Model deletion complete!")


class TranslateDocumentOperator(GoogleCloudBaseOperator):
    """
    Translate document provided.

    Wraps the Google cloud Translate Text (Advanced) functionality.
    Supports wide range of input/output file types, please visit the
    https://cloud.google.com/translate/docs/advanced/translate-documents for more details.

    For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TranslateDocumentOperator`.

    :param project_id: Optional. The ID of the Google Cloud project that the
        service belongs to. If not specified the hook project_id will be used.
    :param source_language_code: Optional. The ISO-639 language code of the
        input document text if known. If the source language isn't specified,
        the API attempts to identify the source language automatically and returns
        the source language within the response.
    :param target_language_code: Required. The ISO-639 language code to use
        for translation of the input document text.
    :param location: Optional. Project or location to make a call. Must refer to a caller's project.
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
    :param is_translate_native_pdf_only: Optional. Param for external customers.
        If true, the page limit of online native PDF translation is 300 and only native PDF pages
        will be translated.
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
           ``projects/{project-number-or-id}/locations/{location-id}/models/general/nmt``

        If not provided, the default Google model (NMT) will be used
        for translation.
    :param glossary_config: Optional. Glossary to be applied.
    :param transliteration_config: Optional. Transliteration to be applied.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (TranslateResultByOutputConfigLink(),)

    template_fields: Sequence[str] = (
        "source_language_code",
        "target_language_code",
        "document_input_config",
        "document_output_config",
        "model",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        source_language_code: str | None = None,
        target_language_code: str,
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
        retry: Retry | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.source_language_code = source_language_code
        self.target_language_code = target_language_code
        self.document_input_config = document_input_config
        self.document_output_config = document_output_config
        self.customized_attribution = customized_attribution
        self.is_translate_native_pdf_only = is_translate_native_pdf_only
        self.enable_shadow_removal_native_pdf = enable_shadow_removal_native_pdf
        self.enable_rotation_correction = enable_rotation_correction
        self.location = location
        self.labels = labels
        self.model = model
        self.glossary_config = glossary_config
        self.metadate = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            self.log.info("Starting the document translation")
            doc_translation_result = hook.translate_document(
                source_language_code=self.source_language_code,
                target_language_code=self.target_language_code,
                document_input_config=self.document_input_config,
                document_output_config=self.document_output_config,
                customized_attribution=self.customized_attribution,
                is_translate_native_pdf_only=self.is_translate_native_pdf_only,
                enable_shadow_removal_native_pdf=self.enable_shadow_removal_native_pdf,
                enable_rotation_correction=self.enable_rotation_correction,
                location=self.location,
                labels=self.labels,
                model=self.model,
                glossary_config=self.glossary_config,
                timeout=self.timeout,
                retry=self.retry,
                metadata=self.metadate,
            )
            self.log.info("Document translation completed")
        except GoogleAPICallError as e:
            self.log.error("An error occurred executing translate_document method: \n%s", e)
            raise AirflowException(e)
        if self.document_output_config:
            TranslateResultByOutputConfigLink.persist(
                context=context,
                project_id=self.project_id or hook.project_id,
                output_config=self.document_output_config,
            )
        return cast("dict", type(doc_translation_result).to_dict(doc_translation_result))


class TranslateDocumentBatchOperator(GoogleCloudBaseOperator):
    """
    Translate documents provided via input and output configurations.

    Up to 10 target languages per operation supported.
    Wraps the Google cloud Translate Text (Advanced) functionality.
    See https://cloud.google.com/translate/docs/advanced/batch-translation.

    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateDocumentBatchOperator`.

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
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (TranslateResultByOutputConfigLink(),)

    template_fields: Sequence[str] = (
        "input_configs",
        "output_config",
        "target_language_codes",
        "source_language_code",
        "models",
        "glossaries",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
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
        metadata: Sequence[tuple[str, str]] = (),
        timeout: float | _MethodDefault = DEFAULT,
        retry: Retry | _MethodDefault | None = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.target_language_codes = target_language_codes
        self.source_language_code = source_language_code
        self.input_configs = input_configs
        self.output_config = output_config
        self.customized_attribution = customized_attribution
        self.format_conversions = format_conversions
        self.enable_shadow_removal_native_pdf = enable_shadow_removal_native_pdf
        self.enable_rotation_correction = enable_rotation_correction
        self.models = models
        self.glossaries = glossaries
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            batch_document_translate_operation = hook.batch_translate_document(
                project_id=self.project_id,
                location=self.location,
                target_language_codes=self.target_language_codes,
                source_language_code=self.source_language_code,
                input_configs=self.input_configs,
                output_config=self.output_config,
                customized_attribution=self.customized_attribution,
                format_conversions=self.format_conversions,
                enable_shadow_removal_native_pdf=self.enable_shadow_removal_native_pdf,
                enable_rotation_correction=self.enable_rotation_correction,
                models=self.models,
                glossaries=self.glossaries,
                metadata=self.metadata,
                timeout=self.timeout,
                retry=self.retry,
            )
        except GoogleAPICallError as e:
            self.log.error("An error occurred executing batch_translate_document method: \n%s", e)
            raise AirflowException(e)
        self.log.info("Batch document translation job started.")
        TranslateResultByOutputConfigLink.persist(
            context=context,
            project_id=self.project_id or hook.project_id,
            output_config=self.output_config,
        )
        result = hook.wait_for_operation_result(batch_document_translate_operation)
        self.log.info("Batch document translation job finished")
        return cast("dict", type(result).to_dict(result))


class TranslateCreateGlossaryOperator(GoogleCloudBaseOperator):
    """
    Creates a Google Cloud Translation Glossary.

    Creates a translation glossary, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateCreateGlossaryOperator`.

    :param glossary_id: User-specified id to built glossary resource name.
    :param input_config: The input configuration of examples to built glossary from.
        Total glossary must not exceed 10M Unicode codepoints.
        The headers should not be included into the input file table, as languages specified with the
        ``language_pair`` or ``language_codes_set`` params.
    :param language_pair: Pair of language codes to be used for glossary creation.
        Used to built unidirectional glossary. If specified, the ``language_codes_set`` should be empty.
    :param language_codes_set: Set of language codes to create the equivalent term sets glossary.
        Meant multiple languages mapping. If specified, the ``language_pair`` should be empty.
    :param project_id: ID of the Google Cloud project where glossary is located.
        If not provided default project_id is used.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "glossary_id",
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        glossary_id: str,
        input_config: GlossaryInputConfig | dict,
        language_pair: Glossary.LanguageCodePair | dict | None = None,
        language_codes_set: Glossary.LanguageCodesSet | MutableSequence[str] | None = None,
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.glossary_id = glossary_id
        self.input_config = input_config
        self.language_pair = language_pair
        self.language_codes_set = language_codes_set
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        project_id = self.project_id or hook.project_id
        try:
            result_operation = hook.create_glossary(
                glossary_id=self.glossary_id,
                input_config=self.input_config,
                language_pair=self.language_pair,
                language_codes_set=self.language_codes_set,
                project_id=project_id,
                location=self.location,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except GoogleAPICallError as e:
            self.log.error("Error submitting create_glossary operation ")
            raise AirflowException(e)
        self.log.info("Glossary creation started, glossary_id %s...", self.glossary_id)

        result = hook.wait_for_operation_result(operation=result_operation)
        result = type(result).to_dict(result)

        glossary_id = hook.extract_object_id(result)
        context["ti"].xcom_push(key="glossary_id", value=glossary_id)
        self.log.info("Glossary creation complete. The glossary_id: %s.", glossary_id)
        return result


class TranslateUpdateGlossaryOperator(GoogleCloudBaseOperator):
    """
    Update glossary item with values provided.

    Updates the translation glossary, using translation API V3.
    Only ``display_name`` and ``input_config`` fields are allowed for update.

    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateUpdateGlossaryOperator`.

    :param glossary_id: User-specified id to built glossary resource name.
    :param input_config: The input configuration of examples to built glossary from.
        Total glossary must not exceed 10M Unicode codepoints.
        The headers should not be included into the input file table, as languages specified with the
        ``language_pair`` or ``language_codes_set`` params.
    :param language_pair: Pair of language codes to be used for glossary creation.
        Used to built unidirectional glossary. If specified, the ``language_codes_set`` should be empty.
    :param language_codes_set: Set of language codes to create the equivalent term sets glossary.
        Meant multiple languages mapping. If specified, the ``language_pair`` should be empty.
    :param project_id: ID of the Google Cloud project where glossary is located.
        If not provided default project_id is used.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "glossary_id",
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        glossary_id: str,
        new_display_name: str,
        new_input_config: GlossaryInputConfig | dict | None = None,
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.glossary_id = glossary_id
        self.new_display_name = new_display_name
        self.new_input_config = new_input_config
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        project_id = self.project_id or hook.project_id
        glossary = hook.get_glossary(
            glossary_id=self.glossary_id,
            project_id=project_id,
            location=self.location,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        try:
            result_operation = hook.update_glossary(
                glossary=glossary,
                new_display_name=self.new_display_name,
                new_input_config=self.new_input_config,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except GoogleAPICallError as e:
            self.log.error("Error submitting update_glossary operation ")
            raise AirflowException(e)
        self.log.info("Glossary update started, glossary_id %s...", self.glossary_id)

        result = hook.wait_for_operation_result(operation=result_operation)
        result = type(result).to_dict(result)
        self.log.info("Glossary update complete. The glossary_id: %s.", self.glossary_id)
        return result


class TranslateListGlossariesOperator(GoogleCloudBaseOperator):
    """
    Get a list of translation glossaries in a project.

    List the translation glossaries, using translation API V3.

    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateListGlossariesOperator`.

    :param project_id: ID of the Google Cloud project where glossary is located.
        If not provided default project_id is used.
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
            For the equivalent term set glossaries, the ``src`` and/or ``tgt``
            add restrictions on the term set.
            For example: ``src=en-US AND tgt=zh-CN`` will only pick the unidirectional glossaries which
            exactly match the source language code as ``en-US`` and the target language code ``zh-CN``,
            but all equivalent term set glossaries which contain ``en-US`` and ``zh-CN`` in their language
            set will be picked.
            If missing, no filtering is performed.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (TranslationGlossariesListLink(),)

    template_fields: Sequence[str] = (
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter_str: str | None = None,
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.page_size = page_size
        self.page_token = page_token
        self.filter_str = filter_str
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> Sequence[str]:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        project_id = self.project_id or hook.project_id
        TranslationGlossariesListLink.persist(
            context=context,
            project_id=project_id,
        )
        self.log.info("Requesting glossaries list")
        try:
            results_pager = hook.list_glossaries(
                project_id=project_id,
                location=self.location,
                page_size=self.page_size,
                page_token=self.page_token,
                filter_str=self.filter_str,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except GoogleAPICallError as e:
            self.log.error("Error submitting list_glossaries request")
            raise AirflowException(e)

        result_ids = []
        for glossary_item_raw in results_pager:
            glossary_item = type(glossary_item_raw).to_dict(glossary_item_raw)
            glossary_id = hook.extract_object_id(glossary_item)
            result_ids.append(glossary_id)
        self.log.info("Fetching the glossaries list complete. Glossary id-s: %s", result_ids)
        return result_ids


class TranslateDeleteGlossaryOperator(GoogleCloudBaseOperator):
    """
    Delete a Google Cloud Translation Glossary.

    Deletes a translation glossary, using API V3.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:TranslateDeleteGlossaryOperator`.

    :param glossary_id: User-specified id to delete glossary resource item.
    :param project_id: ID of the Google Cloud project where glossary is located.
        If not provided default project_id is used.
    :param location: The location of the project.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata:  Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "glossary_id",
        "location",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str,
        glossary_id: str,
        timeout: float | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        gcp_conn_id: str = "google_cloud_default",
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.glossary_id = glossary_id
        self.project_id = project_id
        self.location = location
        self.metadata = metadata
        self.timeout = timeout
        self.retry = retry
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        hook = TranslateHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        project_id = self.project_id or hook.project_id
        try:
            result_operation = hook.delete_glossary(
                glossary_id=self.glossary_id,
                project_id=project_id,
                location=self.location,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except GoogleAPICallError as e:
            self.log.error("Error submitting delete_glossary operation ")
            raise AirflowException(e)
        self.log.info("Glossary delete started, glossary_id %s...", self.glossary_id)

        result = hook.wait_for_operation_result(operation=result_operation)
        result = type(result).to_dict(result)
        self.log.info("Glossary deletion complete. The glossary_id: %s.", self.glossary_id)
        return result
