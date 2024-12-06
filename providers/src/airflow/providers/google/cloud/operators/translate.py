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
from typing import TYPE_CHECKING

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.translate import CloudTranslateHook, TranslateHook
from airflow.providers.google.cloud.links.translate import (
    TranslateTextBatchLink,
    TranslationDatasetsListLink,
    TranslationModelLink,
    TranslationModelsListLink,
    TranslationNativeDatasetLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.translate_v3.types import (
        DatasetInputConfig,
        InputConfig,
        OutputConfig,
        TranslateTextGlossaryConfig,
        TransliterationConfig,
        automl_translation,
    )

    from airflow.utils.context import Context


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
            task_instance=self,
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
        self.xcom_push(context, key="dataset_id", value=dataset_id)
        self.log.info("Dataset creation complete. The dataset_id: %s.", dataset_id)

        project_id = self.project_id or hook.project_id
        TranslationNativeDatasetLink.persist(
            context=context,
            task_instance=self,
            dataset_id=dataset_id,
            project_id=project_id,
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
            task_instance=self,
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


class TranslateImportDataOperator(GoogleCloudBaseOperator):
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

    def execute(self, context: Context):
        hook = TranslateHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
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
            task_instance=self,
            dataset_id=self.dataset_id,
            project_id=project_id,
        )
        hook.wait_for_operation_done(operation=operation, timeout=self.timeout)
        self.log.info("Importing data finished!")


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
        self.xcom_push(context, key="model_id", value=model_id)
        self.log.info("Model creation complete. The model_id: %s.", model_id)

        project_id = self.project_id or hook.project_id
        TranslationModelLink.persist(
            context=context,
            task_instance=self,
            dataset_id=self.dataset_id,
            model_id=model_id,
            project_id=project_id,
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
            task_instance=self,
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
