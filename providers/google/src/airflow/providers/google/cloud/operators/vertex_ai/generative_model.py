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
"""This module contains Google Vertex AI Generative AI operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

from google.api_core import exceptions

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.generative_model import (
    ExperimentRunHook,
    GenerativeModelHook,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.deprecated import deprecated

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


@deprecated(
    planned_removal_date="January 3, 2026",
    use_instead="airflow.providers.google.cloud.operators.gen_ai.generative_model.GenAIGenerateEmbeddingsOperator",
    category=AirflowProviderDeprecationWarning,
)
class TextEmbeddingModelGetEmbeddingsOperator(GoogleCloudBaseOperator):
    """
    Uses the Vertex AI Embeddings API to generate embeddings based on prompt.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param prompt: Required. Inputs or queries that a user or a program gives
        to the Vertex AI Generative Model API, in order to elicit a specific response (templated).
    :param pretrained_model: Required. Model, optimized for performing text embeddings.
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

    template_fields = ("location", "project_id", "impersonation_chain", "prompt", "pretrained_model")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        prompt: str,
        pretrained_model: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.prompt = prompt
        self.pretrained_model = pretrained_model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Generating text embeddings")
        response = self.hook.text_embedding_model_get_embeddings(
            project_id=self.project_id,
            location=self.location,
            prompt=self.prompt,
            pretrained_model=self.pretrained_model,
        )

        self.log.info("Model response: %s", response)
        context["ti"].xcom_push(key="model_response", value=response)

        return response


@deprecated(
    planned_removal_date="January 3, 2026",
    use_instead="airflow.providers.google.cloud.operators.gen_ai.generative_model.GenAIGenerateContentOperator",
    category=AirflowProviderDeprecationWarning,
)
class GenerativeModelGenerateContentOperator(GoogleCloudBaseOperator):
    """
    Use the Vertex AI Gemini Pro foundation model to generate content.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param contents: Required. The multi-part content of a message that a user or a program
        gives to the generative model, in order to elicit a specific response.
    :param generation_config: Optional. Generation configuration settings.
    :param safety_settings: Optional. Per request settings for blocking unsafe content.
    :param tools: Optional. A list of tools available to the model during evaluation, such as a data store.
    :param system_instruction: Optional. An instruction given to the model to guide its behavior.
    :param pretrained_model: Required. The name of the model to use for content generation,
        which can be a text-only or multimodal model. For example, `gemini-pro` or
        `gemini-pro-vision`.
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

    template_fields = ("location", "project_id", "impersonation_chain", "contents", "pretrained_model")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        contents: list,
        tools: list | None = None,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        system_instruction: str | None = None,
        pretrained_model: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.contents = contents
        self.tools = tools
        self.generation_config = generation_config
        self.safety_settings = safety_settings
        self.system_instruction = system_instruction
        self.pretrained_model = pretrained_model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.generative_model_generate_content(
            project_id=self.project_id,
            location=self.location,
            contents=self.contents,
            tools=self.tools,
            generation_config=self.generation_config,
            safety_settings=self.safety_settings,
            system_instruction=self.system_instruction,
            pretrained_model=self.pretrained_model,
        )

        self.log.info("Model response: %s", response)
        context["ti"].xcom_push(key="model_response", value=response)

        return response


@deprecated(
    planned_removal_date="January 3, 2026",
    use_instead="airflow.providers.google.cloud.operators.gen_ai.generative_model.GenAISupervisedFineTuningTrainOperator",
    category=AirflowProviderDeprecationWarning,
)
class SupervisedFineTuningTrainOperator(GoogleCloudBaseOperator):
    """
    Use the Supervised Fine Tuning API to create a tuning job.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param source_model: Required. A pre-trained model optimized for performing natural
        language tasks such as classification, summarization, extraction, content
        creation, and ideation.
    :param train_dataset: Required. Cloud Storage URI of your training dataset. The dataset
        must be formatted as a JSONL file. For best results, provide at least 100 to 500 examples.
    :param tuned_model_display_name: Optional. Display name of the TunedModel. The name can be up
        to 128 characters long and can consist of any UTF-8 characters.
    :param validation_dataset: Optional. Cloud Storage URI of your training dataset. The dataset must be
        formatted as a JSONL file. For best results, provide at least 100 to 500 examples.
    :param epochs: Optional. To optimize performance on a specific dataset, try using a higher
        epoch value. Increasing the number of epochs might improve results. However, be cautious
        about over-fitting, especially when dealing with small datasets. If over-fitting occurs,
        consider lowering the epoch number.
    :param adapter_size: Optional. Adapter size for tuning.
    :param learning_multiplier_rate: Optional. Multiplier for adjusting the default learning rate.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "train_dataset",
        "validation_dataset",
        "source_model",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        source_model: str,
        train_dataset: str,
        tuned_model_display_name: str | None = None,
        validation_dataset: str | None = None,
        epochs: int | None = None,
        adapter_size: Literal[1, 4, 8, 16] | None = None,
        learning_rate_multiplier: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.source_model = source_model
        self.train_dataset = train_dataset
        self.tuned_model_display_name = tuned_model_display_name
        self.validation_dataset = validation_dataset
        self.epochs = epochs
        self.adapter_size = adapter_size
        self.learning_rate_multiplier = learning_rate_multiplier
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.supervised_fine_tuning_train(
            project_id=self.project_id,
            location=self.location,
            source_model=self.source_model,
            train_dataset=self.train_dataset,
            validation_dataset=self.validation_dataset,
            epochs=self.epochs,
            adapter_size=self.adapter_size,
            learning_rate_multiplier=self.learning_rate_multiplier,
            tuned_model_display_name=self.tuned_model_display_name,
        )

        self.log.info("Tuned Model Name: %s", response.tuned_model_name)
        self.log.info("Tuned Model Endpoint Name: %s", response.tuned_model_endpoint_name)

        context["ti"].xcom_push(key="tuned_model_name", value=response.tuned_model_name)
        context["ti"].xcom_push(key="tuned_model_endpoint_name", value=response.tuned_model_endpoint_name)

        result = {
            "tuned_model_name": response.tuned_model_name,
            "tuned_model_endpoint_name": response.tuned_model_endpoint_name,
        }

        return result


@deprecated(
    planned_removal_date="January 3, 2026",
    use_instead="airflow.providers.google.cloud.operators.gen_ai.generative_model.GenAICountTokensOperator",
    category=AirflowProviderDeprecationWarning,
)
class CountTokensOperator(GoogleCloudBaseOperator):
    """
    Use the Vertex AI Count Tokens API to calculate the number of input tokens before sending a request to the Gemini API.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param contents: Required. The multi-part content of a message that a user or a program
        gives to the generative model, in order to elicit a specific response.
    :param pretrained_model: Required. Model, supporting prompts with text-only input,
        including natural language tasks, multi-turn text and code chat,
        and code generation. It can output text and code.
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

    template_fields = ("location", "project_id", "impersonation_chain", "contents", "pretrained_model")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        contents: list,
        pretrained_model: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.contents = contents
        self.pretrained_model = pretrained_model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.count_tokens(
            project_id=self.project_id,
            location=self.location,
            contents=self.contents,
            pretrained_model=self.pretrained_model,
        )

        self.log.info("Total tokens: %s", response.total_tokens)
        self.log.info("Total billable characters: %s", response.total_billable_characters)

        context["ti"].xcom_push(key="total_tokens", value=response.total_tokens)
        context["ti"].xcom_push(key="total_billable_characters", value=response.total_billable_characters)


class RunEvaluationOperator(GoogleCloudBaseOperator):
    """
    Use the Rapid Evaluation API to evaluate a model.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param pretrained_model: Required. A pre-trained model optimized for performing natural
        language tasks such as classification, summarization, extraction, content
        creation, and ideation.
    :param eval_dataset: Required. A fixed dataset for evaluating a model against. Adheres to Rapid Evaluation API.
    :param metrics: Required. A list of evaluation metrics to be used in the experiment. Adheres to Rapid Evaluation API.
    :param experiment_name: Required. The name of the evaluation experiment.
    :param experiment_run_name: Required. The specific run name or ID for this experiment.
    :param prompt_template: Required. The template used to format the model's prompts during evaluation. Adheres to Rapid Evaluation API.
    :param generation_config: Optional. A dictionary containing generation parameters for the model.
    :param safety_settings: Optional. A dictionary specifying harm category thresholds for blocking model outputs.
    :param system_instruction: Optional. An instruction given to the model to guide its behavior.
    :param tools: Optional. A list of tools available to the model during evaluation, such as a data store.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "pretrained_model",
        "eval_dataset",
        "prompt_template",
        "experiment_name",
        "experiment_run_name",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        pretrained_model: str,
        eval_dataset: dict,
        metrics: list,
        experiment_name: str,
        experiment_run_name: str,
        prompt_template: str,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        system_instruction: str | None = None,
        tools: list | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.pretrained_model = pretrained_model
        self.eval_dataset = eval_dataset
        self.metrics = metrics
        self.experiment_name = experiment_name
        self.experiment_run_name = experiment_run_name
        self.prompt_template = prompt_template
        self.generation_config = generation_config
        self.safety_settings = safety_settings
        self.system_instruction = system_instruction
        self.tools = tools
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.run_evaluation(
            project_id=self.project_id,
            location=self.location,
            pretrained_model=self.pretrained_model,
            eval_dataset=self.eval_dataset,
            metrics=self.metrics,
            experiment_name=self.experiment_name,
            experiment_run_name=self.experiment_run_name,
            prompt_template=self.prompt_template,
            generation_config=self.generation_config,
            safety_settings=self.safety_settings,
            system_instruction=self.system_instruction,
            tools=self.tools,
        )

        return response.summary_metrics


@deprecated(
    planned_removal_date="January 3, 2026",
    use_instead="airflow.providers.google.cloud.operators.gen_ai.generative_model.GenAICreateCachedContentOperator",
    category=AirflowProviderDeprecationWarning,
)
class CreateCachedContentOperator(GoogleCloudBaseOperator):
    """
    Create CachedContent to reduce the cost of requests that contain repeat content with high input token counts.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model_name: Required. The name of the publisher model to use for cached content.
    :param system_instruction: Developer set system instruction.
    :param contents: The content to cache.
    :param ttl_hours: The TTL for this resource in hours. The expiration time is computed: now + TTL.
        Defaults to one hour.
    :param display_name: The user-generated meaningful display name of the cached content
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "model_name",
        "contents",
        "system_instruction",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        model_name: str,
        system_instruction: Any | None = None,
        contents: list[Any] | None = None,
        ttl_hours: float = 1,
        display_name: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.model_name = model_name
        self.system_instruction = system_instruction
        self.contents = contents
        self.ttl_hours = ttl_hours
        self.display_name = display_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        cached_content_name = self.hook.create_cached_content(
            project_id=self.project_id,
            location=self.location,
            model_name=self.model_name,
            system_instruction=self.system_instruction,
            contents=self.contents,
            ttl_hours=self.ttl_hours,
            display_name=self.display_name,
        )

        self.log.info("Cached Content Name: %s", cached_content_name)

        return cached_content_name


@deprecated(
    planned_removal_date="January 3, 2026",
    use_instead="airflow.providers.google.cloud.operators.gen_ai.generative_model.GenAIGenerateContentOperator",
    category=AirflowProviderDeprecationWarning,
)
class GenerateFromCachedContentOperator(GoogleCloudBaseOperator):
    """
    Generate a response from CachedContent.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param cached_content_name: Required. The name of the cached content resource.
    :param contents: Required. The multi-part content of a message that a user or a program
        gives to the generative model, in order to elicit a specific response.
    :param generation_config: Optional. Generation configuration settings.
    :param safety_settings: Optional. Per request settings for blocking unsafe content.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "cached_content_name",
        "contents",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        cached_content_name: str,
        contents: list,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.cached_content_name = cached_content_name
        self.contents = contents
        self.generation_config = generation_config
        self.safety_settings = safety_settings
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        cached_content_text = self.hook.generate_from_cached_content(
            project_id=self.project_id,
            location=self.location,
            cached_content_name=self.cached_content_name,
            contents=self.contents,
            generation_config=self.generation_config,
            safety_settings=self.safety_settings,
        )

        self.log.info("Cached Content Response: %s", cached_content_text)

        return cached_content_text


@deprecated(
    planned_removal_date="January 3, 2026",
    use_instead="airflow.providers.google.cloud.operators.vertex_ai.experiment_service.DeleteExperimentRunOperator",
    category=AirflowProviderDeprecationWarning,
)
class DeleteExperimentRunOperator(GoogleCloudBaseOperator):
    """
    Use the Rapid Evaluation API to evaluate a model.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param experiment_name: Required. The name of the evaluation experiment.
    :param experiment_run_name: Required. The specific run name or ID for this experiment.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "experiment_name",
        "experiment_run_name",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        experiment_name: str,
        experiment_run_name: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.experiment_name = experiment_name
        self.experiment_run_name = experiment_run_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        self.hook = ExperimentRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.hook.delete_experiment_run(
                project_id=self.project_id,
                location=self.location,
                experiment_name=self.experiment_name,
                experiment_run_name=self.experiment_run_name,
            )
        except exceptions.NotFound:
            raise AirflowException(f"Experiment Run with name {self.experiment_run_name} not found")

        self.log.info("Deleted experiment run: %s", self.experiment_run_name)
