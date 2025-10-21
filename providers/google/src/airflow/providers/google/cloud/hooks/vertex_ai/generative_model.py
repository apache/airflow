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
"""This module contains a Google Cloud Vertex AI Generative Model hook."""

from __future__ import annotations

import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal

import vertexai
from google.cloud import aiplatform
from vertexai.generative_models import GenerativeModel
from vertexai.language_models import TextEmbeddingModel
from vertexai.preview import generative_models as preview_generative_model
from vertexai.preview.caching import CachedContent
from vertexai.preview.evaluation import EvalResult, EvalTask
from vertexai.preview.tuning import sft

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.cloud.aiplatform_v1beta1 import types as types_v1beta1


class GenerativeModelHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Generative Model APIs."""

    def get_text_embedding_model(self, pretrained_model: str):
        """Return a Model Garden Model object based on Text Embedding."""
        model = TextEmbeddingModel.from_pretrained(pretrained_model)
        return model

    def get_generative_model(
        self,
        pretrained_model: str,
        system_instruction: Any | None = None,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        tools: list | None = None,
    ) -> GenerativeModel:
        """Return a Generative Model object."""
        model = GenerativeModel(
            model_name=pretrained_model,
            system_instruction=system_instruction,
            generation_config=generation_config,
            safety_settings=safety_settings,
            tools=tools,
        )
        return model

    def get_eval_task(
        self,
        dataset: dict,
        metrics: list,
        experiment: str,
    ) -> EvalTask:
        """Return an EvalTask object."""
        eval_task = EvalTask(
            dataset=dataset,
            metrics=metrics,
            experiment=experiment,
        )
        return eval_task

    def get_cached_context_model(
        self,
        cached_content_name: str,
    ) -> Any:
        """Return a Generative Model with Cached Context."""
        cached_content = CachedContent(cached_content_name=cached_content_name)

        cached_context_model = preview_generative_model.GenerativeModel.from_cached_content(cached_content)
        return cached_context_model

    @deprecated(
        planned_removal_date="January 3, 2026",
        use_instead="airflow.providers.google.cloud.hooks.gen_ai.generative_model.GenAIGenerativeModelHook.embed_content",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def text_embedding_model_get_embeddings(
        self,
        prompt: str,
        pretrained_model: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> list:
        """
        Use the Vertex AI PaLM API to generate text embeddings.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Vertex AI PaLM API, in order to elicit a specific response.
        :param pretrained_model: A pre-trained model optimized for generating text embeddings.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())
        model = self.get_text_embedding_model(pretrained_model)

        response = model.get_embeddings([prompt])[0]  # single prompt

        return response.values

    @deprecated(
        planned_removal_date="January 3, 2026",
        use_instead="airflow.providers.google.cloud.hooks.gen_ai.generative_model.GenAIGenerativeModelHook.generate_content",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def generative_model_generate_content(
        self,
        contents: list,
        location: str,
        pretrained_model: str,
        tools: list | None = None,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        system_instruction: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Use the Vertex AI Gemini Pro foundation model to generate natural language text.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param contents: Required. The multi-part content of a message that a user or a program
            gives to the generative model, in order to elicit a specific response.
        :param generation_config: Optional. Generation configuration settings.
        :param safety_settings: Optional. Per request settings for blocking unsafe content.
        :param tools: Optional. A list of tools available to the model during evaluation, such as a data store.
        :param system_instruction: Optional. An instruction given to the model to guide its behavior.
        :param pretrained_model: Required. Model,
            supporting prompts with text-only input, including natural language
            tasks, multi-turn text and code chat, and code generation. It can
            output text and code.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        model = self.get_generative_model(
            pretrained_model=pretrained_model, system_instruction=system_instruction
        )
        response = model.generate_content(
            contents=contents,
            tools=tools,
            generation_config=generation_config,
            safety_settings=safety_settings,
        )

        return response.text

    @deprecated(
        planned_removal_date="January 3, 2026",
        use_instead="airflow.providers.google.cloud.hooks.gen_ai.generative_model.GenAIGenerativeModelHook.supervised_fine_tuning_train",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def supervised_fine_tuning_train(
        self,
        source_model: str,
        train_dataset: str,
        location: str,
        tuned_model_display_name: str | None = None,
        validation_dataset: str | None = None,
        epochs: int | None = None,
        adapter_size: Literal[1, 4, 8, 16] | None = None,
        learning_rate_multiplier: float | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Any:
        """
        Use the Supervised Fine Tuning API to create a tuning job.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
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
        :param learning_rate_multiplier: Optional. Multiplier for adjusting the default learning rate.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        sft_tuning_job = sft.train(
            source_model=source_model,
            train_dataset=train_dataset,
            validation_dataset=validation_dataset,
            epochs=epochs,
            adapter_size=adapter_size,
            learning_rate_multiplier=learning_rate_multiplier,
            tuned_model_display_name=tuned_model_display_name,
        )

        # Polling for job completion
        while not sft_tuning_job.has_ended:
            time.sleep(60)
            sft_tuning_job.refresh()

        return sft_tuning_job

    @deprecated(
        planned_removal_date="January 3, 2026",
        use_instead="airflow.providers.google.cloud.hooks.gen_ai.generative_model.GenAIGenerativeModelHook.count_tokens",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def count_tokens(
        self,
        contents: list,
        location: str,
        pretrained_model: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types_v1beta1.CountTokensResponse:
        """
        Use the Vertex AI Count Tokens API to calculate the number of input tokens before sending a request to the Gemini API.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param contents: Required. The multi-part content of a message that a user or a program
            gives to the generative model, in order to elicit a specific response.
        :param pretrained_model: Required. Model,
            supporting prompts with text-only input, including natural language
            tasks, multi-turn text and code chat, and code generation. It can
            output text and code.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        model = self.get_generative_model(pretrained_model=pretrained_model)
        response = model.count_tokens(
            contents=contents,
        )

        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def run_evaluation(
        self,
        pretrained_model: str,
        eval_dataset: dict,
        metrics: list,
        experiment_name: str,
        experiment_run_name: str,
        prompt_template: str,
        location: str,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        system_instruction: str | None = None,
        tools: list | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> EvalResult:
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
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        model = self.get_generative_model(
            pretrained_model=pretrained_model,
            system_instruction=system_instruction,
            generation_config=generation_config,
            safety_settings=safety_settings,
            tools=tools,
        )

        eval_task = self.get_eval_task(
            dataset=eval_dataset,
            metrics=metrics,
            experiment=experiment_name,
        )

        eval_result = eval_task.evaluate(
            model=model,
            prompt_template=prompt_template,
            experiment_run_name=experiment_run_name,
        )

        return eval_result

    @deprecated(
        planned_removal_date="January 3, 2026",
        use_instead="airflow.providers.google.cloud.hooks.gen_ai.generative_model.GenAIGenerativeModelHook.create_cached_content",
        category=AirflowProviderDeprecationWarning,
    )
    def create_cached_content(
        self,
        model_name: str,
        location: str,
        ttl_hours: float = 1,
        system_instruction: Any | None = None,
        contents: list[Any] | None = None,
        display_name: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
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
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        response = CachedContent.create(
            model_name=model_name,
            system_instruction=system_instruction,
            contents=contents,
            ttl=timedelta(hours=ttl_hours),
            display_name=display_name,
        )

        return response.name

    @deprecated(
        planned_removal_date="January 3, 2026",
        use_instead="airflow.providers.google.cloud.hooks.gen_ai.generative_model.GenAIGenerativeModelHook.generate_content",
        category=AirflowProviderDeprecationWarning,
    )
    def generate_from_cached_content(
        self,
        location: str,
        cached_content_name: str,
        contents: list,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Generate a response from CachedContent.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param cached_content_name: Required. The name of the cached content resource.
        :param contents: Required. The multi-part content of a message that a user or a program
            gives to the generative model, in order to elicit a specific response.
        :param generation_config: Optional. Generation configuration settings.
        :param safety_settings: Optional. Per request settings for blocking unsafe content.
        """
        # During run of the system test it was found out that names from xcom, e.g. 3402922389 can be
        # treated as int and throw an error TypeError: expected string or bytes-like object, got 'int'
        cached_content_name = str(cached_content_name)
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        cached_context_model = self.get_cached_context_model(cached_content_name=cached_content_name)

        response = cached_context_model.generate_content(
            contents=contents,
            generation_config=generation_config,
            safety_settings=safety_settings,
        )

        return response.text


@deprecated(
    planned_removal_date="January 3, 2026",
    use_instead="airflow.providers.google.cloud.hooks.vertex_ai.experiment_service.ExperimentRunHook",
    category=AirflowProviderDeprecationWarning,
)
class ExperimentRunHook(GoogleBaseHook):
    """Use the Vertex AI SDK for Python to create and manage your experiment runs."""

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_experiment_run(
        self,
        experiment_run_name: str,
        experiment_name: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        delete_backing_tensorboard_run: bool = False,
    ) -> None:
        """
        Delete experiment run from the experiment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param experiment_name: Required. The name of the evaluation experiment.
        :param experiment_run_name: Required. The specific run name or ID for this experiment.
        :param delete_backing_tensorboard_run: Whether to delete the backing Vertex AI TensorBoard run
            that stores time series metrics for this run.
        """
        self.log.info("Next experiment run will be deleted: %s", experiment_run_name)
        experiment_run = aiplatform.ExperimentRun(
            run_name=experiment_run_name, experiment=experiment_name, project=project_id, location=location
        )
        experiment_run.delete(delete_backing_tensorboard_run=delete_backing_tensorboard_run)
