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

from typing import Any

import vertexai
from vertexai.generative_models import GenerativeModel
from vertexai.language_models import TextEmbeddingModel
from vertexai.preview import generative_models as preview_generative_model
from vertexai.preview.caching import CachedContent
from vertexai.preview.evaluation import EvalResult, EvalTask

from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook


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
