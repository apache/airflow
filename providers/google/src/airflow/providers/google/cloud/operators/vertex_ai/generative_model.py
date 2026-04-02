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
from typing import TYPE_CHECKING

from airflow.providers.google.cloud.hooks.vertex_ai.generative_model import (
    GenerativeModelHook,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


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
