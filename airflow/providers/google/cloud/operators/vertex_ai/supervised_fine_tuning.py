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
"""This module contains Google Vertex AI Supervised Fine Tuning operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.providers.google.cloud.hooks.vertex_ai.supervised_fine_tuning import SupervisedFineTuningHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SupervisedFineTuningTrainOperator(GoogleCloudBaseOperator):
    """
    Use the Supervised Fine Tuning API to create a tuning job.

    :param source_model: Required. A pre-trained model optimized for performing natural
        language tasks such as classification, summarization, extraction, content
        creation, and ideation.
    :param training_dataset: Required. Cloud Storage URI of your training dataset. The dataset
        must be formatted as a JSONL file. For best results, provide at least 100 to 500 examples.
    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
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

    template_fields = ("location", "project_id", "impersonation_chain", "train_dataset", "validation_dataset")

    def __init__(
        self,
        *,
        source_model: str,
        train_dataset: str,
        project_id: str,
        location: str,
        tuned_model_display_name: str | None = None,
        validation_dataset: str | None = None,
        epochs: int | None = None,
        adapter_size: int | None = None,
        learning_rate_multiplier: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_model = source_model
        self.train_dataset = train_dataset
        self.tuned_model_display_name = tuned_model_display_name
        self.validation_dataset = validation_dataset
        self.epochs = epochs
        self.adapter_size = adapter_size
        self.learning_rate_multiplier = learning_rate_multiplier
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = SupervisedFineTuningHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.train(
            source_model=self.source_model,
            train_dataset=self.train_dataset,
            project_id=self.project_id,
            location=self.location,
            validation_dataset=self.validation_dataset,
            epochs=self.epochs,
            adapter_size=self.adapter_size,
            learning_rate_multiplier=self.learning_rate_multiplier,
            tuned_model_display_name=self.tuned_model_display_name,
        )

        self.log.info("Tuned Model Name: %s", response.tuned_model_name)
        self.log.info("Tuned Model Endpoint Name: %s", response.tuned_model_endpoint_name)

        self.xcom_push(context, key="tuned_model_name", value=response.tuned_model_name)
        self.xcom_push(context, key="tuned_model_endpoint_name", value=response.tuned_model_endpoint_name)
        return response
