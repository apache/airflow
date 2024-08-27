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
"""This module contains a Google Cloud Vertex AI Supervised Fine Tuning hook."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Sequence

import vertexai
from vertexai.preview.tuning import sft

if TYPE_CHECKING:
    from vertexai.preview.tuning.sft import SupervisedTuningJob


from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook


class SupervisedFineTuningHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Supervised Fine Tuning APIs."""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)

    @GoogleBaseHook.fallback_to_default_project_id
    def train(
        self,
        source_model: str,
        train_dataset: str,
        location: str,
        tuned_model_display_name: str | None = None,
        validation_dataset: str | None = None,
        epochs: int | None = None,
        adapter_size: int | None = None,
        learning_rate_multiplier: float | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> SupervisedTuningJob:
        """
        Use the Supervised Fine Tuning API to create a tuning job.

        :param source_model: Required. A pre-trained model optimized for performing natural
            language tasks such as classification, summarization, extraction, content
            creation, and ideation.
        :param training_dataset: Required. Cloud Storage URI of your training dataset. The dataset
            must be formatted as a JSONL file. For best results, provide at least 100 to 500 examples.
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
