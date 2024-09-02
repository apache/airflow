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
from typing import TYPE_CHECKING, Sequence

import vertexai
from vertexai.generative_models import GenerativeModel, Part
from vertexai.language_models import TextEmbeddingModel, TextGenerationModel
from vertexai.preview.tuning import sft

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.cloud.aiplatform_v1 import types as types_v1
    from google.cloud.aiplatform_v1beta1 import types as types_v1beta1


class GenerativeModelHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Generative Model APIs."""

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

    def get_text_generation_model(self, pretrained_model: str):
        """Return a Model Garden Model object based on Text Generation."""
        model = TextGenerationModel.from_pretrained(pretrained_model)
        return model

    def get_text_embedding_model(self, pretrained_model: str):
        """Return a Model Garden Model object based on Text Embedding."""
        model = TextEmbeddingModel.from_pretrained(pretrained_model)
        return model

    def get_generative_model(self, pretrained_model: str) -> GenerativeModel:
        """Return a Generative Model object."""
        model = GenerativeModel(pretrained_model)
        return model

    @deprecated(
        planned_removal_date="January 01, 2025",
        use_instead="Part objects included in contents parameter of "
        "airflow.providers.google.cloud.hooks.generative_model."
        "GenerativeModelHook.generative_model_generate_content",
        category=AirflowProviderDeprecationWarning,
    )
    def get_generative_model_part(self, content_gcs_path: str, content_mime_type: str | None = None) -> Part:
        """Return a Generative Model Part object."""
        part = Part.from_uri(content_gcs_path, mime_type=content_mime_type)
        return part

    @deprecated(
        planned_removal_date="January 01, 2025",
        use_instead="airflow.providers.google.cloud.hooks.generative_model."
        "GenerativeModelHook.text_generation_model_predict",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def prompt_language_model(
        self,
        prompt: str,
        pretrained_model: str,
        temperature: float,
        max_output_tokens: int,
        top_p: float,
        top_k: int,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Use the Vertex AI PaLM API to generate natural language text.

        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Vertex AI PaLM API, in order to elicit a specific response.
        :param pretrained_model: A pre-trained model optimized for performing natural
            language tasks such as classification, summarization, extraction, content
            creation, and ideation.
        :param temperature: Temperature controls the degree of randomness in token
            selection.
        :param max_output_tokens: Token limit determines the maximum amount of text
            output.
        :param top_p: Tokens are selected from most probable to least until the sum
            of their probabilities equals the top_p value. Defaults to 0.8.
        :param top_k: A top_k of 1 means the selected token is the most probable
            among all tokens.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        parameters = {
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
            "top_p": top_p,
            "top_k": top_k,
        }

        model = self.get_text_generation_model(pretrained_model)

        response = model.predict(
            prompt=prompt,
            **parameters,
        )
        return response.text

    @deprecated(
        planned_removal_date="January 01, 2025",
        use_instead="airflow.providers.google.cloud.hooks.generative_model."
        "GenerativeModelHook.text_embedding_model_get_embeddings",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def generate_text_embeddings(
        self,
        prompt: str,
        pretrained_model: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> list:
        """
        Use the Vertex AI PaLM API to generate text embeddings.

        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Vertex AI PaLM API, in order to elicit a specific response.
        :param pretrained_model: A pre-trained model optimized for generating text embeddings.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())
        model = self.get_text_embedding_model(pretrained_model)

        response = model.get_embeddings([prompt])[0]  # single prompt

        return response.values

    @deprecated(
        planned_removal_date="January 01, 2025",
        use_instead="airflow.providers.google.cloud.hooks.generative_model."
        "GenerativeModelHook.generative_model_generate_content",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def prompt_multimodal_model(
        self,
        prompt: str,
        location: str,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        pretrained_model: str = "gemini-pro",
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Use the Vertex AI Gemini Pro foundation model to generate natural language text.

        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Multi-modal model, in order to elicit a specific response.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param generation_config: Optional. Generation configuration settings.
        :param safety_settings: Optional. Per request settings for blocking unsafe content.
        :param pretrained_model: By default uses the pre-trained model `gemini-pro`,
            supporting prompts with text-only input, including natural language
            tasks, multi-turn text and code chat, and code generation. It can
            output text and code.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        model = self.get_generative_model(pretrained_model)
        response = model.generate_content(
            contents=[prompt], generation_config=generation_config, safety_settings=safety_settings
        )

        return response.text

    @deprecated(
        planned_removal_date="January 01, 2025",
        use_instead="airflow.providers.google.cloud.hooks.generative_model."
        "GenerativeModelHook.generative_model_generate_content",
        category=AirflowProviderDeprecationWarning,
    )
    @GoogleBaseHook.fallback_to_default_project_id
    def prompt_multimodal_model_with_media(
        self,
        prompt: str,
        location: str,
        media_gcs_path: str,
        mime_type: str,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        pretrained_model: str = "gemini-pro-vision",
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Use the Vertex AI Gemini Pro foundation model to generate natural language text.

        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Multi-modal model, in order to elicit a specific response.
        :param generation_config: Optional. Generation configuration settings.
        :param safety_settings: Optional. Per request settings for blocking unsafe content.
        :param pretrained_model: By default uses the pre-trained model `gemini-pro-vision`,
            supporting prompts with text-only input, including natural language
            tasks, multi-turn text and code chat, and code generation. It can
            output text and code.
        :param media_gcs_path: A GCS path to a content file such as an image or a video.
            Can be passed to the multi-modal model as part of the prompt. Used with vision models.
        :param mime_type: Validates the media type presented by the file in the media_gcs_path.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        model = self.get_generative_model(pretrained_model)
        part = self.get_generative_model_part(media_gcs_path, mime_type)
        response = model.generate_content(
            contents=[prompt, part], generation_config=generation_config, safety_settings=safety_settings
        )

        return response.text

    @GoogleBaseHook.fallback_to_default_project_id
    def text_generation_model_predict(
        self,
        prompt: str,
        pretrained_model: str,
        temperature: float,
        max_output_tokens: int,
        top_p: float,
        top_k: int,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Use the Vertex AI PaLM API to generate natural language text.

        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Vertex AI PaLM API, in order to elicit a specific response.
        :param pretrained_model: A pre-trained model optimized for performing natural
            language tasks such as classification, summarization, extraction, content
            creation, and ideation.
        :param temperature: Temperature controls the degree of randomness in token
            selection.
        :param max_output_tokens: Token limit determines the maximum amount of text
            output.
        :param top_p: Tokens are selected from most probable to least until the sum
            of their probabilities equals the top_p value. Defaults to 0.8.
        :param top_k: A top_k of 1 means the selected token is the most probable
            among all tokens.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        parameters = {
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
            "top_p": top_p,
            "top_k": top_k,
        }

        model = self.get_text_generation_model(pretrained_model)

        response = model.predict(
            prompt=prompt,
            **parameters,
        )
        return response.text

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

        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Vertex AI PaLM API, in order to elicit a specific response.
        :param pretrained_model: A pre-trained model optimized for generating text embeddings.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())
        model = self.get_text_embedding_model(pretrained_model)

        response = model.get_embeddings([prompt])[0]  # single prompt

        return response.values

    @GoogleBaseHook.fallback_to_default_project_id
    def generative_model_generate_content(
        self,
        contents: list,
        location: str,
        tools: list | None = None,
        generation_config: dict | None = None,
        safety_settings: dict | None = None,
        pretrained_model: str = "gemini-pro",
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Use the Vertex AI Gemini Pro foundation model to generate natural language text.

        :param contents: Required. The multi-part content of a message that a user or a program
            gives to the generative model, in order to elicit a specific response.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param generation_config: Optional. Generation configuration settings.
        :param safety_settings: Optional. Per request settings for blocking unsafe content.
        :param pretrained_model: By default uses the pre-trained model `gemini-pro`,
            supporting prompts with text-only input, including natural language
            tasks, multi-turn text and code chat, and code generation. It can
            output text and code.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        model = self.get_generative_model(pretrained_model)
        response = model.generate_content(
            contents=contents,
            tools=tools,
            generation_config=generation_config,
            safety_settings=safety_settings,
        )

        return response.text

    @GoogleBaseHook.fallback_to_default_project_id
    def supervised_fine_tuning_train(
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
    ) -> types_v1.TuningJob:
        """
        Use the Supervised Fine Tuning API to create a tuning job.

        :param source_model: Required. A pre-trained model optimized for performing natural
            language tasks such as classification, summarization, extraction, content
            creation, and ideation.
        :param train_dataset: Required. Cloud Storage URI of your training dataset. The dataset
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

    @GoogleBaseHook.fallback_to_default_project_id
    def count_tokens(
        self,
        contents: list,
        location: str,
        pretrained_model: str = "gemini-pro",
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types_v1beta1.CountTokensResponse:
        """
        Use the Vertex AI Count Tokens API to calculate the number of input tokens before sending a request to the Gemini API.

        :param contents: Required. The multi-part content of a message that a user or a program
            gives to the generative model, in order to elicit a specific response.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param pretrained_model: By default uses the pre-trained model `gemini-pro`,
            supporting prompts with text-only input, including natural language
            tasks, multi-turn text and code chat, and code generation. It can
            output text and code.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        """
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

        model = self.get_generative_model(pretrained_model)
        response = model.count_tokens(
            contents=contents,
        )

        return response
