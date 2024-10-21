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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.exceptions import (
    AirflowProviderDeprecationWarning,
)

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")
from datetime import timedelta

from vertexai.generative_models import HarmBlockThreshold, HarmCategory, Part, Tool, grounding
from vertexai.preview.evaluation import MetricPromptTemplateExamples

from airflow.providers.google.cloud.hooks.vertex_ai.generative_model import (
    GenerativeModelHook,
)

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"

TEST_PROMPT = "In 10 words or less, what is apache airflow?"
TEST_CONTENTS = [TEST_PROMPT]
TEST_LANGUAGE_PRETRAINED_MODEL = "text-bison"
TEST_TEMPERATURE = 0.0
TEST_MAX_OUTPUT_TOKENS = 256
TEST_TOP_P = 0.8
TEST_TOP_K = 40

TEST_TEXT_EMBEDDING_MODEL = ""

TEST_MULTIMODAL_PRETRAINED_MODEL = "gemini-pro"
TEST_SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
}
TEST_GENERATION_CONFIG = {
    "max_output_tokens": TEST_MAX_OUTPUT_TOKENS,
    "top_p": TEST_TOP_P,
    "temperature": TEST_TEMPERATURE,
}
TEST_TOOLS = [Tool.from_google_search_retrieval(grounding.GoogleSearchRetrieval())]

TEST_MULTIMODAL_VISION_MODEL = "gemini-pro-vision"
TEST_VISION_PROMPT = "In 10 words or less, describe this content."
TEST_MEDIA_GCS_PATH = "gs://download.tensorflow.org/example_images/320px-Felis_catus-cat_on_snow.jpg"
TEST_MIME_TYPE = "image/jpeg"

SOURCE_MODEL = "gemini-1.0-pro-002"
TRAIN_DATASET = "gs://cloud-samples-data/ai-platform/generative_ai/sft_train_data.jsonl"

TEST_EVAL_DATASET = {
    "context": [
        "To make a classic spaghetti carbonara, start by bringing a large pot of salted water to a boil. While the water is heating up, cook pancetta or guanciale in a skillet with olive oil over medium heat until it's crispy and golden brown. Once the pancetta is done, remove it from the skillet and set it aside. In the same skillet, whisk together eggs, grated Parmesan cheese, and black pepper to make the sauce. When the pasta is cooked al dente, drain it and immediately toss it in the skillet with the egg mixture, adding a splash of the pasta cooking water to create a creamy sauce.",
        "Preparing a perfect risotto requires patience and attention to detail. Begin by heating butter in a large, heavy-bottomed pot over medium heat. Add finely chopped onions and minced garlic to the pot, and cook until they're soft and translucent, about 5 minutes. Next, add Arborio rice to the pot and cook, stirring constantly, until the grains are coated with the butter and begin to toast slightly. Pour in a splash of white wine and cook until it's absorbed. From there, gradually add hot chicken or vegetable broth to the rice, stirring frequently, until the risotto is creamy and the rice is tender with a slight bite.",
        "For a flavorful grilled steak, start by choosing a well-marbled cut of beef like ribeye or New York strip. Season the steak generously with kosher salt and freshly ground black pepper on both sides, pressing the seasoning into the meat. Preheat a grill to high heat and brush the grates with oil to prevent sticking. Place the seasoned steak on the grill and cook for about 4-5 minutes on each side for medium-rare, or adjust the cooking time to your desired level of doneness. Let the steak rest for a few minutes before slicing against the grain and serving.",
        "Creating a creamy homemade tomato soup is a comforting and simple process. Begin by heating olive oil in a large pot over medium heat. Add diced onions and minced garlic to the pot and cook until they're soft and fragrant. Next, add chopped fresh tomatoes, chicken or vegetable broth, and a sprig of fresh basil to the pot. Simmer the soup for about 20-30 minutes, or until the tomatoes are tender and falling apart. Remove the basil sprig and use an immersion blender to puree the soup until smooth. Season with salt and pepper to taste before serving.",
        "To bake a decadent chocolate cake from scratch, start by preheating your oven to 350°F (175°C) and greasing and flouring two 9-inch round cake pans. In a large mixing bowl, cream together softened butter and granulated sugar until light and fluffy. Beat in eggs one at a time, making sure each egg is fully incorporated before adding the next. In a separate bowl, sift together all-purpose flour, cocoa powder, baking powder, baking soda, and salt. Divide the batter evenly between the prepared cake pans and bake for 25-30 minutes, or until a toothpick inserted into the center comes out clean.",
    ],
    "instruction": ["Summarize the following article"] * 5,
    "reference": [
        "The process of making spaghetti carbonara involves boiling pasta, crisping pancetta or guanciale, whisking together eggs and Parmesan cheese, and tossing everything together to create a creamy sauce.",
        "Preparing risotto entails sautéing onions and garlic, toasting Arborio rice, adding wine and broth gradually, and stirring until creamy and tender.",
        "Grilling a flavorful steak involves seasoning generously, preheating the grill, cooking to desired doneness, and letting it rest before slicing.",
        "Creating homemade tomato soup includes sautéing onions and garlic, simmering with tomatoes and broth, pureeing until smooth, and seasoning to taste.",
        "Baking a decadent chocolate cake requires creaming butter and sugar, beating in eggs and alternating dry ingredients with buttermilk before baking until done.",
    ],
}
TEST_METRICS = [
    MetricPromptTemplateExamples.Pointwise.SUMMARIZATION_QUALITY,
    MetricPromptTemplateExamples.Pointwise.GROUNDEDNESS,
    MetricPromptTemplateExamples.Pointwise.VERBOSITY,
    MetricPromptTemplateExamples.Pointwise.INSTRUCTION_FOLLOWING,
    "exact_match",
    "bleu",
    "rouge_1",
    "rouge_2",
    "rouge_l_sum",
]
TEST_EXPERIMENT_NAME = "eval-experiment-airflow-operator"
TEST_EXPERIMENT_RUN_NAME = "eval-experiment-airflow-operator-run"
TEST_PROMPT_TEMPLATE = "{instruction}. Article: {context}. Summary:"

TEST_CACHED_CONTENT_NAME = "test-example-cache"
TEST_CACHED_CONTENT_PROMPT = ["What are these papers about?"]
TEST_CACHED_MODEL = "gemini-1.5-pro-002"
TEST_CACHED_SYSTEM_INSTRUCTION = """
You are an expert researcher. You always stick to the facts in the sources provided, and never make up new facts.
Now look at these research papers, and answer the following questions.
"""

TEST_CACHED_CONTENTS = [
    Part.from_uri(
        "gs://cloud-samples-data/generative-ai/pdf/2312.11805v3.pdf",
        mime_type="application/pdf",
    ),
    Part.from_uri(
        "gs://cloud-samples-data/generative-ai/pdf/2403.05530.pdf",
        mime_type="application/pdf",
    ),
]
TEST_CACHED_TTL = 1
TEST_CACHED_DISPLAY_NAME = "test-example-cache"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
GENERATIVE_MODEL_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.generative_model.{}"


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class TestGenerativeModelWithDefaultProjectIdHook:
    def dummy_get_credentials(self):
        pass

    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = GenerativeModelHook(gcp_conn_id=TEST_GCP_CONN_ID)
            self.hook.get_credentials = self.dummy_get_credentials

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_text_generation_model"))
    def test_prompt_language_model(self, mock_model) -> None:
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            self.hook.prompt_language_model(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=TEST_PROMPT,
                pretrained_model=TEST_LANGUAGE_PRETRAINED_MODEL,
                temperature=TEST_TEMPERATURE,
                max_output_tokens=TEST_MAX_OUTPUT_TOKENS,
                top_p=TEST_TOP_P,
                top_k=TEST_TOP_K,
            )
            assert_warning("text_generation_model_predict", warnings)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_text_embedding_model"))
    def test_generate_text_embeddings(self, mock_model) -> None:
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            self.hook.generate_text_embeddings(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=TEST_PROMPT,
                pretrained_model=TEST_TEXT_EMBEDDING_MODEL,
            )
            assert_warning("text_embedding_model_get_embeddings", warnings)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_generative_model"))
    def test_prompt_multimodal_model(self, mock_model) -> None:
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            self.hook.prompt_multimodal_model(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=TEST_PROMPT,
                generation_config=TEST_GENERATION_CONFIG,
                safety_settings=TEST_SAFETY_SETTINGS,
                pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL,
            )
            assert_warning("generative_model_generate_content", warnings)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_generative_model_part"))
    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_generative_model"))
    def test_prompt_multimodal_model_with_media(self, mock_model, mock_part) -> None:
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            self.hook.prompt_multimodal_model_with_media(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=TEST_VISION_PROMPT,
                generation_config=TEST_GENERATION_CONFIG,
                safety_settings=TEST_SAFETY_SETTINGS,
                pretrained_model=TEST_MULTIMODAL_VISION_MODEL,
                media_gcs_path=TEST_MEDIA_GCS_PATH,
                mime_type=TEST_MIME_TYPE,
            )
            assert_warning("generative_model_generate_content", warnings)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_text_generation_model"))
    def test_text_generation_model_predict(self, mock_model) -> None:
        self.hook.text_generation_model_predict(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=TEST_PROMPT,
            pretrained_model=TEST_LANGUAGE_PRETRAINED_MODEL,
            temperature=TEST_TEMPERATURE,
            max_output_tokens=TEST_MAX_OUTPUT_TOKENS,
            top_p=TEST_TOP_P,
            top_k=TEST_TOP_K,
        )
        mock_model.assert_called_once_with(TEST_LANGUAGE_PRETRAINED_MODEL)
        mock_model.return_value.predict.assert_called_once_with(
            prompt=TEST_PROMPT,
            temperature=TEST_TEMPERATURE,
            max_output_tokens=TEST_MAX_OUTPUT_TOKENS,
            top_p=TEST_TOP_P,
            top_k=TEST_TOP_K,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_text_embedding_model"))
    def test_text_embedding_model_get_embeddings(self, mock_model) -> None:
        self.hook.text_embedding_model_get_embeddings(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=TEST_PROMPT,
            pretrained_model=TEST_TEXT_EMBEDDING_MODEL,
        )
        mock_model.assert_called_once_with(TEST_TEXT_EMBEDDING_MODEL)
        mock_model.return_value.get_embeddings.assert_called_once_with([TEST_PROMPT])

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_generative_model"))
    def test_generative_model_generate_content(self, mock_model) -> None:
        self.hook.generative_model_generate_content(
            project_id=GCP_PROJECT,
            contents=TEST_CONTENTS,
            location=GCP_LOCATION,
            tools=TEST_TOOLS,
            generation_config=TEST_GENERATION_CONFIG,
            safety_settings=TEST_SAFETY_SETTINGS,
            pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL,
        )
        mock_model.assert_called_once_with(
            pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL,
            system_instruction=None,
        )
        mock_model.return_value.generate_content.assert_called_once_with(
            contents=TEST_CONTENTS,
            tools=TEST_TOOLS,
            generation_config=TEST_GENERATION_CONFIG,
            safety_settings=TEST_SAFETY_SETTINGS,
        )

    @mock.patch("vertexai.preview.tuning.sft.train")
    def test_supervised_fine_tuning_train(self, mock_sft_train) -> None:
        self.hook.supervised_fine_tuning_train(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=SOURCE_MODEL,
            train_dataset=TRAIN_DATASET,
        )

        mock_sft_train.assert_called_once_with(
            source_model=SOURCE_MODEL,
            train_dataset=TRAIN_DATASET,
            validation_dataset=None,
            epochs=None,
            adapter_size=None,
            learning_rate_multiplier=None,
            tuned_model_display_name=None,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_generative_model"))
    def test_count_tokens(self, mock_model) -> None:
        self.hook.count_tokens(
            project_id=GCP_PROJECT,
            contents=TEST_CONTENTS,
            location=GCP_LOCATION,
            pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL,
        )
        mock_model.assert_called_once_with(
            pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL,
        )
        mock_model.return_value.count_tokens.assert_called_once_with(
            contents=TEST_CONTENTS,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_generative_model"))
    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_eval_task"))
    def test_run_evaluation(self, mock_eval_task, mock_model) -> None:
        self.hook.run_evaluation(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL,
            eval_dataset=TEST_EVAL_DATASET,
            metrics=TEST_METRICS,
            experiment_name=TEST_EXPERIMENT_NAME,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
            prompt_template=TEST_PROMPT_TEMPLATE,
        )

        mock_model.assert_called_once_with(
            pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL,
            system_instruction=None,
            generation_config=None,
            safety_settings=None,
            tools=None,
        )
        mock_eval_task.assert_called_once_with(
            dataset=TEST_EVAL_DATASET,
            metrics=TEST_METRICS,
            experiment=TEST_EXPERIMENT_NAME,
        )
        mock_eval_task.return_value.evaluate.assert_called_once_with(
            model=mock_model.return_value,
            prompt_template=TEST_PROMPT_TEMPLATE,
            experiment_run_name=TEST_EXPERIMENT_RUN_NAME,
        )

    @mock.patch("vertexai.preview.caching.CachedContent.create")
    def test_create_cached_content(self, mock_cached_content_create) -> None:
        self.hook.create_cached_content(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model_name=TEST_CACHED_MODEL,
            system_instruction=TEST_CACHED_SYSTEM_INSTRUCTION,
            contents=TEST_CACHED_CONTENTS,
            ttl_hours=TEST_CACHED_TTL,
            display_name=TEST_CACHED_DISPLAY_NAME,
        )

        mock_cached_content_create.assert_called_once_with(
            model_name=TEST_CACHED_MODEL,
            system_instruction=TEST_CACHED_SYSTEM_INSTRUCTION,
            contents=TEST_CACHED_CONTENTS,
            ttl=timedelta(hours=TEST_CACHED_TTL),
            display_name=TEST_CACHED_DISPLAY_NAME,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenerativeModelHook.get_cached_context_model"))
    def test_generate_from_cached_content(self, mock_cached_context_model) -> None:
        self.hook.generate_from_cached_content(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cached_content_name=TEST_CACHED_CONTENT_NAME,
            contents=TEST_CACHED_CONTENT_PROMPT,
        )

        mock_cached_context_model.return_value.generate_content.assert_called_once_with(
            contents=TEST_CACHED_CONTENT_PROMPT,
            generation_config=None,
            safety_settings=None,
        )
