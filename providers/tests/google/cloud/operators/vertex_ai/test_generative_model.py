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
pytest.importorskip("google.cloud.aiplatform_v1beta1")
vertexai = pytest.importorskip("vertexai.generative_models")
from vertexai.generative_models import (
    HarmBlockThreshold,
    HarmCategory,
    Part,
    Tool,
    grounding,
)
from vertexai.preview.evaluation import MetricPromptTemplateExamples

from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    CountTokensOperator,
    CreateCachedContentOperator,
    GenerateFromCachedContentOperator,
    GenerateTextEmbeddingsOperator,
    GenerativeModelGenerateContentOperator,
    PromptLanguageModelOperator,
    PromptMultimodalModelOperator,
    PromptMultimodalModelWithMediaOperator,
    RunEvaluationOperator,
    SupervisedFineTuningTrainOperator,
    TextEmbeddingModelGetEmbeddingsOperator,
    TextGenerationModelPredictOperator,
)

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class TestVertexAIPromptLanguageModelOperator:
    prompt = "In 10 words or less, what is Apache Airflow?"
    pretrained_model = "text-bison"
    temperature = 0.0
    max_output_tokens = 256
    top_p = 0.8
    top_k = 40

    def test_deprecation_warning(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            PromptLanguageModelOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=self.prompt,
                pretrained_model=self.pretrained_model,
                temperature=self.temperature,
                max_output_tokens=self.max_output_tokens,
                top_p=self.top_p,
                top_k=self.top_k,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            assert_warning("TextGenerationModelPredictOperator", warnings)

    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = PromptLanguageModelOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=self.prompt,
                pretrained_model=self.pretrained_model,
                temperature=self.temperature,
                max_output_tokens=self.max_output_tokens,
                top_p=self.top_p,
                top_k=self.top_k,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.prompt_language_model.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=self.prompt,
            pretrained_model=self.pretrained_model,
            temperature=self.temperature,
            max_output_tokens=self.max_output_tokens,
            top_p=self.top_p,
            top_k=self.top_k,
        )


class TestVertexAIGenerateTextEmbeddingsOperator:
    prompt = "In 10 words or less, what is Apache Airflow?"
    pretrained_model = "textembedding-gecko"

    def test_deprecation_warning(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            GenerateTextEmbeddingsOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=self.prompt,
                pretrained_model=self.pretrained_model,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            assert_warning("TextEmbeddingModelGetEmbeddingsOperator", warnings)

    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = GenerateTextEmbeddingsOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=self.prompt,
                pretrained_model=self.pretrained_model,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.generate_text_embeddings.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=self.prompt,
            pretrained_model=self.pretrained_model,
        )


class TestVertexAIPromptMultimodalModelOperator:
    prompt = "In 10 words or less, what is Apache Airflow?"
    pretrained_model = "gemini-pro"
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    }
    generation_config = {"max_output_tokens": 256, "top_p": 0.8, "temperature": 0.0}

    def test_deprecation_warning(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            PromptMultimodalModelOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=self.prompt,
                generation_config=self.generation_config,
                safety_settings=self.safety_settings,
                pretrained_model=self.pretrained_model,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            assert_warning("GenerativeModelGenerateContentOperator", warnings)

    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = PromptMultimodalModelOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=self.prompt,
                generation_config=self.generation_config,
                safety_settings=self.safety_settings,
                pretrained_model=self.pretrained_model,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.prompt_multimodal_model.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=self.prompt,
            generation_config=self.generation_config,
            safety_settings=self.safety_settings,
            pretrained_model=self.pretrained_model,
        )


class TestVertexAIPromptMultimodalModelWithMediaOperator:
    pretrained_model = "gemini-pro-vision"
    vision_prompt = "In 10 words or less, describe this content."
    media_gcs_path = (
        "gs://download.tensorflow.org/example_images/320px-Felis_catus-cat_on_snow.jpg"
    )
    mime_type = "image/jpeg"
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    }
    generation_config = {"max_output_tokens": 256, "top_p": 0.8, "temperature": 0.0}

    def test_deprecation_warning(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as warnings:
            PromptMultimodalModelWithMediaOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=self.vision_prompt,
                generation_config=self.generation_config,
                safety_settings=self.safety_settings,
                pretrained_model=self.pretrained_model,
                media_gcs_path=self.media_gcs_path,
                mime_type=self.mime_type,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
            assert_warning("GenerativeModelGenerateContentOperator", warnings)

    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = PromptMultimodalModelWithMediaOperator(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                prompt=self.vision_prompt,
                generation_config=self.generation_config,
                safety_settings=self.safety_settings,
                pretrained_model=self.pretrained_model,
                media_gcs_path=self.media_gcs_path,
                mime_type=self.mime_type,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.prompt_multimodal_model_with_media.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=self.vision_prompt,
            generation_config=self.generation_config,
            safety_settings=self.safety_settings,
            pretrained_model=self.pretrained_model,
            media_gcs_path=self.media_gcs_path,
            mime_type=self.mime_type,
        )


class TestVertexAITextGenerationModelPredictOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        prompt = "In 10 words or less, what is Apache Airflow?"
        pretrained_model = "text-bison"
        temperature = 0.0
        max_output_tokens = 256
        top_p = 0.8
        top_k = 40

        op = TextGenerationModelPredictOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            top_p=top_p,
            top_k=top_k,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.text_generation_model_predict.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            top_p=top_p,
            top_k=top_k,
        )


class TestVertexAITextEmbeddingModelGetEmbeddingsOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        prompt = "In 10 words or less, what is Apache Airflow?"
        pretrained_model = "textembedding-gecko"

        op = TextEmbeddingModelGetEmbeddingsOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.text_embedding_model_get_embeddings.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
        )


class TestVertexAIGenerativeModelGenerateContentOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        contents = ["In 10 words or less, what is Apache Airflow?"]
        tools = [Tool.from_google_search_retrieval(grounding.GoogleSearchRetrieval())]
        pretrained_model = "gemini-pro"
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        }
        generation_config = {"max_output_tokens": 256, "top_p": 0.8, "temperature": 0.0}
        system_instruction = "be concise."

        op = GenerativeModelGenerateContentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=contents,
            tools=tools,
            generation_config=generation_config,
            safety_settings=safety_settings,
            pretrained_model=pretrained_model,
            system_instruction=system_instruction,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.generative_model_generate_content.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=contents,
            tools=tools,
            generation_config=generation_config,
            safety_settings=safety_settings,
            pretrained_model=pretrained_model,
            system_instruction=system_instruction,
        )


class TestVertexAISupervisedFineTuningTrainOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    @mock.patch("google.cloud.aiplatform_v1.types.TuningJob.to_dict")
    def test_execute(
        self,
        to_dict_mock,
        mock_hook,
    ):
        source_model = "gemini-1.0-pro-002"
        train_dataset = (
            "gs://cloud-samples-data/ai-platform/generative_ai/sft_train_data.jsonl"
        )

        op = SupervisedFineTuningTrainOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=source_model,
            train_dataset=train_dataset,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.supervised_fine_tuning_train.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=source_model,
            train_dataset=train_dataset,
            adapter_size=None,
            epochs=None,
            learning_rate_multiplier=None,
            tuned_model_display_name=None,
            validation_dataset=None,
        )


class TestVertexAICountTokensOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    @mock.patch("google.cloud.aiplatform_v1beta1.types.CountTokensResponse.to_dict")
    def test_execute(self, to_dict_mock, mock_hook):
        contents = ["In 10 words or less, what is Apache Airflow?"]
        pretrained_model = "gemini-pro"

        op = CountTokensOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=contents,
            pretrained_model=pretrained_model,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.count_tokens.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=contents,
            pretrained_model=pretrained_model,
        )


class TestVertexAIRunEvaluationOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(
        self,
        mock_hook,
    ):
        tools = [Tool.from_google_search_retrieval(grounding.GoogleSearchRetrieval())]
        pretrained_model = "gemini-pro"
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        }
        generation_config = {"max_output_tokens": 256, "top_p": 0.8, "temperature": 0.0}

        eval_dataset = {
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
        metrics = [
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
        experiment_name = "eval-experiment-airflow-operator"
        experiment_run_name = "eval-experiment-airflow-operator-run"
        prompt_template = "{instruction}. Article: {context}. Summary:"
        system_instruction = "be concise."

        op = RunEvaluationOperator(
            task_id=TASK_ID,
            pretrained_model=pretrained_model,
            eval_dataset=eval_dataset,
            metrics=metrics,
            experiment_name=experiment_name,
            experiment_run_name=experiment_run_name,
            prompt_template=prompt_template,
            system_instruction=system_instruction,
            generation_config=generation_config,
            safety_settings=safety_settings,
            tools=tools,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.run_evaluation.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            pretrained_model=pretrained_model,
            eval_dataset=eval_dataset,
            metrics=metrics,
            experiment_name=experiment_name,
            experiment_run_name=experiment_run_name,
            prompt_template=prompt_template,
            system_instruction=system_instruction,
            generation_config=generation_config,
            safety_settings=safety_settings,
            tools=tools,
        )


class TestVertexAICreateCachedContentOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        model_name = "gemini-1.5-pro-002"
        system_instruction = """
        You are an expert researcher. You always stick to the facts in the sources provided, and never make up new facts.
        Now look at these research papers, and answer the following questions.
        """

        contents = [
            Part.from_uri(
                "gs://cloud-samples-data/generative-ai/pdf/2312.11805v3.pdf",
                mime_type="application/pdf",
            ),
            Part.from_uri(
                "gs://cloud-samples-data/generative-ai/pdf/2403.05530.pdf",
                mime_type="application/pdf",
            ),
        ]
        ttl_hours = 1
        display_name = "test-example-cache"

        op = CreateCachedContentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model_name=model_name,
            system_instruction=system_instruction,
            contents=contents,
            ttl_hours=ttl_hours,
            display_name=display_name,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_cached_content.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model_name=model_name,
            system_instruction=system_instruction,
            contents=contents,
            ttl_hours=ttl_hours,
            display_name=display_name,
        )


class TestVertexAIGenerateFromCachedContentOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        cached_content_name = "test"
        contents = ["what are in these papers"]

        op = GenerateFromCachedContentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cached_content_name=cached_content_name,
            contents=contents,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.generate_from_cached_content.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cached_content_name=cached_content_name,
            contents=contents,
            generation_config=None,
            safety_settings=None,
        )
