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

"""
Example Airflow DAG for Google Vertex AI Generative Model prompting.
"""

from __future__ import annotations

import os
from datetime import datetime

from vertexai.generative_models import HarmBlockThreshold, HarmCategory

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    GenerateTextEmbeddingsOperator,
    PromptLanguageModelOperator,
    PromptMultimodalModelOperator,
    PromptMultimodalModelWithMediaOperator,
)

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "example_vertex_ai_generative_model_dag"
REGION = "us-central1"
PROMPT = "In 10 words or less, why is Apache Airflow amazing?"
LANGUAGE_MODEL = "text-bison"
TEXT_EMBEDDING_MODEL = "textembedding-gecko"
MULTIMODAL_MODEL = "gemini-pro"
MULTIMODAL_VISION_MODEL = "gemini-pro-vision"
VISION_PROMPT = "In 10 words or less, describe this content."
MEDIA_GCS_PATH = "gs://download.tensorflow.org/example_images/320px-Felis_catus-cat_on_snow.jpg"
MIME_TYPE = "image/jpeg"
GENERATION_CONFIG = {"max_output_tokens": 256, "top_p": 0.95, "temperature": 0.0}
SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
}

with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with generative models.",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "generative_model"],
) as dag:
    # [START how_to_cloud_vertex_ai_prompt_language_model_operator]
    prompt_language_model_task = PromptLanguageModelOperator(
        task_id="prompt_language_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        prompt=PROMPT,
        pretrained_model=LANGUAGE_MODEL,
    )
    # [END how_to_cloud_vertex_ai_prompt_language_model_operator]

    # [START how_to_cloud_vertex_ai_generate_text_embeddings_operator]
    generate_text_embeddings_task = GenerateTextEmbeddingsOperator(
        task_id="generate_text_embeddings_task",
        project_id=PROJECT_ID,
        location=REGION,
        prompt=PROMPT,
        pretrained_model=TEXT_EMBEDDING_MODEL,
    )
    # [END how_to_cloud_vertex_ai_generate_text_embeddings_operator]

    # [START how_to_cloud_vertex_ai_prompt_multimodal_model_operator]
    prompt_multimodal_model_task = PromptMultimodalModelOperator(
        task_id="prompt_multimodal_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        prompt=PROMPT,
        generation_config=GENERATION_CONFIG,
        safety_settings=SAFETY_SETTINGS,
        pretrained_model=MULTIMODAL_MODEL,
    )
    # [END how_to_cloud_vertex_ai_prompt_multimodal_model_operator]

    # [START how_to_cloud_vertex_ai_prompt_multimodal_model_with_media_operator]
    prompt_multimodal_model_with_media_task = PromptMultimodalModelWithMediaOperator(
        task_id="prompt_multimodal_model_with_media_task",
        project_id=PROJECT_ID,
        location=REGION,
        prompt=VISION_PROMPT,
        generation_config=GENERATION_CONFIG,
        safety_settings=SAFETY_SETTINGS,
        pretrained_model=MULTIMODAL_VISION_MODEL,
        media_gcs_path=MEDIA_GCS_PATH,
        mime_type=MIME_TYPE,
    )
    # [END how_to_cloud_vertex_ai_prompt_multimodal_model_with_media_operator]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
