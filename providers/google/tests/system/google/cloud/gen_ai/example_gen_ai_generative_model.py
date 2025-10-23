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
Example Airflow DAG for Google Gen AI Generative Model prompting.
"""

from __future__ import annotations

import os
from datetime import datetime

import requests
from vertexai.preview.evaluation import MetricPromptTemplateExamples

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from google.genai.types import (
    Content,
    CreateCachedContentConfig,
    GenerateContentConfig,
    GoogleSearch,
    Part,
    Tool,
)

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gen_ai import (
    GenAICountTokensOperator,
    GenAICreateCachedContentOperator,
    GenAIGenerateContentOperator,
    GenAIGenerateEmbeddingsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.experiment_service import (
    CreateExperimentOperator,
    DeleteExperimentOperator,
    DeleteExperimentRunOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import RunEvaluationOperator
from airflow.providers.google.common.utils.get_secret import get_secret


def _get_actual_models(key) -> dict[str, str]:
    models: dict[str, str] = {
        "multimodal": "",
        "text-embedding": "",
        "cached-model": "",
    }
    try:
        response = requests.get(
            "https://generativelanguage.googleapis.com/v1/models",
            {"key": key},
            timeout=10,
        )
        response.raise_for_status()
        available_models = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching models from API: {e}")
        return models

    for model in available_models.get("models", []):
        try:
            model_name = model["name"].split("/")[-1]
            splited_model_name = model_name.split("-")
            if not models["text-embedding"] and ("text" in model_name and "embedding" in model_name):
                models["text-embedding"] = model_name
            elif (
                models["text-embedding"]
                and ("text" in model_name and "embedding" in model_name)
                and int(splited_model_name[-1]) > int(models["text-embedding"].split("-")[-1])
            ):
                models["text-embedding"] = model_name
            elif ("vision" not in model_name or "image" in model_name) and (
                "flash" in model_name or "pro" in model_name
            ):
                if not models["multimodal"] and "pro" in model_name:
                    models["multimodal"] = model_name
                elif (
                    models["multimodal"]
                    and "pro" in model_name
                    and float(models["multimodal"].split("-")[1]) < float(splited_model_name[1])
                ):
                    models["multimodal"] = model_name
                elif (
                    models["multimodal"]
                    and "pro" in model_name
                    and (
                        float(models["multimodal"].split("-")[1]) == float(splited_model_name[1])
                        and int(splited_model_name[-1]) > int(models["multimodal"].split("-")[-1])
                    )
                ):
                    models["multimodal"] = model_name
                if "createCachedContent" in model["supportedGenerationMethods"]:
                    if not models["cached-model"]:
                        models["cached-model"] = model_name
                    elif models["cached-model"] and float(models["cached-model"].split("-")[1]) < float(
                        splited_model_name[1]
                    ):
                        models["cached-model"] = model_name
                    elif (
                        models["cached-model"]
                        and float(models["cached-model"].split("-")[1]) == float(splited_model_name[1])
                        and int(splited_model_name[-1]) > int(models["cached-model"].split("-")[-1])
                    ):
                        models["cached-model"] = model_name
        except (ValueError, IndexError) as e:
            print(f"Could not parse model name '{model.get('name')}'. Skipping. Error: {e}")
            continue
    if not any(models.values()):
        raise ValueError(f"Some of the models not found {models}")
    return models


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
GEMINI_API_KEY = "api_key"
MODELS = "{{ task_instance.xcom_pull('get_actual_models') }}"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "gen_ai_generative_model_dag"
REGION = "us-central1"
PROMPT = "In 10 words or less, why is Apache Airflow amazing?"
CONTENTS = [PROMPT]
TEXT_EMBEDDING_MODEL = "{{ task_instance.xcom_pull('get_actual_models')['text-embedding'] }}"
MULTIMODAL_MODEL = "{{ task_instance.xcom_pull('get_actual_models')['multimodal'] }}"
MEDIA_GCS_PATH = "gs://download.tensorflow.org/example_images/320px-Felis_catus-cat_on_snow.jpg"
MIME_TYPE = "image/jpeg"
TOOLS = [Tool(google_search=GoogleSearch())]
GENERATION_CONFIG_CREATE_CONTENT = GenerateContentConfig(
    max_output_tokens=256,
    top_p=0.95,
    temperature=0.0,
    tools=TOOLS,  # type: ignore[union-attr,arg-type]
)

EVAL_DATASET = {
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
METRICS = [
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
EXPERIMENT_NAME = f"eval-test-experiment-airflow-operator-{ENV_ID}".replace("_", "-")
EXPERIMENT_RUN_NAME = f"eval-experiment-airflow-operator-run-{ENV_ID}".replace("_", "-")
PROMPT_TEMPLATE = "{instruction}. Article: {context}. Summary:"

CACHED_MODEL = "{{ task_instance.xcom_pull('get_actual_models')['cached-model'] }}"
CACHED_SYSTEM_INSTRUCTION = """
You are an expert researcher. You always stick to the facts in the sources provided, and never make up new facts.
Now look at these research papers, and answer the following questions.
"""
CACHED_CONTENT_CONFIG = CreateCachedContentConfig(
    contents=[
        Content(
            role="user",
            parts=[
                Part.from_uri(
                    file_uri="gs://cloud-samples-data/generative-ai/pdf/2312.11805v3.pdf",
                    mime_type="application/pdf",
                ),
                Part.from_uri(
                    file_uri="gs://cloud-samples-data/generative-ai/pdf/2403.05530.pdf",
                    mime_type="application/pdf",
                ),
            ],
        )
    ],
    system_instruction=CACHED_SYSTEM_INSTRUCTION,
    display_name="test-cache",
    ttl="3600s",
)

with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with generative models.",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "gen_ai", "generative_model"],
    render_template_as_native_obj=True,
) as dag:

    @task
    def get_gemini_api_key():
        return get_secret(GEMINI_API_KEY)

    get_gemini_api_key_task = get_gemini_api_key()

    @task
    def get_actual_models(key):
        return _get_actual_models(key)

    get_actual_models_task = get_actual_models(get_gemini_api_key_task)

    # [START how_to_cloud_gen_ai_generate_embeddings_task]
    generate_embeddings_task = GenAIGenerateEmbeddingsOperator(
        task_id="generate_embeddings_task",
        project_id=PROJECT_ID,
        location=REGION,
        contents=CONTENTS,
        model=TEXT_EMBEDDING_MODEL,
    )
    # [END how_to_cloud_gen_ai_generate_embeddings_task]

    # [START how_to_cloud_gen_ai_count_tokens_operator]
    count_tokens_task = GenAICountTokensOperator(
        task_id="count_tokens_task",
        project_id=PROJECT_ID,
        contents=CONTENTS,
        location=REGION,
        model=MULTIMODAL_MODEL,
    )
    # [END how_to_cloud_gen_ai_count_tokens_operator]

    # [START how_to_cloud_gen_ai_generate_content_operator]
    generate_content_task = GenAIGenerateContentOperator(
        task_id="generate_content_task",
        project_id=PROJECT_ID,
        contents=CONTENTS,
        location=REGION,
        generation_config=GENERATION_CONFIG_CREATE_CONTENT,
        model=MULTIMODAL_MODEL,
    )
    # [END how_to_cloud_gen_ai_generate_content_operator]

    create_experiment_task = CreateExperimentOperator(
        task_id="create_experiment_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
    )

    delete_experiment_task = DeleteExperimentOperator(
        task_id="delete_experiment_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
    )

    # [START how_to_cloud_vertex_ai_run_evaluation_operator]
    run_evaluation_task = RunEvaluationOperator(
        task_id="run_evaluation_task",
        project_id=PROJECT_ID,
        location=REGION,
        pretrained_model=MULTIMODAL_MODEL,
        eval_dataset=EVAL_DATASET,
        metrics=METRICS,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME,
        prompt_template=PROMPT_TEMPLATE,
    )
    # [END how_to_cloud_vertex_ai_run_evaluation_operator]

    delete_experiment_run_task = DeleteExperimentRunOperator(
        task_id="delete_experiment_run_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME,
    )

    # [START how_to_cloud_gen_ai_create_cached_content_operator]
    create_cached_content_task = GenAICreateCachedContentOperator(
        task_id="create_cached_content_task",
        project_id=PROJECT_ID,
        location=REGION,
        model=CACHED_MODEL,
        cached_content_config=CACHED_CONTENT_CONFIG,
    )
    # [END how_to_cloud_gen_ai_create_cached_content_operator]

    # [START how_to_cloud_gen_ai_generate_from_cached_content_operator]
    generate_from_cached_content_task = GenAIGenerateContentOperator(
        task_id="generate_from_cached_content_task",
        project_id=PROJECT_ID,
        location=REGION,
        contents=["What are the papers about?"],
        generation_config={
            "cached_content": create_cached_content_task.output,
        },
        model=CACHED_MODEL,
    )
    # [END how_to_cloud_gen_ai_generate_from_cached_content_operator]
    get_gemini_api_key_task >> get_actual_models_task
    get_actual_models_task >> [generate_embeddings_task, count_tokens_task, generate_content_task]
    get_actual_models_task >> create_cached_content_task >> generate_from_cached_content_task
    (
        get_actual_models_task
        >> create_experiment_task
        >> run_evaluation_task
        >> delete_experiment_run_task
        >> delete_experiment_task
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
