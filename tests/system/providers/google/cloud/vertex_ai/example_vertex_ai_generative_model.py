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

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    PromptLanguageModelOperator,
    PromptMultimodalModelOperator,
)

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "example_vertex_ai_generative_model_dag"
REGION = "us-central1"
PROMPT = "In 10 words or less, why is Apache Airflow amazing?"
LANGUAGE_MODEL = "text-bison"
MULTIMODAL_MODAL = "gemini-pro"

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

    # [START how_to_cloud_vertex_ai_prompt_multimodal_model_operator]
    prompt_multimodal_model_task = PromptMultimodalModelOperator(
        task_id="generative_model_task",
        project_id=PROJECT_ID,
        location=REGION,
        prompt=PROMPT,
        pretrained_model=MULTIMODAL_MODAL,
    )
    # [END how_to_cloud_vertex_ai_prompt_multimodal_model_operator]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
