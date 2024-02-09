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
Example Airflow DAG for Google Vertex AI Generative AI prompting.
"""
from __future__ import annotations

from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    LanguageModelGenerateTextOperator,
    MultimodalModelChatOperator,
)

with models.DAG(
    dag_id="example_vertex_ai_generative_model_dag",
    description="Sample DAG with generative models.",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "generativeai"],
) as dag:
    text_generation_model_task = LanguageModelGenerateTextOperator(
        task_id="text_generation_model_task",
        prompt="Give me a sample itinerary for a trip to New Zealand.",
        pretrained_model="text-bison",
    )

    generative_model_task = MultimodalModelChatOperator(
        task_id="generative_model_task",
        prompt="Give me a sample itinerary for a trip to Australia.",
        pretrained_model="gemini-pro",
    )


from tests.system.utils import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
