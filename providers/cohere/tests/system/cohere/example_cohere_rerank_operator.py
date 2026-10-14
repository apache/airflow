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

from datetime import datetime

from airflow import DAG
from airflow.providers.cohere.operators.rerank import CohereRerankOperator

with DAG("example_cohere_rerank", schedule=None, start_date=datetime(2023, 1, 1), catchup=False) as dag:
    # [START howto_operator_cohere_rerank]
    CohereRerankOperator(
        task_id="rerank_documents",
        query="What is the capital of the United States?",
        documents=[
            "Carson City is the capital city of Nevada.",
            "Washington, D.C. is the capital of the United States.",
            "The capital city of France is Paris.",
        ],
        top_n=2,
    )
    # [END howto_operator_cohere_rerank]


from tests_common.test_utils.system_tests import get_test_run

test_run = get_test_run(dag)
