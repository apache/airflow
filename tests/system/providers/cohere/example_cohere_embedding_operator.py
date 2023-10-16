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

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.cohere.operators.embedding import CohereEmbeddingOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_cohere_embedding"
CONN_ID = "cohere_default"

# [START howto_cohere_operator]
with DAG(DAG_ID, schedule=None, start_date=datetime(2023, 1, 1), catchup=False) as dag:
    texts = [
        "On Kernel-Target Alignment. We describe a family of global optimization procedures",
        " that automatically decompose optimization problems into smaller loosely coupled",
        " problems, then combine the solutions of these with message passing algorithms.",
    ]

    def get_text():
        return texts

    CohereEmbeddingOperator(input_text=texts, task_id="embedding_via_text", conn_id=CONN_ID)
    CohereEmbeddingOperator(input_callable=get_text, task_id="embedding_via_callable", conn_id=CONN_ID)
# [START howto_cohere_operator]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
