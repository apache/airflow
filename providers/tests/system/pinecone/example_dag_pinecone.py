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
from airflow.providers.pinecone.operators.pinecone import PineconeIngestOperator

index_name = os.getenv("INDEX_NAME", "test")
namespace = os.getenv("NAMESPACE", "example-pinecone-namespace")


with DAG(
    "example_pinecone_ingest",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # [START howto_operator_pinecone_ingest]
    PineconeIngestOperator(
        task_id="pinecone_vector_ingest",
        index_name=index_name,
        input_vectors=[
            ("id1", [1.0, 2.0, 3.0], {"key": "value"}),
            ("id2", [1.0, 2.0, 3.0]),
        ],
        namespace=namespace,
        batch_size=1,
    )
    # [END howto_operator_pinecone_ingest]


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
