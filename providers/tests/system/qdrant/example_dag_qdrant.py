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
from airflow.providers.qdrant.operators.qdrant import QdrantIngestOperator

with DAG(
    "example_qdrant_ingest",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    # [START howto_operator_qdrant_ingest]
    vectors = [
        [0.732, 0.611, 0.289, 0.421],
        [0.217, 0.526, 0.416, 0.981],
        [0.326, 0.483, 0.376, 0.136],
    ]
    ids: list[str | int] = [32, 21, "b626f6a9-b14d-4af9-b7c3-43d8deb719a6"]
    payload = [{"meta": "data"}, {"meta": "data_2"}, {"meta": "data_3", "extra": "data"}]

    QdrantIngestOperator(
        task_id="qdrant_ingest",
        collection_name="test_collection",
        vectors=vectors,
        ids=ids,
        payload=payload,
        batch_size=1,
    )
    # [END howto_operator_qdrant_ingest]


from tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
