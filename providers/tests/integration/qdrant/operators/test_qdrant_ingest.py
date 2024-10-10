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

import random
from unittest.mock import MagicMock

import pytest
from qdrant_client.models import Distance, VectorParams

from airflow.models.dag import DAG
from airflow.providers.qdrant.operators.qdrant import QdrantIngestOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2024, 1, 1)


@pytest.mark.integration("qdrant")
class TestQdrantIngestOperator:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.dag = DAG("test_qdrant_dag_id", default_args=args)

        self.mock_context = MagicMock()
        self.channel = "test"

    def test_execute_hello(self):
        collection_name = "test-operator-collection"
        dimensions = 384
        points_count = 100
        vectors = [[random.uniform(0, 1) for _ in range(dimensions)] for _ in range(points_count)]
        ids = random.sample(range(100, 10000), points_count)
        payload = [{"some_number": i % 10} for i in range(points_count)]

        operator = QdrantIngestOperator(
            task_id="qdrant_ingest",
            conn_id="qdrant_default",
            collection_name=collection_name,
            vectors=vectors,
            ids=ids,
            payload=payload,
            batch_size=1,
        )

        hook = operator.hook

        hook.conn.create_collection(
            collection_name, vectors_config=VectorParams(size=dimensions, distance=Distance.COSINE)
        )

        operator.execute(self.mock_context)

        assert (
            hook.conn.count(collection_name=collection_name).count == points_count
        ), f"Added {points_count} points to the Qdrant collection"
