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

import numpy as np
import pytest
from qdrant_client import models

from airflow.providers.qdrant.hooks.qdrant import QdrantHook


@pytest.mark.integration("qdrant")
class TestQdrant:
    def setup_method(self):
        self.test_collection_name = "test-hook-collection"
        self.test_collection_dimension = random.randint(100, 2000)
        self.hook = QdrantHook()

        self.hook.conn.recreate_collection(
            self.test_collection_name,
            vectors_config=models.VectorParams(
                size=self.test_collection_dimension, distance=models.Distance.MANHATTAN
            ),
        )

    def test_connection(self):
        response, message = self.hook.verify_connection()
        assert response
        assert message == "Connection established!", "Successfully connected to Qdrant."

    def test_upsert_points(self):
        vectors = np.random.rand(100, self.test_collection_dimension)
        self.hook.conn.upsert(
            self.test_collection_name,
            points=[
                models.PointStruct(
                    id=idx, vector=vector.tolist(), payload={"color": "red", "rand_number": idx % 10}
                )
                for idx, vector in enumerate(vectors)
            ],
        )

        assert self.hook.conn.count(self.test_collection_name).count == 100

    def test_delete_points(self):
        self.hook.conn.delete(
            self.test_collection_name,
            points_selector=models.Filter(
                must=[models.FieldCondition(key="color", match=models.MatchValue(value="red"))]
            ),
        )

        assert self.hook.conn.count(self.test_collection_name).count == 0
