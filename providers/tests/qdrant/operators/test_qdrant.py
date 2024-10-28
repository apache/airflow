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

from unittest.mock import patch

import pytest

qdrant_client = pytest.importorskip("qdrant_client")


from airflow.providers.qdrant.operators.qdrant import QdrantIngestOperator


class TestQdrantIngestOperator:
    @pytest.mark.db_test
    def test_operator_execution(self, dag_maker):
        """
        Test the execution of the QdrantIngestOperator.
        Ensures that the upsert method on the hook is correctly called.
        """
        with dag_maker(dag_id="test_dag") as dummy_dag:
            vectors = [
                [0.732, 0.611, 0.289],
                [0.217, 0.526, 0.416],
                [0.326, 0.483, 0.376],
            ]
            ids = [32, 21, "b626f6a9-b14d-4af9-b7c3-43d8deb719a6"]
            payload = [
                {"meta": "data"},
                {"meta": "data_2"},
                {"meta": "data_3", "extra": "data"},
            ]

            task = QdrantIngestOperator(
                task_id="ingest_vectors",
                collection_name="test_collection",
                vectors=vectors,
                ids=ids,
                payload=payload,
                wait=False,
                max_retries=1,
                parallel=3,
                dag=dummy_dag,
            )

        with patch(
            "airflow.providers.qdrant.operators.qdrant.QdrantIngestOperator.hook"
        ) as mock_hook_instance:
            task.execute(context={})
            mock_hook_instance.conn.upload_collection.assert_called_once_with(
                collection_name="test_collection",
                vectors=vectors,
                ids=ids,
                payload=payload,
                batch_size=64,
                wait=False,
                max_retries=1,
                parallel=3,
                method=None,
            )
