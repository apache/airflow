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
from unittest.mock import Mock, patch

import pytest

from airflow.models import DAG
from airflow.providers.pinecone.operators.pinecone import PineconeIngestOperator


class MockPineconeHook:
    """Mocking PineconeHook to avoid actual external calls"""

    def create_index(self, *args, **kwargs):
        pass

    @staticmethod
    def upsert(*args, **kwargs):
        return Mock()


@pytest.fixture
def dummy_dag():
    """Fixture to provide a dummy Airflow DAG for testing."""
    return DAG(dag_id="test_dag", start_date=datetime(2023, 9, 29))


class TestPineconeVectorIngestOperator:
    def test_vector_ingest_operator_execution(self, dummy_dag):
        """
        Test the execution of the PineconeVectorIngestOperator.
        Ensures that the upsert method on the hook is correctly called.
        """
        test_vectors = [("id1", [1.0, 2.0, 3.0], {"meta": "data"})]

        task = PineconeIngestOperator(
            task_id="ingest_vectors",
            index_name="test_index",
            input_vectors=test_vectors,
            dag=dummy_dag,
        )

        with patch(
            "airflow.providers.pinecone.operators.pinecone.PineconeIngestOperator.hook",
            new_callable=MockPineconeHook,
        ) as mock_hook_instance:
            mock_hook_instance.upsert = Mock()

            task.execute(context={})
            mock_hook_instance.upsert.assert_called_once_with(
                index_name="test_index",
                vectors=test_vectors,
                namespace="",
                batch_size=None,
            )

    def test_vector_ingest_operator_with_extra_args(self, dummy_dag):
        """
        Test the execution of the PineconeVectorIngestOperator with additional parameters.
        """
        test_vectors = [("id1", [1.0, 2.0, 3.0], {"meta": "data"})]

        task = PineconeIngestOperator(
            task_id="ingest_vectors",
            index_name="test_index",
            input_vectors=test_vectors,
            namespace="test_namespace",
            batch_size=100,
            upsert_kwargs={"custom_param": "value"},
            dag=dummy_dag,
        )

        with patch(
            "airflow.providers.pinecone.operators.pinecone.PineconeIngestOperator.hook",
            new_callable=MockPineconeHook,
        ) as mock_hook_instance:
            mock_hook_instance.upsert = Mock()

            task.execute(context={})

            mock_hook_instance.upsert.assert_called_once_with(
                index_name="test_index",
                vectors=test_vectors,
                namespace="test_namespace",
                batch_size=100,
                custom_param="value",
            )
