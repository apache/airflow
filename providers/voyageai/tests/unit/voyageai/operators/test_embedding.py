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

# Unit tests for the VoyageEmbeddingOperator.
from __future__ import annotations

import unittest
from unittest import mock

from airflow.providers.voyageai.operators.embedding import VoyageEmbeddingOperator


class TestVoyageEmbeddingOperator(unittest.TestCase):
    """
    Unit tests for the VoyageEmbeddingOperator.

    This test class verifies the behavior of the VoyageEmbeddingOperator,
    ensuring it correctly interacts with the VoyageAIHook to generate embeddings.
    """

    @mock.patch("airflow.providers.voyageai.operators.embedding.VoyageAIHook")
    def test_execute(self, mock_voyage_hook):
        """
        Test the execute method of the VoyageEmbeddingOperator.

        This test mocks the VoyageAIHook to isolate the operator's logic,
        verifying that the operator calls the hook's embed method with the correct parameters
        and returns the expected embeddings.

        Assertions:
            - The hook is instantiated with the correct connection ID.
            - The embed method is called with the correct texts and model.
            - The execute method returns the embeddings provided by the hook.
        """
        # 1. Set up test data and the mock hook's return value.
        conn_id = "voyage_test_conn"
        input_texts = ["hello world", "this is a test"]
        model = "voyage-2"
        mock_embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]

        # Configure the mock hook instance to return our mock data when `embed` is called
        mock_hook_instance = mock_voyage_hook.return_value
        mock_hook_instance.embed.return_value = mock_embeddings

        # 2. Instantiate the operator.
        operator = VoyageEmbeddingOperator(
            task_id="test_voyage_embedding",
            conn_id=conn_id,
            input_texts=input_texts,
            model=model,
        )

        # 3. Call operator.execute().
        # The 'context' argument is required by the execute method but not used in our operator.
        result = operator.execute(context={})

        # 4. Assert that the hook was instantiated correctly.
        mock_voyage_hook.assert_called_once_with(conn_id=conn_id)

        # 5. Assert that the hook's embed method was called with the right parameters.
        mock_hook_instance.embed.assert_called_once_with(
            texts=input_texts,
            model=model,
        )

        # 6. Assert that the operator returns the expected value from the hook.
        self.assertEqual(result, mock_embeddings)
