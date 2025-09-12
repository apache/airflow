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

# Unit tests for the VoyageAIHook.
from __future__ import annotations

import unittest
from unittest import mock

from airflow.models.connection import Connection
from airflow.providers.voyageai.hooks.voyage import VoyageAIHook

AIRFLOW_CONNECTION_PATCH = "airflow.providers.voyageai.hooks.voyage.VoyageAIHook.get_connection"


class TestVoyageAIHook(unittest.TestCase):
    """
    Unit tests for the VoyageAIHook.

    This test class verifies the behavior of the VoyageAIHook,
    ensuring it correctly initializes the client and calls the embed method as expected.
    """

    def setUp(self):
        """
        Set up the test environment for all tests in this class.

        Initializes common test variables and a mock Airflow connection object
        to be used across multiple test methods.
        """
        self.conn_id = "voyage_test_conn"
        self.api_key = "test_api_key_from_password"

        # Create a mock Airflow connection object
        self.mock_connection = Connection(
            conn_id=self.conn_id,
            conn_type="http",
            password=self.api_key,
        )

    @mock.patch(AIRFLOW_CONNECTION_PATCH)
    @mock.patch("voyageai.Client")
    def test_get_conn_initializes_client(self, mock_voyage_client, mock_get_connection):
        """
        Test that the get_conn method correctly initializes and returns the Voyage AI client.

        This test mocks the voyageai.Client to avoid making real API calls,
        and verifies that the client is initialized with the correct API key
        and that the connection retrieval method is called properly.
        """
        mock_get_connection.return_value = self.mock_connection

        hook = VoyageAIHook(conn_id=self.conn_id)
        client = hook.get_conn()

        mock_get_connection.assert_called_once_with(self.conn_id)
        mock_voyage_client.assert_called_once_with(api_key=self.api_key)
        self.assertEqual(client, mock_voyage_client.return_value)

    @mock.patch(AIRFLOW_CONNECTION_PATCH)
    def test_embed_calls_client_method(self, mock_get_connection):
        """
        Test that the hook's embed method calls the underlying client's embed method
        with the correct parameters.

        This test verifies that the embed method of the hook correctly delegates
        the embedding request to the underlying client with the expected arguments.
        """
        mock_get_connection.return_value = self.mock_connection

        hook = VoyageAIHook(conn_id=self.conn_id)
        hook.client = mock.MagicMock()

        test_texts = ["test sentence 1", "test sentence 2"]
        test_model = "voyage-2"
        hook.embed(texts=test_texts, model=test_model)

        hook.client.embed.assert_called_once_with(test_texts, model=test_model, input_type="document")
