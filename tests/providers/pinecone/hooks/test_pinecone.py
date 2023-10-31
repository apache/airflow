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

from unittest.mock import Mock, patch

from airflow.providers.pinecone.hooks.pinecone import PineconeHook


class TestPineconeHook:
    def setup_method(self):
        """Set up the test environment, mocking necessary connections and initializing the
        PineconeHook object."""
        with patch("airflow.models.Connection.get_connection_from_secrets") as mock_get_connection:
            mock_conn = Mock()
            mock_conn.host = "pinecone.io"
            mock_conn.login = "test_user"
            mock_conn.password = "test_password"
            mock_get_connection.return_value = mock_conn
            self.pinecone_hook = PineconeHook()
            self.index_name = "test_index"

    @patch("airflow.providers.pinecone.hooks.pinecone.pinecone.Index")
    def test_upsert(self, mock_index):
        """Test the upsert_data_async method of PineconeHook for correct data insertion asynchronously."""
        data = [("id1", [1.0, 2.0, 3.0], {"meta": "data"})]
        mock_upsert = Mock()
        mock_index.return_value.upsert = mock_upsert
        self.pinecone_hook.upsert(self.index_name, data)
        mock_upsert.assert_called_once_with(vectors=data, namespace="", batch_size=None, show_progress=True)
