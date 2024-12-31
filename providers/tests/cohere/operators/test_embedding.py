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

from unittest.mock import MagicMock, patch

from airflow.models import Connection
from airflow.providers.cohere.operators.embedding import CohereEmbeddingOperator


@patch("airflow.providers.cohere.hooks.cohere.CohereHook.get_connection")
@patch("cohere.ClientV2")
def test_cohere_embedding_operator(cohere_client, get_connection):
    """
    Test Cohere client is getting called with the correct key and that
     the execute methods returns expected response.
    """
    embedded_obj = [1, 2, 3]

    class resp:
        embeddings = embedded_obj

    api_key = "test"
    base_url = "http://some_host.com"
    timeout = 150
    texts = ["On Kernel-Target Alignment. We describe a family of global optimization procedures"]
    request_options = None

    get_connection.return_value = Connection(conn_type="cohere", password=api_key, host=base_url)
    client_obj = MagicMock()
    cohere_client.return_value = client_obj
    client_obj.embed.return_value = resp

    op = CohereEmbeddingOperator(
        task_id="embed",
        conn_id="some_conn",
        input_text=texts,
        timeout=timeout,
        request_options=request_options,
    )

    val = op.execute(context={})
    cohere_client.assert_called_once_with(api_key=api_key, base_url=base_url, timeout=timeout)
    assert val == embedded_obj
