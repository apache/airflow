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

from airflow.providers.common.ai.operators.llamaindex_retrieval import RetrievalOperator


def _make_mock_node_with_score(text="chunk text", score=0.9, metadata=None, node_id="node-1"):
    node = MagicMock()
    node.get_content.return_value = text
    node.metadata = metadata or {}
    node.node_id = node_id

    node_with_score = MagicMock()
    node_with_score.node = node
    node_with_score.score = score
    return node_with_score


def _make_mock_llamaindex_modules(retrieval_results=None):
    """Create mock llama_index modules for sys.modules injection."""
    if retrieval_results is None:
        retrieval_results = [_make_mock_node_with_score()]

    mock_core = MagicMock()
    mock_index = MagicMock()
    mock_retriever = MagicMock()
    mock_retriever.retrieve.return_value = retrieval_results
    mock_index.as_retriever.return_value = mock_retriever
    mock_core.load_index_from_storage.return_value = mock_index

    return (
        {
            "llama_index": MagicMock(),
            "llama_index.core": mock_core,
            "llama_index.embeddings": MagicMock(),
            "llama_index.embeddings.openai": MagicMock(),
        },
        mock_core,
        mock_index,
        mock_retriever,
    )


class TestRetrievalOperator:
    def test_template_fields(self):
        expected = {"query", "index_persist_dir", "llm_conn_id"}
        assert set(RetrievalOperator.template_fields) == expected

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_execute_returns_expected_shape(self, mock_hook_cls):
        results = [_make_mock_node_with_score(text="relevant chunk", score=0.95)]
        mock_modules, mock_core, _, _ = _make_mock_llamaindex_modules(results)

        op = RetrievalOperator(
            task_id="test",
            query="What is Airflow?",
            index_persist_dir="/tmp/index",
            llm_conn_id="my_conn",
        )

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        assert "question" in result
        assert "chunks" in result
        assert result["question"] == "What is Airflow?"
        assert len(result["chunks"]) == 1

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_chunks_have_required_keys(self, mock_hook_cls):
        results = [
            _make_mock_node_with_score(
                text="chunk text", score=0.8, metadata={"file": "doc.txt"}, node_id="abc-123"
            )
        ]
        mock_modules, _, _, _ = _make_mock_llamaindex_modules(results)

        op = RetrievalOperator(
            task_id="test",
            query="test query",
            index_persist_dir="/tmp/index",
            llm_conn_id="my_conn",
        )

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        chunk = result["chunks"][0]
        assert chunk["text"] == "chunk text"
        assert chunk["score"] == 0.8
        assert chunk["metadata"] == {"file": "doc.txt"}
        assert chunk["source"] == "abc-123"

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_top_k_forwarded_to_retriever(self, mock_hook_cls):
        mock_modules, _, mock_index, _ = _make_mock_llamaindex_modules([])

        op = RetrievalOperator(
            task_id="test",
            query="test",
            index_persist_dir="/tmp/index",
            llm_conn_id="my_conn",
            top_k=10,
        )

        with patch.dict("sys.modules", mock_modules):
            op.execute(context=MagicMock())

        mock_index.as_retriever.assert_called_once_with(similarity_top_k=10)

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_query_value_in_output(self, mock_hook_cls):
        mock_modules, _, _, _ = _make_mock_llamaindex_modules([])

        op = RetrievalOperator(
            task_id="test",
            query="How does Airflow scheduling work?",
            index_persist_dir="/tmp/index",
            llm_conn_id="my_conn",
        )

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        assert result["question"] == "How does Airflow scheduling work?"
        assert result["chunks"] == []

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_multiple_results_returned(self, mock_hook_cls):
        results = [
            _make_mock_node_with_score(text=f"chunk {i}", score=0.9 - i * 0.1, node_id=f"node-{i}")
            for i in range(3)
        ]
        mock_modules, _, _, _ = _make_mock_llamaindex_modules(results)

        op = RetrievalOperator(
            task_id="test",
            query="test",
            index_persist_dir="/tmp/index",
            llm_conn_id="my_conn",
        )

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        assert len(result["chunks"]) == 3
        assert result["chunks"][0]["text"] == "chunk 0"
        assert result["chunks"][2]["text"] == "chunk 2"

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_hook_configured_with_params(self, mock_hook_cls):
        mock_modules, _, _, _ = _make_mock_llamaindex_modules([])

        op = RetrievalOperator(
            task_id="test",
            query="test",
            index_persist_dir="/tmp/index",
            llm_conn_id="custom_conn",
            embed_model="text-embedding-ada-002",
        )

        with patch.dict("sys.modules", mock_modules):
            op.execute(context=MagicMock())

        mock_hook_cls.assert_called_once_with(llm_conn_id="custom_conn", embed_model="text-embedding-ada-002")
        mock_hook_cls.return_value.configure_settings.assert_called_once()

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_persist_dir_passed_to_storage_context(self, mock_hook_cls):
        mock_modules, mock_core, _, _ = _make_mock_llamaindex_modules([])

        op = RetrievalOperator(
            task_id="test",
            query="test",
            index_persist_dir="/data/my_index",
            llm_conn_id="my_conn",
        )

        with patch.dict("sys.modules", mock_modules):
            op.execute(context=MagicMock())

        mock_core.StorageContext.from_defaults.assert_called_once_with(persist_dir="/data/my_index")
