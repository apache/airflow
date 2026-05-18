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

from airflow.providers.common.ai.operators.llamaindex_embedding import EmbeddingOperator


def _make_mock_node(text="chunk text", metadata=None, embedding=None):
    node = MagicMock()
    node.text = text
    node.metadata = metadata or {}
    node.embedding = embedding
    return node


def _make_mock_llamaindex_modules(nodes=None):
    """Create mock llama_index modules for sys.modules injection."""
    if nodes is None:
        nodes = [_make_mock_node()]

    mock_core = MagicMock()
    mock_core.Document = MagicMock(side_effect=lambda text, metadata: MagicMock(text=text, metadata=metadata))
    mock_core.StorageContext.from_defaults.return_value = MagicMock()
    mock_core.VectorStoreIndex = MagicMock()

    mock_node_parser = MagicMock()
    mock_splitter = MagicMock()
    mock_splitter.get_nodes_from_documents.return_value = nodes
    mock_node_parser.SentenceSplitter.return_value = mock_splitter

    return (
        {
            "llama_index": MagicMock(),
            "llama_index.core": mock_core,
            "llama_index.core.node_parser": mock_node_parser,
            "llama_index.embeddings": MagicMock(),
            "llama_index.embeddings.openai": MagicMock(),
        },
        mock_core,
        mock_splitter,
    )


class TestEmbeddingOperator:
    def test_template_fields(self):
        expected = {"documents", "llm_conn_id", "persist_dir"}
        assert set(EmbeddingOperator.template_fields) == expected

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_execute_returns_expected_shape(self, mock_hook_cls):
        docs = [{"text": "Hello world", "metadata": {"source": "test"}}]
        nodes = [_make_mock_node(text="Hello world", metadata={"source": "test"})]
        mock_modules, mock_core, mock_splitter = _make_mock_llamaindex_modules(nodes)

        op = EmbeddingOperator(task_id="test", documents=docs, llm_conn_id="my_conn")

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        assert "document_count" in result
        assert "chunk_count" in result
        assert "persist_dir" in result
        assert "chunks" in result
        assert result["document_count"] == 1
        assert result["chunk_count"] == 1

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_chunking_node_count(self, mock_hook_cls):
        docs = [{"text": "A long document " * 100, "metadata": {}}]
        nodes = [_make_mock_node(text=f"chunk {i}") for i in range(5)]
        mock_modules, mock_core, mock_splitter = _make_mock_llamaindex_modules(nodes)

        op = EmbeddingOperator(task_id="test", documents=docs, llm_conn_id="my_conn")

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        assert result["chunk_count"] == 5
        assert len(result["chunks"]) == 5

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_persist_dir_creates_and_persists(self, mock_hook_cls, tmp_path):
        docs = [{"text": "test", "metadata": {}}]
        persist_dir = str(tmp_path / "index_storage")
        mock_modules, mock_core, _ = _make_mock_llamaindex_modules()
        mock_storage_ctx = mock_core.StorageContext.from_defaults.return_value

        op = EmbeddingOperator(task_id="test", documents=docs, llm_conn_id="my_conn", persist_dir=persist_dir)

        with patch.dict("sys.modules", mock_modules):
            op.execute(context=MagicMock())

        mock_storage_ctx.persist.assert_called_once_with(persist_dir=persist_dir)

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_no_persist_when_none(self, mock_hook_cls):
        docs = [{"text": "test", "metadata": {}}]
        mock_modules, mock_core, _ = _make_mock_llamaindex_modules()
        mock_storage_ctx = mock_core.StorageContext.from_defaults.return_value

        op = EmbeddingOperator(task_id="test", documents=docs, llm_conn_id="my_conn")

        with patch.dict("sys.modules", mock_modules):
            op.execute(context=MagicMock())

        mock_storage_ctx.persist.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_chunks_have_text_and_metadata(self, mock_hook_cls):
        docs = [{"text": "test", "metadata": {"src": "a"}}]
        nodes = [_make_mock_node(text="chunk1", metadata={"src": "a"})]
        mock_modules, _, _ = _make_mock_llamaindex_modules(nodes)

        op = EmbeddingOperator(task_id="test", documents=docs, llm_conn_id="my_conn")

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        chunk = result["chunks"][0]
        assert "text" in chunk
        assert "metadata" in chunk
        assert chunk["text"] == "chunk1"
        assert chunk["metadata"] == {"src": "a"}

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_chunks_include_vector_when_present(self, mock_hook_cls):
        docs = [{"text": "test", "metadata": {}}]
        nodes = [_make_mock_node(text="chunk1", embedding=[0.1, 0.2, 0.3])]
        mock_modules, _, _ = _make_mock_llamaindex_modules(nodes)

        op = EmbeddingOperator(task_id="test", documents=docs, llm_conn_id="my_conn")

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        assert result["chunks"][0]["vector"] == [0.1, 0.2, 0.3]

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_chunks_omit_vector_when_not_present(self, mock_hook_cls):
        docs = [{"text": "test", "metadata": {}}]
        nodes = [_make_mock_node(text="chunk1", embedding=None)]
        mock_modules, _, _ = _make_mock_llamaindex_modules(nodes)

        op = EmbeddingOperator(task_id="test", documents=docs, llm_conn_id="my_conn")

        with patch.dict("sys.modules", mock_modules):
            result = op.execute(context=MagicMock())

        assert "vector" not in result["chunks"][0]

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_hook_configured_with_params(self, mock_hook_cls):
        docs = [{"text": "test", "metadata": {}}]
        mock_modules, _, _ = _make_mock_llamaindex_modules()

        op = EmbeddingOperator(
            task_id="test",
            documents=docs,
            llm_conn_id="custom_conn",
            embed_model="text-embedding-ada-002",
        )

        with patch.dict("sys.modules", mock_modules):
            op.execute(context=MagicMock())

        mock_hook_cls.assert_called_once_with(llm_conn_id="custom_conn", embed_model="text-embedding-ada-002")
        mock_hook_cls.return_value.configure_settings.assert_called_once()

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook", autospec=True)
    def test_splitter_params_forwarded(self, mock_hook_cls):
        docs = [{"text": "test", "metadata": {}}]
        mock_modules, _, _ = _make_mock_llamaindex_modules()
        mock_node_parser = mock_modules["llama_index.core.node_parser"]

        op = EmbeddingOperator(
            task_id="test",
            documents=docs,
            llm_conn_id="my_conn",
            chunk_size=256,
            chunk_overlap=25,
        )

        with patch.dict("sys.modules", mock_modules):
            op.execute(context=MagicMock())

        mock_node_parser.SentenceSplitter.assert_called_once_with(chunk_size=256, chunk_overlap=25)
