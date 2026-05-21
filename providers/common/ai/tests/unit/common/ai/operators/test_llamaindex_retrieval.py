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

import pytest

from airflow.providers.common.ai.operators.llamaindex_retrieval import LlamaIndexRetrievalOperator


@pytest.fixture
def _li(monkeypatch):
    """Patch the two LlamaIndex symbols the retrieval operator uses inside execute().

    ``llama_index`` (core + openai embeddings) is a real test dependency
    declared in ``providers/common/ai/pyproject.toml``'s dev group, so
    ``monkeypatch.setattr("llama_index.core.X", ...)`` resolves against the
    real module.
    """
    StorageContext = MagicMock(name="StorageContext")
    load_index_from_storage = MagicMock(name="load_index_from_storage")
    monkeypatch.setattr("llama_index.core.StorageContext", StorageContext)
    monkeypatch.setattr("llama_index.core.load_index_from_storage", load_index_from_storage)
    return {
        "StorageContext": StorageContext,
        "load_index_from_storage": load_index_from_storage,
    }


def _scored_node(text: str, score: float, metadata: dict | None = None, node_id: str = "n"):
    node = MagicMock()
    node.get_content.return_value = text
    node.metadata = metadata or {}
    node.node_id = node_id
    wrapped = MagicMock()
    wrapped.node = node
    wrapped.score = score
    return wrapped


def _byo_embedding():
    """Return a duck-typed ``BaseEmbedding`` stand-in."""
    return MagicMock(name="MyBaseEmbedding", spec=["get_text_embedding", "_get_query_embedding"])


class TestRetrievalOperatorInit:
    def test_template_fields(self):
        assert set(LlamaIndexRetrievalOperator.template_fields) == {
            "query",
            "index_persist_dir",
            "persist_conn_id",
            "embed_model",
            "llm_conn_id",
            "embed_conn_id",
        }


class TestRetrievalOperatorOutput:
    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_chunk_shape(self, mock_get_embed, _li, tmp_path):
        # Make the persist_dir existence check pass.
        (tmp_path / "idx").mkdir()

        index = _li["load_index_from_storage"].return_value
        retriever = index.as_retriever.return_value
        retriever.retrieve.return_value = [
            _scored_node("chunk a", 0.91, {"src": "x"}, "node-a"),
            _scored_node("chunk b", 0.85, {"src": "y"}, "node-b"),
        ]

        op = LlamaIndexRetrievalOperator(
            task_id="test",
            query="what is airflow",
            index_persist_dir=str(tmp_path / "idx"),
            embed_model="text-embedding-3-small",
        )
        result = op.execute(context=MagicMock())

        assert result == {
            "query": "what is airflow",
            "chunks": [
                {"text": "chunk a", "score": 0.91, "metadata": {"src": "x"}, "node_id": "node-a"},
                {"text": "chunk b", "score": 0.85, "metadata": {"src": "y"}, "node_id": "node-b"},
            ],
        }
        # The retrieval-time embedding model is passed directly (no Settings mutation).
        _li["load_index_from_storage"].assert_called_once()
        kwargs = _li["load_index_from_storage"].call_args.kwargs
        assert "embed_model" in kwargs
        index.as_retriever.assert_called_once_with(similarity_top_k=5)

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_top_k_forwarded(self, mock_get_embed, _li, tmp_path):
        (tmp_path / "idx").mkdir()
        index = _li["load_index_from_storage"].return_value
        index.as_retriever.return_value.retrieve.return_value = []

        op = LlamaIndexRetrievalOperator(
            task_id="test",
            query="q",
            index_persist_dir=str(tmp_path / "idx"),
            embed_model="text-embedding-3-small",
            top_k=12,
        )
        op.execute(context=MagicMock())

        index.as_retriever.assert_called_once_with(similarity_top_k=12)

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook")
    def test_string_embed_model_forwards_embed_conn_id(self, mock_hook_cls, _li, tmp_path):
        # ``embed_conn_id`` overrides ``llm_conn_id`` for the embedding API.
        (tmp_path / "idx").mkdir()
        index = _li["load_index_from_storage"].return_value
        index.as_retriever.return_value.retrieve.return_value = []

        op = LlamaIndexRetrievalOperator(
            task_id="test",
            query="q",
            index_persist_dir=str(tmp_path / "idx"),
            embed_model="text-embedding-3-small",
            llm_conn_id="my_llm_conn",
            embed_conn_id="my_embed_conn",
        )
        op.execute(context=MagicMock())

        mock_hook_cls.assert_called_once_with(
            llm_conn_id="my_llm_conn",
            embed_conn_id="my_embed_conn",
            embed_model="text-embedding-3-small",
        )

    def test_byo_embed_model_bypasses_hook(self, _li, tmp_path):
        (tmp_path / "idx").mkdir()
        byo = _byo_embedding()
        index = _li["load_index_from_storage"].return_value
        index.as_retriever.return_value.retrieve.return_value = []

        op = LlamaIndexRetrievalOperator(
            task_id="test",
            query="q",
            index_persist_dir=str(tmp_path / "idx"),
            embed_model=byo,
        )
        op.execute(context=MagicMock())

        kwargs = _li["load_index_from_storage"].call_args.kwargs
        assert kwargs["embed_model"] is byo

    def test_invalid_embed_model_raises_typeerror(self, _li, tmp_path):
        # An object that's neither None/str nor duck-types as BaseEmbedding
        # raises TypeError with a clear pointer.
        (tmp_path / "idx").mkdir()

        op = LlamaIndexRetrievalOperator(
            task_id="test",
            query="q",
            index_persist_dir=str(tmp_path / "idx"),
            embed_model=12345,  # type: ignore[arg-type]
        )
        with pytest.raises(TypeError, match="embed_model must be"):
            op.execute(context=MagicMock())


class TestRetrievalOperatorMissingIndex:
    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_local_missing_dir_raises_with_hint(self, mock_get_embed, _li, tmp_path):
        op = LlamaIndexRetrievalOperator(
            task_id="test",
            query="q",
            index_persist_dir=str(tmp_path / "no_such_dir"),
            embed_model="text-embedding-3-small",
        )
        with pytest.raises(FileNotFoundError, match="LlamaIndexEmbeddingOperator"):
            op.execute(context=MagicMock())

    @patch("airflow.sdk.ObjectStoragePath")
    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_cloud_missing_uri_raises_with_hint(self, mock_get_embed, mock_osp_cls, _li):
        missing = MagicMock()
        missing.is_dir.return_value = False
        mock_osp_cls.return_value = missing

        op = LlamaIndexRetrievalOperator(
            task_id="test",
            query="q",
            index_persist_dir="s3://bucket/missing/",
            embed_model="text-embedding-3-small",
        )
        with pytest.raises(FileNotFoundError, match="LlamaIndexEmbeddingOperator"):
            op.execute(context=MagicMock())


class TestRetrievalOperatorCloudURI:
    @patch("airflow.sdk.ObjectStoragePath")
    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_cloud_uri_opens_storage_with_fs(self, mock_get_embed, mock_osp_cls, _li):
        # ``ObjectStoragePath.__str__`` returns ``<scheme>://<conn_id>@<bucket>/...``
        # when ``conn_id`` is set, which fsspec misinterprets. The operator must
        # pass the **raw** user URI to ``persist_dir=`` and supply
        # ``fs=target.fs`` for credentials. Asserting against the raw URI here
        # catches a regression where ``str(target)`` is used instead.
        target = MagicMock()
        target.is_dir.return_value = True
        target.fs = MagicMock(name="s3fs")
        mock_osp_cls.return_value = target

        index = _li["load_index_from_storage"].return_value
        index.as_retriever.return_value.retrieve.return_value = []

        op = LlamaIndexRetrievalOperator(
            task_id="test",
            query="q",
            index_persist_dir="s3://bucket/idx/",
            persist_conn_id="aws_default",
            embed_model="text-embedding-3-small",
        )
        op.execute(context=MagicMock())

        mock_osp_cls.assert_called_once_with("s3://bucket/idx/", conn_id="aws_default")
        _li["StorageContext"].from_defaults.assert_called_once_with(
            persist_dir="s3://bucket/idx/",
            fs=target.fs,
        )
