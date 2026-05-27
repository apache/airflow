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

from airflow.providers.common.ai.operators.llamaindex_embedding import LlamaIndexEmbeddingOperator


@pytest.fixture
def _li(monkeypatch):
    """Patch the two LlamaIndex constructors the operator uses inside execute().

    ``llama_index`` (core + openai embeddings) is a real test dependency
    declared in ``providers/common/ai/pyproject.toml``'s dev group, so
    ``@patch("llama_index.core.X")`` resolves against the real module.
    """
    VectorStoreIndex = MagicMock(name="VectorStoreIndex")
    SentenceSplitter = MagicMock(name="SentenceSplitter")
    monkeypatch.setattr("llama_index.core.VectorStoreIndex", VectorStoreIndex)
    monkeypatch.setattr("llama_index.core.node_parser.SentenceSplitter", SentenceSplitter)
    return {"VectorStoreIndex": VectorStoreIndex, "SentenceSplitter": SentenceSplitter}


def _node(text: str = "chunk text", metadata: dict | None = None, vector=None):
    node = MagicMock()
    node.text = text
    node.metadata = metadata or {}
    node.embedding = vector
    return node


def _byo_embedding():
    """Return a duck-typed ``BaseEmbedding`` stand-in (has the two methods the operator checks)."""
    return MagicMock(name="MyBaseEmbedding", spec=["get_text_embedding", "_get_query_embedding"])


class TestEmbeddingOperatorInit:
    def test_template_fields(self):
        # ``documents`` must be templated so ``loader.output`` (XComArg) is
        # resolved before execute. The earlier rationale that "list[dict]
        # doesn't survive Jinja stringification" was wrong -- Templater
        # unwraps resolvables before Jinja runs.
        assert set(LlamaIndexEmbeddingOperator.template_fields) == {
            "documents",
            "embed_model",
            "llm_conn_id",
            "embed_conn_id",
            "persist_dir",
            "persist_conn_id",
        }


class TestEmbeddingOperatorExecute:
    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_string_embed_model_goes_through_hook(self, mock_get_embed, _li):
        # `embed_model` as a string -> hook builds OpenAIEmbedding.
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [
            _node(text="chunk a", vector=[0.1, 0.2]),
        ]

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc", "metadata": {"src": "x"}}],
            embed_model="text-embedding-3-small",
            llm_conn_id="my_conn",
        )
        result = op.execute(context=MagicMock())

        mock_get_embed.assert_called_once()
        assert result["document_count"] == 1
        assert result["chunk_count"] == 1
        assert result["chunks"][0]["text"] == "chunk a"
        assert result["chunks"][0]["vector"] == [0.1, 0.2]

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook")
    def test_string_embed_model_forwards_embed_conn_id(self, mock_hook_cls, _li):
        # ``embed_conn_id`` overrides ``llm_conn_id`` for the embedding API.
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [_node()]

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
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

    def test_byo_embed_model_bypasses_hook(self, _li):
        # `embed_model` is a non-string instance -> hook is bypassed.
        byo = _byo_embedding()
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [_node()]

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model=byo,
        )
        op.execute(context=MagicMock())

        # VectorStoreIndex called with the user's instance, not anything else.
        _li["VectorStoreIndex"].assert_called_once()
        kwargs = _li["VectorStoreIndex"].call_args.kwargs
        assert kwargs["embed_model"] is byo

    def test_invalid_embed_model_raises_typeerror(self, _li):
        # An object that's neither None/str nor duck-types as BaseEmbedding
        # (e.g. an unresolved XComArg or random user input) raises TypeError
        # with a clear pointer rather than a cryptic downstream error.
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [_node()]

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model=12345,  # type: ignore[arg-type]
        )
        with pytest.raises(TypeError, match="embed_model must be"):
            op.execute(context=MagicMock())

    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_chunks_carry_text_metadata_vector(self, mock_get_embed, _li):
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [
            _node(text="x", metadata={"k": "v"}, vector=[1.0, 2.0]),
            _node(text="y", metadata={"k": "v2"}, vector=[3.0, 4.0]),
        ]

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model="text-embedding-3-small",
        )
        result = op.execute(context=MagicMock())

        assert result["chunks"] == [
            {"text": "x", "metadata": {"k": "v"}, "vector": [1.0, 2.0]},
            {"text": "y", "metadata": {"k": "v2"}, "vector": [3.0, 4.0]},
        ]


class TestEmbeddingOperatorPersist:
    @patch("os.makedirs")
    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_local_persist_dir_calls_makedirs_and_storage_persist(
        self, mock_get_embed, mock_makedirs, _li, tmp_path
    ):
        node = _node()
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [node]
        index = _li["VectorStoreIndex"].return_value

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model="text-embedding-3-small",
            persist_dir=str(tmp_path / "idx"),
        )
        op.execute(context=MagicMock())

        mock_makedirs.assert_called_once_with(str(tmp_path / "idx"), exist_ok=True)
        index.storage_context.persist.assert_called_once_with(persist_dir=str(tmp_path / "idx"))

    @patch("airflow.sdk.ObjectStoragePath")
    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_cloud_uri_persist_dir_uses_object_storage_path(self, mock_get_embed, mock_osp_cls, _li):
        # ``ObjectStoragePath.__str__`` returns ``<scheme>://<conn_id>@<bucket>/...``
        # when ``conn_id`` is set, which fsspec misinterprets. The operator must
        # pass the **raw** user URI to ``persist_dir=`` and supply
        # ``fs=target.fs`` for credentials. Asserting against the raw URI here
        # catches a regression where ``str(target)`` is used instead.
        target = MagicMock()
        target.fs = MagicMock(name="s3fs")
        mock_osp_cls.return_value = target

        node = _node()
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [node]
        index = _li["VectorStoreIndex"].return_value

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model="text-embedding-3-small",
            persist_dir="s3://bucket/idx/",
            persist_conn_id="aws_default",
        )
        op.execute(context=MagicMock())

        mock_osp_cls.assert_called_once_with("s3://bucket/idx/", conn_id="aws_default")
        target.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        index.storage_context.persist.assert_called_once_with(persist_dir="s3://bucket/idx/", fs=target.fs)
