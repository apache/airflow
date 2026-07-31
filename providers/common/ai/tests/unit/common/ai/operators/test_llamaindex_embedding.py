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

pytest.importorskip("llama_index.core")

from llama_index.core import MockEmbedding
from llama_index.core.schema import MetadataMode

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


def _node(text: str = "chunk text", metadata: dict | None = None):
    node = MagicMock()
    node.text = text
    node.metadata = metadata or {}
    node.embedding = None
    node.get_content.return_value = text
    return node


def _byo_embedding(vectors: list[list[float]] | None = None):
    """Return a duck-typed ``BaseEmbedding`` stand-in (has the methods the operator checks and calls)."""
    embedding = MagicMock(
        name="MyBaseEmbedding",
        spec=["get_text_embedding_batch", "_get_query_embedding"],
    )
    embedding.get_text_embedding_batch.return_value = [[0.0]] if vectors is None else vectors
    return embedding


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
            _node(text="chunk a"),
        ]
        mock_get_embed.return_value = _byo_embedding(vectors=[[0.1, 0.2]])

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
        mock_hook_cls.return_value.get_embedding_model.return_value = _byo_embedding()

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
        # `embed_model` is a non-string instance -> hook is bypassed and the
        # user's instance does the embedding.
        byo = _byo_embedding(vectors=[[0.5]])
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [_node()]

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model=byo,
        )
        result = op.execute(context=MagicMock())

        byo.get_text_embedding_batch.assert_called_once()
        assert result["chunks"][0]["vector"] == [0.5]

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

    def test_chunks_carry_text_metadata_vector(self, _li):
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [
            _node(text="x", metadata={"k": "v"}),
            _node(text="y", metadata={"k": "v2"}),
        ]

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model=_byo_embedding(vectors=[[1.0, 2.0], [3.0, 4.0]]),
        )
        result = op.execute(context=MagicMock())

        assert result["chunks"] == [
            {"text": "x", "metadata": {"k": "v"}, "vector": [1.0, 2.0]},
            {"text": "y", "metadata": {"k": "v2"}, "vector": [3.0, 4.0]},
        ]

    def test_nodes_embedded_with_embed_metadata_mode(self, _li):
        # llama-index's own ``embed_nodes()`` embeds
        # ``node.get_content(metadata_mode=MetadataMode.EMBED)`` (includes
        # metadata, respects ``excluded_embed_metadata_keys``). The pre-embed
        # step must match, or the vectors silently change semantics.
        node = _node(text="chunk a")
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [node]
        byo = _byo_embedding()

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model=byo,
        )
        op.execute(context=MagicMock())

        node.get_content.assert_called_once_with(metadata_mode=MetadataMode.EMBED)
        byo.get_text_embedding_batch.assert_called_once()
        assert byo.get_text_embedding_batch.call_args.args[0] == ["chunk a"]

    def test_index_only_built_when_persisting(self, _li):
        # Without ``persist_dir`` the index would be built and immediately
        # discarded; the vectors come from the pre-embed step.
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [_node()]

        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "doc"}],
            embed_model=_byo_embedding(),
        )
        op.execute(context=MagicMock())

        _li["VectorStoreIndex"].assert_not_called()

    def test_vectors_populated_with_real_llama_index(self):
        # Regression test for #68416: ``VectorStoreIndex`` attaches embeddings
        # to node *copies* (``model_copy()`` in ``_get_node_with_embedding``),
        # so reading ``node.embedding`` after index construction returns
        # ``None``. Run the real llama-index code path with its offline
        # ``MockEmbedding`` -- no mocks on the operator's internals.
        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "hello world", "metadata": {"src": "a"}}],
            embed_model=MockEmbedding(embed_dim=8),
        )
        result = op.execute(context=MagicMock())

        assert result["chunk_count"] >= 1
        assert all(chunk["vector"] is not None for chunk in result["chunks"])
        assert all(len(chunk["vector"]) == 8 for chunk in result["chunks"])


class TestEmbeddingOperatorPersist:
    def test_persist_path_embeds_each_chunk_once_with_real_llama_index(self, tmp_path):
        # ``embed_nodes()`` inside ``VectorStoreIndex`` must skip the
        # pre-embedded nodes -- if it re-embeds, every chunk pays the
        # embedding API twice. Runs the real llama-index persist path.
        embedded_texts: list[str] = []

        class CountingMockEmbedding(MockEmbedding):
            def _get_text_embedding(self, text: str) -> list[float]:
                embedded_texts.append(text)
                return super()._get_text_embedding(text)

        persist_dir = tmp_path / "idx"
        op = LlamaIndexEmbeddingOperator(
            task_id="test",
            documents=[{"text": "hello world", "metadata": {"src": "a"}}],
            embed_model=CountingMockEmbedding(embed_dim=8),
            persist_dir=str(persist_dir),
        )
        result = op.execute(context=MagicMock())

        assert result["chunk_count"] == 1
        assert len(embedded_texts) == result["chunk_count"]
        assert all(chunk["vector"] is not None for chunk in result["chunks"])
        assert any(persist_dir.iterdir())

    @patch("os.makedirs")
    @patch("airflow.providers.common.ai.hooks.llamaindex.LlamaIndexHook.get_embedding_model")
    def test_local_persist_dir_calls_makedirs_and_storage_persist(
        self, mock_get_embed, mock_makedirs, _li, tmp_path
    ):
        node = _node()
        _li["SentenceSplitter"].return_value.get_nodes_from_documents.return_value = [node]
        mock_get_embed.return_value = _byo_embedding(vectors=[[0.1]])
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
        # Nodes are already embedded when handed to the index (the
        # no-double-embed behavior itself is pinned by
        # ``test_persist_path_embeds_each_chunk_once_with_real_llama_index``).
        nodes_arg = _li["VectorStoreIndex"].call_args.args[0]
        assert nodes_arg[0].embedding == [0.1]

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
        mock_get_embed.return_value = _byo_embedding()
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
