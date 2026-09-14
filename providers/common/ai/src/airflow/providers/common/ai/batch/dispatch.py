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
"""
Two-layer dispatch from a connection to a :class:`~airflow.providers.common.ai.batch.base.BatchAdapter`.

``conn_type`` decides auth; the ``model_id`` prefix (e.g. ``"openai:gpt-5"``)
decides request shape. This is a separate decision from
:func:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook.infer_model`
(the sync ``Agent`` path) because that helper has no notion of "does this
provider have a batch API".

This module must not import a provider SDK, directly or transitively --
adapter classes are imported lazily, only once a request is actually
dispatched to a supported provider (see :func:`import_adapter_class`), so
importing this module never requires ``openai``/``anthropic`` to be
installed.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from airflow.providers.common.ai.exceptions import (
    BatchProviderNotYetSupportedError,
    UnsupportedBatchProviderError,
)

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchAdapter

CONN_TYPE_PYDANTIC_AI = "pydanticai"

#: adapter name -> (module path, class name). Resolved lazily by
#: :func:`import_adapter_class`, never imported at module scope.
_ADAPTER_MODULES: dict[str, tuple[str, str]] = {
    "openai": ("airflow.providers.common.ai.batch.openai", "OpenAIBatchAdapter"),
    "anthropic": ("airflow.providers.common.ai.batch.anthropic", "AnthropicBatchAdapter"),
}

#: conn_type -> "known, not built yet" message. Kept separate from the
#: catch-all ``UnsupportedBatchProviderError`` below so the two failure modes
#: give the user different (correct) information: "this path is known but not
#: built yet" vs. "this path does not exist".
_NOT_YET_SUPPORTED: dict[str, str] = {
    "pydanticai_azure": (
        "Azure OpenAI batch is not supported yet by @task.llm_batch (connection type "
        "'pydanticai_azure'). Azure's batch API is deployment-scoped and uses a different "
        "client than the public OpenAI endpoint, so it needs its own adapter; support is "
        "planned as a follow-up. For now, use a 'pydanticai' connection pointing at the "
        "public OpenAI endpoint, or the LiteLLM gateway passthrough described in the "
        "provider docs."
    ),
    "pydanticai_bedrock": (
        "Amazon Bedrock batch is not supported yet by @task.llm_batch (connection type "
        "'pydanticai_bedrock'). Bedrock batch inference is an S3 + boto3 workflow rather "
        "than an HTTP JSONL API, so it needs its own adapter; support is planned as a "
        "follow-up. For now, use a 'pydanticai' connection pointing at the public OpenAI or "
        "Anthropic endpoint, or the LiteLLM gateway passthrough described in the provider "
        "docs."
    ),
    "pydanticai_vertex": (
        "Vertex AI batch is not supported yet by @task.llm_batch (connection type "
        "'pydanticai_vertex'). Vertex batch prediction is a GCS + client-library workflow "
        "rather than an HTTP JSONL API, so it needs its own adapter; support is planned as "
        "a follow-up. For now, use a 'pydanticai' connection pointing at the public OpenAI "
        "or Anthropic endpoint, or the LiteLLM gateway passthrough described in the "
        "provider docs."
    ),
}


def resolve_adapter_name(conn_type: str, model_id: str | None) -> str:
    """
    Return the adapter name (``"openai"`` / ``"anthropic"``) for a connection/model pair.

    :raises BatchProviderNotYetSupportedError: ``conn_type`` is a known
        pydantic-ai connection type whose batch adapter is not built yet
        (Azure, Bedrock, Vertex).
    :raises UnsupportedBatchProviderError: ``conn_type`` is not recognized at
        all, ``model_id`` has no ``"<provider>:"`` prefix, or the prefix
        names a provider with no batch API.
    """
    if conn_type in _NOT_YET_SUPPORTED:
        raise BatchProviderNotYetSupportedError(_NOT_YET_SUPPORTED[conn_type])
    if conn_type != CONN_TYPE_PYDANTIC_AI:
        raise UnsupportedBatchProviderError(
            f"@task.llm_batch does not recognize connection type {conn_type!r}. Batch is "
            "only available for 'pydanticai' connections pointing at an OpenAI or Anthropic "
            "model (model_id prefixes 'openai:' / 'anthropic:')."
        )
    if not model_id or ":" not in model_id:
        raise UnsupportedBatchProviderError(
            "model_id must be written as '<provider>:<model>' (e.g. 'openai:gpt-5') to "
            f"select a batch adapter; got {model_id!r}."
        )
    prefix, _, _ = model_id.partition(":")
    if prefix not in _ADAPTER_MODULES:
        raise UnsupportedBatchProviderError(
            f"{prefix!r} has no batch API. @task.llm_batch only supports the 'openai' and "
            "'anthropic' model_id prefixes; use @task.llm for a synchronous call instead."
        )
    return prefix


def import_adapter_class(name: str) -> type[BatchAdapter]:
    """Import and return the adapter class for an already-resolved adapter name."""
    module_path, class_name = _ADAPTER_MODULES[name]
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def get_adapter_class(conn_type: str, model_id: str | None) -> type[BatchAdapter]:
    """Resolve a connection/model pair straight to an adapter class. See :func:`resolve_adapter_name`."""
    return import_adapter_class(resolve_adapter_name(conn_type, model_id))


def build_adapter(name: str, *, llm_conn_id: str) -> BatchAdapter:
    """
    Instantiate an already-resolved adapter, authenticated from an Airflow connection.

    Both the operator (which resolves ``name`` itself, at submit time) and the
    trigger (which only carries the already-resolved ``name`` in its
    serialized state, per §4) need this same "conn_id -> live adapter"
    step, so it lives here rather than being duplicated in both.

    Uses the connection's ``password``/``host`` fields as ``api_key``/
    ``base_url`` -- the same fields :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
    reads for the synchronous ``@task.llm`` path.
    """
    from airflow.providers.common.compat.sdk import BaseHook

    conn = BaseHook.get_connection(llm_conn_id)
    adapter_cls = import_adapter_class(name)
    return adapter_cls(api_key=conn.password or None, base_url=conn.host or None)
