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
Dispatch from a connection and a ``model_id`` to a :class:`~airflow.providers.common.ai.batch.base.BatchAdapter`.

The ``model_id`` prefix (``"openai"`` in ``"openai:gpt-5"``) selects the adapter,
because the prefix decides the request shape and the batch API being called.
The connection type is then checked against the adapter's
:attr:`~airflow.providers.common.ai.batch.base.BatchAdapter.conn_types`, since
the adapter has to know how to turn that connection's fields into credentials.
This is a separate decision from
:func:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook.infer_model`
(the synchronous ``Agent`` path), which has no notion of "does this provider
have a batch API".

Adapters are imported lazily, only once a request is dispatched to them, so
importing this module never requires ``openai``/``anthropic``/``google-genai`` to be installed.
Another package can add an adapter in two ways: call :func:`register_adapter`
at import time, or declare an entry point in the
``airflow.providers.common.ai.batch_adapters`` group whose name is the model
prefix and whose value is ``module:Class``.
"""

from __future__ import annotations

import importlib
from importlib.metadata import entry_points
from typing import TYPE_CHECKING

from airflow.providers.common.ai.exceptions import (
    BatchProviderNotYetSupportedError,
    UnsupportedBatchProviderError,
)
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchAdapter
    from airflow.providers.common.ai.batch.output_schema import OutputSpec
    from airflow.sdk import Connection

ENTRY_POINT_GROUP = "airflow.providers.common.ai.batch_adapters"

#: adapter name -> (module path, class name), resolved lazily by :func:`import_adapter_class`.
_ADAPTER_MODULES: dict[str, tuple[str, str]] = {
    "openai": ("airflow.providers.common.ai.batch.pydantic_ai_backend", "OpenAIBatchAdapter"),
    "anthropic": ("airflow.providers.common.ai.batch.pydantic_ai_backend", "AnthropicBatchAdapter"),
    "google": ("airflow.providers.common.ai.batch.pydantic_ai_backend", "GoogleBatchAdapter"),
}

#: model_id prefix -> the adapter that serves it under another name. ``openai-chat`` is
#: pydantic-ai's Chat Completions prefix; the ``openai`` adapter batches on the Responses API
#: instead and builds the ``openai:`` model of the same name.
_ADAPTER_ALIASES: dict[str, str] = {"openai-chat": "openai"}

#: model_id prefixes that name a platform whose batch API has no adapter, with the fix to offer.
_GEMINI_PLATFORM_REFUSAL = (
    "Gemini batch runs on the Gemini Developer API; Vertex AI batch prediction reads from and writes "
    "to Cloud Storage or BigQuery, which this provider does not drive. Use a 'google:<model>' model id "
    "on a 'pydanticai' connection whose API Key is a Gemini API key."
)
_REFUSED_PREFIXES: dict[str, str] = {
    "google-cloud": _GEMINI_PLATFORM_REFUSAL,
    "google-vertex": _GEMINI_PLATFORM_REFUSAL,
}

#: Adapter classes registered at runtime through :func:`register_adapter`.
_REGISTERED_ADAPTERS: dict[str, type[BatchAdapter]] = {}


def adapter_name_for_prefix(prefix: str) -> str:
    """Return the name of the adapter that serves a ``model_id`` prefix (``openai-chat`` -> ``openai``)."""
    return _ADAPTER_ALIASES.get(prefix, prefix)


def register_adapter(adapter_cls: type[BatchAdapter]) -> None:
    """
    Register an adapter class for its own :attr:`~airflow.providers.common.ai.batch.base.BatchAdapter.name` prefix.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Intended for other provider packages that ship a batch engine (Bedrock,
    Vertex, Azure OpenAI). A registration overrides a built-in or entry-point
    adapter with the same prefix.
    """
    _REGISTERED_ADAPTERS[adapter_cls.name] = adapter_cls


def _entry_point_adapter(name: str) -> type[BatchAdapter] | None:
    for ep in entry_points(group=ENTRY_POINT_GROUP):
        if ep.name == name:
            return ep.load()
    return None


def import_adapter_class(name: str) -> type[BatchAdapter]:
    """Return the adapter class for an already-resolved adapter name."""
    if name in _REGISTERED_ADAPTERS:
        return _REGISTERED_ADAPTERS[name]
    if name in _ADAPTER_MODULES:
        module_path, class_name = _ADAPTER_MODULES[name]
        return getattr(importlib.import_module(module_path), class_name)
    adapter_cls = _entry_point_adapter(name)
    if adapter_cls is None:
        raise UnsupportedBatchProviderError(
            f"{name!r} has no batch adapter. @task.llm_batch ships adapters for the "
            f"{sorted(_ADAPTER_MODULES)} model_id prefixes; use @task.llm for a synchronous call instead."
        )
    return adapter_cls


def split_model_id(model_id: str | None) -> tuple[str, str]:
    """
    Split ``"<provider>:<model>"`` into its two parts, rejecting a missing or empty half.

    :raises UnsupportedBatchProviderError: ``model_id`` is ``None``, has no
        ``":"``, or has an empty prefix or model name.
    """
    if not model_id or ":" not in model_id:
        raise UnsupportedBatchProviderError(
            "model_id must be written as '<provider>:<model>' (e.g. 'openai:gpt-5') to "
            f"select a batch adapter; got {model_id!r}. Set it on the operator or in the "
            "connection's Model field, or set the connection's Default Model Provider."
        )
    prefix, _, bare = model_id.strip().partition(":")
    if not prefix or not bare:
        raise UnsupportedBatchProviderError(
            f"model_id {model_id!r} must have both a provider prefix and a model name (e.g. 'openai:gpt-5')."
        )
    return prefix, bare


def get_adapter_class(conn_type: str | None, model_id: str | None) -> type[BatchAdapter]:
    """
    Resolve a connection type and ``model_id`` to an adapter class.

    :raises UnsupportedBatchProviderError: no adapter serves the ``model_id``
        prefix, or ``model_id`` is malformed.
    :raises BatchProviderNotYetSupportedError: an adapter exists but does not
        accept this connection type (e.g. ``pydanticai_azure`` today).
    """
    prefix, _ = split_model_id(model_id)
    try:
        adapter_cls = import_adapter_class(adapter_name_for_prefix(prefix))
    except UnsupportedBatchProviderError:
        if prefix in _REFUSED_PREFIXES:
            raise BatchProviderNotYetSupportedError(
                f"No batch adapter serves {prefix!r}. {_REFUSED_PREFIXES[prefix]}"
            ) from None
        raise
    if conn_type not in adapter_cls.conn_types:
        raise BatchProviderNotYetSupportedError(
            f"The {prefix!r} batch adapter does not support connection type {conn_type!r}; it "
            f"accepts {sorted(adapter_cls.conn_types)}. Azure OpenAI, Bedrock and Vertex batch "
            "need their own adapters (deployment-scoped or object-storage based APIs). Until "
            "one exists, point a 'pydanticai' connection at the public endpoint, or route "
            "through a gateway that exposes /v1/files and /v1/batches for /v1/responses."
        )
    return adapter_cls


def resolve_adapter_name(conn_type: str | None, model_id: str | None) -> str:
    """Return the adapter name for a connection/model pair. See :func:`get_adapter_class`."""
    return get_adapter_class(conn_type, model_id).name


def build_adapter_from_connection(
    name: str,
    conn: Connection,
    *,
    model_id: str | None = None,
    batch_handle: str | None = None,
    output_spec: OutputSpec | None = None,
) -> BatchAdapter:
    """
    Instantiate an already-resolved adapter from a fetched connection.

    The adapter decides how to use the connection through
    :meth:`~airflow.providers.common.ai.batch.base.BatchAdapter.from_connection`. The default
    reads ``password``/``host`` as ``api_key``/``base_url``; the pydantic-ai adapters resolve the
    connection through :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
    instead, so they get the same credentials ``@task.llm`` does.
    """
    adapter_cls = import_adapter_class(name)
    return adapter_cls.from_connection(
        conn, model_id=model_id, batch_handle=batch_handle, output_spec=output_spec
    )


def build_adapter(
    name: str,
    *,
    llm_conn_id: str,
    model_id: str | None = None,
    batch_handle: str | None = None,
    output_spec: OutputSpec | None = None,
) -> BatchAdapter:
    """Fetch ``llm_conn_id`` and instantiate the adapter; the trigger's entry point, which only carries the id."""
    return build_adapter_from_connection(
        name,
        BaseHook.get_connection(llm_conn_id),
        model_id=model_id,
        batch_handle=batch_handle,
        output_spec=output_spec,
    )
