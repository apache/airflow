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
Dispatch from a connection and a ``model_id`` to an :class:`~airflow.providers.common.ai.ocr.base.OCRAdapter`.

The ``model_id`` prefix (``"mistral"`` in ``"mistral:mistral-ocr-latest"``) selects the adapter,
as it does for the batch adapters; the connection type is then checked against the adapter's
:attr:`~airflow.providers.common.ai.ocr.base.OCRAdapter.conn_types`, since the adapter has to
know how to turn that connection's fields into credentials.

Adapters are imported lazily, only once a request is dispatched to them, so importing this module
never requires a vendor's library. Another package adds an adapter with :func:`register_adapter`.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from airflow.providers.common.ai.exceptions import UnsupportedOCRProviderError

if TYPE_CHECKING:
    from airflow.providers.common.ai.ocr.base import OCRAdapter

#: adapter name -> (module path, class name), resolved lazily by :func:`get_adapter_class`.
_ADAPTER_MODULES: dict[str, tuple[str, str]] = {
    "mistral": ("airflow.providers.common.ai.ocr.mistral", "MistralOCRAdapter"),
}

#: Adapter classes registered at runtime through :func:`register_adapter`.
_REGISTERED_ADAPTERS: dict[str, type[OCRAdapter]] = {}


def register_adapter(adapter_cls: type[OCRAdapter]) -> None:
    """
    Register an adapter class for its own :attr:`~airflow.providers.common.ai.ocr.base.OCRAdapter.name` prefix.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Intended for other packages that ship an OCR service. A registration overrides a built-in
    adapter with the same prefix.
    """
    _REGISTERED_ADAPTERS[adapter_cls.name] = adapter_cls


def split_model_id(model_id: str | None) -> tuple[str, str]:
    """
    Split ``"<provider>:<model>"`` into its two parts, rejecting a missing or empty half.

    :raises UnsupportedOCRProviderError: ``model_id`` is ``None``, has no ``":"``, or has an
        empty prefix or model name.
    """
    prefix, separator, model = (model_id or "").strip().partition(":")
    if not separator or not prefix or not model:
        raise UnsupportedOCRProviderError(
            "model_id must be written as '<provider>:<model>' (e.g. 'mistral:mistral-ocr-latest') "
            f"to select an OCR adapter; got {model_id!r}."
        )
    return prefix, model


def get_adapter_class(conn_type: str | None, model_id: str | None) -> type[OCRAdapter]:
    """
    Resolve a connection type and ``model_id`` to an adapter class.

    :raises UnsupportedOCRProviderError: no adapter serves the ``model_id`` prefix, ``model_id``
        is malformed, or the adapter does not accept this connection type.
    """
    prefix, _ = split_model_id(model_id)
    adapter_cls = _REGISTERED_ADAPTERS.get(prefix)
    if adapter_cls is None:
        if prefix not in _ADAPTER_MODULES:
            raise UnsupportedOCRProviderError(
                f"{prefix!r} has no OCR adapter. @task.llm_ocr ships adapters for the "
                f"{sorted({*_ADAPTER_MODULES, *_REGISTERED_ADAPTERS})} model_id prefixes."
            )
        module_path, class_name = _ADAPTER_MODULES[prefix]
        adapter_cls = getattr(importlib.import_module(module_path), class_name)
    if conn_type not in adapter_cls.conn_types:
        raise UnsupportedOCRProviderError(
            f"The {prefix!r} OCR adapter does not support connection type {conn_type!r}; it "
            f"accepts {sorted(adapter_cls.conn_types)}."
        )
    return adapter_cls
