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
"""Apply Airflow's secret masker to what a tool hands back to a model."""

from __future__ import annotations

import dataclasses
import json
from typing import Any, overload

from pydantic import BaseModel
from pydantic_ai.messages import MULTI_MODAL_CONTENT_TYPES
from pydantic_core import to_jsonable_python

from airflow.providers.common.compat.sdk import redact

# Stands in for a container that contains itself, which would otherwise recurse forever.
_CYCLE = "<circular reference>"


@overload
def mask_secrets(value: str) -> str: ...


@overload
def mask_secrets(value: Any) -> Any: ...


def mask_secrets(value: Any) -> Any:
    """
    Return ``value`` with every secret Airflow has registered replaced by ``***``.

    Strings are masked wherever they sit in nested dicts, lists, tuples, sets and dataclasses,
    dict keys included, and the shape and types are kept. Bytes are masked as UTF-8 text. A
    Pydantic model is turned into the JSON-compatible data the model would be shown. Images,
    documents and other multimodal content, and any other object, pass through as they are.
    Registered secrets are the ones Airflow knows about, such as connection passwords and
    sensitive connection extras; a credential that only appears in the data itself is not
    recognized.

    ``redact()`` does part of this, but stops descending at a fixed depth, and it hides every
    string under a key that looks sensitive: a model reading a query result needs
    ``{"auth_type": "oauth"}`` as it is. Two dict keys that both mask to ``***`` collapse into
    one.
    """
    return _mask(value, frozenset())


def dumps_masked(value: Any, **kwargs: Any) -> str:
    """
    Serialize ``value`` to JSON for a model, with registered secrets masked first.

    Masking a JSON string afterwards is not enough: JSON escapes quotes, backslashes,
    control characters and, by default, non-ASCII characters, so a password containing
    any of them no longer matches the registered value once it is inside the document.
    Bytes become their UTF-8 text and dataclasses their fields; any other object JSON
    cannot represent is rendered with ``str()``, after the values inside it are masked.

    :param kwargs: Passed to :func:`json.dumps`.
    """
    return json.dumps(mask_secrets(value), default=_json_default, **kwargs)


def _json_default(value: object) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    return mask_secrets(str(value))


def _mask(value: Any, seen: frozenset[int]) -> Any:
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, bytes):
        text = value.decode("utf-8", "surrogateescape")
        masked = mask_secrets(text)
        return value if masked == text else masked.encode("utf-8", "surrogateescape")
    if isinstance(value, BaseModel):
        return _mask(to_jsonable_python(value), seen)
    if isinstance(value, (dict, list, tuple, set, frozenset)) or _is_masked_dataclass(value):
        if id(value) in seen:
            return _CYCLE
        seen = seen | {id(value)}
    if isinstance(value, dict):
        return {_mask(key, seen): _mask(item, seen) for key, item in value.items()}
    if isinstance(value, list):
        return [_mask(item, seen) for item in value]
    if isinstance(value, tuple):
        return tuple(_mask(item, seen) for item in value)
    if isinstance(value, (set, frozenset)):
        return type(value)(_mask(item, seen) for item in value)
    if _is_masked_dataclass(value):
        # Rebuilt rather than turned into a dict, so a type the framework acts on, such as
        # pydantic-ai's TextContent, still is one.
        fields = [field for field in dataclasses.fields(value) if field.init]
        return dataclasses.replace(value, **{f.name: _mask(getattr(value, f.name), seen) for f in fields})
    return value


def _is_masked_dataclass(value: object) -> bool:
    return (
        dataclasses.is_dataclass(value)
        and not isinstance(value, type)
        and not isinstance(value, MULTI_MODAL_CONTENT_TYPES)
    )
