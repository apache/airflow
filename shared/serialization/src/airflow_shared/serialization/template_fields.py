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

import inspect
from typing import TYPE_CHECKING, Protocol, TypeGuard

from ..module_loading import qualname

if TYPE_CHECKING:
    from pydantic import JsonValue


class SupportsSerialize(Protocol):
    """An object with a no-argument serialization method."""

    def serialize(self) -> object: ...


class SupportsToDict(Protocol):
    """An object with a no-argument dictionary conversion method."""

    def to_dict(self) -> object: ...


def has_serialize(obj: object) -> TypeGuard[SupportsSerialize]:
    """Check for a callable serialize attribute without invoking user attribute hooks."""
    return callable(inspect.getattr_static(obj, "serialize", None))


def has_to_dict(obj: object) -> TypeGuard[SupportsToDict]:
    """Check for a callable to_dict attribute without invoking user attribute hooks."""
    return callable(inspect.getattr_static(obj, "to_dict", None))


def normalize_template_field(template_field: object) -> JsonValue:
    """
    Normalize a template field into a deterministic JSON-compatible structure.

    Rebuild containers with primitive leaves, sort dictionary keys and set elements,
    and represent callables by qualified name. Objects with conversion methods are
    normalized recursively; other objects retain custom textual representations or
    use qualified-name markers instead of memory addresses.
    """

    def normalize_dict_key(key: object) -> str:
        # JSON keys must be strings so json.dumps(..., sort_keys=True) accepts mixed-type keys.
        return str(serialize_object(key))

    def serialize_object(obj: object) -> JsonValue:
        if obj is None or isinstance(obj, (str, int, float, bool)):
            return obj

        if isinstance(obj, dict):
            serialized_pairs = [(normalize_dict_key(k), serialize_object(v)) for k, v in obj.items()]
            return dict(sorted(serialized_pairs, key=lambda kv: kv[0]))

        if isinstance(obj, (list, tuple)):
            return [serialize_object(item) for item in obj]

        if isinstance(obj, (set, frozenset)):
            serialized_set = [serialize_object(e) for e in obj]
            return sorted(serialized_set, key=lambda x: (type(x).__name__, str(x)))

        if has_serialize(obj):
            return serialize_object(obj.serialize())

        # Restrict to_dict() to Kubernetes clients so unrelated user classes keep the normal fallback.
        if getattr(type(obj), "__module__", "").startswith(
            ("kubernetes.", "kubernetes_asyncio.")
        ) and has_to_dict(obj):
            return serialize_object(obj.to_dict())

        if callable(obj):
            # Default callable reprs contain memory addresses that change the Dag hash across parses.
            return f"<callable {qualname(obj, True)}>"

        if type(obj).__str__ is not object.__str__ or type(obj).__repr__ is not object.__repr__:
            return str(obj)

        # Plain object reprs also embed memory addresses; custom representations above are intentional.
        return f"<{qualname(type(obj), True)} object>"

    return serialize_object(template_field)
