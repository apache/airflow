#
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

import re
from typing import Any, TypeVar

T = TypeVar("T", bool, float, int, dict, list, str, tuple, set)

# DagBag prefixes user-DAG modules with ``unusual_prefix_<40-char-sha>_`` so two
# DAG files with the same name in different bundles don't clash in ``sys.modules``.
# That prefix is deterministic and load-bearing for round-trip deserialization,
# but it has no place in the human-readable XCom value rendering.
_DAGBAG_PREFIX_RE = re.compile(r"unusual_prefix_[a-f0-9]{40}_")

_FIELD_RENDERED_MODULE = "airflow.sdk.definitions.asset."


CLASSNAME = "__classname__"
VERSION = "__version__"
DATA = "__data__"
OLD_TYPE = "__type"
OLD_DATA = "__var"
OLD_DICT = "dict"
DEFAULT_VERSION = 0

_primitives = (int, bool, float, str)
_builtin_collections = (frozenset, list, set, tuple)


def _convert(old: dict) -> dict:
    """Convert an old style serialization to new style."""
    if OLD_TYPE in old and OLD_DATA in old:
        # Return old style dicts directly as they do not need wrapping
        if old[OLD_TYPE] == OLD_DICT:
            return old[OLD_DATA]
        return {CLASSNAME: old[OLD_TYPE], VERSION: DEFAULT_VERSION, DATA: old[OLD_DATA]}

    return old


def decode(d: dict[str, Any]) -> tuple[str, int, Any]:
    """Extract classname, version, and data from a serialized dict."""
    classname = d[CLASSNAME]
    version = d[VERSION]

    if not isinstance(classname, str) or not isinstance(version, int):
        raise ValueError(f"cannot decode {d!r}")

    data = d.get(DATA)

    return classname, version, data


def _stringify_builtin_collection(classname: str, value: Any):
    if classname in ("builtins.tuple", "builtins.set", "builtins.frozenset"):
        if isinstance(value, (list, tuple, set, frozenset)):
            items = [str(stringify(v)) for v in value]
            return f"({','.join(items)})"
    return None


def _render_builtin_collection_field(o: dict) -> str | None:
    """Render a serialized tuple, set or frozenset field the way ``repr`` renders it, else ``None``."""
    classname, data = o[CLASSNAME], o.get(DATA)
    if classname not in ("builtins.tuple", "builtins.set", "builtins.frozenset") or not isinstance(
        data, (list, tuple, set, frozenset)
    ):
        return None
    items = ", ".join(_render_field(v) for v in data)
    if classname == "builtins.tuple":
        return f"({items},)" if len(data) == 1 else f"({items})"
    if classname == "builtins.set":
        return f"{{{items}}}" if data else "set()"
    return f"frozenset({{{items}}})" if data else "frozenset()"


def _render_field(o: Any) -> str:
    """Render a field value of an Airflow object the way ``repr`` would render the deserialized value."""
    if isinstance(o, str):
        return repr(o)
    if isinstance(o, list):
        return f"[{', '.join(_render_field(v) for v in o)}]"
    if isinstance(o, dict):
        o = _convert(o)
        if CLASSNAME in o and VERSION in o:
            if (rendered := _render_builtin_collection_field(o)) is not None:
                return rendered
            return str(stringify(o))
        return "{" + ", ".join(f"{k!r}: {_render_field(v)}" for k, v in o.items()) + "}"
    return str(stringify(o))


def _stringify_airflow_object(classname: str, value: Any) -> str:
    """
    Render an Airflow class from its serialized fields, e.g. ``AssetAlias(name='x', group='asset')``.

    The class is neither imported nor instantiated, so whether it is an attrs type, whose serialized
    fields mirror its ``repr``, cannot be checked here. Fields are rendered only for the asset
    definitions, which are attrs types. Other Airflow classes may define ``serialize()`` returning
    internal state, such as the storage options of ``ObjectStoragePath``; they are shown by their
    ``path`` or ``uri`` field only.
    """
    short_name = classname.rsplit(".", 1)[-1]
    if not classname.startswith(_FIELD_RENDERED_MODULE):
        locator = next(
            (value[k] for k in ("path", "uri") if isinstance(value, dict) and isinstance(value.get(k), str)),
            None,
        )
        return f"{short_name}({locator!r})" if locator is not None else f"{short_name}(...)"
    if value is None:
        args = ""
    elif isinstance(value, dict):
        args = ", ".join(f"{k}={_render_field(v)}" for k, v in value.items())
    elif isinstance(value, _builtin_collections):
        args = ", ".join(_render_field(v) for v in value)
    else:
        args = _render_field(value)
    return f"{short_name}({args})"


def stringify(o: T | None) -> object:
    """
    Convert a serialized object to a human readable representation without importing any class.

    Matches the behavior of deserialize(full=False), except for Airflow classes:
    - Returns objects as-is for primitives, collections, plain dicts
    - Only returns string when encountering a serialized object (__classname__)
    - Asset definitions are rendered like their ``repr``, e.g. ``AssetAlias(name='x', group='asset')``
    - Other Airflow classes are rendered by their path or URI only, e.g. ``ObjectStoragePath('s3://b/k')``
    """
    if o is None:
        return o

    if isinstance(o, _primitives):
        return o

    # tuples, sets are included here for backwards compatibility
    if isinstance(o, _builtin_collections):
        col = [stringify(d) for d in o]
        if isinstance(o, tuple):
            return tuple(col)

        if isinstance(o, set):
            return set(col)

        return col

    if not isinstance(o, dict):
        # if o is not a dict, then it's already deserialized
        # in this case we should return it as is
        return o

    o = _convert(o)

    # plain dict and no type hint
    if CLASSNAME not in o or VERSION not in o:
        return {str(k): stringify(v) for k, v in o.items()}

    classname, version, value = decode(o)

    if not classname:
        raise TypeError("classname cannot be empty")

    if classname.startswith("airflow."):
        return _stringify_airflow_object(classname, value)

    result = _stringify_builtin_collection(classname, value)
    if result is not None:
        return result

    # only return string representation
    display_classname = _DAGBAG_PREFIX_RE.sub("", classname)
    s = f"{display_classname}@version={version}("
    if isinstance(value, _primitives):
        s += f"{value}"
    elif isinstance(value, _builtin_collections):
        # deserialized values can be != str
        s += ",".join(str(stringify(v)) for v in value)
    elif isinstance(value, dict):
        # Render string field values with ``repr`` so the output reads like a
        # Pydantic/dataclass instance (``field='value'``) instead of an
        # ambiguous ``field=value`` that could be mistaken for a bare token.
        # Non-string field values keep their natural rendering (numbers stay
        # bare, nested serialized objects keep their own ``ClassName@...`` form).
        parts = []
        for k, v in value.items():
            rendered = stringify(v)
            if isinstance(v, str):
                parts.append(f"{k}={v!r}")
            else:
                parts.append(f"{k}={rendered}")
        s += ", ".join(parts)
    s += ")"

    return s
