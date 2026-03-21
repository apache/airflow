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

from typing import Any, TypeVar

T = TypeVar("T", bool, float, int, dict, list, str, tuple, set)


class StringifyNotSupportedError(ValueError):
    """
    Raised when stringify() cannot handle a serialized object and full deserialization is required.

    This typically occurs when trying to stringify Airflow classes that need to be fully deserialized
    using XComModel.deserialize_value() instead.
    """


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


def stringify(o: T | None) -> object:
    """
    Convert a serialized object to a human readable representation.

    Matches the behavior of deserialize(full=False) exactly:
    - Returns objects as-is for primitives, collections, plain dicts
    - Only returns string when encountering a serialized object (__classname__)
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

    # Early detection for `airflow.` classes. These classes will need full deserialization, not just stringification
    if isinstance(classname, str) and classname.startswith("airflow."):
        raise StringifyNotSupportedError(
            f"Cannot stringify Airflow class '{classname}'. "
            f"Use XComModel.deserialize_value() to deserialize Airflow classes."
        )

    result = _stringify_builtin_collection(classname, value)
    if result is not None:
        return result

    # only return string representation
    s = f"{classname}@version={version}("
    if isinstance(value, _primitives):
        s += f"{value}"
    elif isinstance(value, _builtin_collections):
        # deserialized values can be != str
        s += ",".join(str(stringify(v)) for v in value)
    elif isinstance(value, dict):
        s += ",".join(f"{k}={stringify(v)}" for k, v in value.items())
    s += ")"

    return s
