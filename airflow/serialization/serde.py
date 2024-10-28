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

import dataclasses
import enum
import functools
import logging
import sys
from fnmatch import fnmatch
from importlib import import_module
from typing import TYPE_CHECKING, Any, Pattern, TypeVar, Union, cast

import attr
import re2

import airflow.serialization.serializers
from airflow.configuration import conf
from airflow.stats import Stats
from airflow.utils.module_loading import import_string, iter_namespace, qualname

if TYPE_CHECKING:
    from types import ModuleType

log = logging.getLogger(__name__)

MAX_RECURSION_DEPTH = sys.getrecursionlimit() - 1

CLASSNAME = "__classname__"
VERSION = "__version__"
DATA = "__data__"
SCHEMA_ID = "__id__"
CACHE = "__cache__"

OLD_TYPE = "__type"
OLD_SOURCE = "__source"
OLD_DATA = "__var"
OLD_DICT = "dict"

DEFAULT_VERSION = 0

T = TypeVar("T", bool, float, int, dict, list, str, tuple, set)
U = Union[bool, float, int, dict, list, str, tuple, set]
S = Union[list, tuple, set]

_serializers: dict[str, ModuleType] = {}
_deserializers: dict[str, ModuleType] = {}
_stringifiers: dict[str, ModuleType] = {}
_extra_allowed: set[str] = set()

_primitives = (int, bool, float, str)
_builtin_collections = (frozenset, list, set, tuple)  # dict is treated specially.


def encode(cls: str, version: int, data: T) -> dict[str, str | int | T]:
    """Encode an object so it can be understood by the deserializer."""
    return {CLASSNAME: cls, VERSION: version, DATA: data}


def decode(d: dict[str, Any]) -> tuple[str, int, Any]:
    classname = d[CLASSNAME]
    version = d[VERSION]

    if not isinstance(classname, str) or not isinstance(version, int):
        raise ValueError(f"cannot decode {d!r}")

    data = d.get(DATA)

    return classname, version, data


def serialize(o: object, depth: int = 0) -> U | None:
    """
    Serialize an object into a representation consisting only built-in types.

    Primitives (int, float, bool, str) are returned as-is. Built-in collections
    are iterated over, where it is assumed that keys in a dict can be represented
    as str.

    Values that are not of a built-in type are serialized if a serializer is
    found for them. The order in which serializers are used is

    1. A ``serialize`` function provided by the object.
    2. A registered serializer in the namespace of ``airflow.serialization.serializers``
    3. Annotations from attr or dataclass.

    Limitations: attr and dataclass objects can lose type information for nested objects
    as they do not store this when calling ``asdict``. This means that at deserialization values
    will be deserialized as a dict as opposed to reinstating the object. Provide
    your own serializer to work around this.

    :param o: The object to serialize.
    :param depth: Private tracker for nested serialization.
    :raise TypeError: A serializer cannot be found.
    :raise RecursionError: The object is too nested for the function to handle.
    :return: A representation of ``o`` that consists of only built-in types.
    """
    if depth == MAX_RECURSION_DEPTH:
        raise RecursionError("maximum recursion depth reached for serialization")

    # None remains None
    if o is None:
        return o

    # primitive types are returned as is
    if isinstance(o, _primitives):
        if isinstance(o, enum.Enum):
            return o.value

        return o

    if isinstance(o, list):
        return [serialize(d, depth + 1) for d in o]

    if isinstance(o, dict):
        if CLASSNAME in o or SCHEMA_ID in o:
            raise AttributeError(
                f"reserved key {CLASSNAME} or {SCHEMA_ID} found in dict to serialize"
            )

        return {str(k): serialize(v, depth + 1) for k, v in o.items()}

    cls = type(o)
    qn = qualname(o)
    classname = None

    # Serialize namedtuple like tuples
    # We also override the classname returned by the builtin.py serializer. The classname
    # has to be "builtins.tuple", so that the deserializer can deserialize the object into tuple.
    if _is_namedtuple(o):
        qn = "builtins.tuple"
        classname = qn

    # if there is a builtin serializer available use that
    if qn in _serializers:
        data, serialized_classname, version, is_serialized = _serializers[qn].serialize(o)
        if is_serialized:
            return encode(
                classname or serialized_classname, version, serialize(data, depth + 1)
            )

    # custom serializers
    dct = {
        CLASSNAME: qn,
        VERSION: getattr(cls, "__version__", DEFAULT_VERSION),
    }

    # object / class brings their own
    if hasattr(o, "serialize"):
        data = getattr(o, "serialize")()

        # if we end up with a structure, ensure its values are serialized
        if isinstance(data, dict):
            data = serialize(data, depth + 1)

        dct[DATA] = data
        return dct

    # pydantic models are recursive
    if _is_pydantic(cls):
        data = o.model_dump()  # type: ignore[attr-defined]
        dct[DATA] = serialize(data, depth + 1)
        return dct

    # dataclasses
    if dataclasses.is_dataclass(cls):
        # fixme: unfortunately using asdict with nested dataclasses it looses information
        data = dataclasses.asdict(o)  # type: ignore[call-overload]
        dct[DATA] = serialize(data, depth + 1)
        return dct

    # attr annotated
    if attr.has(cls):
        # Only include attributes which we can pass back to the classes constructor
        data = attr.asdict(
            cast(attr.AttrsInstance, o), recurse=False, filter=lambda a, v: a.init
        )
        dct[DATA] = serialize(data, depth + 1)
        return dct

    raise TypeError(f"cannot serialize object of type {cls}")


def deserialize(o: T | None, full=True, type_hint: Any = None) -> object:
    """
    Deserialize an object of primitive type and uses an allow list to determine if a class can be loaded.

    :param o: primitive to deserialize into an arbitrary object.
    :param full: if False it will return a stringified representation
                 of an object and will not load any classes
    :param type_hint: if set it will be used to help determine what
                 object to deserialize in. It does not override if another
                 specification is found
    :return: object
    """
    if o is None:
        return o

    if isinstance(o, _primitives):
        return o

    # tuples, sets are included here for backwards compatibility
    if isinstance(o, _builtin_collections):
        col = [deserialize(d) for d in o]
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
    if CLASSNAME not in o and not type_hint or VERSION not in o:
        return {str(k): deserialize(v, full) for k, v in o.items()}

    # custom deserialization starts here
    cls: Any
    version = 0
    value: Any = None
    classname = ""

    if type_hint:
        cls = type_hint
        classname = qualname(cls)
        version = 0  # type hinting always sets version to 0
        value = o

    if CLASSNAME in o and VERSION in o:
        classname, version, value = decode(o)

    if not classname:
        raise TypeError("classname cannot be empty")

    # only return string representation
    if not full:
        return _stringify(classname, version, value)
    if not _match(classname) and classname not in _extra_allowed:
        raise ImportError(
            f"{classname} was not found in allow list for deserialization imports. "
            f"To allow it, add it to allowed_deserialization_classes in the configuration"
        )

    cls = import_string(classname)

    # registered deserializer
    if classname in _deserializers:
        return _deserializers[classname].deserialize(
            classname, version, deserialize(value)
        )

    # class has deserialization function
    if hasattr(cls, "deserialize"):
        return getattr(cls, "deserialize")(deserialize(value), version)

    # attr or dataclass or pydantic
    if attr.has(cls) or dataclasses.is_dataclass(cls) or _is_pydantic(cls):
        class_version = getattr(cls, "__version__", 0)
        if int(version) > class_version:
            raise TypeError(
                "serialized version of %s is newer than module version (%s > %s)",
                classname,
                version,
                class_version,
            )

        return cls(**deserialize(value))

    # no deserializer available
    raise TypeError(f"No deserializer found for {classname}")


def _convert(old: dict) -> dict:
    """Convert an old style serialization to new style."""
    if OLD_TYPE in old and OLD_DATA in old:
        # Return old style dicts directly as they do not need wrapping
        if old[OLD_TYPE] == OLD_DICT:
            return old[OLD_DATA]
        else:
            return {
                CLASSNAME: old[OLD_TYPE],
                VERSION: DEFAULT_VERSION,
                DATA: old[OLD_DATA],
            }

    return old


def _match(classname: str) -> bool:
    """Check if the given classname matches a path pattern either using glob format or regexp format."""
    return _match_glob(classname) or _match_regexp(classname)


@functools.lru_cache(maxsize=None)
def _match_glob(classname: str):
    """Check if the given classname matches a pattern from allowed_deserialization_classes using glob syntax."""
    patterns = _get_patterns()
    return any(fnmatch(classname, p.pattern) for p in patterns)


@functools.lru_cache(maxsize=None)
def _match_regexp(classname: str):
    """Check if the given classname matches a pattern from allowed_deserialization_classes_regexp using regexp."""
    patterns = _get_regexp_patterns()
    return any(p.match(classname) is not None for p in patterns)


def _stringify(classname: str, version: int, value: T | None) -> str:
    """
    Convert a previously serialized object in a somewhat human-readable format.

    This function is not designed to be exact, and will not extensively traverse
    the whole tree of an object.
    """
    if classname in _stringifiers:
        return _stringifiers[classname].stringify(classname, version, value)

    s = f"{classname}@version={version}("
    if isinstance(value, _primitives):
        s += f"{value}"
    elif isinstance(value, _builtin_collections):
        # deserialized values can be != str
        s += ",".join(str(deserialize(value, full=False)))
    elif isinstance(value, dict):
        s += ",".join(f"{k}={deserialize(v, full=False)}" for k, v in value.items())
    s += ")"

    return s


def _is_pydantic(cls: Any) -> bool:
    """
    Return True if the class is a pydantic model.

    Checking is done by attributes as it is significantly faster than
    using isinstance.
    """
    return (
        hasattr(cls, "model_config")
        and hasattr(cls, "model_fields")
        and hasattr(cls, "model_fields_set")
    )


def _is_namedtuple(cls: Any) -> bool:
    """
    Return True if the class is a namedtuple.

    Checking is done by attributes as it is significantly faster than
    using isinstance.
    """
    return (
        hasattr(cls, "_asdict")
        and hasattr(cls, "_fields")
        and hasattr(cls, "_field_defaults")
    )


def _register():
    """Register builtin serializers and deserializers for types that don't have any themselves."""
    _serializers.clear()
    _deserializers.clear()
    _stringifiers.clear()

    with Stats.timer("serde.load_serializers") as timer:
        for _, name, _ in iter_namespace(airflow.serialization.serializers):
            name = import_module(name)
            for s in getattr(name, "serializers", ()):
                if not isinstance(s, str):
                    s = qualname(s)
                if s in _serializers and _serializers[s] != name:
                    raise AttributeError(
                        f"duplicate {s} for serialization in {name} and {_serializers[s]}"
                    )
                log.debug("registering %s for serialization", s)
                _serializers[s] = name
            for d in getattr(name, "deserializers", ()):
                if not isinstance(d, str):
                    d = qualname(d)
                if d in _deserializers and _deserializers[d] != name:
                    raise AttributeError(
                        f"duplicate {d} for deserialization in {name} and {_serializers[d]}"
                    )
                log.debug("registering %s for deserialization", d)
                _deserializers[d] = name
                _extra_allowed.add(d)
            for c in getattr(name, "stringifiers", ()):
                if not isinstance(c, str):
                    c = qualname(c)
                if c in _deserializers and _deserializers[c] != name:
                    raise AttributeError(
                        f"duplicate {c} for stringifiers in {name} and {_stringifiers[c]}"
                    )
                log.debug("registering %s for stringifying", c)
                _stringifiers[c] = name

    log.debug("loading serializers took %.3f seconds", timer.duration)


@functools.lru_cache(maxsize=None)
def _get_patterns() -> list[Pattern]:
    return [
        re2.compile(p)
        for p in conf.get("core", "allowed_deserialization_classes").split()
    ]


@functools.lru_cache(maxsize=None)
def _get_regexp_patterns() -> list[Pattern]:
    return [
        re2.compile(p)
        for p in conf.get("core", "allowed_deserialization_classes_regexp").split()
    ]


_register()
