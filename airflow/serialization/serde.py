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
import re
import sys
from importlib import import_module
from types import ModuleType
from typing import Any, TypeVar, Union, cast

import attr

import airflow.serialization.serializers
from airflow.configuration import conf
from airflow.stats import Stats
from airflow.utils.module_loading import import_string, iter_namespace, qualname

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

DEFAULT_VERSION = 0

T = TypeVar("T", bool, float, int, dict, list, str, tuple, set)
U = Union[bool, float, int, dict, list, str, tuple, set]
S = Union[list, tuple, set]

_serializers: dict[str, ModuleType] = {}
_deserializers: dict[str, ModuleType] = {}
_extra_allowed: set[str] = set()

_primitives = (int, bool, float, str)
_builtin_collections = (frozenset, list, set, tuple)  # dict is treated specially.


def encode(cls: str, version: int, data: T) -> dict[str, str | int | T]:
    """Encodes o so it can be understood by the deserializer"""
    return {CLASSNAME: cls, VERSION: version, DATA: data}


def decode(d: dict[str, str | int | T]) -> tuple:
    return d[CLASSNAME], d[VERSION], d.get(DATA, None)


def serialize(o: object, depth: int = 0) -> U | None:
    """Serialize an object into a representation consisting only built-in types.

    Primitives (int, float, bool, str) are returned as-is. Built-in collections
    are iterated over, where it is assumed that keys in a dict can be represented
    as str.

    Values that are not of a built-in type are serialized if a serializer is
    found for them. The order in which serializers are used is

    1. A ``serialize`` function provided by the object.
    2. A registered serializer in the namespace of ``airflow.serialization.serializers``
    3. Annotations from attr or dataclass.

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

    # tuples and plain dicts are iterated over recursively
    if isinstance(o, _builtin_collections):
        s = [serialize(d, depth + 1) for d in o]
        if isinstance(o, tuple):
            return tuple(s)
        if isinstance(o, set):
            return set(s)
        return s

    if isinstance(o, dict):
        if CLASSNAME in o or SCHEMA_ID in o:
            raise AttributeError(f"reserved key {CLASSNAME} or {SCHEMA_ID} found in dict to serialize")

        return {str(k): serialize(v, depth + 1) for k, v in o.items()}

    cls = type(o)
    qn = qualname(o)

    # custom serializers
    dct = {
        CLASSNAME: qn,
        VERSION: getattr(cls, "__version__", DEFAULT_VERSION),
    }

    # if there is a builtin serializer available use that
    if qn in _serializers:
        data, classname, version, is_serialized = _serializers[qn].serialize(o)
        if is_serialized:
            return encode(classname, version, serialize(data, depth + 1))

    # object / class brings their own
    if hasattr(o, "serialize"):
        data = getattr(o, "serialize")()

        # if we end up with a structure, ensure its values are serialized
        if isinstance(data, dict):
            data = serialize(data, depth + 1)

        dct[DATA] = data
        return dct

    # dataclasses
    if dataclasses.is_dataclass(cls):
        data = dataclasses.asdict(o)
        dct[DATA] = serialize(data, depth + 1)
        return dct

    # attr annotated
    if attr.has(cls):
        # Only include attributes which we can pass back to the classes constructor
        data = attr.asdict(cast(attr.AttrsInstance, o), recurse=True, filter=lambda a, v: a.init)
        dct[DATA] = serialize(data, depth + 1)
        return dct

    raise TypeError(f"cannot serialize object of type {cls}")


def deserialize(o: T | None, full=True, type_hint: Any = None) -> object:
    """
    Deserializes an object of primitive type T into an object. Uses an allow
    list to determine if a class can be loaded.

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

    if isinstance(o, _builtin_collections):
        return [deserialize(d) for d in o]

    if not isinstance(o, dict):
        raise TypeError()

    o = _convert(o)

    # plain dict and no type hint
    if CLASSNAME not in o and not type_hint or VERSION not in o:
        return {str(k): deserialize(v, full) for k, v in o.items()}

    # custom deserialization starts here
    cls: Any
    version = 0
    value: Any
    classname: str

    if type_hint:
        cls = type_hint
        classname = qualname(cls)
        version = 0  # type hinting always sets version to 0
        value = o

    if CLASSNAME in o and VERSION in o:
        classname, version, value = decode(o)
        if not _match(classname) and classname not in _extra_allowed:
            raise ImportError(
                f"{classname} was not found in allow list for deserialization imports. "
                f"To allow it, add it to allowed_deserialization_classes in the configuration"
            )

        if full:
            cls = import_string(classname)

    # only return string representation
    if not full:
        return _stringify(classname, version, value)

    # registered deserializer
    if classname in _deserializers:
        return _deserializers[classname].deserialize(classname, version, deserialize(value))

    # class has deserialization function
    if hasattr(cls, "deserialize"):
        return getattr(cls, "deserialize")(deserialize(value), version)

    # attr or dataclass
    if attr.has(cls) or dataclasses.is_dataclass(cls):
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
    """Converts an old style serialization to new style"""
    if OLD_TYPE in old and OLD_DATA in old:
        return {CLASSNAME: old[OLD_TYPE], VERSION: DEFAULT_VERSION, DATA: old[OLD_DATA][OLD_DATA]}

    return old


def _match(classname: str) -> bool:
    return any(p.match(classname) is not None for p in _get_patterns())


def _stringify(classname: str, version: int, value: T | None) -> str:
    s = f"{classname}@version={version}("
    if isinstance(value, _primitives):
        s += f"{value})"
    elif isinstance(value, _builtin_collections):
        s += ",".join(str(serialize(value)))
    elif isinstance(value, dict):
        for k, v in value.items():
            s += f"{k}={serialize(v)},"
        s = s[:-1] + ")"

    return s


def _register():
    """Register builtin serializers and deserializers for types that don't have any themselves"""
    _serializers.clear()
    _deserializers.clear()

    with Stats.timer("serde.load_serializers") as timer:
        for _, name, _ in iter_namespace(airflow.serialization.serializers):
            name = import_module(name)
            for s in getattr(name, "serializers", list()):
                if not isinstance(s, str):
                    s = qualname(s)
                if s in _serializers and _serializers[s] != name:
                    raise AttributeError(f"duplicate {s} for serialization in {name} and {_serializers[s]}")
                log.debug("registering %s for serialization", s)
                _serializers[s] = name
            for d in getattr(name, "deserializers", list()):
                if not isinstance(d, str):
                    d = qualname(d)
                if d in _deserializers and _deserializers[d] != name:
                    raise AttributeError(f"duplicate {d} for deserialization in {name} and {_serializers[d]}")
                log.debug("registering %s for deserialization", d)
                _deserializers[d] = name
                _extra_allowed.add(d)

    log.info("loading serializers took %.3f seconds", timer.duration)


@functools.lru_cache(maxsize=None)
def _get_patterns() -> list[re.Pattern]:
    patterns = conf.get("core", "allowed_deserialization_classes").split()
    return [re.compile(re.sub(r"(\w)\.", r"\1\..", p)) for p in patterns]


_register()
