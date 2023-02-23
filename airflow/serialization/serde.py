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
from typing import Any, Callable, TypeVar, Union, cast

import attr

from airflow.configuration import conf
from airflow.utils.module_loading import import_string, qualname

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
    serializer = _find_airflow_serializer(o)
    if serializer is not None:
        data, classname, version, is_serialized = serializer(o)
        if is_serialized:
            return encode(classname, version, serialize(data, depth + 1))

    # object / class brings their own
    if hasattr(o, "serialize"):
        data = getattr(o, "serialize")()

        # if we end up with a structure, ensure its values are serialized
        if isinstance(data, dict):
            dct[DATA] = serialize(data, depth + 1)
        else:
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

    # only return string representation
    if not full:
        return _stringify(classname, version, value)

    # registered deserializer
    deserializer = _find_airflow_deserializer(classname)
    if deserializer is not None:
        return deserializer(classname, version, deserialize(value))

    # not registered; load ad-hoc class if allowed
    if not _match(classname):
        raise ImportError(
            f"{classname} was not found in allow list for deserialization imports. "
            f"To allow it, add it to allowed_deserialization_classes in the configuration"
        )
    cls = import_string(classname)

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


@functools.lru_cache(maxsize=None)
def _get_patterns() -> list[re.Pattern]:
    patterns = conf.get("core", "allowed_deserialization_classes").split()
    return [re.compile(re.sub(r"(\w)\.", r"\1\..", p)) for p in patterns]


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


_AIRFLOW_SERIALIZERS = {
    "decimal": {"Decimal": "airflow.serialization.serializers.bignum"},
    "datetime": {
        "date": "airflow.serialization.serializers.datetime",
        "datetime": "airflow.serialization.serializers.datetime",
        "timedelta": "airflow.serialization.serializers.datetime",
    },
    "numpy": {
        "bool_": "airflow.serialization.serializers.numpy",
        "complex64": "airflow.serialization.serializers.numpy",
        "complex128": "airflow.serialization.serializers.numpy",
        "float16": "airflow.serialization.serializers.numpy",
        "float64": "airflow.serialization.serializers.numpy",
        "intc": "airflow.serialization.serializers.numpy",
        "intp": "airflow.serialization.serializers.numpy",
        "int8": "airflow.serialization.serializers.numpy",
        "int16": "airflow.serialization.serializers.numpy",
        "int32": "airflow.serialization.serializers.numpy",
        "int64": "airflow.serialization.serializers.numpy",
        "uint8": "airflow.serialization.serializers.numpy",
        "uint16": "airflow.serialization.serializers.numpy",
        "uint32": "airflow.serialization.serializers.numpy",
        "uint64": "airflow.serialization.serializers.numpy",
    },
    "pendulum": {
        "datetime.DateTime": "airflow.serialization.serializers.datetime",
        "tz.timezone.FixedTimezone": "airflow.serialization.serializers.timezone",
        "tz.timezone.Timezone": "airflow.serialization.serializers.timezone",
    },
}

_AIRFLOW_DESERIALIZERS = {
    "decimal": {"Decimal": "airflow.serialization.serializers.bignum"},
    "datetime": {
        "date": "airflow.serialization.serializers.datetime",
        "datetime": "airflow.serialization.serializers.datetime",
        "timedelta": "airflow.serialization.serializers.datetime",
    },
    "kubernetes": {
        "client.models.v1_resource_requirements.V1ResourceRequirements": (
            "airflow.serialization.serializers.kubernetes"
        ),
        "client.models.v1_pod.V1Pod": "airflow.serialization.serializers.kubernetes",
    },
    "numpy": {
        "bool_": "airflow.serialization.serializers.numpy",
        "complex64": "airflow.serialization.serializers.numpy",
        "complex128": "airflow.serialization.serializers.numpy",
        "float16": "airflow.serialization.serializers.numpy",
        "float64": "airflow.serialization.serializers.numpy",
        "intc": "airflow.serialization.serializers.numpy",
        "intp": "airflow.serialization.serializers.numpy",
        "int8": "airflow.serialization.serializers.numpy",
        "int16": "airflow.serialization.serializers.numpy",
        "int32": "airflow.serialization.serializers.numpy",
        "int64": "airflow.serialization.serializers.numpy",
        "uint8": "airflow.serialization.serializers.numpy",
        "uint16": "airflow.serialization.serializers.numpy",
        "uint32": "airflow.serialization.serializers.numpy",
        "uint64": "airflow.serialization.serializers.numpy",
    },
    "pendulum": {
        "datetime.DateTime": "airflow.serialization.serializers.datetime",
        "tz.timezone.FixedTimezone": "airflow.serialization.serializers.timezone",
        "tz.timezone.Timezone": "airflow.serialization.serializers.timezone",
    },
}


def _find_airflow_serializer(value: object) -> None | Callable[[object], tuple[U, str, int, bool]]:
    for klass in type(value).__mro__:
        try:
            top_level = qualname(klass).split(".", 1)[0]
            serializers = _AIRFLOW_SERIALIZERS[top_level]
        except (KeyError, ValueError):
            continue
        for class_path, serde_path in serializers.items():
            klass = import_string(f"{top_level}.{class_path}")
            if not isinstance(value, klass):
                continue
            return getattr(import_module(serde_path), "serialize")
    return None


def _find_airflow_deserializer(classname: str) -> None | Callable[[str, int, object], object]:
    try:
        top_level = classname.split(".", 1)[0]
        deserializers = _AIRFLOW_DESERIALIZERS[top_level]
    except (KeyError, ValueError):
        return None
    for class_path, serde_path in deserializers.items():
        if classname == f"{top_level}.{class_path}":
            return getattr(import_module(serde_path), "deserialize")
    return None
