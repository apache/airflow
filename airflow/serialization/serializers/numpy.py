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

from typing import TYPE_CHECKING, Any

from airflow.utils.module_loading import qualname

serializers = []

try:
    import numpy as np

    serializers = [
        np.int_,
        np.intc,
        np.intp,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.bool_,
        np.float_,
        np.float16,
        np.float64,
        np.complex_,
        np.complex64,
        np.complex128,
    ]
except ImportError:
    np = None  # type: ignore


if TYPE_CHECKING:
    from airflow.serialization.serde import U

deserializers: list = serializers
_deserializers: dict[str, type[object]] = {qualname(x): x for x in deserializers}

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    if np is None:
        return "", "", 0, False

    name = qualname(o)
    if isinstance(
        o,
        (
            np.int_,
            np.intc,
            np.intp,
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            np.uint8,
            np.uint16,
            np.uint32,
            np.uint64,
        ),
    ):
        return int(o), name, __version__, True

    if isinstance(o, np.bool_):
        return bool(np), name, __version__, True

    if isinstance(
        o, (np.float_, np.float16, np.float32, np.float64, np.complex_, np.complex64, np.complex128)
    ):
        return float(o), name, __version__, True

    return "", "", 0, False


def deserialize(classname: str, version: int, data: str) -> Any:
    if version > __version__:
        raise TypeError("serialized version is newer than class version")

    f = _deserializers.get(classname, None)
    if callable(f):
        return f(data)  # type: ignore [call-arg]

    raise TypeError(f"unsupported {classname} found for numpy deserialization")
