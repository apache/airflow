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

# lazy loading for performance reasons
serializers = [
    "numpy.int8",
    "numpy.int16",
    "numpy.int32",
    "numpy.int64",
    "numpy.uint8",
    "numpy.uint16",
    "numpy.uint32",
    "numpy.uint64",
    "numpy.float64",
    "numpy.float32",
    "numpy.float16",
    "numpy.complex128",
    "numpy.complex64",
    "numpy.bool",
    "numpy.bool_",
]

if TYPE_CHECKING:
    from airflow.sdk.serialization.serde import U

deserializers = serializers

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    import numpy as np

    if np is None:
        return "", "", 0, False

    name = qualname(o)
    metadata = (name, __version__, True)
    if isinstance(
        o,
        np.int_
        | np.intc
        | np.intp
        | np.int8
        | np.int16
        | np.int32
        | np.int64
        | np.uint8
        | np.uint16
        | np.uint32
        | np.uint64,
    ):
        return int(o), *metadata

    if hasattr(np, "bool") and isinstance(o, np.bool) or isinstance(o, np.bool_):
        return bool(o), *metadata

    if isinstance(o, (np.float16, np.float32, np.float64, np.complex64, np.complex128)):
        return float(o), *metadata

    return "", "", 0, False


def deserialize(cls: type, version: int, data: str) -> Any:
    if version > __version__:
        raise TypeError("serialized version is newer than class version")

    return cls(data)
