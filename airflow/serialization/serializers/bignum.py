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

from typing import TYPE_CHECKING

from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    import decimal

    from airflow.serialization.serde import U


serializers = ["decimal.Decimal"]
deserializers = serializers

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    from decimal import Decimal

    if not isinstance(o, Decimal):
        return "", "", 0, False
    name = qualname(o)
    _, _, exponent = o.as_tuple()
    if isinstance(exponent, int) and exponent >= 0:  # No digits after the decimal point.
        return int(o), name, __version__, True
    # Technically lossy due to floating point errors, but the best we
    # can do without implementing a custom encode function.
    return float(o), name, __version__, True


def deserialize(classname: str, version: int, data: object) -> decimal.Decimal:
    from decimal import Decimal

    if version > __version__:
        raise TypeError(f"serialized {version} of {classname} > {__version__}")

    if classname != qualname(Decimal):
        raise TypeError(f"{classname} != {qualname(Decimal)}")

    return Decimal(str(data))
