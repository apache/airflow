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

import pendulum
from pendulum.tz.timezone import FixedTimezone, Timezone

from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    from airflow.serialization.serde import U


serializers = [FixedTimezone, Timezone]
deserializers = serializers

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    """Encode a Pendulum Timezone for serialization.

    Airflow only supports timezone objects that implements Pendulum's Timezone
    interface. We try to keep as much information as possible to make conversion
    round-tripping possible (see ``decode_timezone``). We need to special-case
    UTC; Pendulum implements it as a FixedTimezone (i.e. it gets encoded as
    0 without the special case), but passing 0 into ``pendulum.timezone`` does
    not give us UTC (but ``+00:00``).
    """
    name = qualname(o)
    if isinstance(o, FixedTimezone):
        if o.offset == 0:
            return "UTC", name, __version__, True
        return o.offset, name, __version__, True

    if isinstance(o, Timezone):
        return o.name, name, __version__, True

    return "", "", 0, False


def deserialize(classname: str, version: int, data: object) -> Timezone:
    if not isinstance(data, (str, int)):
        raise TypeError(f"{data} is not of type int or str but of {type(data)}")

    if version > __version__:
        raise TypeError(f"serialized {version} of {classname} > {__version__}")

    if isinstance(data, int):
        return pendulum.tz.fixed_timezone(data)

    return pendulum.tz.timezone(data)
