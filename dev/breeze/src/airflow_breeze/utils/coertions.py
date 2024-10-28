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

from collections.abc import Iterable


def coerce_bool_value(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    elif not value:  # handle "" and other false-y coerce-able values
        return False
    else:
        return value[0].lower() in [
            "t",
            "y",
        ]  # handle all kinds of truth-y/yes-y/false-y/non-sy strings


def one_or_none_set(iterable: Iterable[bool]) -> bool:
    return 0 <= sum(1 for i in iterable if i) <= 1
