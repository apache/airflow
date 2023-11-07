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

import warnings
from enum import Enum


class Catchup(str, Enum):
    """Enum for catchup policy."""

    ENABLE = "enable"
    DISABLE = "disable"
    IGNORE_FIRST = "ignore_first"


def _catchup_backwards_compatibility(cls, value: str | bool) -> Catchup:
    should_warn = False
    if isinstance(value, bool):
        should_warn = True
        value = "enable" if value else "disable"
    if value in ("True", "False"):
        should_warn = True
        value = "enable" if value == "True" else "disable"
    if should_warn:
        warnings.warn(
            "Passing a boolean to Catchup is deprecated. "
            "Please pass one of ['enable', 'disable', 'ignore_first'] instead.",
            DeprecationWarning,
            stacklevel=2,
        )
    return Enum.__new__(cls, value)


Catchup.__new__ = _catchup_backwards_compatibility  # type: ignore


__all__ = ["Catchup"]
