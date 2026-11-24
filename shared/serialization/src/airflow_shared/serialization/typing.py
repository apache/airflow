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

import inspect
from typing import Protocol, TypeGuard


class SupportsSerialize(Protocol):
    """An object with a no-argument serialization method."""

    def serialize(self) -> object: ...


class SupportsToDict(Protocol):
    """An object with a no-argument dictionary conversion method."""

    def to_dict(self) -> object: ...


def has_serialize(obj: object) -> TypeGuard[SupportsSerialize]:
    """Check for a callable serialize attribute without invoking user attribute hooks."""
    return callable(inspect.getattr_static(obj, "serialize", None))


def has_to_dict(obj: object) -> TypeGuard[SupportsToDict]:
    """Check for a callable to_dict attribute without invoking user attribute hooks."""
    return callable(inspect.getattr_static(obj, "to_dict", None))
