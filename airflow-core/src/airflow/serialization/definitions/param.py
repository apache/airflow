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

from collections.abc import Collection, Iterator, Mapping
from typing import Any

NOTSET = object()


class SerializedParam:
    """Server-side Param class for deserialization."""

    def __init__(self, default=None, description: str | None = None, **schema):
        # No validation needed - the SDK already validated during serialization
        # NOTSET is converted to None during serialization, so we only see None here
        self.value = default
        self.description = description
        self.schema = schema

    def resolve(self, value: Any = NOTSET, suppress_exception: bool = False) -> Any:
        """Return the default value for compatibility."""
        return self.value if value is NOTSET else value

    def dump(self) -> Any:
        """Return the parameter value for API responses."""
        return self.value


def _collect_params(container: Mapping[str, Any] | None) -> Iterator[tuple[str, SerializedParam]]:
    if not container:
        return
    for k, v in container.items():
        if isinstance(v, SerializedParam):
            yield k, v
        else:
            yield k, SerializedParam(v)


class SerializedParamsDict:
    """Server-side ParamsDict class for deserialization."""

    __dict: dict[str, SerializedParam]

    def __init__(self, container: Mapping[str, Any] | None = None) -> None:
        self.__dict = dict(_collect_params(container))

    def __eq__(self, other: Any) -> bool:
        """Compare ParamsDict objects using their dumped content, matching SDK behavior."""
        if hasattr(other, "dump"):  # ParamsDict or SerializedParamsDict
            return self.dump() == other.dump()
        if isinstance(other, Mapping):
            return self.dump() == other
        return NotImplemented

    def __contains__(self, key: str) -> bool:
        return key in self.__dict

    def __len__(self) -> int:
        return len(self.__dict)

    def __iter__(self) -> Iterator[str]:
        return iter(self.__dict)

    def __getitem__(self, key: str) -> Any:
        """
        Get the resolved value for this key.

        This matches SDK ParamsDict behavior.
        """
        return self.__dict[key].value

    def items(self) -> Collection[tuple[str, SerializedParam]]:
        return self.__dict.items()

    def get_param(self, key: str) -> SerializedParam:
        """Get the internal SerializedParam object for this key."""
        return self.__dict[key]

    def dump(self) -> Mapping[str, Any]:
        """Dump the resolved values as a mapping."""
        return {k: v.value for k, v in self.__dict.items()}
