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

import collections.abc
import copy
from typing import TYPE_CHECKING, Any, Literal

from airflow.serialization.definitions.notset import NOTSET, is_arg_set

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


class SerializedParam:
    """Server-side param class for deserialization."""

    def __init__(
        self,
        default: Any = NOTSET,
        description: str | None = None,
        source: Literal["dag", "task"] | None = None,
        **schema,
    ):
        # No validation needed - the SDK already validated the default.
        self.value = default
        self.description = description
        self.schema = schema
        self.source = source

    def resolve(self, *, raises: bool = False) -> Any:
        """
        Run the validations and returns the param's final value.

        Different from SDK Param, this function never raises by default. *None*
        is returned if validation fails, no value is available, or the return
        value is not JSON-serializable.

        :param raises: All exceptions during validation are suppressed by
            default. They are only raised if this is set to *True* instead.
        """
        import jsonschema

        try:
            if not is_arg_set(value := self.value):
                raise ValueError("No value passed")
            jsonschema.validate(value, self.schema, format_checker=jsonschema.FormatChecker())
        except Exception:
            if not raises:
                return None
            raise
        return value

    def dump(self) -> dict[str, Any]:
        """Return the full param spec for API consumers."""
        return {
            "value": self.resolve(),
            "schema": self.schema,
            "description": self.description,
            "source": self.source,
        }


def _coerce_param(v: Any) -> SerializedParam:
    if isinstance(v, SerializedParam):
        return v
    return SerializedParam(v)


def _collect_params(container: Mapping[str, Any] | None) -> Iterator[tuple[str, SerializedParam]]:
    if not container:
        return
    for k, v in container.items():
        yield k, _coerce_param(v)


class SerializedParamsDict(collections.abc.Mapping[str, Any]):
    """Server-side ParamsDict class for deserialization."""

    __dict: dict[str, SerializedParam]

    def __init__(self, d: Mapping[str, Any] | None = None) -> None:
        self.__dict = dict(_collect_params(d))

    def __eq__(self, other: Any) -> bool:
        """Compare params dicts using their dumped content, matching SDK behavior."""
        if hasattr(other, "dump"):  # ParamsDict or SerializedParamsDict
            return self.dump() == other.dump()
        if isinstance(other, collections.abc.Mapping):
            return self.dump() == other
        return NotImplemented

    def __hash__(self):
        return hash(self.dump())

    def __contains__(self, key: object) -> bool:
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

    def get_param(self, key: str) -> SerializedParam:
        """Get the internal SerializedParam object for this key."""
        return self.__dict[key]

    def items(self):
        return collections.abc.ItemsView(self.__dict)

    def values(self):
        return collections.abc.ValuesView(self.__dict)

    def validate(self) -> dict[str, Any]:
        """Validate & returns all the params stored in the dictionary."""

        def _validate_one(k: str, v: SerializedParam):
            try:
                return v.resolve(raises=True)
            except Exception as e:
                raise ValueError(f"Invalid input for param {k}: {e}") from None

        return {k: _validate_one(k, v) for k, v in self.__dict.items()}

    def dump(self) -> Mapping[str, Any]:
        """Dump the resolved values as a mapping."""
        return {k: v.resolve() for k, v in self.__dict.items()}

    def deep_merge(self, data: Mapping[str, Any] | None) -> SerializedParamsDict:
        """Create a new params dict by merging incoming data into this params dict."""
        params = copy.deepcopy(self)
        if not data:
            return params
        for k, v in data.items():
            if k not in params:
                params.__dict[k] = _coerce_param(v)
            elif isinstance(v, SerializedParam):
                params.__dict[k] = v
            else:
                params.__dict[k].value = v
        return params
