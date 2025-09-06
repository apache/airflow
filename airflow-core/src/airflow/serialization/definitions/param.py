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
import json
from typing import TYPE_CHECKING, Any

from airflow.exceptions import ParamValidationError
from airflow.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


def _check_json(value):
    try:
        json.dumps(value)
    except Exception:
        raise ParamValidationError(
            f"All provided parameters must be JSON-serializable. The value '{value}' is not."
        )


class SerializedParam:
    """Server-side Param class for deserialization."""

    def __init__(self, default: Any = NOTSET, description: str | None = None, **schema):
        # No validation needed - the SDK already validated the default.
        self.value = default
        self.description = description
        self.schema = schema

    def resolve(self, value: Any = NOTSET, suppress_exception: bool = False) -> Any:
        """
        Run the validations and returns the Param's final value.

        May raise ValueError on failed validations, or TypeError
        if no value is passed and no value already exists.
        We first check that value is json-serializable; if not, warn.
        In future release we will require the value to be json-serializable.

        :param value: The value to be updated for the Param
        :param suppress_exception: To raise an exception or not when validation
            fails. If true and validations fails, *None* is returned.
        """
        import jsonschema
        from jsonschema import FormatChecker
        from jsonschema.exceptions import ValidationError

        if not isinstance(value, ArgNotSet):
            _check_json(value)
            final_val = value
        elif isinstance(self.value, ArgNotSet):
            raise ParamValidationError("No value passed and Param has no default value")
        else:
            final_val = self.value
        try:
            jsonschema.validate(final_val, self.schema, format_checker=FormatChecker())
        except ValidationError as err:
            if suppress_exception:
                return None
            raise ParamValidationError(err)
        self.value = final_val
        return final_val

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


class SerializedParamsDict(collections.abc.Mapping[str, Any]):
    """Server-side ParamsDict class for deserialization."""

    _dict: dict[str, SerializedParam]

    def __init__(self, d: Mapping[str, Any] | None = None, *, suppress_exception: bool = False) -> None:
        self._dict = dict(_collect_params(d))
        self.suppress_exception = suppress_exception

    def __eq__(self, other: Any) -> bool:
        """Compare ParamsDict objects using their dumped content, matching SDK behavior."""
        if hasattr(other, "dump"):  # ParamsDict or SerializedParamsDict
            return self.dump() == other.dump()
        if isinstance(other, collections.abc.Mapping):
            return self.dump() == other
        return NotImplemented

    def __contains__(self, key: object) -> bool:
        return key in self._dict

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def __getitem__(self, key: str) -> Any:
        """
        Get the resolved value for this key.

        This matches SDK ParamsDict behavior.
        """
        return self._dict[key].value

    def get_param(self, key: str) -> SerializedParam:
        """Get the internal SerializedParam object for this key."""
        return self._dict[key]

    def validate(self) -> dict[str, Any]:
        """Validate & returns all the Params object stored in the dictionary."""

        def _validate_one(k: str, v: SerializedParam):
            try:
                return v.resolve(suppress_exception=self.suppress_exception)
            except ParamValidationError as e:
                raise ParamValidationError(f"Invalid input for param {k}: {e}")

        return {k: _validate_one(k, v) for k, v in self._dict.items()}

    def dump(self) -> Mapping[str, Any]:
        """Dump the resolved values as a mapping."""
        return {k: v.value for k, v in self._dict.items()}


def merge_into_params(params: SerializedParamsDict, inp: dict | None) -> SerializedParamsDict:
    params = copy.deepcopy(params)
    if not inp:
        return params
    merged = params._dict
    for key, value in inp.items():
        try:
            param = merged[key]
        except KeyError:
            param = SerializedParam(value)
        try:
            param.resolve(value)
        except ParamValidationError as e:
            raise ParamValidationError(f"Invalid input for param {key}: {e}")
        merged[key] = param
    return SerializedParamsDict(merged)
