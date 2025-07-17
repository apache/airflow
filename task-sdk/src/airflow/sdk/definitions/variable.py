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

import logging
from typing import Any

import attrs

from airflow.sdk.definitions._internal.types import NOTSET

log = logging.getLogger(__name__)


@attrs.define
class Variable:
    """
    A generic way to store and retrieve arbitrary content or settings as a simple key/value store.

    :param key: The variable key.
    :param value: The variable value.
    :param description: The variable description.

    """

    key: str
    # keeping as any for supporting deserialize_json
    value: Any | None = None
    description: str | None = None

    # TODO: Extend this definition for reading/writing variables without context
    @classmethod
    def get(cls, key: str, default: Any = NOTSET, deserialize_json: bool = False):
        from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
        from airflow.sdk.execution_time.context import _get_variable

        try:
            return _get_variable(key, deserialize_json=deserialize_json)
        except AirflowRuntimeError as e:
            if e.error.error == ErrorType.VARIABLE_NOT_FOUND and default is not NOTSET:
                return default
            raise

    @classmethod
    def set(cls, key: str, value: Any, description: str | None = None, serialize_json: bool = False) -> None:
        from airflow.sdk.exceptions import AirflowRuntimeError
        from airflow.sdk.execution_time.context import _set_variable

        try:
            return _set_variable(key, value, description, serialize_json=serialize_json)
        except AirflowRuntimeError as e:
            log.exception(e)

    @classmethod
    def delete(cls, key: str) -> None:
        from airflow.sdk.exceptions import AirflowRuntimeError
        from airflow.sdk.execution_time.context import _delete_variable

        try:
            _delete_variable(key=key)
        except AirflowRuntimeError as e:
            log.exception(e)
