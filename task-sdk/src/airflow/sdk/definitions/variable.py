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

from airflow.exceptions import AirflowNotFoundException
from airflow.sdk.api.datamodels._generated import VariableResponse
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
            return _get_variable(key, deserialize_json=deserialize_json).value
        except AirflowRuntimeError as e:
            if e.error.error == ErrorType.VARIABLE_NOT_FOUND and default is not NOTSET:
                return default
            raise

    @classmethod
    def get_variable_from_secrets(cls, key: str):
        """
        Get Airflow Variable by iterating over all Secret Backends.

        :param key: Variable Key
        :return: Variable Value
        """
        # TODO: check cache first
        # enabled only if SecretCache.init() has been called first
        from airflow.sdk.execution_time.supervisor import SECRETS_BACKEND

        var_val = NOTSET
        # iterate over backends if not in cache (or expired)
        for secrets_backend in SECRETS_BACKEND:
            try:
                var_val = secrets_backend.get_variable(key=key)
                if var_val is not NOTSET:
                    return var_val
            except Exception:
                log.exception(
                    "Unable to retrieve variable from secrets backend (%s). "
                    "Checking subsequent secrets backend.",
                    type(secrets_backend).__name__,
                )

        if var_val is not NOTSET:
            raise AirflowNotFoundException(f"The variable with key `{key}` isn't defined")

        return var_val

    @staticmethod
    def _convert_variable_to_response(key: str, value: str | None) -> VariableResponse:
        return VariableResponse(key=key, value=value)
