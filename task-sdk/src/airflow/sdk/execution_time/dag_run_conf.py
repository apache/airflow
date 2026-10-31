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

from typing import TYPE_CHECKING, Any

from pydantic import ConfigDict, TypeAdapter
from pydantic_core import from_json

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import TIRunContext

_CONF_ADAPTER = TypeAdapter(dict[str, Any] | None, config=ConfigDict(hide_input_in_errors=True))


def decode_dag_run_conf(serialized: str) -> dict[str, Any] | None:
    """Validate configuration without including its contents in validation errors."""
    try:
        # Cache repeated object keys without filling the cache with unique manifest values.
        return _CONF_ADAPTER.validate_python(from_json(serialized, cache_strings="keys"))
    except ValueError:
        raise ValueError("Serialized Dag run configuration must be a JSON object or null") from None


def materialize_dag_run_conf(context: TIRunContext) -> None:
    """Restore conf once; when serialized conf is absent, the mutable dictionary is authoritative."""
    serialized = context.serialized_dag_run_conf
    if serialized is None:
        return
    if context.dag_run.conf is not None:
        raise ValueError("Task startup contains both decoded and serialized Dag run configuration")
    context.dag_run.conf = decode_dag_run_conf(serialized)
    context.serialized_dag_run_conf = None
