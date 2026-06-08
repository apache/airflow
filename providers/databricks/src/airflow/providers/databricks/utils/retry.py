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

from collections.abc import Callable, Mapping
from importlib import import_module
from typing import Any


def get_serde_serialize() -> Callable[[Any], Any]:
    try:
        return import_module("airflow.sdk.serde").serialize
    except ImportError:
        return import_module("airflow.serialization.serde").serialize


def validate_deferrable_databricks_retry_args(retry_args: Mapping[str, Any] | None, *, owner: str) -> None:
    """Validate retry args that need to cross the trigger serialization boundary."""
    if retry_args is None:
        return

    try:
        get_serde_serialize()(retry_args)
    except (AttributeError, RecursionError, TypeError, ValueError) as err:
        raise ValueError(
            f"{owner} does not support non-serializable retry_args/databricks_retry_args "
            "when deferrable=True. "
            "Use Airflow-serializable values, remove callable retry strategies, or disable deferrable mode."
        ) from err
