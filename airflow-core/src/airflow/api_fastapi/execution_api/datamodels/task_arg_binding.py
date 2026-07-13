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
"""
Positional-argument binding spec for stub (foreign-runtime) tasks.

Captured at parse time from a stub task's TaskFlow call (``@task.stub``), stored in the
serialized Dag, and delivered to the lang-SDK runtime through ``TIRunContext.arg_bindings``
so it can bind the values onto the native task function's parameters.
"""

from __future__ import annotations

from typing import Literal

from pydantic import JsonValue

from airflow.api_fastapi.core_api.base import BaseModel

ArgBindingDataType = Literal["string", "integer", "number", "boolean", "object", "array", "any"]
"""Language-neutral value type a stub-task argument binds to in the foreign runtime."""


class TaskArgBinding(BaseModel):
    """
    One positional argument of a stub (foreign-runtime) task, in declaration order.

    A deliberately flat shape (``kind`` discriminates instead of a union) so the JSON schema
    generates a plain struct in the foreign-language SDKs consuming the supervisor schema.
    """

    kind: Literal["xcom", "literal"]
    """Whether the value comes from an upstream task's XCom or is a literal from the Dag file."""

    data_type: ArgBindingDataType = "any"
    """Declared type from the stub function's annotation; runtimes type-check against it."""

    task_id: str | None = None
    """Upstream task id to pull the XCom from. Only set when ``kind`` is ``xcom``."""

    key: str = "return_value"
    """XCom key to pull. Only meaningful when ``kind`` is ``xcom``."""

    value: JsonValue | None = None
    """The literal value from the Dag file. Only set when ``kind`` is ``literal``."""
