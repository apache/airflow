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
# mypy: warn-unused-ignores
"""Unused-ignore errors ensure these invalid registrations stay rejected by mypy-task-sdk."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydantic import BaseModel

    from airflow.sdk.execution_time.comms import GetConnection, GetVariable
    from airflow.sdk.execution_time.request_handlers import handle_get_connection
    from airflow.sdk.execution_time.supervisor import (
        ActivitySubprocess,
        RequestHandler,
        WatchedSubprocess,
        _register_client_handler,
        register_request_method,
    )

    _register_client_handler(GetVariable, handle_get_connection)  # type: ignore[arg-type]
    register_request_method(GetConnection, ActivitySubprocess._handle_get_dag)  # type: ignore[arg-type]
    clientless_handlers: dict[type[BaseModel], RequestHandler[WatchedSubprocess]] = (
        WatchedSubprocess._get_shared_request_handlers(GetConnection)  # type: ignore[assignment]
    )
