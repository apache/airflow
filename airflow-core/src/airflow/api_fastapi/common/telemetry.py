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

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from fastapi.telemetry import TelemetryConfig


def _exclude_every_request(scope: MutableMapping[str, Any]) -> bool:
    return True


# Airflow controls request tracing itself (``[execution_api] otel_trace_propagation``, query
# string redaction), so FastAPI's native OpenTelemetry must stay out of the way. Excluding every
# request, rather than turning the signals off, also keeps the FastAPI apps mounted underneath
# (auth managers, plugins) from tracing it.
NATIVE_TELEMETRY_DISABLED: TelemetryConfig = {
    "exclude": _exclude_every_request,
    "auto_configure": False,
}
