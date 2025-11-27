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

from typing import TYPE_CHECKING

from airflow.sdk._shared.observability.traces import otel_tracer
from airflow.sdk.configuration import conf

if TYPE_CHECKING:
    from airflow.sdk._shared.observability.traces.otel_tracer import OtelTrace


def get_otel_tracer(cls, use_simple_processor: bool = False) -> OtelTrace:
    host = conf.get("traces", "otel_host")
    port = conf.getint("traces", "otel_port")
    ssl_active = conf.getboolean("traces", "otel_ssl_active")

    otel_service = conf.get("traces", "otel_service")
    debug = conf.getboolean("traces", "otel_debugging_on")

    return otel_tracer.get_otel_tracer(cls, use_simple_processor, host, port, ssl_active, otel_service, debug)


def get_otel_tracer_for_task(cls) -> OtelTrace:
    return get_otel_tracer(cls, use_simple_processor=True)
