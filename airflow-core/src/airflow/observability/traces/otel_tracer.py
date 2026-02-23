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

from airflow._shared.observability.traces import otel_tracer
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow._shared.observability.traces.otel_tracer import OtelTrace


def get_otel_tracer(cls, use_simple_processor: bool = False) -> OtelTrace:
    # The config values have been deprecated and therefore,
    # if the user hasn't added them to the config, the default values won't be used.
    # A fallback is needed to avoid an exception.
    port = None
    if conf.has_option("traces", "otel_port"):
        port = conf.getint("traces", "otel_port")

    return otel_tracer.get_otel_tracer(
        cls,
        use_simple_processor,
        host=conf.get("traces", "otel_host", fallback=None),
        port=port,
        ssl_active=conf.getboolean("traces", "otel_ssl_active", fallback=False),
        otel_service=conf.get("traces", "otel_service", fallback=None),
        debug=conf.getboolean("traces", "otel_debugging_on", fallback=False),
    )


def get_otel_tracer_for_task(cls) -> OtelTrace:
    return get_otel_tracer(cls, use_simple_processor=True)
