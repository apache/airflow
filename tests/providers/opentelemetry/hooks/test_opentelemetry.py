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

from airflow.providers.opentelemetry.hooks.otel import OtelHook


class TestOpenTelemetryHook:
    def test_hook_search(self):
        """test whether the hook operates with fallback if no conn id is found."""
        hook = OtelHook("test_conn_id")
        hook.gauge(stat="stat", value=1)
        hook.incr(stat="counter1")
        hook.decr(stat="counter1")
        with hook.start_as_current_span("my_span1") as s1:
            s1.set_attribute("attr1", "val1")
            with hook.start_as_current_span("my_span2") as s2:
                s2.set_attribute("attr2", "val2")
                pass
        span = hook.start_span("test_span")
        span.set_attribute("attr1", "val1")
        span.add_event("event1")
        assert True
