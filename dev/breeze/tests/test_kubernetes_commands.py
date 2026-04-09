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

import pytest

from airflow_breeze.commands.kubernetes_commands import _otel_flags_from_options


@pytest.mark.parametrize(
    ("extra_options", "expected_flags"),
    [
        (None, set()),
        ((), set()),
        # single flag
        (("--set", "otelCollector.tracesEnabled=true"), {"tracesEnabled"}),
        (("--set", "otelCollector.metricsEnabled=true"), {"metricsEnabled"}),
        (("--set", "otelCollector.enabled=true"), {"enabled"}),
        # both traces and metrics
        (
            (
                "--set",
                "otelCollector.tracesEnabled=true",
                "--set",
                "otelCollector.metricsEnabled=true",
            ),
            {"tracesEnabled", "metricsEnabled"},
        ),
        # unrelated --set values are ignored
        (("--set", "executor=KubernetesExecutor"), set()),
        (
            ("--set", "executor=KubernetesExecutor", "--set", "otelCollector.tracesEnabled=true"),
            {"tracesEnabled"},
        ),
        # value=false is not included
        (("--set", "otelCollector.tracesEnabled=false"), set()),
        # --set without a following value
        (("--set",), set()),
        # flag not preceded by --set is ignored
        (("otelCollector.tracesEnabled=true",), set()),
        # other flags between --set pairs don't break iteration
        (
            (
                "--set",
                "otelCollector.tracesEnabled=true",
                "--timeout",
                "20m",
                "--set",
                "otelCollector.metricsEnabled=true",
            ),
            {"tracesEnabled", "metricsEnabled"},
        ),
    ],
)
def test_otel_flags_from_options(extra_options, expected_flags):
    assert _otel_flags_from_options(extra_options) == expected_flags
