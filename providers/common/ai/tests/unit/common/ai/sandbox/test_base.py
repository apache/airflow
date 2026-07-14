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

import dataclasses

import pytest

from airflow.providers.common.ai.sandbox.base import (
    _MAX_OUTPUT_CHARS,
    SandboxBackend,
    SandboxResult,
    _cap_output,
    _new_sandbox_name,
    _validate_positive_finite,
)


class TestSandboxResult:
    def test_defaults(self):
        result = SandboxResult(exit_code=0, stdout="out", stderr="err")
        assert result.timed_out is False
        assert result.truncated is False
        assert result.sandbox_terminated is False

    def test_frozen(self):
        result = SandboxResult(exit_code=0, stdout="", stderr="")
        with pytest.raises(dataclasses.FrozenInstanceError):
            result.exit_code = 1  # type: ignore[misc]


class TestNewSandboxName:
    def test_prefix(self):
        assert _new_sandbox_name().startswith("airflow-sandbox-")

    def test_unique(self):
        names = {_new_sandbox_name() for _ in range(100)}
        assert len(names) == 100


class TestCapOutput:
    def test_short_text_is_untouched(self):
        assert _cap_output("hello") == ("hello", False)

    def test_long_text_is_capped_and_flagged(self):
        text, truncated = _cap_output("x" * (_MAX_OUTPUT_CHARS + 1))
        assert len(text) == _MAX_OUTPUT_CHARS
        assert truncated is True


class TestSandboxBackend:
    def test_abstract_not_instantiable(self):
        with pytest.raises(TypeError):
            SandboxBackend()  # type: ignore[abstract]


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("-inf"), float("nan")])
def test_validate_positive_finite_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="timeout must be a positive finite number"):
        _validate_positive_finite(value, "timeout")
