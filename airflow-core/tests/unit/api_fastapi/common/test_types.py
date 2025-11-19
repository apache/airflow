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
from pydantic import ValidationError

from airflow.api_fastapi.common.types import OklchColor


class TestOklchColor:
    @pytest.mark.parametrize(
        ("input_str", "expected"),
        [
            ("oklch(0.637 0.237 25.331)", (0.637, 0.237, 25.331, "oklch(0.637 0.237 25.331)")),
            ("oklch(1 0.230 25.331)", (1.0, 0.23, 25.331, "oklch(1.0 0.23 25.331)")),
        ],
    )
    def test_valid_oklch(self, input_str, expected):
        color = OklchColor.model_validate(input_str)
        assert color.lightness == pytest.approx(expected[0])
        assert color.chroma == pytest.approx(expected[1])
        assert color.hue == pytest.approx(expected[2])
        assert color.model_dump() == expected[3]

    @pytest.mark.parametrize(
        ("input_str", "error_message"),
        [
            ("oklch(-0.1 0.15 240)", "Invalid lightness: -0.1 Must be between 0 and 1"),
            ("oklch(1.5 0.15 240)", "Invalid lightness: 1.5 Must be between 0 and 1"),
            ("oklch(0.5 -0.1 240)", "Invalid chroma: -0.1 Must be between 0 and 0.5"),
            ("oklch(0.5 0.6 240)", "Invalid chroma: 0.6 Must be between 0 and 0.5"),
            ("oklch(0.5 0.15 -10)", "Invalid hue: -10.0 Must be between 0 and 360"),
            ("oklch(0.5 0.15 400)", "Invalid hue: 400.0 Must be between 0 and 360"),
            ("rgb(255, 0, 0)", "Invalid OKLCH format: rgb(255, 0, 0) Expected format oklch(l c h)"),
            ("oklch(0.5 0.15)", "Invalid OKLCH format: oklch(0.5 0.15) Expected format oklch(l c h)"),
            (
                "oklch(0.5 0.15 240 0.5)",
                "Invalid OKLCH format: oklch(0.5 0.15 240 0.5) Expected format oklch(l c h)",
            ),
            (
                "oklch(abc 0.15 240)",
                "Invalid OKLCH format: oklch(abc 0.15 240) Expected format oklch(l c h)",
            ),
            (
                "oklch(10 0. 240)",
                "Invalid OKLCH format: oklch(10 0. 240) Expected format oklch(l c h)",
            ),
            (
                "oklch(10 3 .240)",
                "Invalid OKLCH format: oklch(10 3 .240) Expected format oklch(l c h)",
            ),
            (
                "oklch(. 3 240)",
                "Invalid OKLCH format: oklch(. 3 240) Expected format oklch(l c h)",
            ),
        ],
    )
    def test_invalid_oklch(self, input_str, error_message):
        with pytest.raises(ValidationError) as exc_info:
            OklchColor.model_validate(input_str)
        assert error_message in str(exc_info.value)
