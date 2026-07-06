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

from airflow.api_fastapi.common.types import OklchColor, Theme, ThemeColors


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


# Shared test data for Theme/ThemeColors tests
_BRAND_SCALE = {
    "50": {"value": "oklch(0.975 0.007 298.0)"},
    "100": {"value": "oklch(0.950 0.014 298.0)"},
    "200": {"value": "oklch(0.900 0.023 298.0)"},
    "300": {"value": "oklch(0.800 0.030 298.0)"},
    "400": {"value": "oklch(0.680 0.038 298.0)"},
    "500": {"value": "oklch(0.560 0.044 298.0)"},
    "600": {"value": "oklch(0.460 0.048 298.0)"},
    "700": {"value": "oklch(0.390 0.049 298.0)"},
    "800": {"value": "oklch(0.328 0.050 298.0)"},
    "900": {"value": "oklch(0.230 0.043 298.0)"},
    "950": {"value": "oklch(0.155 0.035 298.0)"},
}
_GRAY_SCALE = {
    "50": {"value": "oklch(0.975 0.002 264.0)"},
    "100": {"value": "oklch(0.950 0.003 264.0)"},
    "200": {"value": "oklch(0.880 0.005 264.0)"},
    "300": {"value": "oklch(0.780 0.008 264.0)"},
    "400": {"value": "oklch(0.640 0.012 264.0)"},
    "500": {"value": "oklch(0.520 0.015 264.0)"},
    "600": {"value": "oklch(0.420 0.015 264.0)"},
    "700": {"value": "oklch(0.340 0.012 264.0)"},
    "800": {"value": "oklch(0.260 0.009 264.0)"},
    "900": {"value": "oklch(0.200 0.007 264.0)"},
    "950": {"value": "oklch(0.145 0.005 264.0)"},
}
_BLACK_SHADE = {"value": "oklch(0.220 0.025 288.6)"}
_WHITE_SHADE = {"value": "oklch(0.985 0.002 264.0)"}


class TestThemeColors:
    def test_brand_only(self):
        colors = ThemeColors.model_validate({"brand": _BRAND_SCALE})
        assert colors.brand is not None
        assert colors.gray is None
        assert colors.black is None
        assert colors.white is None

    def test_gray_only(self):
        colors = ThemeColors.model_validate({"gray": _GRAY_SCALE})
        assert colors.gray is not None
        assert colors.brand is None

    def test_black_and_white_only(self):
        colors = ThemeColors.model_validate({"black": _BLACK_SHADE, "white": _WHITE_SHADE})
        assert colors.black is not None
        assert colors.white is not None
        assert colors.brand is None
        assert colors.gray is None

    def test_all_tokens(self):
        colors = ThemeColors.model_validate(
            {"brand": _BRAND_SCALE, "gray": _GRAY_SCALE, "black": _BLACK_SHADE, "white": _WHITE_SHADE}
        )
        assert colors.brand is not None
        assert colors.gray is not None
        assert colors.black is not None
        assert colors.white is not None

    def test_empty_colors_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            ThemeColors.model_validate({})
        assert "At least one color token must be provided" in str(exc_info.value)

    def test_invalid_shade_key_rejected(self):
        with pytest.raises(ValidationError):
            ThemeColors.model_validate({"gray": {"999": {"value": "oklch(0.5 0.1 264.0)"}}})

    def test_serialization_excludes_none_fields(self):
        colors = ThemeColors.model_validate({"brand": _BRAND_SCALE})
        dumped = colors.model_dump(exclude_none=True)
        assert "brand" in dumped
        assert "gray" not in dumped
        assert "black" not in dumped
        assert "white" not in dumped


class TestTheme:
    def test_brand_only_theme(self):
        """Backwards-compatible: existing configs with only brand still work."""
        theme = Theme.model_validate({"tokens": {"colors": {"brand": _BRAND_SCALE}}})
        assert theme.tokens["colors"].brand is not None
        assert theme.tokens["colors"].gray is None
        assert theme.globalCss is None

    def test_gray_only_theme(self):
        """New: brand is no longer required."""
        theme = Theme.model_validate({"tokens": {"colors": {"gray": _GRAY_SCALE}}})
        assert theme.tokens["colors"].gray is not None
        assert theme.tokens["colors"].brand is None

    def test_black_white_theme(self):
        theme = Theme.model_validate({"tokens": {"colors": {"black": _BLACK_SHADE, "white": _WHITE_SHADE}}})
        assert theme.tokens["colors"].black is not None
        assert theme.tokens["colors"].white is not None

    def test_all_tokens_theme(self):
        theme = Theme.model_validate(
            {
                "tokens": {
                    "colors": {
                        "brand": _BRAND_SCALE,
                        "gray": _GRAY_SCALE,
                        "black": _BLACK_SHADE,
                        "white": _WHITE_SHADE,
                    }
                }
            }
        )
        colors = theme.tokens["colors"]
        assert colors.brand is not None
        assert colors.gray is not None
        assert colors.black is not None
        assert colors.white is not None

    def test_empty_colors_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            Theme.model_validate({"tokens": {"colors": {}}})
        assert "At least one color token must be provided" in str(exc_info.value)

    def test_serialization_round_trip(self):
        """Verify None color fields are excluded and OklchColor values are serialized as strings."""
        theme = Theme.model_validate({"tokens": {"colors": {"brand": _BRAND_SCALE}}})
        dumped = theme.model_dump(exclude_none=True)
        colors = dumped["tokens"]["colors"]
        assert "brand" in colors
        assert "gray" not in colors
        assert "black" not in colors
        assert "white" not in colors
        assert colors["brand"]["50"]["value"] == "oklch(0.975 0.007 298.0)"

    def test_globalcss_only_theme(self):
        """tokens is optional; globalCss alone is sufficient."""
        theme = Theme.model_validate({"globalCss": {"button": {"text-transform": "uppercase"}}})
        assert theme.tokens is None
        assert theme.globalCss == {"button": {"text-transform": "uppercase"}}

    def test_icon_only_theme(self):
        """tokens is optional; an icon URL alone is sufficient."""
        theme = Theme.model_validate({"icon": "https://example.com/logo.svg"})
        assert theme.tokens is None
        assert theme.icon == "https://example.com/logo.svg"

    def test_empty_theme(self):
        """An empty theme object is valid — it means 'use OSS defaults'."""
        theme = Theme.model_validate({})
        assert theme.tokens is None
        assert theme.globalCss is None
        assert theme.icon is None
        assert theme.icon_dark_mode is None

    def test_theme_serialization_excludes_none_tokens(self):
        """When tokens is None it must not appear in the serialized output."""
        theme = Theme.model_validate({"globalCss": {"a": {"color": "red"}}})
        dumped = theme.model_dump(exclude_none=True)
        assert "tokens" not in dumped
        assert dumped == {"globalCss": {"a": {"color": "red"}}}
