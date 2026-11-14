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

import json

import pytest

from airflow.exceptions import ParamValidationError
from airflow.sdk import Param
from airflow.serialization.definitions.param import SerializedParam, SerializedParamsDict
from airflow.serialization.serialized_objects import BaseSerialization


class TestSerializedParam:
    def test_resolve_no_schema(self):
        """Test resolve when no schema is provided."""
        assert SerializedParam(default=42).resolve() == 42

    @pytest.mark.parametrize(
        "duration",
        [
            pytest.param("PT15M", id="minutes-only"),
            pytest.param("P1Y", id="years-only"),
            pytest.param("P1W", id="weeks-only"),
            pytest.param("P1D", id="days-only"),
            pytest.param("PT1H", id="hours-only"),
            pytest.param("PT30S", id="seconds-only"),
            pytest.param("P1DT2H", id="days-and-hours"),
            pytest.param("P1Y2M3DT4H5M6S", id="full-duration"),
            pytest.param("PT1.5H", id="fractional-hours-dot"),
        ],
    )
    def test_string_duration_format(self, duration):
        """Test valid ISO 8601 duration strings."""
        assert SerializedParam(duration, type="string", format="duration").resolve(raises=True) == duration

    @pytest.mark.parametrize(
        "duration",
        [
            pytest.param("P", id="bare-P"),
            pytest.param("PT", id="bare-PT"),
            pytest.param("invalid", id="plain-text"),
            pytest.param("15M", id="missing-P-prefix"),
            pytest.param("1Y2M", id="no-P-prefix"),
        ],
    )
    def test_string_duration_format_error(self, duration):
        """Test invalid ISO 8601 duration strings."""
        with pytest.raises(Exception, match="is not a 'duration'"):
            SerializedParam(duration, type="string", format="duration").resolve(raises=True)


def _reserialize(params: dict) -> SerializedParamsDict:
    encoded = json.loads(json.dumps(BaseSerialization._serialize_params_dict(params)))
    return BaseSerialization._deserialize_params_dict(encoded)


class TestSerializedParamsDictDependentOptions:
    @staticmethod
    def _build_params(country, city):
        return _reserialize(
            {
                "country": Param(country, type=["null", "string"], enum=[None, "de", "fr"]),
                "city": Param(
                    city,
                    type=["null", "string"],
                    depends_on="country",
                    options_map={"de": ["berlin", "munich"], "fr": ["paris"]},
                ),
            }
        )

    @pytest.mark.parametrize(
        ("country", "valid_city", "invalid_city", "match"),
        [
            pytest.param(
                "de",
                "berlin",
                "paris",
                "Invalid input for param city: 'paris' is not an option for country='de'",
                id="option-of-other-parent-value",
            ),
            pytest.param("fr", None, "berlin", "'berlin' is not an option for country='fr'", id="null-child"),
            pytest.param(
                None, None, "berlin", "'berlin' is not an option for country=None", id="null-parent"
            ),
        ],
    )
    def test_validate_dependent_option(self, country, valid_city, invalid_city, match):
        assert self._build_params(country, valid_city).validate() == {"country": country, "city": valid_city}
        with pytest.raises(ParamValidationError, match=match):
            self._build_params(country, invalid_city).validate()

    def test_validate_non_string_keys_after_serialization(self):
        def build_params(level):
            return _reserialize(
                {
                    "level": Param(level, type="integer"),
                    "item": Param("a", type="string", depends_on="level", options_map={1: ["a"], 2: ["b"]}),
                }
            )

        assert build_params(1).validate() == {"level": 1, "item": "a"}
        with pytest.raises(ParamValidationError, match="'a' is not an option for level=2"):
            build_params(2).validate()

    def test_validate_unknown_parent(self):
        params = _reserialize({"city": Param("berlin", depends_on="nope", options_map={})})
        with pytest.raises(ParamValidationError, match="depends_on names 'nope', which is not a param"):
            params.validate()
