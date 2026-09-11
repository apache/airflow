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

import jsonschema
import pytest
from jsonschema import Draft7Validator

from airflow.config_templates.schema import build_airflow_cfg_json_schema
from airflow.configuration import AirflowConfigParser, retrieve_configuration_description

from tests_common.test_utils.config import create_fresh_airflow_config


def _option(**overrides) -> dict:
    """Build a config.yml-shaped option dict, matching the keys retrieve_configuration_description() produces."""
    option = {
        "description": None,
        "version_added": None,
        "version_deprecated": None,
        "deprecation_reason": None,
        "type": "string",
        "example": None,
        "default": None,
        "sensitive": False,
    }
    option.update(overrides)
    return option


def _schema_for_option(option: dict, *, enums_options=None) -> dict:
    schema = build_airflow_cfg_json_schema(
        {"sec": {"description": None, "options": {"opt": option}}}, enums_options=enums_options
    )
    return schema["properties"]["sec"]["properties"]["opt"]


class TestBuildAirflowCfgJsonSchema:
    def test_schema_is_a_valid_draft7_schema(self):
        schema = build_airflow_cfg_json_schema(
            {
                "core": {
                    "description": "Core",
                    "options": {"parallelism": _option(type="integer", default="32")},
                }
            }
        )
        Draft7Validator.check_schema(schema)

    def test_top_level_allows_unknown_sections(self):
        # Provider-contributed sections aren't enumerable from config.yml alone, so the schema
        # must not reject sections it doesn't know about.
        schema = build_airflow_cfg_json_schema({})
        assert schema["additionalProperties"] is True
        assert schema["properties"] == {}

    def test_unknown_option_in_known_section_is_rejected(self):
        schema = build_airflow_cfg_json_schema(
            {"core": {"description": None, "options": {"parallelism": _option(type="integer", default="32")}}}
        )
        validator = Draft7Validator(schema)
        assert validator.is_valid({"core": {"parallelism": "32"}})
        assert not validator.is_valid({"core": {"made_up_option": "x"}})

    @pytest.mark.parametrize(
        ("option_type", "default", "valid_values", "invalid_values"),
        [
            pytest.param(
                "boolean", "True", [True, False, "True", "false", "1", "0"], ["yes", "on", 2], id="boolean"
            ),
            pytest.param("integer", "32", [7, "32", "-1"], ["3.5", "abc"], id="integer"),
            pytest.param("float", "1.5", [3.0, "1.5", "-2"], ["abc"], id="float"),
            pytest.param("string", "foo", ["anything"], [1, True], id="string"),
        ],
    )
    def test_typed_option_accepts_native_type_and_matching_string(
        self, option_type, default, valid_values, invalid_values
    ):
        option_schema = _schema_for_option(_option(type=option_type, default=default))
        validator = Draft7Validator(option_schema)
        for value in valid_values:
            assert validator.is_valid(value), f"{value!r} should be valid for type {option_type}"
        for value in invalid_values:
            assert not validator.is_valid(value), f"{value!r} should be invalid for type {option_type}"

    def test_integer_option_with_empty_string_default_allows_empty_string(self):
        # core.default_task_execution_timeout is the one real config.yml option where an
        # integer-typed value legitimately defaults to "" (gettimedelta() treats it as unset).
        option_schema = _schema_for_option(_option(type="integer", default=""))
        validator = Draft7Validator(option_schema)
        assert validator.is_valid("")
        assert validator.is_valid("300")
        assert not validator.is_valid("abc")

    def test_integer_option_without_empty_default_rejects_empty_string(self):
        option_schema = _schema_for_option(_option(type="integer", default="32"))
        assert not Draft7Validator(option_schema).is_valid("")

    def test_enum_option_overrides_type_and_restricts_values(self):
        option_schema = _schema_for_option(
            _option(type="string", default="regexp"),
            enums_options={("sec", "opt"): ["regexp", "glob"]},
        )
        assert option_schema["enum"] == ["regexp", "glob"]
        validator = Draft7Validator(option_schema)
        assert validator.is_valid("glob")
        assert not validator.is_valid("not-a-real-value")

    def test_deprecated_option_is_flagged_with_reason_in_description(self):
        option_schema = _schema_for_option(
            _option(
                type="string",
                default="x",
                description="Old option.",
                version_deprecated="2.0.0",
                deprecation_reason="Use new_opt instead.",
            )
        )
        assert option_schema["deprecated"] is True
        assert "Use new_opt instead." in option_schema["description"]

    def test_deprecated_option_without_reason_or_description_has_no_description_key(self):
        option_schema = _schema_for_option(_option(type="string", default="x", version_deprecated="2.0.0"))
        assert option_schema["deprecated"] is True
        assert "description" not in option_schema

    def test_sensitive_option_is_write_only(self):
        option_schema = _schema_for_option(_option(type="string", default="", sensitive=True))
        assert option_schema["writeOnly"] is True

    def test_non_sensitive_option_has_no_write_only_key(self):
        option_schema = _schema_for_option(_option(type="string", default=""))
        assert "writeOnly" not in option_schema

    def test_empty_string_default_is_preserved_not_omitted(self):
        # 29 real config.yml options (e.g. logging.log_format) legitimately default to "" --
        # that must be kept, not treated the same as "no default".
        option_schema = _schema_for_option(_option(type="string", default=""))
        assert option_schema["default"] == ""

    def test_none_default_is_omitted(self):
        option_schema = _schema_for_option(_option(type="string", default=None))
        assert "default" not in option_schema

    def test_templated_default_and_example_are_unescaped(self):
        option_schema = _schema_for_option(
            _option(type="string", default="{{asctime}} test", example="{{levelname}}")
        )
        assert option_schema["default"] == "{asctime} test"
        assert option_schema["examples"] == ["{levelname}"]

    def test_version_added_surfaces_as_vendor_extension(self):
        option_schema = _schema_for_option(_option(type="string", default="x", version_added="2.9.0"))
        assert option_schema["x-version-added"] == "2.9.0"

    def test_option_without_version_added_has_no_vendor_extension(self):
        option_schema = _schema_for_option(_option(type="string", default="x"))
        assert "x-version-added" not in option_schema

    def test_section_description_included_when_present(self):
        schema = build_airflow_cfg_json_schema({"core": {"description": "Core section.", "options": {}}})
        assert schema["properties"]["core"]["description"] == "Core section."

    def test_section_without_description_omits_key(self):
        schema = build_airflow_cfg_json_schema({"core": {"description": None, "options": {}}})
        assert "description" not in schema["properties"]["core"]


class TestBuildAirflowCfgJsonSchemaAgainstRealConfig:
    """Ground the generated schema against Airflow's real, current config.yml and parser."""

    def test_schema_covers_every_real_section_and_is_valid(self):
        config_descriptions = retrieve_configuration_description(include_providers=False)
        schema = build_airflow_cfg_json_schema(config_descriptions, AirflowConfigParser.enums_options)

        Draft7Validator.check_schema(schema)
        assert set(schema["properties"]) == set(config_descriptions)

    def test_generated_schema_validates_a_real_materialized_configuration(self):
        # This is the concrete answer to "can this actually be validated, and has it been
        # tested against a real config?": materialize a real AirflowConfigParser's defaults
        # the way an ini-parsed airflow.cfg would look, and validate it against the schema.
        config_descriptions = retrieve_configuration_description(include_providers=False)
        schema = build_airflow_cfg_json_schema(config_descriptions, AirflowConfigParser.enums_options)

        parser = create_fresh_airflow_config()
        materialized = parser.as_dict(display_source=False, display_sensitive=True, raw=True)
        core_only = {
            section: dict(options)
            for section, options in materialized.items()
            if section in config_descriptions
        }

        jsonschema.validate(instance=core_only, schema=schema)

    def test_logging_level_enum_matches_available_logging_levels(self):
        config_descriptions = retrieve_configuration_description(include_providers=False)
        schema = build_airflow_cfg_json_schema(config_descriptions, AirflowConfigParser.enums_options)

        option_schema = schema["properties"]["logging"]["properties"]["logging_level"]
        assert set(option_schema["enum"]) == set(
            AirflowConfigParser.enums_options[("logging", "logging_level")]
        )

    def test_invalid_materialized_configuration_is_rejected(self):
        config_descriptions = retrieve_configuration_description(include_providers=False)
        schema = build_airflow_cfg_json_schema(config_descriptions, AirflowConfigParser.enums_options)

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(instance={"core": {"parallelism": "not-a-number"}}, schema=schema)
