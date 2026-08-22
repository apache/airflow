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
"""
Build a JSON Schema describing a materialized Airflow configuration (``airflow.cfg``).

The schema is derived from the same configuration-description data structure that
:func:`airflow.configuration.retrieve_configuration_description` returns (the parsed
``config.yml``, optionally merged with installed providers' config blocks), so it never
duplicates the option list, types, or defaults by hand -- it only adds the JSON Schema
vocabulary around data Airflow already maintains as its single source of truth.

See ``dev/generate_config_json_schema.py`` for the CLI that regenerates the checked-in
``airflow.cfg.schema.json`` artifact from this module.
"""

from __future__ import annotations

import re
from typing import Any

JSON_SCHEMA_DRAFT = "http://json-schema.org/draft-07/schema#"

# AirflowConfigParser.getboolean() (shared/configuration/src/airflow_shared/configuration/parser.py)
# is what actually parses a boolean option's raw string value, and it only accepts these tokens,
# case-insensitively. This is *not* the same set Python's stdlib configparser.BOOLEAN_STATES
# accepts (which also allows "yes"/"no"/"on"/"off") -- the schema must match Airflow's own parser,
# not configparser's.
_BOOLEAN_TOKENS = ("t", "true", "1", "f", "false", "0")

_INTEGER_PATTERN = r"[+-]?[0-9]+"
_FLOAT_PATTERN = r"[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?"


def _case_insensitive_literal(token: str) -> str:
    """
    Build a case-insensitive regex for a literal token without relying on inline flags.

    JSON Schema's ``pattern`` keyword is just an ECMA-262 regex source string with no way to
    attach flags, and support for inline flag groups like ``(?i:...)`` varies across the engines
    that consume this schema (Python's ``re``, IDEs' JS-based validators, ...). Spelling out a
    character class per letter works everywhere.
    """
    return "".join(f"[{c.upper()}{c.lower()}]" if c.isalpha() else re.escape(c) for c in token)


def _boolean_value_schema() -> dict[str, Any]:
    alternatives = "|".join(_case_insensitive_literal(token) for token in _BOOLEAN_TOKENS)
    return {
        "oneOf": [
            {"type": "boolean"},
            {"type": "string", "pattern": f"^({alternatives})$"},
        ]
    }


def _numeric_value_schema(number_type: str, pattern_body: str, *, allow_empty: bool) -> dict[str, Any]:
    pattern = f"^{pattern_body}$" if not allow_empty else f"^({pattern_body})?$"
    return {
        "oneOf": [
            {"type": number_type},
            {"type": "string", "pattern": pattern},
        ]
    }


def _build_value_schema(option_type: str, *, empty_string_is_default: bool) -> dict[str, Any]:
    if option_type == "boolean":
        return _boolean_value_schema()
    if option_type == "integer":
        return _numeric_value_schema("integer", _INTEGER_PATTERN, allow_empty=empty_string_is_default)
    if option_type == "float":
        return _numeric_value_schema("number", _FLOAT_PATTERN, allow_empty=empty_string_is_default)
    # "string" (the config.yml default when `type` is omitted) and anything unrecognized: a bare
    # string is the only thing every option accepts, since airflow.cfg is an ini file and every
    # raw value read from it is a string.
    return {"type": "string"}


def _unescape_templated_value(value: Any) -> Any:
    """
    Undo the doubled-brace escaping config.yml uses for literal ``{``/``}``.

    Mirrors the replacement in ``devel-common/src/docs/utils/conf_constants.py``
    (``get_configs_and_deprecations``), which the Sphinx docs build applies to the same
    ``default``/``example`` fields before rendering the Configuration Reference page --
    without it, values like the ``logging.log_format`` example would show doubled braces
    that never appear in a real generated ``airflow.cfg``.
    """
    if isinstance(value, str) and "{{" in value:
        return value.replace("{{", "{").replace("}}", "}")
    return value


def _build_option_schema(option: dict[str, Any], enum_values: list[str] | None) -> dict[str, Any]:
    default = option.get("default")
    option_type = option.get("type") or "string"
    schema = _build_value_schema(option_type, empty_string_is_default=(default == ""))

    if enum_values is not None:
        # These constraints come from AirflowConfigParser.enums_options / _validate_enums, which
        # is Python code, not data in config.yml -- config.yml has no `enum` field at all, so this
        # is the one piece of real validation logic that has to be supplied from outside the YAML.
        schema = {"type": "string", "enum": list(enum_values)}

    description = (option.get("description") or "").strip()
    if option.get("version_deprecated"):
        reason = (option.get("deprecation_reason") or "").strip()
        schema["deprecated"] = True
        description = f"{description}\n\nDeprecated: {reason}".strip() if reason else description
    if description:
        schema["description"] = description

    if default is not None:
        # Deliberately kept as the literal (unescaped) string config.yml declares, matching what
        # actually lives in an ini file, rather than coerced to a native int/float/bool -- config.yml
        # itself never stores a bare numeric/boolean YAML scalar for `default` (every value is
        # either a quoted string or `~`), so coercing here would be inventing a type config.yml does
        # not claim.
        schema["default"] = _unescape_templated_value(default)

    example = option.get("example")
    if example:
        schema["examples"] = [_unescape_templated_value(example)]

    if option.get("sensitive"):
        # Sensitive options (fernet_key, secret keys, DB URIs with embedded credentials, ...) can
        # also be supplied out-of-band via the *_CMD / *_SECRET env var conventions; `writeOnly`
        # tells schema-aware tooling not to echo the value back, not that it must be absent.
        schema["writeOnly"] = True

    if option.get("version_added"):
        schema["x-version-added"] = option["version_added"]

    return schema


def _build_section_schema(section: dict[str, Any], enums_for_section: dict[str, list[str]]) -> dict[str, Any]:
    options = section.get("options") or {}
    section_schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            option_name: _build_option_schema(option, enums_for_section.get(option_name))
            for option_name, option in options.items()
        },
        # Every option of a section is exhaustively declared in config.yml (or, for a
        # provider-contributed section, in that provider's own config block) -- no provider has
        # been observed to add options to a section owned by another distribution -- so an unknown
        # option within a *known* section is treated as an error.
        "additionalProperties": False,
    }
    description = (section.get("description") or "").strip()
    if description:
        section_schema["description"] = description
    return section_schema


def build_airflow_cfg_json_schema(
    config_descriptions: dict[str, dict[str, Any]],
    enums_options: dict[tuple[str, str], list[str]] | None = None,
) -> dict[str, Any]:
    """
    Build a JSON Schema for a materialized Airflow configuration.

    :param config_descriptions: same shape as returned by
        ``airflow.configuration.retrieve_configuration_description``. Pass
        ``include_providers=True`` there to also cover the config sections contributed by
        whichever providers happen to be installed in the current environment.
    :param enums_options: ``AirflowConfigParser.enums_options`` (or an equivalent mapping of
        ``(section, option) -> [allowed values]``), layered on top of the declared `type`
        for options whose accepted values are enforced by Python code rather than by
        config.yml itself.
    """
    enums_by_section: dict[str, dict[str, list[str]]] = {}
    for (section_name, option_name), values in (enums_options or {}).items():
        enums_by_section.setdefault(section_name, {})[option_name] = values

    properties = {
        section_name: _build_section_schema(section, enums_by_section.get(section_name, {}))
        for section_name, section in sorted(config_descriptions.items())
    }

    return {
        "$schema": JSON_SCHEMA_DRAFT,
        "title": "Apache Airflow configuration",
        "description": (
            "Describes the section/option values of a materialized Airflow configuration "
            "(airflow.cfg), generated from config.yml. Every value is validated the way "
            "Airflow's own config parser reads it: as a string (the raw ini representation), "
            "or, for tooling that materializes typed values, as the corresponding native JSON "
            "type. Installed provider packages (celery, fab, ...) contribute additional "
            "sections that are not enumerated here, so unrecognized top-level sections are "
            "allowed; unrecognized options within a *known* section are not."
        ),
        "type": "object",
        "properties": properties,
        "additionalProperties": True,
    }
