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

"""Utilities for loading DAG parameter options from external configuration files."""

from __future__ import annotations

import configparser
import json
from pathlib import Path
from typing import Any, Literal

import yaml


def load_options_from_file(
    file_path: str | Path,
    extension: Literal["ini", "json", "yaml"],
    filter_conditions: dict[str, Any] | None = None,
    key_field: str | None = None,
) -> list[str]:
    """
    Load dropdown options from a configuration file.

    This function reads a configuration file (INI, JSON, or YAML) and extracts
    option values based on the specified parameters. It can filter items based
    on key-value pairs and choose which field to use as the option value.

    :param file_path: Path to the configuration file
    :param extension: File format type - "ini", "json", or "yaml"
    :param filter_conditions: Optional dictionary of key-value pairs to filter items.
        Only items where all specified keys match the given values will be included.
        Example: {"TYPE": "Script"} will only include items where TYPE=Script
    :param key_field: Which field to use as the option value.
        For INI files: Special value "section" uses the section name itself,
        otherwise uses the value of the specified key within each section.
        For JSON/YAML files: Uses the value of the specified field from each object.
        Defaults: "section" for INI, "name" for JSON/YAML
    :return: List of option values extracted from the configuration file
    :raises FileNotFoundError: If the specified file does not exist
    :raises ValueError: If the file format is invalid or unsupported

    Example INI file (interfaces.ini)::

        [InterfaceA]
        TYPE = Script
        DESCRIPTION = Script interface A

        [InterfaceB]
        TYPE = EBICS
        DESCRIPTION = Electronic banking interface

    Example JSON file (interfaces.json)::

        [
            {"name": "InterfaceA", "type": "Script", "description": "Script interface A"},
            {"name": "InterfaceB", "type": "EBICS", "description": "Electronic banking"},
        ]

    Example YAML file (interfaces.yaml)::

        - name: InterfaceA
          type: Script
          description: Script interface A
        - name: InterfaceB
          type: EBICS
          description: Electronic banking

    Example usage::

        # Load from INI file - get section names where TYPE=Script
        options = load_options_from_file(
            "config/interfaces.ini",
            extension="ini",
            filter_conditions={"TYPE": "Script"},
            key_field="section",
        )
        # Returns: ["InterfaceA"]

        # Load from JSON file - get names where type=Script
        options = load_options_from_file(
            "config/interfaces.json", extension="json", filter_conditions={"type": "Script"}, key_field="name"
        )
        # Returns: ["InterfaceA"]
    """
    # Validate file path
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    # Set default key_field based on extension
    if key_field is None:
        key_field = "section" if extension == "ini" else "name"

    # Load and parse based on extension
    if extension == "ini":
        return _load_from_ini(file_path, filter_conditions, key_field)
    if extension == "json":
        return _load_from_json(file_path, filter_conditions, key_field)
    if extension == "yaml":
        return _load_from_yaml(file_path, filter_conditions, key_field)
    raise ValueError(f"Unsupported extension: {extension}. Supported: ini, json, yaml")


def _load_from_ini(
    file_path: Path,
    filter_conditions: dict[str, Any] | None,
    key_field: str,
) -> list[str]:
    """Load options from INI file."""
    config = configparser.ConfigParser()
    try:
        config.read(file_path)
    except configparser.Error as e:
        raise ValueError(f"Invalid INI file format: {e}") from e

    options = []
    for section in config.sections():
        # Apply filter conditions
        if filter_conditions:
            matches = all(
                config.get(section, key, fallback=None) == value for key, value in filter_conditions.items()
            )
            if not matches:
                continue

        # Extract option value
        if key_field == "section":
            options.append(section)
        else:
            value = config.get(section, key_field, fallback=None)
            if value:
                options.append(value)

    return options


def _load_from_json(
    file_path: Path,
    filter_conditions: dict[str, Any] | None,
    key_field: str,
) -> list[str]:
    """Load options from JSON file."""
    try:
        with open(file_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON file format: {e.msg}") from e

    if not isinstance(data, list):
        raise ValueError("JSON file must contain an array of objects at the root level")

    return _extract_options_from_list(data, filter_conditions, key_field)


def _load_from_yaml(
    file_path: Path,
    filter_conditions: dict[str, Any] | None,
    key_field: str,
) -> list[str]:
    """Load options from YAML file."""
    try:
        with open(file_path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML file format: {e}") from e

    if not isinstance(data, list):
        raise ValueError("YAML file must contain an array of objects at the root level")

    return _extract_options_from_list(data, filter_conditions, key_field)


def _extract_options_from_list(
    data: list,
    filter_conditions: dict[str, Any] | None,
    key_field: str,
) -> list[str]:
    """Extract options from a list of dictionaries with filtering."""
    options = []
    for item in data:
        if not isinstance(item, dict):
            continue

        # Apply filter conditions
        if filter_conditions:
            matches = all(item.get(key) == value for key, value in filter_conditions.items())
            if not matches:
                continue

        # Extract option value
        value = item.get(key_field)
        if value is not None:
            options.append(str(value))

    return options
