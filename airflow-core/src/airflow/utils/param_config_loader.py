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
from typing import Any


def load_options_from_ini(
    file_path: str | Path,
    filter_conditions: dict[str, Any] | None = None,
    key_field: str = "section",
) -> list[str]:
    """
    Load dropdown options from an INI configuration file.

    This function reads an INI file and extracts option values based on the specified
    parameters. It can filter sections based on key-value pairs and choose which field
    to use as the option value.

    :param file_path: Path to the INI configuration file
    :param filter_conditions: Optional dictionary of key-value pairs to filter sections.
        Only sections where all specified keys match the given values will be included.
        Example: {"TYPE": "Script"} will only include sections where TYPE=Script
    :param key_field: Which field to use as the option value. Special value "section"
        uses the section name itself. Otherwise, uses the value of the specified key
        within each section.
    :return: List of option values extracted from the INI file
    :raises FileNotFoundError: If the specified file does not exist
    :raises ValueError: If the INI file is malformed

    Example INI file (interfaces.ini)::

        [InterfaceA]
        TYPE = Script
        DESCRIPTION = Script interface A

        [InterfaceB]
        TYPE = EBICS
        DESCRIPTION = Electronic banking interface

    Example usage::

        # Get all section names where TYPE=Script
        options = load_options_from_ini(
            "config/interfaces.ini", filter_conditions={"TYPE": "Script"}, key_field="section"
        )
        # Returns: ["InterfaceA"]

        # Get all DESCRIPTION values
        options = load_options_from_ini("config/interfaces.ini", key_field="DESCRIPTION")
        # Returns: ["Script interface A", "Electronic banking interface"]
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    config = configparser.ConfigParser()
    try:
        config.read(file_path)
    except configparser.Error as e:
        raise ValueError(f"Invalid INI file format: {e}") from e

    options = []
    for section in config.sections():
        # Check if section matches filter conditions
        if filter_conditions:
            matches = all(
                config.get(section, key, fallback=None) == value for key, value in filter_conditions.items()
            )
            if not matches:
                continue

        # Get the value to use as option
        if key_field == "section":
            options.append(section)
        else:
            value = config.get(section, key_field, fallback=None)
            if value:
                options.append(value)

    return options


def load_options_from_json(
    file_path: str | Path,
    filter_conditions: dict[str, Any] | None = None,
    key_field: str = "name",
) -> list[str]:
    """
    Load dropdown options from a JSON configuration file.

    This function reads a JSON file containing an array of objects and extracts
    option values based on the specified parameters. It can filter objects based
    on key-value pairs and choose which field to use as the option value.

    :param file_path: Path to the JSON configuration file
    :param filter_conditions: Optional dictionary of key-value pairs to filter objects.
        Only objects where all specified keys match the given values will be included.
    :param key_field: Which field to use as the option value from each object
    :return: List of option values extracted from the JSON file
    :raises FileNotFoundError: If the specified file does not exist
    :raises ValueError: If the JSON file is malformed or not an array
    :raises json.JSONDecodeError: If the file contains invalid JSON

    Example JSON file (interfaces.json)::

        [
            {"name": "InterfaceA", "type": "Script", "description": "Script interface A"},
            {"name": "InterfaceB", "type": "EBICS", "description": "Electronic banking"},
        ]

    Example usage::

        # Get all names where type=Script
        options = load_options_from_json(
            "config/interfaces.json", filter_conditions={"type": "Script"}, key_field="name"
        )
        # Returns: ["InterfaceA"]
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    try:
        with open(file_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON file format: {e.msg}", e.doc, e.pos) from e

    if not isinstance(data, list):
        raise ValueError("JSON file must contain an array of objects at the root level")

    options = []
    for item in data:
        if not isinstance(item, dict):
            continue

        # Check if item matches filter conditions
        if filter_conditions:
            matches = all(item.get(key) == value for key, value in filter_conditions.items())
            if not matches:
                continue

        # Get the value to use as option
        value = item.get(key_field)
        if value is not None:
            options.append(str(value))

    return options


def load_options_from_yaml(
    file_path: str | Path,
    filter_conditions: dict[str, Any] | None = None,
    key_field: str = "name",
) -> list[str]:
    """
    Load dropdown options from a YAML configuration file.

    This function reads a YAML file containing an array of objects and extracts
    option values based on the specified parameters. Similar to JSON loading but
    supports YAML format.

    :param file_path: Path to the YAML configuration file
    :param filter_conditions: Optional dictionary of key-value pairs to filter objects
    :param key_field: Which field to use as the option value from each object
    :return: List of option values extracted from the YAML file
    :raises FileNotFoundError: If the specified file does not exist
    :raises ValueError: If the YAML file is malformed or not an array
    :raises ImportError: If PyYAML is not installed

    Example YAML file (interfaces.yaml)::

        - name: InterfaceA
          type: Script
          description: Script interface A
        - name: InterfaceB
          type: EBICS
          description: Electronic banking

    Example usage::

        # Get all names where type=Script
        options = load_options_from_yaml(
            "config/interfaces.yaml", filter_conditions={"type": "Script"}, key_field="name"
        )
        # Returns: ["InterfaceA"]
    """
    try:
        import yaml
    except ImportError as e:
        raise ImportError("PyYAML is required to load YAML files. Install it with: pip install PyYAML") from e

    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    try:
        with open(file_path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML file format: {e}") from e

    if not isinstance(data, list):
        raise ValueError("YAML file must contain an array of objects at the root level")

    options = []
    for item in data:
        if not isinstance(item, dict):
            continue

        # Check if item matches filter conditions
        if filter_conditions:
            matches = all(item.get(key) == value for key, value in filter_conditions.items())
            if not matches:
                continue

        # Get the value to use as option
        value = item.get(key_field)
        if value is not None:
            options.append(str(value))

    return options
