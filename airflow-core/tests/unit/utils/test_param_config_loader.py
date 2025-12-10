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

"""Unit tests for param_config_loader module."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from airflow.utils.param_config_loader import (
    load_options_from_ini,
    load_options_from_json,
    load_options_from_yaml,
)


class TestLoadOptionsFromIni:
    """Test load_options_from_ini function."""

    def test_load_all_sections(self, tmp_path):
        """Test loading all section names from INI file."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text(
            "[SectionA]\n"
            "key1 = value1\n"
            "\n"
            "[SectionB]\n"
            "key1 = value2\n"
        )

        result = load_options_from_ini(ini_file, key_field="section")
        assert result == ["SectionA", "SectionB"]

    def test_load_with_filter(self, tmp_path):
        """Test loading sections with filter condition."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text(
            "[InterfaceA]\n"
            "TYPE = Script\n"
            "DESC = A\n"
            "\n"
            "[InterfaceB]\n"
            "TYPE = API\n"
            "DESC = B\n"
            "\n"
            "[InterfaceC]\n"
            "TYPE = Script\n"
            "DESC = C\n"
        )

        result = load_options_from_ini(
            ini_file,
            filter_conditions={"TYPE": "Script"},
            key_field="section",
        )
        assert result == ["InterfaceA", "InterfaceC"]

    def test_load_specific_key(self, tmp_path):
        """Test loading specific key values from sections."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text(
            "[SectionA]\n"
            "name = NameA\n"
            "\n"
            "[SectionB]\n"
            "name = NameB\n"
        )

        result = load_options_from_ini(ini_file, key_field="name")
        assert result == ["NameA", "NameB"]

    def test_file_not_found(self):
        """Test FileNotFoundError when file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            load_options_from_ini("/nonexistent/file.ini")

    def test_invalid_ini_format(self, tmp_path):
        """Test ValueError for invalid INI format."""
        ini_file = tmp_path / "invalid.ini"
        ini_file.write_text("this is not valid ini format [[[")

        with pytest.raises(ValueError, match="Invalid INI file format"):
            load_options_from_ini(ini_file)

    def test_missing_key_field(self, tmp_path):
        """Test that sections without the specified key are skipped."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text(
            "[SectionA]\n"
            "name = NameA\n"
            "\n"
            "[SectionB]\n"
            "other = OtherValue\n"
        )

        result = load_options_from_ini(ini_file, key_field="name")
        assert result == ["NameA"]  # SectionB is skipped


class TestLoadOptionsFromJson:
    """Test load_options_from_json function."""

    def test_load_all_items(self, tmp_path):
        """Test loading all items from JSON array."""
        json_file = tmp_path / "test.json"
        json_file.write_text(
            json.dumps([
                {"name": "ItemA", "type": "Type1"},
                {"name": "ItemB", "type": "Type2"},
            ])
        )

        result = load_options_from_json(json_file, key_field="name")
        assert result == ["ItemA", "ItemB"]

    def test_load_with_filter(self, tmp_path):
        """Test loading items with filter condition."""
        json_file = tmp_path / "test.json"
        json_file.write_text(
            json.dumps([
                {"name": "ItemA", "type": "Script"},
                {"name": "ItemB", "type": "API"},
                {"name": "ItemC", "type": "Script"},
            ])
        )

        result = load_options_from_json(
            json_file,
            filter_conditions={"type": "Script"},
            key_field="name",
        )
        assert result == ["ItemA", "ItemC"]

    def test_file_not_found(self):
        """Test FileNotFoundError when file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            load_options_from_json("/nonexistent/file.json")

    def test_invalid_json_format(self, tmp_path):
        """Test JSONDecodeError for invalid JSON."""
        json_file = tmp_path / "invalid.json"
        json_file.write_text("this is not valid json {{{")

        with pytest.raises(json.JSONDecodeError):
            load_options_from_json(json_file)

    def test_non_array_json(self, tmp_path):
        """Test ValueError when JSON is not an array."""
        json_file = tmp_path / "object.json"
        json_file.write_text('{"key": "value"}')

        with pytest.raises(ValueError, match="JSON file must contain an array"):
            load_options_from_json(json_file)

    def test_missing_key_field(self, tmp_path):
        """Test that items without the specified key are skipped."""
        json_file = tmp_path / "test.json"
        json_file.write_text(
            json.dumps([
                {"name": "ItemA"},
                {"other": "value"},
            ])
        )

        result = load_options_from_json(json_file, key_field="name")
        assert result == ["ItemA"]


class TestLoadOptionsFromYaml:
    """Test load_options_from_yaml function."""

    def test_load_all_items(self, tmp_path):
        """Test loading all items from YAML array."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(
            "- name: ItemA\n"
            "  type: Type1\n"
            "- name: ItemB\n"
            "  type: Type2\n"
        )

        result = load_options_from_yaml(yaml_file, key_field="name")
        assert result == ["ItemA", "ItemB"]

    def test_load_with_filter(self, tmp_path):
        """Test loading items with filter condition."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(
            "- name: ItemA\n"
            "  type: Script\n"
            "- name: ItemB\n"
            "  type: API\n"
            "- name: ItemC\n"
            "  type: Script\n"
        )

        result = load_options_from_yaml(
            yaml_file,
            filter_conditions={"type": "Script"},
            key_field="name",
        )
        assert result == ["ItemA", "ItemC"]

    def test_file_not_found(self):
        """Test FileNotFoundError when file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            load_options_from_yaml("/nonexistent/file.yaml")

    def test_invalid_yaml_format(self, tmp_path):
        """Test ValueError for invalid YAML."""
        yaml_file = tmp_path / "invalid.yaml"
        yaml_file.write_text("invalid: yaml: content: [[[")

        with pytest.raises(ValueError, match="Invalid YAML file format"):
            load_options_from_yaml(yaml_file)

    def test_non_array_yaml(self, tmp_path):
        """Test ValueError when YAML is not an array."""
        yaml_file = tmp_path / "object.yaml"
        yaml_file.write_text("key: value\n")

        with pytest.raises(ValueError, match="YAML file must contain an array"):
            load_options_from_yaml(yaml_file)

    def test_yaml_not_installed(self, tmp_path, monkeypatch):
        """Test ImportError when PyYAML is not installed."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("- name: ItemA\n")

        # Mock ImportError when importing yaml
        def mock_import(*args, **kwargs):
            if args[0] == "yaml":
                raise ImportError("No module named 'yaml'")
            return __import__(*args, **kwargs)

        monkeypatch.setattr("builtins.__import__", mock_import)

        with pytest.raises(ImportError, match="PyYAML is required"):
            load_options_from_yaml(yaml_file)
