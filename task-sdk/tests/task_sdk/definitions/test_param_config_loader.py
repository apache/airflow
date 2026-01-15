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

import pytest

from airflow.sdk.definitions.param_config_loader import load_options_from_file


class TestLoadOptionsFromFile:
    """Test load_options_from_file function with different file formats."""

    def test_load_ini_all_sections(self, tmp_path):
        """Test loading all section names from INI file."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text(
            "[InterfaceA]\n"
            "TYPE = Script\n"
            "DESCRIPTION = Interface A\n\n"
            "[InterfaceB]\n"
            "TYPE = EBICS\n"
            "DESCRIPTION = Interface B\n"
        )

        result = load_options_from_file(ini_file, extension="ini")
        assert result == ["InterfaceA", "InterfaceB"]

    def test_load_ini_with_filter(self, tmp_path):
        """Test loading INI sections with filter condition."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text(
            "[InterfaceA]\nTYPE = Script\n[InterfaceB]\nTYPE = API\n[InterfaceC]\nTYPE = Script\n"
        )

        result = load_options_from_file(
            ini_file,
            extension="ini",
            filter_conditions={"TYPE": "Script"},
        )
        assert result == ["InterfaceA", "InterfaceC"]

    def test_load_ini_specific_key(self, tmp_path):
        """Test loading specific key values from INI file."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text(
            "[InterfaceA]\nNAME = Interface A\nTYPE = Script\n[InterfaceB]\nNAME = Interface B\nTYPE = API\n"
        )

        result = load_options_from_file(ini_file, extension="ini", key_field="NAME")
        assert result == ["Interface A", "Interface B"]

    def test_load_json_all_items(self, tmp_path):
        """Test loading all items from JSON file."""
        json_file = tmp_path / "test.json"
        data = [
            {"name": "InterfaceA", "type": "Script"},
            {"name": "InterfaceB", "type": "EBICS"},
        ]
        json_file.write_text(json.dumps(data))

        result = load_options_from_file(json_file, extension="json")
        assert result == ["InterfaceA", "InterfaceB"]

    def test_load_json_with_filter(self, tmp_path):
        """Test loading JSON items with filter condition."""
        json_file = tmp_path / "test.json"
        data = [
            {"name": "InterfaceA", "type": "Script"},
            {"name": "InterfaceB", "type": "API"},
            {"name": "InterfaceC", "type": "Script"},
        ]
        json_file.write_text(json.dumps(data))

        result = load_options_from_file(
            json_file,
            extension="json",
            filter_conditions={"type": "Script"},
        )
        assert result == ["InterfaceA", "InterfaceC"]

    def test_load_json_custom_key(self, tmp_path):
        """Test loading specific key from JSON items."""
        json_file = tmp_path / "test.json"
        data = [
            {"name": "InterfaceA", "code": "IF_A"},
            {"name": "InterfaceB", "code": "IF_B"},
        ]
        json_file.write_text(json.dumps(data))

        result = load_options_from_file(json_file, extension="json", key_field="code")
        assert result == ["IF_A", "IF_B"]

    def test_load_yaml_all_items(self, tmp_path):
        """Test loading all items from YAML file."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("- name: InterfaceA\n  type: Script\n- name: InterfaceB\n  type: EBICS\n")

        result = load_options_from_file(yaml_file, extension="yaml")
        assert result == ["InterfaceA", "InterfaceB"]

    def test_load_yaml_with_filter(self, tmp_path):
        """Test loading YAML items with filter condition."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(
            "- name: InterfaceA\n"
            "  type: Script\n"
            "- name: InterfaceB\n"
            "  type: API\n"
            "- name: InterfaceC\n"
            "  type: Script\n"
        )

        result = load_options_from_file(
            yaml_file,
            extension="yaml",
            filter_conditions={"type": "Script"},
        )
        assert result == ["InterfaceA", "InterfaceC"]

    def test_file_not_found(self, tmp_path):
        """Test error handling when file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            load_options_from_file(tmp_path / "nonexistent.ini", extension="ini")

    def test_invalid_ini_format(self, tmp_path):
        """Test error handling for invalid INI format."""
        ini_file = tmp_path / "invalid.ini"
        ini_file.write_text("[Section\nMissing closing bracket")

        with pytest.raises(ValueError, match="Invalid INI file format"):
            load_options_from_file(ini_file, extension="ini")

    def test_invalid_json_format(self, tmp_path):
        """Test error handling for invalid JSON format."""
        json_file = tmp_path / "invalid.json"
        json_file.write_text("{invalid json")

        with pytest.raises(ValueError, match="Invalid JSON file format"):
            load_options_from_file(json_file, extension="json")

    def test_invalid_yaml_format(self, tmp_path):
        """Test error handling for invalid YAML format."""
        yaml_file = tmp_path / "invalid.yaml"
        yaml_file.write_text("- item:\n  - nested:\n    bad indentation")

        with pytest.raises(ValueError, match="Invalid YAML file format"):
            load_options_from_file(yaml_file, extension="yaml")

    def test_json_non_array(self, tmp_path):
        """Test error when JSON file doesn't contain array."""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"key": "value"}')

        with pytest.raises(ValueError, match="must contain an array"):
            load_options_from_file(json_file, extension="json")

    def test_yaml_non_array(self, tmp_path):
        """Test error when YAML file doesn't contain array."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("key: value\n")

        with pytest.raises(ValueError, match="must contain an array"):
            load_options_from_file(yaml_file, extension="yaml")

    def test_ini_missing_key_field(self, tmp_path):
        """Test handling of missing key field in INI file."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text("[InterfaceA]\nTYPE = Script\n[InterfaceB]\nTYPE = API\n")

        # Request a key that doesn't exist - should return empty list
        result = load_options_from_file(ini_file, extension="ini", key_field="NONEXISTENT")
        assert result == []

    def test_json_missing_key_field(self, tmp_path):
        """Test handling of missing key field in JSON file."""
        json_file = tmp_path / "test.json"
        data = [
            {"name": "InterfaceA"},
            {"name": "InterfaceB"},
        ]
        json_file.write_text(json.dumps(data))

        # Request a key that doesn't exist - should return empty list
        result = load_options_from_file(json_file, extension="json", key_field="nonexistent")
        assert result == []

    def test_default_key_field_ini(self, tmp_path):
        """Test default key_field for INI files is 'section'."""
        ini_file = tmp_path / "test.ini"
        ini_file.write_text("[SectionA]\nkey=value\n[SectionB]\nkey=value\n")

        result = load_options_from_file(ini_file, extension="ini")
        assert result == ["SectionA", "SectionB"]

    def test_default_key_field_json(self, tmp_path):
        """Test default key_field for JSON files is 'name'."""
        json_file = tmp_path / "test.json"
        data = [{"name": "ItemA"}, {"name": "ItemB"}]
        json_file.write_text(json.dumps(data))

        result = load_options_from_file(json_file, extension="json")
        assert result == ["ItemA", "ItemB"]

    def test_default_key_field_yaml(self, tmp_path):
        """Test default key_field for YAML files is 'name'."""
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("- name: ItemA\n- name: ItemB\n")

        result = load_options_from_file(yaml_file, extension="yaml")
        assert result == ["ItemA", "ItemB"]
