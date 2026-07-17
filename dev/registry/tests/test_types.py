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
"""Validates the registry_tools.types module — single source of truth for module types."""

from __future__ import annotations

import pytest
from registry_tools.types import (
    ALL_TYPE_IDS,
    BASE_CLASS_IMPORTS,
    CLASS_LEVEL_SECTIONS,
    FLAT_LEVEL_SECTIONS,
    MODULE_LEVEL_SECTIONS,
    MODULE_TYPES,
    TYPE_SUFFIXES,
)

REQUIRED_FIELDS = {"yaml_key", "level", "label", "icon", "suffixes"}
VALID_LEVELS = {"module", "flat"}


class TestModuleTypes:
    def test_all_type_ids_lowercase_alphanumeric(self):
        for type_id in ALL_TYPE_IDS:
            assert type_id == type_id.lower(), f"{type_id} is not lowercase"
            assert type_id.replace("_", "").isalnum(), f"{type_id} contains invalid chars"

    def test_every_type_has_required_fields(self):
        for type_id, info in MODULE_TYPES.items():
            missing = REQUIRED_FIELDS - set(info.keys())
            assert not missing, f"{type_id} missing fields: {missing}"

    def test_no_duplicate_yaml_keys(self):
        yaml_keys = [info["yaml_key"] for info in MODULE_TYPES.values()]
        assert len(yaml_keys) == len(set(yaml_keys)), f"Duplicate yaml_keys: {yaml_keys}"

    def test_valid_level_values(self):
        for type_id, info in MODULE_TYPES.items():
            assert info["level"] in VALID_LEVELS, f"{type_id} has invalid level: {info['level']}"

    def test_icons_are_single_character(self):
        for type_id, info in MODULE_TYPES.items():
            assert len(info["icon"]) == 1, f"{type_id} icon is not single char: {info['icon']}"

    def test_labels_are_nonempty(self):
        for type_id, info in MODULE_TYPES.items():
            assert info["label"].strip(), f"{type_id} has empty label"

    def test_suffixes_are_lists(self):
        for type_id, info in MODULE_TYPES.items():
            assert isinstance(info["suffixes"], list), f"{type_id} suffixes is not a list"


class TestDerivedLookups:
    def test_module_level_sections_match_module_types(self):
        for yaml_key, type_id in MODULE_LEVEL_SECTIONS.items():
            assert type_id in MODULE_TYPES
            assert MODULE_TYPES[type_id]["yaml_key"] == yaml_key
            assert MODULE_TYPES[type_id]["level"] == "module"

    def test_flat_level_sections_match_module_types(self):
        for yaml_key, type_id in FLAT_LEVEL_SECTIONS.items():
            assert type_id in MODULE_TYPES
            assert MODULE_TYPES[type_id]["yaml_key"] == yaml_key
            assert MODULE_TYPES[type_id]["level"] == "flat"

    def test_all_types_covered_by_sections(self):
        """Every type should appear in either MODULE_LEVEL or FLAT_LEVEL."""
        covered = set(MODULE_LEVEL_SECTIONS.values()) | set(FLAT_LEVEL_SECTIONS.values())
        assert covered == set(ALL_TYPE_IDS)

    def test_type_suffixes_matches_module_types(self):
        for type_id, suffixes in TYPE_SUFFIXES.items():
            assert type_id in MODULE_TYPES
            assert suffixes == MODULE_TYPES[type_id]["suffixes"]

    def test_class_level_sections_are_subset_of_flat(self):
        for yaml_key, type_id in CLASS_LEVEL_SECTIONS.items():
            assert yaml_key in FLAT_LEVEL_SECTIONS
            assert FLAT_LEVEL_SECTIONS[yaml_key] == type_id


class TestBaseClassImports:
    def test_all_entries_are_tuples(self):
        for entry in BASE_CLASS_IMPORTS:
            assert isinstance(entry, tuple)
            assert len(entry) == 2

    @pytest.mark.parametrize(("type_name", "import_path"), BASE_CLASS_IMPORTS)
    def test_type_names_exist_in_module_types(self, type_name, import_path):
        assert type_name in MODULE_TYPES

    @pytest.mark.parametrize(("type_name", "import_path"), BASE_CLASS_IMPORTS)
    def test_import_paths_are_dotted(self, type_name, import_path):
        assert "." in import_path
