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
"""Unit tests for dev/registry/extract_versions.py."""

from __future__ import annotations

import textwrap
from unittest.mock import patch

import pytest
from extract_versions import (
    AIRFLOW_ROOT,
    PROVIDERS_JSON_CANDIDATES,
    SCRIPT_DIR,
    extract_modules_from_yaml,
    extract_version_data,
)
from registry_tools.types import CLASS_LEVEL_SECTIONS, DICT_SHAPED_CLASS_LEVEL_SECTIONS


class TestProvidersJsonCandidates:
    """Lock down the lookup order for providers.json.

    Regression test for the workflow path mismatch where the
    `Download existing providers.json` step writes to dev/registry/ but
    the script previously only checked the eleventy data dir.
    """

    def test_dev_registry_path_first(self):
        # The workflow's S3 download writes here; checked first so CI runs
        # work without having to do a full local extract pass first.
        assert PROVIDERS_JSON_CANDIDATES[0] == SCRIPT_DIR / "providers.json"

    def test_eleventy_data_dir_fallback(self):
        # Local dev convenience: after a full extract pass, the data dir
        # has providers.json and we don't need to also keep a copy in
        # dev/registry/.
        assert PROVIDERS_JSON_CANDIDATES[1] == AIRFLOW_ROOT / "registry" / "src" / "_data" / "providers.json"

    def test_no_other_candidates(self):
        # Adding silently to the list could mask path mismatches that
        # should be caught and fixed at the source. Match siblings (extract_
        # parameters.py, extract_metadata.py) which use exactly these two.
        assert len(PROVIDERS_JSON_CANDIDATES) == 2


# Expected (category, description_suffix) per class-level section, entered by
# hand from the PR #70190 spec rather than derived from production code, so a
# regression in extract_versions.py's mapping logic is actually caught.
EXPECTED_CLASS_LEVEL_CATEGORIES = {
    "notifications": "notifications",
    "secrets-backends": "secrets",
    "logging": "logging",
    "executors": "executors",
    "extra-links": "extra-links",
    "queues": "queues",
    "auth-managers": "auth-managers",
    "db-managers": "db-managers",
}

EXPECTED_CLASS_LEVEL_DESC_SUFFIXES = {
    "notifier": "notifier",
    "secret": "secrets backend",
    "logging": "log handler",
    "executor": "executor",
    "extra_link": "extra link",
    "queue": "queue",
    "auth_manager": "auth manager",
    "db_manager": "db manager",
}


def _extract_class_level_modules(provider_yaml: dict) -> list[dict]:
    return extract_modules_from_yaml(
        provider_yaml,
        tag="providers-test/1.0.0",
        layout="new",
        dir_path="test",
        provider_id="test",
        version="1.0.0",
    )


class TestExtractModulesFromYamlClassLevelSections:
    @pytest.mark.parametrize(("yaml_key", "mod_type"), list(CLASS_LEVEL_SECTIONS.items()))
    def test_class_level_section_produces_module(self, yaml_key, mod_type):
        class_path = f"airflow.providers.test.{yaml_key.replace('-', '_')}.example.ExampleClass"
        provider_yaml = {yaml_key: [class_path]}

        modules = _extract_class_level_modules(provider_yaml)

        assert len(modules) == 1
        module = modules[0]
        assert module["type"] == mod_type
        assert module["category"] == EXPECTED_CLASS_LEVEL_CATEGORIES[yaml_key]
        expected_desc_suffix = EXPECTED_CLASS_LEVEL_DESC_SUFFIXES[mod_type]
        assert module["short_description"] == f"ExampleClass {expected_desc_suffix}"

    def test_all_class_level_sections_produce_covered_types(self):
        """Type coverage must track CLASS_LEVEL_SECTIONS; a key added there
        without matching handling in extract_versions.py fails here."""
        provider_yaml = {
            yaml_key: [f"airflow.providers.test.{yaml_key.replace('-', '_')}.example.ExampleClass"]
            for yaml_key in CLASS_LEVEL_SECTIONS
        }

        modules = _extract_class_level_modules(provider_yaml)

        assert {m["type"] for m in modules} == set(CLASS_LEVEL_SECTIONS.values())


# plugins/dialects aren't in CLASS_LEVEL_CATEGORY_OVERRIDES, so category ==
# yaml_key itself (same fallback as EXPECTED_CLASS_LEVEL_CATEGORIES above).
EXPECTED_DICT_SHAPED_CATEGORIES = {
    "plugins": "plugins",
    "dialects": "dialects",
}

EXPECTED_DICT_SHAPED_DESC_SUFFIXES = {
    "plugin": "plugin",
    "dialect": "dialect",
}


class TestExtractModulesFromYamlDictShapedSections:
    @pytest.mark.parametrize(
        ("yaml_key", "type_and_field"),
        list(DICT_SHAPED_CLASS_LEVEL_SECTIONS.items()),
    )
    def test_dict_shaped_section_produces_module(self, yaml_key, type_and_field):
        mod_type, class_path_field, _integration_field = type_and_field
        class_path = f"airflow.providers.test.{yaml_key.replace('-', '_')}.example.ExampleClass"
        provider_yaml = {yaml_key: [{class_path_field: class_path}]}

        modules = _extract_class_level_modules(provider_yaml)

        assert len(modules) == 1
        module = modules[0]
        assert module["type"] == mod_type
        assert module["category"] == EXPECTED_DICT_SHAPED_CATEGORIES[yaml_key]
        expected_desc_suffix = EXPECTED_DICT_SHAPED_DESC_SUFFIXES[mod_type]
        assert module["short_description"] == f"ExampleClass {expected_desc_suffix}"

    def test_all_dict_shaped_sections_produce_covered_types(self):
        """Type coverage must track DICT_SHAPED_CLASS_LEVEL_SECTIONS; a key
        added there without matching handling in extract_versions.py fails here."""
        provider_yaml = {
            yaml_key: [
                {
                    class_path_field: f"airflow.providers.test.{yaml_key.replace('-', '_')}.example.ExampleClass"
                }
            ]
            for yaml_key, (
                _,
                class_path_field,
                _integration_field,
            ) in DICT_SHAPED_CLASS_LEVEL_SECTIONS.items()
        }

        modules = _extract_class_level_modules(provider_yaml)

        assert {m["type"] for m in modules} == {t for t, _, _ in DICT_SHAPED_CLASS_LEVEL_SECTIONS.values()}


class TestExtractVersionDataConnectionTypes:
    """`external-services` on a connection-types entry must survive into the
    per-version metadata.json (extract_versions.py:399) the same way it does
    for the latest release in providers.json (extract_metadata.py). A
    superseded release only has this file as its data source, so a dropped
    or mis-keyed field here silently vanishes from just that version's page.
    """

    PROVIDER_YAML = textwrap.dedent("""\
        name: Test Provider
        connection-types:
          - connection-type: testconn
            hook-class-name: airflow.providers.test.hooks.TestHook
            external-services:
              - openai
              - anthropic
        """)

    @patch("extract_versions.extract_modules_from_yaml", autospec=True, return_value=[])
    @patch("extract_versions.fetch_provider_inventory", autospec=True, return_value=None)
    @patch("extract_versions.git_show", autospec=True)
    @patch("extract_versions.detect_layout", autospec=True, return_value="new")
    @patch("extract_versions.git_tag_exists", autospec=True, return_value=True)
    def test_external_services_propagates_to_version_metadata(
        self, _tag_exists, _layout, mock_git_show, _inventory, _modules
    ):
        mock_git_show.side_effect = lambda tag, path: (
            self.PROVIDER_YAML if path.endswith("provider.yaml") else None
        )

        result = extract_version_data("test", "1.0.0", "test")

        assert result is not None
        assert result["connection_types"][0]["external_services"] == ["openai", "anthropic"]
