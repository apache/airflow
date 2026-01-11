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

from airflow_breeze.commands.ui_commands import (
    LocaleFiles,
    LocaleKeySet,
    LocaleSummary,
    compare_keys,
    expand_plural_keys,
    flatten_keys,
    get_plural_base,
)


class TestPluralHandling:
    def test_get_plural_base_with_suffix(self):
        suffixes = ["_one", "_other"]
        assert get_plural_base("message_one", suffixes) == "message"
        assert get_plural_base("message_other", suffixes) == "message"

    def test_get_plural_base_without_suffix(self):
        suffixes = ["_one", "_other"]
        assert get_plural_base("message", suffixes) is None

    def test_get_plural_base_with_complex_suffixes(self):
        suffixes = ["_zero", "_one", "_two", "_few", "_many", "_other"]
        assert get_plural_base("item_zero", suffixes) == "item"
        assert get_plural_base("item_many", suffixes) == "item"

    def test_expand_plural_keys_english(self):
        keys = {"message_one", "message_other", "simple"}
        expanded = expand_plural_keys(keys, "en")
        # Should include both _one and _other forms for "message"
        assert "message_one" in expanded
        assert "message_other" in expanded
        assert "simple" in expanded

    def test_expand_plural_keys_polish(self):
        keys = {"message_one"}
        expanded = expand_plural_keys(keys, "pl")
        # Polish has 4 forms: _one, _few, _many, _other
        assert "message_one" in expanded
        assert "message_few" in expanded
        assert "message_many" in expanded
        assert "message_other" in expanded


class TestFlattenKeys:
    def test_flatten_simple_dict(self):
        data = {"key1": "value1", "key2": "value2"}
        keys = flatten_keys(data)
        assert set(keys) == {"key1", "key2"}

    def test_flatten_nested_dict(self):
        data = {"parent": {"child1": "value1", "child2": "value2"}}
        keys = flatten_keys(data)
        assert set(keys) == {"parent.child1", "parent.child2"}

    def test_flatten_deeply_nested_dict(self):
        data = {"level1": {"level2": {"level3": "value"}}}
        keys = flatten_keys(data)
        assert keys == ["level1.level2.level3"]

    def test_flatten_mixed_dict(self):
        data = {"simple": "value", "nested": {"key": "value2"}}
        keys = flatten_keys(data)
        assert set(keys) == {"simple", "nested.key"}


class TestCompareKeys:
    def test_compare_keys_identical(self, tmp_path):
        # Create temporary locale files
        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        test_data = {"greeting": "Hello", "farewell": "Goodbye"}

        (en_dir / "test.json").write_text(json.dumps(test_data))
        (de_dir / "test.json").write_text(json.dumps(test_data))

        # Mock LOCALES_DIR temporarily
        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path

        try:
            locale_files = [
                LocaleFiles(locale="en", files=["test.json"]),
                LocaleFiles(locale="de", files=["test.json"]),
            ]
            summary, missing_counts = compare_keys(locale_files)

            assert "test.json" in summary
            assert summary["test.json"].missing_keys.get("de", []) == []
            assert summary["test.json"].extra_keys.get("de", []) == []
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_compare_keys_with_missing(self, tmp_path):
        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        en_data = {"greeting": "Hello", "farewell": "Goodbye"}
        de_data = {"greeting": "Hallo"}

        (en_dir / "test.json").write_text(json.dumps(en_data))
        (de_dir / "test.json").write_text(json.dumps(de_data))

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path

        try:
            locale_files = [
                LocaleFiles(locale="en", files=["test.json"]),
                LocaleFiles(locale="de", files=["test.json"]),
            ]
            summary, missing_counts = compare_keys(locale_files)

            assert "test.json" in summary
            assert "farewell" in summary["test.json"].missing_keys.get("de", [])
            assert missing_counts["test.json"]["de"] == 1
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_compare_keys_with_extra(self, tmp_path):
        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        en_data = {"greeting": "Hello"}
        de_data = {"greeting": "Hallo", "extra": "Extra"}

        (en_dir / "test.json").write_text(json.dumps(en_data))
        (de_dir / "test.json").write_text(json.dumps(de_data))

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path

        try:
            locale_files = [
                LocaleFiles(locale="en", files=["test.json"]),
                LocaleFiles(locale="de", files=["test.json"]),
            ]
            summary, missing_counts = compare_keys(locale_files)

            assert "test.json" in summary
            assert "extra" in summary["test.json"].extra_keys.get("de", [])
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestLocaleSummary:
    def test_locale_summary_creation(self):
        summary = LocaleSummary(missing_keys={"de": ["key1", "key2"]}, extra_keys={"de": ["key3"]})
        assert summary.missing_keys == {"de": ["key1", "key2"]}
        assert summary.extra_keys == {"de": ["key3"]}


class TestLocaleFiles:
    def test_locale_files_creation(self):
        lf = LocaleFiles(locale="en", files=["test.json", "common.json"])
        assert lf.locale == "en"
        assert len(lf.files) == 2


class TestLocaleKeySet:
    def test_locale_key_set_with_keys(self):
        lks = LocaleKeySet(locale="en", keys={"key1", "key2"})
        assert lks.locale == "en"
        assert lks.keys == {"key1", "key2"}

    def test_locale_key_set_without_keys(self):
        lks = LocaleKeySet(locale="de", keys=None)
        assert lks.locale == "de"
        assert lks.keys is None


class TestCountTodos:
    def test_count_todos_in_string(self):
        from airflow_breeze.commands.ui_commands import count_todos

        assert count_todos("TODO: translate: Hello") == 1
        assert count_todos("Hello") == 0

    def test_count_todos_in_dict(self):
        from airflow_breeze.commands.ui_commands import count_todos

        data = {
            "key1": "TODO: translate: Hello",
            "key2": "Already translated",
            "key3": "TODO: translate: Goodbye",
        }
        assert count_todos(data) == 2

    def test_count_todos_nested(self):
        from airflow_breeze.commands.ui_commands import count_todos

        data = {
            "parent": {
                "child1": "TODO: translate: Hello",
                "child2": "TODO: translate: World",
            },
            "simple": "No TODO",
        }
        assert count_todos(data) == 2


class TestAddMissingTranslations:
    def test_add_missing_translations(self, tmp_path):
        from airflow_breeze.commands.ui_commands import add_missing_translations

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        en_data = {"greeting": "Hello", "farewell": "Goodbye"}
        de_data = {"greeting": "Hallo"}

        (en_dir / "test.json").write_text(json.dumps(en_data))
        (de_dir / "test.json").write_text(json.dumps(de_data))

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path

        try:
            summary = LocaleSummary(
                missing_keys={"de": ["farewell"]},
                extra_keys={"de": []},
            )
            add_missing_translations("de", {"test.json": summary})

            # Check that the file was updated
            de_data_updated = json.loads((de_dir / "test.json").read_text())
            assert "farewell" in de_data_updated
            assert de_data_updated["farewell"].startswith("TODO: translate:")
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestRemoveExtraTranslations:
    def test_remove_extra_translations(self, tmp_path):
        from airflow_breeze.commands.ui_commands import remove_extra_translations

        de_dir = tmp_path / "de"
        de_dir.mkdir()

        de_data = {"greeting": "Hallo", "extra": "Extra Key"}
        (de_dir / "test.json").write_text(json.dumps(de_data))

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path

        try:
            summary = LocaleSummary(
                missing_keys={"de": []},
                extra_keys={"de": ["extra"]},
            )
            remove_extra_translations("de", {"test.json": summary})

            # Check that the extra key was removed
            de_data_updated = json.loads((de_dir / "test.json").read_text())
            assert "extra" not in de_data_updated
            assert "greeting" in de_data_updated
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestNaturalSorting:
    def test_natural_sort_matches_eslint(self, tmp_path):
        """Test that keys are sorted like eslint-plugin-jsonc with natural: true (case-insensitive with case-sensitive tiebreaker)."""
        from airflow_breeze.commands.ui_commands import add_missing_translations

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        # Create English data with mixed-case keys to test sorting behavior
        # This tests the specific cases that differ between ASCII and natural sort
        en_data = {
            "assetEvent_few": "1",
            "asset_few": "2",
            "parseDuration": "3",
            "parsedAt": "4",
            "Zebra": "5",
            "apple": "6",
        }
        de_data = {}

        (en_dir / "test.json").write_text(json.dumps(en_data))
        (de_dir / "test.json").write_text(json.dumps(de_data))

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path

        try:
            summary = LocaleSummary(
                missing_keys={"de": list(en_data.keys())},
                extra_keys={"de": []},
            )
            add_missing_translations("de", {"test.json": summary})

            # Check that keys are sorted using natural sort (case-insensitive with case-sensitive tiebreaker)
            de_data_updated = json.loads((de_dir / "test.json").read_text())
            keys = list(de_data_updated.keys())

            # Expected order matches eslint-plugin-jsonc with natural: true:
            # - "apple" < "asset_few" < "assetEvent_few" (case-insensitive: apple < asset < assetevent)
            # - "parseDuration" < "parsedAt" (case-insensitive equal at "parse", then D < d at position 5)
            # - "Zebra" at the end (case-insensitive: z comes last)
            assert keys == ["apple", "asset_few", "assetEvent_few", "parseDuration", "parsedAt", "Zebra"]
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir
