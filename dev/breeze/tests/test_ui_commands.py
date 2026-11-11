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
import re

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.ui_commands import (
    LocaleFiles,
    LocaleKeySet,
    LocaleSummary,
    build_completeness_report,
    check_translation_completeness,
    compare_keys,
    expand_plural_keys,
    flatten_keys,
    get_locale_files,
    get_plural_base,
    natural_sort_key,
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

    def test_expand_plural_keys_only_expands_when_en_has_count_placeholder(self):
        # When en has error_one without {{count}}, we must not require error_other
        keys = {"error_one"}
        en_key_to_value = {"error_one": "1 Error"}
        expanded = expand_plural_keys(keys, "pt", en_key_to_value)
        assert "error_one" in expanded
        assert "error_other" not in expanded

    def test_expand_plural_keys_expands_when_en_has_count_placeholder(self):
        # Polish has 4 plural forms (_one, _few, _many, _other). Input has only _one and _other;
        # expansion must add _few and _many so the test verifies the expansion logic ran.
        keys = {"warning_one", "warning_other"}
        en_key_to_value = {"warning_one": "1 Warning", "warning_other": "{{count}} Warnings"}
        expanded = expand_plural_keys(keys, "pl", en_key_to_value)
        assert "warning_one" in expanded
        assert "warning_other" in expanded
        assert "warning_few" in expanded
        assert "warning_many" in expanded

    @pytest.mark.parametrize("placeholder", ["{{count}}", "{{count, number}}", "{{ count , number }}"])
    def test_expand_plural_keys_expands_single_en_form_with_count_placeholder(self, placeholder):
        keys = {"warning_other"}
        en_key_to_value = {"warning_other": f"{placeholder} Warnings"}
        expanded = expand_plural_keys(keys, "pl", en_key_to_value)
        assert expanded == {"warning_one", "warning_few", "warning_many", "warning_other"}

    def test_expand_plural_keys_expands_when_en_has_multiple_plural_forms(self):
        # Even without {{count}} in EN values, plural selection is still active when
        # EN defines multiple plural forms for the same base key.
        keys = {"dagRun_one", "dagRun_other"}
        en_key_to_value = {"dagRun_one": "Dag Run", "dagRun_other": "Dag Runs"}
        expanded = expand_plural_keys(keys, "pl", en_key_to_value)
        assert "dagRun_one" in expanded
        assert "dagRun_other" in expanded
        assert "dagRun_few" in expanded
        assert "dagRun_many" in expanded


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
            assert summary["test.json"].unused_keys.get("de", []) == []
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
            assert "extra" in summary["test.json"].unused_keys.get("de", [])
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_compare_keys_optional_plural_unused_when_no_count(self, tmp_path):
        """Plural variant of EN base with no {{count}} in EN is reported as unused."""
        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        # EN has only error_one (no {{count}}), so error_other is never used at runtime
        en_data = {"dagWarnings": {"error_one": "1 Error"}}
        de_data = {"dagWarnings": {"error_one": "1 Fehler", "error_other": "{{count}} Fehler"}}

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
            summary, _ = compare_keys(locale_files)

            # EN base has no {{count}}, so error_other is not required and is unused
            assert "dagWarnings.error_other" in summary["test.json"].unused_keys.get("de", [])
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_compare_keys_multi_plural_without_count_is_not_unused(self, tmp_path):
        """When EN defines multiple plural forms, locale plural forms should not be marked unused."""
        en_dir = tmp_path / "en"
        en_dir.mkdir()
        pl_dir = tmp_path / "pl"
        pl_dir.mkdir()

        # EN has plural variants but no explicit {{count}} interpolation in values.
        # Plural routing is still active and locale-specific forms should be treated as required.
        en_data = {"dagWarnings": {"error_one": "Error", "error_other": "Errors"}}
        pl_data = {
            "dagWarnings": {
                "error_one": "Błąd",
                "error_few": "Błędy",
                "error_many": "Błędów",
                "error_other": "Błędy",
            }
        }

        (en_dir / "test.json").write_text(json.dumps(en_data))
        (pl_dir / "test.json").write_text(json.dumps(pl_data))

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path

        try:
            locale_files = [
                LocaleFiles(locale="en", files=["test.json"]),
                LocaleFiles(locale="pl", files=["test.json"]),
            ]
            summary, _ = compare_keys(locale_files)

            # Previously these could be falsely flagged as unused.
            assert "dagWarnings.error_few" not in summary["test.json"].unused_keys.get("pl", [])
            assert "dagWarnings.error_many" not in summary["test.json"].unused_keys.get("pl", [])
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestGetLocaleFiles:
    def test_hidden_directories_are_not_treated_as_locales(self, tmp_path):
        (tmp_path / "en").mkdir()
        (tmp_path / "en" / "common.json").write_text(json.dumps({"greeting": "Hello"}))
        (tmp_path / ".claude" / ".cc-writes").mkdir(parents=True)

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            assert get_locale_files() == [LocaleFiles(locale="en", files=["common.json"])]
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestLocaleSummary:
    def test_locale_summary_creation(self):
        summary = LocaleSummary(
            missing_keys={"de": ["key1", "key2"]},
            unused_keys={"de": ["key3"]},
        )
        assert summary.missing_keys == {"de": ["key1", "key2"]}
        assert summary.unused_keys == {"de": ["key3"]}


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
                unused_keys={"de": []},
            )
            add_missing_translations("de", {"test.json": summary})

            # Check that the file was updated
            de_data_updated = json.loads((de_dir / "test.json").read_text())
            assert "farewell" in de_data_updated
            assert de_data_updated["farewell"].startswith("TODO: translate:")
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestRemoveUnusedTranslations:
    def test_remove_unused_translations(self, tmp_path):
        from airflow_breeze.commands.ui_commands import remove_unused_translations

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
                unused_keys={"de": ["extra"]},
            )
            remove_unused_translations("de", {"test.json": summary})

            # Check that the unused key was removed
            de_data_updated = json.loads((de_dir / "test.json").read_text())
            assert "extra" not in de_data_updated
            assert "greeting" in de_data_updated
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestNaturalSorting:
    def test_natural_sort_matches_eslint(self, tmp_path):
        """Test that keys are sorted like eslint-plugin-jsonc with natural: true."""
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
                unused_keys={"de": []},
            )
            add_missing_translations("de", {"test.json": summary})

            # Check that keys are sorted like eslint-plugin-jsonc with natural: true
            de_data_updated = json.loads((de_dir / "test.json").read_text())
            keys = list(de_data_updated.keys())

            # natural-compare sorts uppercase before lowercase, so "Zebra" comes first.
            assert keys == ["Zebra", "apple", "asset_few", "assetEvent_few", "parseDuration", "parsedAt"]
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    @pytest.mark.parametrize(
        ("keys", "expected"),
        [
            # Real key pairs from the UI locales where a case-insensitive sort
            # disagrees with natural-compare's case-sensitive ordering.
            (["logicalDate", "logLevel"], ["logLevel", "logicalDate"]),
            (["selected", "selectLanguage"], ["selectLanguage", "selected"]),
            (["modes", "modeTooltip"], ["modeTooltip", "modes"]),
            (["sortedAscending", "sortMultiColumnHint"], ["sortMultiColumnHint", "sortedAscending"]),
            # Digit runs sort numerically, except runs starting with "0".
            (["a2", "a10", "a02"], ["a02", "a2", "a10"]),
        ],
    )
    def test_natural_sort_key_matches_eslint(self, keys, expected):
        assert sorted(keys, key=natural_sort_key) == expected


def _write_locale(tmp_path, locale: str, filename: str, data: dict) -> None:
    locale_dir = tmp_path / locale
    locale_dir.mkdir(exist_ok=True)
    (locale_dir / filename).write_text(json.dumps(data))


class TestBuildCompletenessReport:
    def test_report_counts_and_key_lists(self, tmp_path):
        # en: 3 plain keys + a plural pair with {{count}}; de: one TODO, one missing, one unused
        _write_locale(
            tmp_path,
            "en",
            "test.json",
            {"a": "A", "b": "B", "c": "C", "n_one": "1 thing", "n_other": "{{count}} things"},
        )
        _write_locale(
            tmp_path,
            "de",
            "test.json",
            {"a": "A!", "b": "TODO: translate: B", "n_one": "Ding", "n_other": "Dinge", "zz": "extra"},
        )

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            locale_files = [
                LocaleFiles(locale="en", files=["test.json"]),
                LocaleFiles(locale="de", files=["test.json"]),
            ]
            summary, _ = compare_keys(locale_files)
            report = build_completeness_report(locale_files, summary)

            assert "en" not in report
            stats = report["de"]["files"]["test.json"]
            assert stats["required"] == 5
            assert stats["required_base_en"] == 5
            assert stats["required_plural"] == 0
            assert stats["translated"] == 3  # a, n_one, n_other; b is a TODO and c is missing
            assert stats["missing"] == 1
            assert stats["missing_keys"] == ["c"]
            assert stats["todos"] == 1
            assert stats["unused"] == 1
            assert stats["unused_keys"] == ["zz"]
            assert stats["coverage"] == pytest.approx(60.0)

            totals = report["de"]["totals"]
            assert totals["required"] == 5
            assert totals["translated"] == 3
            assert totals["missing"] == 1
            assert totals["todos"] == 1
            assert totals["unused"] == 1
            assert totals["coverage"] == pytest.approx(60.0)
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_report_expands_locale_specific_plural_forms(self, tmp_path):
        # pl requires _one/_few/_many/_other for a base whose EN value interpolates count
        _write_locale(tmp_path, "en", "test.json", {"n_one": "1 thing", "n_other": "{{count}} things"})
        _write_locale(tmp_path, "pl", "test.json", {"n_one": "rzecz", "n_other": "rzeczy"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            locale_files = [
                LocaleFiles(locale="en", files=["test.json"]),
                LocaleFiles(locale="pl", files=["test.json"]),
            ]
            summary, _ = compare_keys(locale_files)
            report = build_completeness_report(locale_files, summary)

            stats = report["pl"]["files"]["test.json"]
            assert stats["required_base_en"] == 2
            assert stats["required_plural"] == 2  # _few and _many on top of the EN base keys
            assert stats["required"] == 4
            assert stats["missing_keys"] == ["n_few", "n_many"]
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_report_skips_files_without_english_source(self, tmp_path):
        _write_locale(tmp_path, "en", "test.json", {"a": "A"})
        _write_locale(tmp_path, "de", "test.json", {"a": "A!"})
        _write_locale(tmp_path, "de", "orphan.json", {"b": "B"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            locale_files = [
                LocaleFiles(locale="en", files=["test.json"]),
                LocaleFiles(locale="de", files=["test.json", "orphan.json"]),
            ]
            summary, _ = compare_keys(locale_files)
            report = build_completeness_report(locale_files, summary)

            assert list(report["de"]["files"]) == ["test.json"]
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestCheckTranslationCompletenessJson:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_json_prints_a_single_pure_json_document(self, runner, tmp_path):
        _write_locale(tmp_path, "en", "test.json", {"a": "A", "b": "B"})
        _write_locale(tmp_path, "de", "test.json", {"a": "A!", "zz": "extra"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = runner.invoke(check_translation_completeness, ["--json"], catch_exceptions=False)
            assert result.exit_code == 0
            # The whole stdout must parse as JSON — nothing else may be printed around it.
            payload = json.loads(result.stdout)
            assert payload["complete"] is False
            stats = payload["languages"]["de"]["files"]["test.json"]
            assert stats["missing"] == 1
            assert stats["missing_keys"] == ["b"]
            assert stats["unused_keys"] == ["zz"]
            assert stats["coverage"] == 50.0
            assert payload["languages"]["de"]["totals"]["missing"] == 1
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_reports_complete_when_nothing_is_missing(self, runner, tmp_path):
        data = {"a": "A", "b": "B"}
        _write_locale(tmp_path, "en", "test.json", data)
        _write_locale(tmp_path, "de", "test.json", {"a": "A!", "b": "B!"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = runner.invoke(check_translation_completeness, ["--json"], catch_exceptions=False)
            assert result.exit_code == 0
            payload = json.loads(result.stdout)
            assert payload["complete"] is True
            assert payload["languages"]["de"]["totals"]["coverage"] == 100.0
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_complete_stays_false_while_todos_remain(self, runner, tmp_path):
        # The post---add-missing state: nothing missing, nothing unused, only TODO stubs.
        # The table verdict treats TODOs as incomplete, so the JSON flag must as well.
        _write_locale(tmp_path, "en", "test.json", {"a": "A", "b": "B"})
        _write_locale(tmp_path, "de", "test.json", {"a": "TODO: translate: A", "b": "TODO: translate: B"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = runner.invoke(check_translation_completeness, ["--json"], catch_exceptions=False)
            assert result.exit_code == 0
            payload = json.loads(result.stdout)
            totals = payload["languages"]["de"]["totals"]
            assert totals["missing"] == 0
            assert totals["unused"] == 0
            assert totals["todos"] == 2
            assert payload["complete"] is False
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_with_language_filter_reports_only_that_language(self, runner, tmp_path):
        _write_locale(tmp_path, "en", "test.json", {"a": "A"})
        _write_locale(tmp_path, "de", "test.json", {"a": "A!"})
        _write_locale(tmp_path, "pl", "test.json", {"a": "A?"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = runner.invoke(
                check_translation_completeness, ["--json", "--language", "de"], catch_exceptions=False
            )
            assert result.exit_code == 0
            payload = json.loads(result.stdout)
            assert list(payload["languages"]) == ["de"]
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    @pytest.mark.parametrize("language", ["xx", "en"])
    def test_json_exits_2_for_invalid_language_without_printing_json(self, runner, tmp_path, language):
        _write_locale(tmp_path, "en", "test.json", {"a": "A"})
        _write_locale(tmp_path, "de", "test.json", {"a": "A!"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = runner.invoke(check_translation_completeness, ["--json", "--language", language])
            assert result.exit_code == 2
            assert "{" not in result.stdout
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_replays_fatal_error_output_to_stderr(self, runner, tmp_path):
        # A locale without a PLURAL_SUFFIXES entry aborts the checker; in --json mode the
        # suppressed narration must be replayed to stderr instead of vanishing with the exit.
        _write_locale(tmp_path, "en", "test.json", {"a": "A"})
        _write_locale(tmp_path, "xy", "test.json", {"a": "A!"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = runner.invoke(check_translation_completeness, ["--json"])
            assert result.exit_code == 1
            stderr_text = re.sub(r"\x1b\[[0-9;]*m", "", result.stderr)
            assert "No plural suffixes defined for language 'xy'" in stderr_text
            assert "{" not in result.stdout
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_replays_narration_when_computation_raises(self, runner, tmp_path):
        # A malformed locale file makes the computation raise (not sys.exit) after
        # compare_keys() has narrated "Error loading ..."; the narration must surface
        # on stderr instead of vanishing with the suppression buffer.
        _write_locale(tmp_path, "en", "test.json", {"a": "A"})
        locale_dir = tmp_path / "de"
        locale_dir.mkdir(exist_ok=True)
        (locale_dir / "test.json").write_text("{not valid json")

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = runner.invoke(check_translation_completeness, ["--json"])
            assert result.exit_code == 1
            assert "{" not in result.stdout
            stderr_text = re.sub(r"\x1b\[[0-9;]*m", "", result.stderr)
            assert "Error loading" in stderr_text
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_default_table_mode_still_prints_tables(self, runner, tmp_path):
        _write_locale(tmp_path, "en", "test.json", {"a": "A", "b": "B"})
        _write_locale(tmp_path, "de", "test.json", {"a": "A!"})

        import airflow_breeze.commands.ui_commands as ui_commands

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = runner.invoke(check_translation_completeness, [], catch_exceptions=False)
            assert result.exit_code == 0
            assert "Translation Progress: de" in result.output
            assert "Some translations are not complete!" in result.output
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir
