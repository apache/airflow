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


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return " ".join(_ANSI_RE.sub("", text).split())


class TestBuildCompletenessReport:
    def test_complete_report(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import LocaleFiles, LocaleSummary, build_completeness_report

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        fr_dir = tmp_path / "fr"
        fr_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"hello": "Hello", "world": "World"}))
        (fr_dir / "app.json").write_text(json.dumps({"hello": "Bonjour", "world": "Monde"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            locale_files = [
                LocaleFiles("en", ["app.json"]),
                LocaleFiles("fr", ["app.json"]),
            ]
            summary = {"app.json": LocaleSummary(missing_keys={"fr": []}, unused_keys={"fr": []})}
            report = build_completeness_report(locale_files, summary)

            assert report["complete"] is True
            assert "fr" in report["languages"]
            fr_file = report["languages"]["fr"]["files"]["app.json"]
            assert fr_file["required"] == 2
            assert fr_file["translated"] == 2
            assert fr_file["missing"] == 0
            assert fr_file["todos"] == 0
            assert fr_file["unused"] == 0
            assert fr_file["coverage"] == 100.0
            assert fr_file["missing_keys"] == []
            assert fr_file["unused_keys"] == []
            assert report["languages"]["fr"]["totals"]["coverage"] == 100.0
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_missing_and_unused_and_todo_keys(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import LocaleFiles, LocaleSummary, build_completeness_report

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        es_dir = tmp_path / "es"
        es_dir.mkdir()

        en_data = {"key1": "One", "key2": "Two", "key3": "Three"}
        es_data = {"key1": "Uno", "key2": "TODO: translate: Two", "extra": "Sobran"}
        (en_dir / "app.json").write_text(json.dumps(en_data))
        (es_dir / "app.json").write_text(json.dumps(es_data))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            locale_files = [
                LocaleFiles("en", ["app.json"]),
                LocaleFiles("es", ["app.json"]),
            ]
            summary = {
                "app.json": LocaleSummary(missing_keys={"es": ["key3"]}, unused_keys={"es": ["extra"]})
            }
            report = build_completeness_report(locale_files, summary)

            assert report["complete"] is False
            es_file = report["languages"]["es"]["files"]["app.json"]
            assert es_file["required"] == 3
            assert es_file["translated"] == 1
            assert es_file["missing"] == 1
            assert es_file["todos"] == 1
            assert es_file["unused"] == 1
            assert es_file["coverage"] == round(100 * 1 / 3, 1)
            assert es_file["missing_keys"] == ["key3"]
            assert es_file["unused_keys"] == ["extra"]
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_plural_keys_expansion_in_report(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import LocaleFiles, LocaleSummary, build_completeness_report

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        pl_dir = tmp_path / "pl"
        pl_dir.mkdir()

        en_data = {"warning_one": "1 Warning", "warning_other": "{{count}} Warnings"}
        # Polish has 4 plural forms: _one, _few, _many, _other
        pl_data = {
            "warning_one": "1 ostrzeżenie",
            "warning_few": "{{count}} ostrzeżenia",
            "warning_many": "{{count}} ostrzeżeń",
            "warning_other": "{{count}} ostrzeżeń",
        }
        (en_dir / "app.json").write_text(json.dumps(en_data))
        (pl_dir / "app.json").write_text(json.dumps(pl_data))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            locale_files = [
                LocaleFiles("en", ["app.json"]),
                LocaleFiles("pl", ["app.json"]),
            ]
            summary = {"app.json": LocaleSummary(missing_keys={"pl": []}, unused_keys={"pl": []})}
            report = build_completeness_report(locale_files, summary)

            pl_file = report["languages"]["pl"]["files"]["app.json"]
            assert pl_file["required"] == 4
            assert pl_file["translated"] == 4
            assert pl_file["coverage"] == 100.0
            assert report["complete"] is True
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_language_filter_and_found_difference_flag(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import LocaleFiles, LocaleSummary, build_completeness_report

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()
        fr_dir = tmp_path / "fr"
        fr_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"hello": "Hello"}))
        (de_dir / "app.json").write_text(json.dumps({"hello": "Hallo"}))
        (fr_dir / "app.json").write_text(json.dumps({"hello": "Bonjour"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            locale_files = [
                LocaleFiles("en", ["app.json"]),
                LocaleFiles("de", ["app.json"]),
                LocaleFiles("fr", ["app.json"]),
            ]
            summary = {
                "app.json": LocaleSummary(missing_keys={"de": [], "fr": []}, unused_keys={"de": [], "fr": []})
            }
            # Test language filter
            report_de = build_completeness_report(locale_files, summary, language="de")
            assert list(report_de["languages"].keys()) == ["de"]
            assert report_de["complete"] is True

            # If external difference (e.g. file set difference) is detected, complete is False
            report_diff = build_completeness_report(locale_files, summary, found_difference=True)
            assert report_diff["complete"] is False
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir


class TestCheckTranslationCompletenessJsonCommand:
    def test_json_stdout_purity(self, tmp_path, monkeypatch):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import check_translation_completeness

        monkeypatch.setenv("CI", "true")
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setenv("GITHUB_SHA", "0" * 40)

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"hello": "Hello"}))
        (de_dir / "app.json").write_text(json.dumps({"hello": "Hallo"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = CliRunner().invoke(check_translation_completeness, ["--json"], catch_exceptions=False)
            assert result.exit_code == 0
            payload = json.loads(result.stdout)
            assert set(payload.keys()) == {"languages", "complete"}
            assert payload["complete"] is True
            assert "de" in payload["languages"]
            assert payload["languages"]["de"]["totals"]["coverage"] == 100.0
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_with_language_filter(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import check_translation_completeness

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()
        fr_dir = tmp_path / "fr"
        fr_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"hello": "Hello"}))
        (de_dir / "app.json").write_text(json.dumps({"hello": "Hallo"}))
        (fr_dir / "app.json").write_text(json.dumps({"hello": "Bonjour"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = CliRunner().invoke(
                check_translation_completeness, ["--language", "de", "--json"], catch_exceptions=False
            )
            assert result.exit_code == 0
            payload = json.loads(result.stdout)
            assert list(payload["languages"].keys()) == ["de"]
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_invalid_language_exits_2_and_narrates_to_stderr(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import check_translation_completeness

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"hello": "Hello"}))
        (de_dir / "app.json").write_text(json.dumps({"hello": "Hallo"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = CliRunner().invoke(
                check_translation_completeness, ["--language", "nonexistent", "--json"]
            )
            assert result.exit_code == 2
            assert result.stdout == ""
            assert "Language 'nonexistent' not found among locales" in _strip_ansi(result.stderr)
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_composes_with_add_missing(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import check_translation_completeness

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"hello": "Hello", "missing": "Missing"}))
        (de_dir / "app.json").write_text(json.dumps({"hello": "Hallo"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = CliRunner().invoke(
                check_translation_completeness,
                ["--language", "de", "--add-missing", "--json", "--verbose"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            payload = json.loads(result.stdout)
            de_file = payload["languages"]["de"]["files"]["app.json"]
            assert de_file["missing"] == 0
            assert de_file["todos"] == 1
            assert "Added missing translations" in _strip_ansi(result.stderr)
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_json_composes_with_remove_unused(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import check_translation_completeness

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"hello": "Hello"}))
        (de_dir / "app.json").write_text(json.dumps({"hello": "Hallo", "extra": "Extra"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = CliRunner().invoke(
                check_translation_completeness,
                ["--language", "de", "--remove-unused", "--json", "--verbose"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            payload = json.loads(result.stdout)
            de_file = payload["languages"]["de"]["files"]["app.json"]
            assert de_file["unused"] == 0
            assert "Removed 1 unused translations" in _strip_ansi(result.stderr)
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_stdout_stays_pure_when_computation_aborts(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import check_translation_completeness

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        xy_dir = tmp_path / "xy"
        xy_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"a": "A"}))
        (xy_dir / "app.json").write_text(json.dumps({"a": "X"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = CliRunner().invoke(check_translation_completeness, ["--json"])
            assert result.exit_code != 0
            assert result.stdout.strip() == ""
            assert "No plural suffixes defined for language 'xy'" in _strip_ansi(result.stderr)
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_malformed_locale_file_does_not_leak_to_stdout(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import check_translation_completeness

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"a": "A"}))
        (de_dir / "app.json").write_text("{not valid json")

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = CliRunner().invoke(check_translation_completeness, ["--json"])
            assert "Error loading" not in _strip_ansi(result.stdout)
            assert "Error loading" in _strip_ansi(result.stderr)
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir

    def test_default_table_error_path_unchanged(self, tmp_path):
        import airflow_breeze.commands.ui_commands as ui_commands
        from airflow_breeze.commands.ui_commands import check_translation_completeness

        en_dir = tmp_path / "en"
        en_dir.mkdir()
        de_dir = tmp_path / "de"
        de_dir.mkdir()

        (en_dir / "app.json").write_text(json.dumps({"a": "A"}))
        (de_dir / "app.json").write_text(json.dumps({"a": "D"}))

        original_locales_dir = ui_commands.LOCALES_DIR
        ui_commands.LOCALES_DIR = tmp_path
        try:
            result = CliRunner().invoke(check_translation_completeness, ["--language", "zz"])
            assert result.exit_code == 2
            assert "Summary of differences by language:" in _strip_ansi(result.output)
            assert "Language 'zz' not found among locales" in _strip_ansi(result.output)
        finally:
            ui_commands.LOCALES_DIR = original_locales_dir
