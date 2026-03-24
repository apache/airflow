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
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, NamedTuple

import click

from airflow_breeze.commands.common_options import option_dry_run, option_verbose
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.docker_command_utils import perform_environment_checks
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import assert_prek_installed, run_compile_ui_assets

LOCALES_DIR = AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "ui" / "public" / "i18n" / "locales"


def natural_sort_key(text: str) -> tuple:
    """
    Create a sort key that matches eslint-plugin-jsonc behavior with natural: true.

    This mimics JavaScript's localeCompare with numeric: true, which:
    1. Does case-insensitive comparison character-by-character
    2. Uses original case as tiebreaker when characters are equal (ignoring case)
    3. Handles numbers naturally (2 < 10)

    For each character position, we create a tuple: (lowercase_char, original_char)
    This ensures case-insensitive primary sort with case-sensitive tiebreaker.
    """

    def char_key(c: str) -> tuple:
        if c.isdigit():
            return 0, int(c), c  # Numbers sort before letters
        return 1, c.lower(), c  # Lowercase for primary, original for tiebreaker

    # Split on numbers to handle natural number ordering
    parts: list[tuple[int, tuple[tuple[Any, ...], ...] | int]] = []
    for part in re.split(r"(\d+)", text):
        if not part:
            continue
        if part.isdigit():
            # For numeric parts, use integer value
            parts.append((0, int(part)))
        else:
            # For text parts, convert each character
            parts.append((1, tuple(char_key(c) for c in part)))

    return tuple(parts)


MOST_COMMON_PLURAL_SUFFIXES = ["_one", "_other"]
# Plural suffixes per language (expand as needed). The actual suffixes depend on the language
# And can be vastly different in some languages (e.g. Arabic has 6 forms, Polish has 4 forms)
# You can check the rules for your language at https://jsfiddle.net/6bpxsgd4
PLURAL_SUFFIXES = {
    "ar": ["_zero", "_one", "_two", "_few", "_many", "_other"],
    "ca": MOST_COMMON_PLURAL_SUFFIXES,
    "de": MOST_COMMON_PLURAL_SUFFIXES,
    "el": MOST_COMMON_PLURAL_SUFFIXES,
    "en": MOST_COMMON_PLURAL_SUFFIXES,
    "es": ["_one", "_many", "_other"],
    "fr": ["_one", "_many", "_other"],
    "he": ["_one", "_two", "_other"],
    "hi": MOST_COMMON_PLURAL_SUFFIXES,
    "hu": MOST_COMMON_PLURAL_SUFFIXES,
    "it": ["_zero", "_one", "_many", "_other"],
    "ja": ["_other"],
    "ko": ["_other"],
    "nl": MOST_COMMON_PLURAL_SUFFIXES,
    "pl": ["_one", "_few", "_many", "_other"],
    "pt": ["_zero", "_one", "_many", "_other"],
    "ru": ["_one", "_few", "_other"],
    "th": ["_other"],
    "tr": MOST_COMMON_PLURAL_SUFFIXES,
    "zh-CN": ["_other"],
    "zh-TW": ["_other"],
}


class LocaleSummary(NamedTuple):
    """
    Summary of missing and unused translation keys for a file, per locale.

    Attributes:
        missing_keys: Required keys that the locale does not have.
        unused_keys: Keys present in the locale that are not required (never used at runtime).
    """

    missing_keys: dict[str, list[str]]
    unused_keys: dict[str, list[str]]


class LocaleFiles(NamedTuple):
    """
    Represents a locale and its list of translation files.

    Attributes:
        locale: The locale code (e.g., 'en', 'pl').
        files: A list of translation file names for the locale.
    """

    locale: str
    files: list[str]


class LocaleKeySet(NamedTuple):
    """
    Represents a locale and its set of translation keys for a file (or None if file missing).

    Attributes:
        locale: The locale code (e.g., 'en', 'pl').
        keys: A set of translation keys for the file, or None if the file is missing for this locale.
    """

    locale: str
    keys: set[str] | None


def get_plural_base(key: str, suffixes: list[str]) -> str | None:
    for suffix in suffixes:
        if key.endswith(suffix):
            return key[: -len(suffix)]
    return None


COUNT_PLACEHOLDER = "{{count}}"


def expand_plural_keys(keys: set[str], lang: str, en_key_to_value: dict[str, str] | None = None) -> set[str]:
    """
    For a set of keys, expand plural bases to include required suffixes for the language.

    When en_key_to_value is provided, only expand a base to all plural forms when at least
    one of the English values for that base contains {{count}}. Keys without {{count}}
    (e.g. fixed "1 Error") are not expanded, so locales are not required to have
    unused forms like error_other when the source only has error_one.
    """
    console = get_console()
    suffixes = PLURAL_SUFFIXES.get(lang)
    if not suffixes:
        console.print(
            f"\n[red]Error: No plural suffixes defined for language '{lang}'[/red].\n"
            f'[bright_blue]Most languages use ["_one", "_other"] array for suffixes, '
            f"but you can check and play your language at https://jsfiddle.net/6bpxsgd4\n"
        )
        sys.exit(1)
    base_to_suffixes: dict[str, set[str]] = {}
    for key in keys:
        base = get_plural_base(key, suffixes)
        if base:
            base_to_suffixes.setdefault(base, set()).add(key[len(base) :])
    expanded = set(keys)
    for base in base_to_suffixes.keys():
        if en_key_to_value is not None:
            en_keys_for_base = [k for k in keys if get_plural_base(k, suffixes) == base]
            any_has_count = any(COUNT_PLACEHOLDER in en_key_to_value.get(k, "") for k in en_keys_for_base)
            if not any_has_count:
                continue
        for suffix in suffixes:
            expanded.add(base + suffix)
    return expanded


def get_locale_files() -> list[LocaleFiles]:
    return [
        LocaleFiles(
            locale=locale_dir.name, files=[f.name for f in locale_dir.iterdir() if f.suffix == ".json"]
        )
        for locale_dir in LOCALES_DIR.iterdir()
        if locale_dir.is_dir()
    ]


def load_json(filepath: Path) -> Any:
    return json.loads(filepath.read_text(encoding="utf-8"))


def flatten_keys(d: dict, prefix: str = "") -> list[str]:
    keys: list[str] = []
    for k, v in d.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            keys.extend(flatten_keys(v, full_key))
        else:
            keys.append(full_key)
    return keys


def flatten_keys_to_values(d: dict, prefix: str = "") -> dict[str, str]:
    """Build a flat key -> value map for leaf string values (for i18n JSON)."""
    result: dict[str, str] = {}
    for k, v in d.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            result.update(flatten_keys_to_values(v, full_key))
        elif isinstance(v, str):
            result[full_key] = v
    return result


def compare_keys(
    locale_files: list[LocaleFiles],
) -> tuple[dict[str, LocaleSummary], dict[str, dict[str, int]]]:
    """
    Compare all non-English locales with English locale only.

    Returns a tuple:
      - summary dict: filename -> LocaleSummary(missing_keys, unused_keys)
      - missing_counts dict: filename -> {locale: count_of_missing_keys}
    """
    all_files: set[str] = set()
    for lf in locale_files:
        all_files.update(lf.files)
    summary: dict[str, LocaleSummary] = {}
    missing_counts: dict[str, dict[str, int]] = {}
    for filename in all_files:
        key_sets: list[LocaleKeySet] = []
        for lf in locale_files:
            keys = set()
            if filename in lf.files:
                path = LOCALES_DIR / lf.locale / filename
                try:
                    data = load_json(path)
                    keys = set(flatten_keys(data))
                except Exception as e:
                    console_print(f"Error loading {path}: {e}")
            key_sets.append(LocaleKeySet(locale=lf.locale, keys=keys))
        keys_by_locale = {ks.locale: ks.keys for ks in key_sets}
        en_keys = keys_by_locale.get("en", set()) or set()
        en_key_to_value: dict[str, str] = {}
        en_path = LOCALES_DIR / "en" / filename
        if en_path.exists():
            try:
                en_data = load_json(en_path)
                en_key_to_value = flatten_keys_to_values(en_data)
            except Exception as e:
                console_print(f"Error loading en values from {en_path}: {e}")
        # Required = EN keys plus, for bases with {{count}} in EN, all plural forms for the locale
        missing_keys: dict[str, list[str]] = {}
        unused_keys: dict[str, list[str]] = {}
        missing_counts[filename] = {}
        for ks in key_sets:
            if ks.locale == "en":
                continue
            required_keys = expand_plural_keys(en_keys, ks.locale, en_key_to_value)
            if ks.keys is None:
                missing_keys[ks.locale] = list(required_keys)
                unused_keys[ks.locale] = []
                missing_counts[filename][ks.locale] = len(required_keys)
            else:
                missing = list(required_keys - ks.keys)
                missing_keys[ks.locale] = missing
                unused_keys[ks.locale] = sorted(ks.keys - required_keys)
                missing_counts[filename][ks.locale] = len(missing)
        summary[filename] = LocaleSummary(missing_keys=missing_keys, unused_keys=unused_keys)
    return summary, missing_counts


def print_locale_file_table(locale_files: list[LocaleFiles], language: str | None = None) -> None:
    if language and language == "en":
        return
    console = get_console()
    console.print("[bold underline]Locales and their files:[/bold underline]", style="cyan")
    from rich.table import Table

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Locale")
    table.add_column("Files")
    filtered = sorted(
        locale_files if language is None else [lf for lf in locale_files if lf.locale in (language, "en")]
    )
    for lf in filtered:
        table.add_row(lf.locale, ", ".join(lf.files))
    console.print(table)


def print_file_set_differences(locale_files: list[LocaleFiles], language: str | None = None) -> bool:
    if language and language == "en":
        return False
    console = get_console()
    filtered = (
        locale_files if language is None else [lf for lf in locale_files if lf.locale in (language, "en")]
    )
    all_file_sets = {lf.locale: set(lf.files) for lf in filtered}
    file_sets: list[set[str]] = list(all_file_sets.values())
    found_difference = False
    en_files = set(all_file_sets["en"])
    if len(file_sets) > 1 and any(file_sets[0] != fs for fs in file_sets[1:]):
        found_difference = True
        console.print("[bold red]Error: Locales have different set of translation files![/bold red]")
        all_locales_sorted = sorted(all_file_sets.keys())
        for locale in all_locales_sorted:
            files_missing = sorted(en_files - all_file_sets[locale])
            if files_missing:
                console.print(
                    f"[yellow]{locale:<6}[/yellow] Missing {len(files_missing):>2}: {files_missing} "
                )
            files_extra = sorted(all_file_sets[locale] - en_files)
            if files_extra:
                console.print(f"[red]{locale:<6}[/red] Extra   {len(files_extra):>2}: {files_extra} ")
    return found_difference


def print_language_summary(locale_files: list[LocaleFiles], summary: dict[str, LocaleSummary]) -> bool:
    found_difference = False
    console = get_console()
    from rich.panel import Panel

    for lf in sorted(locale_files):
        locale = lf.locale
        file_missing: dict[str, list[str]] = {}
        file_unused: dict[str, list[str]] = {}
        for filename, diff in sorted(summary.items()):
            missing_keys = diff.missing_keys.get(locale, [])
            unused_keys = diff.unused_keys.get(locale, [])
            if missing_keys:
                file_missing[filename] = missing_keys
            if unused_keys:
                file_unused[filename] = unused_keys
        if file_missing or file_unused:
            if locale == "en":
                continue
            console.print(Panel(f"[bold yellow]{locale}[/bold yellow]", style="blue"))
            if file_missing:
                found_difference = True
                console.print("[red]  Missing keys (need to be added):[/red]")
                for filename, keys in file_missing.items():
                    console.print(f"    [magenta]{filename}[/magenta]:")
                    for k in keys:
                        console.print(f"      [yellow]{k}[/yellow]")
            if file_unused:
                console.print("[yellow]  Unused keys (not required, can be removed):[/yellow]")
                for filename, keys in file_unused.items():
                    console.print(f"    [magenta]{filename}[/magenta]:")
                    for k in keys:
                        console.print(f"      [dim]{k}[/dim]")
    return found_difference


def is_todo_value(s: str) -> bool:
    """Return True if the string is a TODO placeholder (e.g. 'TODO: translate: ...')."""
    return isinstance(s, str) and s.strip().startswith("TODO: translate")


def count_todos(obj) -> int:
    """Count TODO: translate entries in a dict or list."""
    if isinstance(obj, dict):
        return sum(count_todos(v) for v in obj.values())
    if isinstance(obj, list):
        return sum(count_todos(v) for v in obj)
    if is_todo_value(obj):
        return 1
    return 0


def print_translation_progress(locale_files, summary):
    console = get_console()
    from rich.table import Table

    tables = defaultdict(lambda: Table(show_lines=True))
    all_files = set()
    coverage_per_language = {}
    for lf in locale_files:
        all_files.update(lf.files)

    has_todos = False

    for lf in sorted(locale_files):
        lang = lf.locale
        if lang == "en":
            continue
        table = tables[lang]
        table.title = f"Translation Progress: {lang}"
        table.add_column("File", style="bold cyan")
        table.add_column("Required (base EN)", style="dim")
        table.add_column("Required (plural)", style="dim")
        table.add_column("Total required", style="bold")
        table.add_column("Translated", style="green")
        table.add_column("Missing", style="red")
        table.add_column("Coverage", style="bold")
        table.add_column("TODOs", style="magenta")
        table.add_column("Unused", style="yellow")
        total_required_base = 0
        total_required_plural = 0
        total_required = 0
        total_translated = 0
        total_missing = 0
        total_todos = 0
        total_unused = 0
        for filename in sorted(all_files):
            en_path = Path(LOCALES_DIR / "en" / filename)
            file_path = Path(LOCALES_DIR / lang / filename)
            diff = summary.get(filename, LocaleSummary({}, {}))
            if not en_path.exists():
                continue
            with open(en_path) as f:
                en_data = json.load(f)
            en_keys = set(flatten_keys(en_data))
            en_key_to_value = flatten_keys_to_values(en_data)
            required_keys = expand_plural_keys(en_keys, lang, en_key_to_value)
            required_base_en = len(en_keys)
            required_plural = len(required_keys) - required_base_en
            total_req = len(required_keys)
            file_missing = len(diff.missing_keys.get(lang, []))
            file_unused = len(diff.unused_keys.get(lang, []))
            if file_path.exists():
                with open(file_path) as f:
                    lang_data = json.load(f)
                lang_key_to_value = flatten_keys_to_values(lang_data)
                file_translated = sum(
                    1
                    for k in required_keys
                    if k in lang_key_to_value and not is_todo_value(lang_key_to_value[k])
                )
                file_todos = sum(
                    1 for k in required_keys if k in lang_key_to_value and is_todo_value(lang_key_to_value[k])
                )
                if file_todos > 0:
                    has_todos = True
            else:
                file_translated = 0
                file_todos = 0
            file_coverage = 100 * file_translated / total_req if total_req else 100.0
            if file_missing > 0:
                style = "red"
            elif file_todos > 0 or file_unused > 0:
                style = "yellow"
            else:
                style = "bold green"
            table.add_row(
                f"[bold reverse]{filename}[/bold reverse]",
                str(required_base_en),
                str(required_plural),
                str(total_req),
                str(file_translated),
                str(file_missing),
                f"{file_coverage:.1f}%",
                str(file_todos),
                str(file_unused),
                style=style,
            )
            total_required_base += required_base_en
            total_required_plural += required_plural
            total_required += total_req
            total_translated += file_translated
            total_missing += file_missing
            total_todos += file_todos
            total_unused += file_unused

        total_coverage = 100 * total_translated / total_required if total_required else 100.0
        coverage_per_language[lang] = total_coverage
        table.add_row(
            "All files",
            str(total_required_base),
            str(total_required_plural),
            str(total_required),
            str(total_translated),
            str(total_missing),
            f"{total_coverage:.1f}%",
            str(total_todos),
            str(total_unused),
            style="red" if total_missing > 0 or total_todos > 0 or total_unused > 0 else "bold green",
        )

    for _lang, table in tables.items():
        console.print(table)

    return has_todos, coverage_per_language


def add_missing_translations(language: str, summary: dict[str, LocaleSummary]):
    """
    Add missing translations for the selected language.

    It does it by copying them from English and prefixing with 'TODO: translate:'.
    Ensures all required plural forms for the language are added.
    """
    console = get_console()
    suffixes = PLURAL_SUFFIXES.get(language, ["_one", "_other"])
    for filename, diff in summary.items():
        missing_keys = set(diff.missing_keys.get(language, []))
        if not missing_keys:
            continue
        en_path = LOCALES_DIR / "en" / filename
        lang_path = LOCALES_DIR / language / filename
        try:
            en_data = load_json(en_path)
        except Exception as e:
            console.print(f"[red]Failed to load English file {en_path}: {e}[/red]")
            continue
        try:
            lang_data = load_json(lang_path)
        except Exception:
            lang_data = {}  # Start with an empty dict if the file doesn't exist

        # Helper to recursively add missing keys, including plural forms
        def add_keys(src, dst, prefix=""):
            for k, v in src.items():
                full_key = f"{prefix}.{k}" if prefix else k
                base = get_plural_base(full_key, suffixes)
                if base and any(full_key == base + s for s in suffixes):
                    for suffix in suffixes:
                        plural_key = base + suffix
                        key_name = plural_key.split(".")[-1]
                        if plural_key in missing_keys:
                            dst[key_name] = f"TODO: translate: {v}"
                    continue
                if full_key in missing_keys:
                    if isinstance(v, dict):
                        dst[k] = {}
                        add_keys(v, dst[k], full_key)
                    else:
                        dst[k] = f"TODO: translate: {v}"
                else:
                    if isinstance(v, dict):
                        if k not in dst or not isinstance(dst[k], dict):
                            dst[k] = {}
                        add_keys(v, dst[k], full_key)

        add_keys(en_data, lang_data)

        def sort_dict_keys(obj):
            if isinstance(obj, dict):
                return {k: sort_dict_keys(obj[k]) for k in sorted(obj.keys(), key=natural_sort_key)}
            return obj

        lang_data = sort_dict_keys(lang_data)
        lang_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lang_path, "w", encoding="utf-8") as f:
            json.dump(lang_data, f, ensure_ascii=False, indent=2)
            f.write("\n")  # Ensure newline at the end of the file
        console.print(f"[green]Added missing translations to {lang_path}[/green]")


def remove_unused_translations(language: str, summary: dict[str, LocaleSummary]):
    """
    Remove unused translations for the selected language.

    Removes keys that are present in the language file but are not required.
    """
    console = get_console()
    for filename, diff in summary.items():
        unused_keys = set(diff.unused_keys.get(language, []))
        if not unused_keys:
            continue
        lang_path = LOCALES_DIR / language / filename
        try:
            lang_data = load_json(lang_path)
        except Exception as e:
            console.print(f"[yellow]Failed to load {language} file {lang_path}: {e}[/yellow]")
            continue

        # Helper to recursively remove unused keys
        def remove_keys(dst, prefix=""):
            keys_to_remove = []
            for k, v in list(dst.items()):
                full_key = f"{prefix}.{k}" if prefix else k
                if full_key in unused_keys:
                    keys_to_remove.append(k)
                elif isinstance(v, dict):
                    remove_keys(v, full_key)
                    # Remove empty dictionaries after recursion
                    if not v:
                        keys_to_remove.append(k)
            for k in keys_to_remove:
                del dst[k]

        remove_keys(lang_data)

        def sort_dict_keys(obj):
            if isinstance(obj, dict):
                return {k: sort_dict_keys(obj[k]) for k in sorted(obj.keys(), key=natural_sort_key)}
            return obj

        lang_data = sort_dict_keys(lang_data)
        with open(lang_path, "w", encoding="utf-8") as f:
            json.dump(lang_data, f, ensure_ascii=False, indent=2)
            f.write("\n")  # Ensure newline at the end of the file
        console.print(f"[green]Removed {len(unused_keys)} unused translations from {lang_path}[/green]")


@click.group(cls=BreezeGroup, name="ui", help="Tools for UI development and maintenance")
def ui_group():
    pass


@ui_group.command(
    name="check-translation-completeness",
    help="Check completeness of UI translations.",
)
@click.option(
    "--language",
    "-l",
    default=None,
    help="Show summary for a single language (e.g. en, de, pl, etc.)",
)
@click.option(
    "--add-missing",
    is_flag=True,
    default=False,
    help="Add missing translations for all languages except English, prefixed with 'TODO: translate:'.",
)
@click.option(
    "--remove-unused",
    is_flag=True,
    default=False,
    help="Remove unused translations (keys present in the language but not required).",
)
@option_verbose
@option_dry_run
def check_translation_completeness(
    language: str | None = None, add_missing: bool = False, remove_unused: bool = False
):
    locale_files = get_locale_files()
    console = get_console()
    print_locale_file_table(locale_files, language)
    found_difference = print_file_set_differences(locale_files, language)
    summary, missing_counts = compare_keys(locale_files)
    console.print("\n[bold underline]Summary of differences by language:[/bold underline]", style="cyan")
    if add_missing and language != "en":
        # Loop through all languages except 'en' and add missing translations
        if language:
            language_files = [lf for lf in locale_files if lf.locale == language]
        else:
            language_files = [lf for lf in locale_files if lf.locale != "en"]
        for lf in language_files:
            filtered_summary = {}
            for filename, diff in summary.items():
                filtered_summary[filename] = LocaleSummary(
                    missing_keys={lf.locale: diff.missing_keys.get(lf.locale, [])},
                    unused_keys={lf.locale: diff.unused_keys.get(lf.locale, [])},
                )
            add_missing_translations(lf.locale, filtered_summary)
        # After adding, re-run the summary for all languages
        summary, missing_counts = compare_keys(get_locale_files())
    if remove_unused and language != "en":
        # Loop through all languages except 'en' and remove unused translations
        if language:
            language_files = [lf for lf in locale_files if lf.locale == language]
        else:
            language_files = [lf for lf in locale_files if lf.locale != "en"]
        for lf in language_files:
            filtered_summary = {}
            for filename, diff in summary.items():
                filtered_summary[filename] = LocaleSummary(
                    missing_keys={lf.locale: diff.missing_keys.get(lf.locale, [])},
                    unused_keys={lf.locale: diff.unused_keys.get(lf.locale, [])},
                )
            remove_unused_translations(lf.locale, filtered_summary)
        # After removing, re-run the summary for all languages
        summary, missing_counts = compare_keys(get_locale_files())
    if language:
        locales = [lf.locale for lf in locale_files]
        if language not in locales:
            console.print(f"[red]Language '{language}' not found among locales: {', '.join(locales)}[/red]")
            sys.exit(2)
        if language == "en":
            console.print("[bold red]Cannot check completeness for English language![/bold red]")
            sys.exit(2)
        else:
            filtered_summary = {}
            for filename, diff in summary.items():
                filtered_summary[filename] = LocaleSummary(
                    missing_keys={language: diff.missing_keys.get(language, [])},
                    unused_keys={language: diff.unused_keys.get(language, [])},
                )
            lang_diff = print_language_summary(
                [lf for lf in locale_files if lf.locale == language], filtered_summary
            )
            found_difference = found_difference or lang_diff
    else:
        lang_diff = print_language_summary(locale_files, summary)
        found_difference = found_difference or lang_diff
    has_todos, coverage_per_language = print_translation_progress(
        [lf for lf in locale_files if language is None or lf.locale == language],
        summary,
    )
    if not found_difference and not has_todos:
        console.print("\n[green]All translations are complete![/green]\n\n")
    else:
        console.print("\n[red]Some translations are not complete![/red]\n\n")
        # Print summary of total coverage per language
        if coverage_per_language:
            from rich.table import Table

            def get_coverage_str(coverage: float) -> str:
                if coverage >= 95:
                    return f"[bold green]{coverage:.1f}%[/bold green]"
                if coverage > 90:
                    return f"[bold yellow]{coverage:.1f}%[/bold yellow]"
                return f"[red]{coverage:.1f}%[/red]"

            summary_table = Table(show_header=True, header_style="bold magenta")
            summary_table.title = "Total Coverage per Language"
            summary_table.add_column("Language", style="cyan")
            summary_table.add_column("Coverage", style="green")
            for lang, coverage in sorted(coverage_per_language.items()):
                summary_table.add_row(lang, get_coverage_str(coverage))
            # Calculate and print median coverage
            coverages = sorted(coverage_per_language.values())
            n = len(coverages)
            if n > 0:
                if n % 2 == 1:
                    median_coverage = coverages[n // 2]
                else:
                    median_coverage = (coverages[n // 2 - 1] + coverages[n // 2]) / 2
                summary_table.add_row("", "", end_section=True)
                summary_table.add_row("Median", get_coverage_str(median_coverage))
            console.print(summary_table)


@ui_group.command(
    name="compile-assets",
    help="Compiles ui assets.",
)
@click.option(
    "--dev",
    help="Run development version of assets compilation - it will not quit and automatically "
    "recompile assets on-the-fly when they are changed.",
    is_flag=True,
)
@click.option(
    "--force-clean",
    help="Force cleanup of compile assets before building them.",
    is_flag=True,
)
@option_verbose
@option_dry_run
def compile_ui_assets(dev: bool, force_clean: bool):
    perform_environment_checks()
    assert_prek_installed()
    compile_ui_assets_result = run_compile_ui_assets(
        dev=dev, run_in_background=False, force_clean=force_clean, additional_ui_hooks=[]
    )
    if compile_ui_assets_result.returncode != 0:
        console_print("[warn]New assets were generated[/]")
    sys.exit(0)
