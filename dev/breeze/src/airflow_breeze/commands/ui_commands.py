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
from rich.panel import Panel
from rich.table import Table

from airflow_breeze.commands.common_options import option_dry_run, option_verbose
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

LOCALES_DIR = AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "ui" / "public" / "i18n" / "locales"
UI_SRC_DIR = AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "ui" / "src"


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
    "ko": ["_other"],
    "nl": MOST_COMMON_PLURAL_SUFFIXES,
    "pl": ["_one", "_few", "_many", "_other"],
    "pt": ["_zero", "_one", "_many", "_other"],
    "th": ["_other"],
    "tr": MOST_COMMON_PLURAL_SUFFIXES,
    "zh-CN": ["_other"],
    "zh-TW": ["_other"],
}


class LocaleSummary(NamedTuple):
    """
    Summary of missing and extra translation keys for a file, per locale.

    Attributes:
        missing_keys: A dictionary mapping locale codes to lists of missing translation keys.
        extra_keys: A dictionary mapping locale codes to lists of extra translation keys.
    """

    missing_keys: dict[str, list[str]]
    extra_keys: dict[str, list[str]]


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


def expand_plural_keys(keys: set[str], lang: str) -> set[str]:
    """
    For a set of keys, expand all plural bases to include all required suffixes for the language.
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


def compare_keys(
    locale_files: list[LocaleFiles],
) -> tuple[dict[str, LocaleSummary], dict[str, dict[str, int]]]:
    """
    Compare all non-English locales with English locale only.

    Returns a tuple:
      - summary dict: filename -> LocaleSummary(missing, extra)
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
                    get_console().print(f"Error loading {path}: {e}")
            key_sets.append(LocaleKeySet(locale=lf.locale, keys=keys))
        keys_by_locale = {ks.locale: ks.keys for ks in key_sets}
        en_keys = keys_by_locale.get("en", set()) or set()
        # Expand English keys for all required plural forms in each language
        expanded_en_keys = {lang: expand_plural_keys(en_keys, lang) for lang in keys_by_locale.keys()}
        missing_keys: dict[str, list[str]] = {}
        extra_keys: dict[str, list[str]] = {}
        missing_counts[filename] = {}
        for ks in key_sets:
            if ks.locale == "en":
                continue
            required_keys = expanded_en_keys.get(ks.locale, en_keys)
            if ks.keys is None:
                missing_keys[ks.locale] = list(required_keys)
                extra_keys[ks.locale] = []
                missing_counts[filename][ks.locale] = len(required_keys)
            else:
                missing = list(required_keys - ks.keys)
                missing_keys[ks.locale] = missing
                extra_keys[ks.locale] = list(ks.keys - required_keys)
                missing_counts[filename][ks.locale] = len(missing)
        summary[filename] = LocaleSummary(missing_keys=missing_keys, extra_keys=extra_keys)
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
        file_extra: dict[str, list[str]] = {}
        for filename, diff in sorted(summary.items()):
            missing_keys = diff.missing_keys.get(locale, [])
            extra_keys = diff.extra_keys.get(locale, [])
            if missing_keys:
                file_missing[filename] = missing_keys
            if extra_keys:
                file_extra[filename] = extra_keys
        if file_missing or file_extra:
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
            if file_extra:
                found_difference = True
                console.print("[red]  Extra keys (need to be removed/updated):[/red]")
                for filename, keys in file_extra.items():
                    console.print(f"    [magenta]{filename}[/magenta]:")
                    for k in keys:
                        console.print(f"      [yellow]{k}[/yellow]")
    return found_difference


def count_todos(obj) -> int:
    """Count TODO: translate entries in a dict or list."""
    if isinstance(obj, dict):
        return sum(count_todos(v) for v in obj.values())
    if isinstance(obj, list):
        return sum(count_todos(v) for v in obj)
    if isinstance(obj, str) and obj.strip().startswith("TODO: translate"):
        return 1
    return 0


def print_translation_progress(locale_files, missing_counts, summary):
    console = get_console()
    from rich.table import Table

    tables = defaultdict(lambda: Table(show_lines=True))
    all_files = set()
    coverage_per_language = {}  # Collect total coverage per language
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
        table.add_column("Missing", style="red")
        table.add_column("Extra", style="yellow")
        table.add_column("TODOs", style="magenta")
        table.add_column("Translated", style="green")
        table.add_column("Total", style="bold")
        table.add_column("Coverage", style="bold")
        table.add_column("Completed", style="bold")
        total_missing = 0
        total_extra = 0
        total_todos = 0
        total_translated = 0
        total_total = 0
        for filename in sorted(all_files):
            file_path = Path(LOCALES_DIR / lang / filename)
            # Always get total from English version
            en_path = Path(LOCALES_DIR / "en" / filename)
            if en_path.exists():
                with open(en_path) as f:
                    en_data = json.load(f)
                file_total = sum(1 for _ in flatten_keys(en_data))
            else:
                file_total = 0
            if file_path.exists():
                with open(file_path) as f:
                    data = json.load(f)
                file_missing = missing_counts.get(filename, {}).get(lang, 0)
                file_extra = len(summary.get(filename, LocaleSummary({}, {})).extra_keys.get(lang, []))

                file_todos = count_todos(data)
                if file_todos > 0:
                    has_todos = True
                file_translated = file_total - file_missing
                # Coverage: translated / total
                file_coverage_percent = 100 * file_translated / file_total if file_total else 100
                # Complete percent: (translated - todos) / translated
                file_actual_translated = file_translated - file_todos
                complete_percent = 100 * file_actual_translated / file_translated if file_translated else 100
                style = (
                    "bold green"
                    if file_missing == 0 and file_extra == 0 and file_todos == 0
                    else (
                        "yellow" if file_missing < file_total or file_extra > 0 or file_todos > 0 else "red"
                    )
                )
            else:
                file_missing = file_total
                file_extra = len(summary.get(filename, LocaleSummary({}, {})).extra_keys.get(lang, []))
                file_todos = 0
                file_translated = 0
                file_coverage_percent = 0
                complete_percent = 0
                style = "red"
            table.add_row(
                f"[bold reverse]{filename}[/bold reverse]",
                str(file_missing),
                str(file_extra),
                str(file_todos),
                str(file_translated),
                str(file_total),
                f"{file_coverage_percent:.1f}%",
                f"{complete_percent:.1f}%",
                style=style,
            )
            total_missing += file_missing
            total_extra += file_extra
            total_todos += file_todos
            total_translated += file_translated
            total_total += file_total

        # Calculate totals for this language
        total_coverage_percent = 100 * total_translated / total_total if total_total else 100
        total_actual_translated = total_translated - total_todos
        total_complete_percent = 100 * total_actual_translated / total_translated if total_translated else 100

        coverage_per_language[lang] = total_coverage_percent
        table.add_row(
            "All files",
            str(total_missing),
            str(total_extra),
            str(total_todos),
            str(total_translated),
            str(total_total),
            f"{total_coverage_percent:.1f}%",
            f"{total_complete_percent:.1f}%",
            style="red" if total_complete_percent < 100 else "bold green",
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


def remove_extra_translations(language: str, summary: dict[str, LocaleSummary]):
    """
    Remove extra translations for the selected language.

    Removes keys that are present in the language file but missing in the English file.
    """
    console = get_console()
    for filename, diff in summary.items():
        extra_keys = set(diff.extra_keys.get(language, []))
        if not extra_keys:
            continue
        lang_path = LOCALES_DIR / language / filename
        try:
            lang_data = load_json(lang_path)
        except Exception as e:
            console.print(f"[yellow]Failed to load {language} file {lang_path}: {e}[/yellow]")
            continue

        # Helper to recursively remove extra keys
        def remove_keys(dst, prefix=""):
            keys_to_remove = []
            for k, v in list(dst.items()):
                full_key = f"{prefix}.{k}" if prefix else k
                if full_key in extra_keys:
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
        console.print(f"[green]Removed {len(extra_keys)} extra translations from {lang_path}[/green]")


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
    "--remove-extra",
    is_flag=True,
    default=False,
    help="Remove extra translations that are present in the language but missing in English.",
)
@option_verbose
@option_dry_run
def check_translation_completeness(
    language: str | None = None, add_missing: bool = False, remove_extra: bool = False
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
                    extra_keys={lf.locale: diff.extra_keys.get(lf.locale, [])},
                )
            add_missing_translations(lf.locale, filtered_summary)
        # After adding, re-run the summary for all languages
        summary, missing_counts = compare_keys(get_locale_files())
    if remove_extra and language != "en":
        # Loop through all languages except 'en' and remove extra translations
        if language:
            language_files = [lf for lf in locale_files if lf.locale == language]
        else:
            language_files = [lf for lf in locale_files if lf.locale != "en"]
        for lf in language_files:
            filtered_summary = {}
            for filename, diff in summary.items():
                filtered_summary[filename] = LocaleSummary(
                    missing_keys={lf.locale: diff.missing_keys.get(lf.locale, [])},
                    extra_keys={lf.locale: diff.extra_keys.get(lf.locale, [])},
                )
            remove_extra_translations(lf.locale, filtered_summary)
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
                    extra_keys={language: diff.extra_keys.get(language, [])},
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
        missing_counts,
        summary,
    )
    if not found_difference and not has_todos:
        console.print("\n[green]All translations are complete![/green]\n\n")
    else:
        console.print("\n[red]Some translations are not complete![/red]\n\n")
        # Print summary of total coverage per language
        if coverage_per_language:
            from rich.table import Table

            summary_table = Table(show_header=True, header_style="bold magenta")
            summary_table.title = "Total Coverage per Language"
            summary_table.add_column("Language", style="cyan")
            summary_table.add_column("Coverage", style="green")
            for lang, coverage in sorted(coverage_per_language.items()):
                if coverage >= 95:
                    coverage_str = f"[bold green]{coverage:.1f}%[/bold green]"
                elif coverage > 80:
                    coverage_str = f"[bold yellow]{coverage:.1f}%[/bold yellow]"
                else:
                    coverage_str = f"[red]{coverage:.1f}%[/red]"
                summary_table.add_row(lang, coverage_str)
            console.print(summary_table)


def collect_translation_keys_from_json(locale_dir: Path) -> dict[str, set[str]]:
    """
    Collect all translation keys from JSON files in the locale directory.

    Returns a dict mapping namespace (filename without .json) to set of keys.
    """
    keys_by_namespace: dict[str, set[str]] = {}
    for json_file in locale_dir.glob("*.json"):
        namespace = json_file.stem
        try:
            data = load_json(json_file)
            keys = set(flatten_keys(data))
            keys_by_namespace[namespace] = keys
        except Exception as e:
            get_console().print(f"[red]Error loading {json_file}: {e}[/red]")
    return keys_by_namespace


def extract_dynamic_keys_from_template(content: str, template_key: str, namespace: str) -> set[str]:
    """
    Extract possible dynamic keys from a template string.

    For example, if we see translate(`admin.${link.title}`) and link.title comes from
    a defined array, we can extract all possible values.
    """
    possible_keys = set()

    # Try to find the variable definition in the content
    # Pattern: const links = [{ title: "Variables" }, { title: "Pools" }, ...]
    # or const states = ["success", "failed", ...]

    # Extract the variable name from the template (e.g., "link.title" -> "link")
    var_match = re.search(r"\$\{(\w+)\.(\w+)\}", template_key)
    if var_match:
        var_name = var_match.group(1)
        property_name = var_match.group(2)

        # Look for array/object definitions
        # Pattern: const links = [...]
        array_pattern = re.compile(rf"\bconst\s+{var_name}s?\s*=\s*\[(.*?)\]", re.DOTALL)
        array_match = array_pattern.search(content)

        if array_match:
            array_content = array_match.group(1)

            # Extract property values: { title: "Variables" }
            prop_pattern = re.compile(rf'{property_name}:\s*["\'](\w+)["\']')
            for prop_match in prop_pattern.finditer(array_content):
                value = prop_match.group(1)
                # Replace the template variable with the actual value
                expanded_key = template_key.replace(f"${{{var_name}.{property_name}}}", value)
                possible_keys.add(expanded_key)

    # Handle simple variable substitution: ${state} where state is from an enum or constant
    simple_var_match = re.search(r"\$\{(\w+)\}", template_key)
    if simple_var_match and not var_match:  # Only if we didn't already handle it above
        var_name = simple_var_match.group(1)

        # Common state values in Airflow
        if var_name in ["state", "taskState", "dagState"]:
            states = [
                "success",
                "running",
                "failed",
                "skipped",
                "upstream_failed",
                "up_for_retry",
                "up_for_reschedule",
                "queued",
                "scheduled",
                "deferred",
                "removed",
                "restarting",
                "no_status",
            ]
            for state in states:
                expanded_key = template_key.replace(f"${{{var_name}}}", state)
                possible_keys.add(expanded_key)

        # Handle wrap/unwrap pattern
        elif var_name == "wrap":
            for value in ["wrap", "unwrap"]:
                expanded_key = template_key.replace(f"${{{var_name}}}", value)
                possible_keys.add(expanded_key)

        # Handle direction pattern
        elif var_name == "direction":
            for value in ["up", "down", "left", "right"]:
                expanded_key = template_key.replace(f"${{{var_name}}}", value)
                possible_keys.add(expanded_key)

    return possible_keys


def find_translation_usage_in_source(src_dir: Path) -> dict[str, set[str]]:
    console = get_console()
    used_keys: dict[str, set[str]] = defaultdict(set)

    # Pattern to match translation calls with static keys
    static_pattern = re.compile(
        r'\b(?:translate|t)\s*\(\s*["\'](?:(\w+):)?([a-zA-Z0-9_.]+(?:\.[a-zA-Z0-9_]+)*)["\']'
    )
    # Pattern to match translation calls with template strings (dynamic keys)
    dynamic_pattern = re.compile(r"\b(?:translate|t)\s*\(\s*`(?:(\w+):)?([^`]+)`")
    # Pattern to find useTranslation hook calls to determine default namespace
    use_translation_pattern = re.compile(r'useTranslation\(\s*(?:\[([^\]]+)\]|["\'](\w+)["\'])\s*\)')

    for ext in ["ts", "tsx"]:
        for src_file in src_dir.rglob(f"*.{ext}"):
            try:
                content = src_file.read_text(encoding="utf-8")

                # Find the namespace(s) used in this file
                file_namespaces = ["common"]  # Default namespace
                for match in use_translation_pattern.finditer(content):
                    if match.group(1):  # Array format: ["admin", "common"]
                        # Parse the array
                        ns_array = match.group(1).replace('"', "").replace("'", "").split(",")
                        file_namespaces = [ns.strip() for ns in ns_array if ns.strip()]
                    elif match.group(2):  # String format: "admin"
                        file_namespaces = [match.group(2)]
                    break  # Use the first useTranslation call found

                # Find all static translation calls
                for match in static_pattern.finditer(content):
                    explicit_namespace = match.group(1)
                    key = match.group(2)

                    # Only add keys that contain at least one letter and look like translation keys
                    if (
                        key
                        and any(c.isalpha() for c in key)
                        and not key.startswith("http")
                        and "/" not in key
                        and not key.replace(".", "")
                        .replace("-", "")
                        .replace(":", "")
                        .replace("_", "")
                        .isdigit()
                    ):
                        if explicit_namespace:
                            used_keys[explicit_namespace].add(key)
                        else:
                            for ns in file_namespaces:
                                used_keys[ns].add(key)

                # Find all dynamic translation calls
                for match in dynamic_pattern.finditer(content):
                    explicit_namespace = match.group(1)
                    template_key = match.group(2)

                    # Try to expand dynamic keys
                    expanded_keys = extract_dynamic_keys_from_template(
                        content, template_key, explicit_namespace
                    )

                    if expanded_keys:
                        for key in expanded_keys:
                            if explicit_namespace:
                                used_keys[explicit_namespace].add(key)
                            else:
                                for ns in file_namespaces:
                                    used_keys[ns].add(key)
                    else:
                        # If we can't expand, mark it as a dynamic key pattern
                        # This prevents false positives for unused keys
                        if explicit_namespace:
                            used_keys[explicit_namespace].add(f"[DYNAMIC] {template_key}")
                        else:
                            for ns in file_namespaces:
                                used_keys[ns].add(f"[DYNAMIC] {template_key}")

            except Exception as e:
                console.print(f"[yellow]Warning: Could not read {src_file}: {e}[/yellow]")

    return dict(used_keys)


def is_key_used_by_dynamic_pattern(key: str, dynamic_patterns: set[str]) -> bool:
    """
    Check if a key could be generated by any dynamic pattern.

    For example, if we have a pattern [DYNAMIC] states.${state}, and the key is
    "states.success", this should return True.
    """
    for pattern in dynamic_patterns:
        if not pattern.startswith("[DYNAMIC] "):
            continue

        pattern_template = pattern.replace("[DYNAMIC] ", "")

        # Replace ${...} with regex pattern
        regex_pattern = re.escape(pattern_template)
        regex_pattern = re.sub(r"\\?\$\\\{[^}]+\\\}", r"[\\w.]+", regex_pattern)
        regex_pattern = f"^{regex_pattern}$"

        try:
            if re.match(regex_pattern, key):
                return True
        except Exception:
            continue

    return False


def get_plural_base_key(key: str) -> str | None:
    """
    Get the base key for plural forms.

    Example: "example_one" -> "example", "example_other" -> "example"
    Returns None if the key is not a plural form.
    """
    for suffix in ["_zero", "_one", "_two", "_few", "_many", "_other"]:
        if key.endswith(suffix):
            return key[: -len(suffix)]
    return None


def find_unused_translation_keys() -> dict[str, dict[str, list[str]]]:
    """
    Find translation keys that are defined but never used in the source code.

    Returns a dict mapping namespace to dict of "unused" and "used" keys.
    """
    console = get_console()

    # Get all keys from English locale
    en_locale_dir = LOCALES_DIR / "en"
    if not en_locale_dir.exists():
        console.print(f"[red]Error: English locale directory not found: {en_locale_dir}[/red]")
        return {}

    console.print("[cyan]Collecting translation keys from English locale...[/cyan]")
    defined_keys = collect_translation_keys_from_json(en_locale_dir)

    console.print(f"[cyan]Found {sum(len(keys) for keys in defined_keys.values())} translation keys[/cyan]")

    # Find used keys in source code
    console.print("[cyan]Scanning source code for translation usage...[/cyan]")
    used_keys_raw = find_translation_usage_in_source(UI_SRC_DIR)

    console.print(
        f"[cyan]Found {sum(len(keys) for keys in used_keys_raw.values())} used translation keys[/cyan]"
    )

    # Separate dynamic patterns from actual keys
    dynamic_patterns: dict[str, set[str]] = {}
    used_keys: dict[str, set[str]] = {}

    for namespace, keys in used_keys_raw.items():
        dynamic_patterns[namespace] = {k for k in keys if k.startswith("[DYNAMIC] ")}
        used_keys[namespace] = {k for k in keys if not k.startswith("[DYNAMIC] ")}

    # Build a global set of all defined keys across all namespaces for cross-namespace lookups
    all_defined_keys = set()
    for keys in defined_keys.values():
        all_defined_keys.update(keys)

    # Find unused keys, considering dynamic patterns and plural forms
    results: dict[str, dict[str, list[str]]] = {}
    for namespace, defined in defined_keys.items():
        used = used_keys.get(namespace, set())
        dynamic = dynamic_patterns.get(namespace, set())

        # Build a set of plural base keys that are used
        # If we see "example" or "example_one" used, then all "example_*" forms are considered used
        used_plural_bases = set()
        for key in used:
            # Check if this key itself is a base for plural forms
            base = get_plural_base_key(key)
            if base:
                used_plural_bases.add(base)
            else:
                # The key itself might be used without suffix (e.g., translate("example", { count: 5 }))
                # In this case, it's a base key
                used_plural_bases.add(key)

        # A key is considered unused if:
        # 1. It's not in the used set, AND
        # 2. It doesn't match any dynamic pattern, AND
        # 3. If it's a plural form (_one, _other, etc.), its base key is not used
        unused = []
        for key in defined:
            # Check if the key itself is used
            if key in used:
                continue

            # Check if it matches a dynamic pattern
            if is_key_used_by_dynamic_pattern(key, dynamic):
                continue

            # Check if it's a plural form and the base key is used
            base = get_plural_base_key(key)
            if base and base in used_plural_bases:
                # The base key is used, so this plural form is considered used
                continue

            # Otherwise, it's unused
            unused.append(key)

        unused = sorted(unused)

        # Filter out "undefined_used" keys that actually exist in other namespaces
        # Only report cross-namespace usage if the namespace is NOT "common"
        # (since "common" is typically the fallback namespace)
        undefined_used = used - defined
        truly_undefined = []
        cross_namespace_used = []

        for key in undefined_used:
            # If plural forms exist for a key, it's a valid use of a base key, not an undefined one.
            if (f"{key}_one" in defined) or (f"{key}_other" in defined):
                continue

            # Also valid if plural forms are defined in another namespace (e.g. 'common' fallback)
            if (f"{key}_one" in all_defined_keys) or (f"{key}_other" in all_defined_keys):
                # This is normal i18next fallback behavior, don't report
                if namespace != "common" and (
                    f"{key}_one" in defined_keys.get("common", set())
                    or f"{key}_other" in defined_keys.get("common", set())
                ):
                    continue
                # If plural forms are defined elsewhere, the base key is considered valid.
                continue

            if key in all_defined_keys:
                # For non-plural keys, check for normal cross-namespace fallback to "common".
                if namespace != "common" and key in defined_keys.get("common", set()):
                    # This is normal i18next fallback behavior, don't report
                    continue
                # Unusual cross-namespace usage, report it
                cross_namespace_used.append(key)
            else:
                truly_undefined.append(key)

        if unused or used or dynamic or truly_undefined or cross_namespace_used:
            results[namespace] = {
                "unused": unused,
                "used": sorted(used),
                "undefined_used": sorted(truly_undefined),
                "cross_namespace_used": sorted(cross_namespace_used),
                "dynamic_patterns": sorted(dynamic),
            }

    return results


@ui_group.command(
    name="check-unused-translations",
    help="Check for unused translation keys in the UI source code.",
)
@click.option(
    "--namespace",
    "-n",
    default=None,
    help="Check only a specific namespace (e.g., common, dags, admin).",
)
@click.option(
    "--show-used",
    is_flag=True,
    default=False,
    help="Also show used translation keys.",
)
@click.option(
    "--remove-unused",
    is_flag=True,
    default=False,
    help="Remove unused translation keys from all locale files.",
)
@option_verbose
@option_dry_run
def check_unused_translations(
    namespace: str | None = None,
    show_used: bool = False,
    remove_unused: bool = False,
):
    console = get_console()

    results = find_unused_translation_keys()

    if not results:
        console.print("[red]No translation files found or error occurred.[/red]")
        sys.exit(1)

    # Filter by namespace if specified
    if namespace:
        if namespace not in results:
            console.print(f"[red]Namespace '{namespace}' not found.[/red]")
            console.print(f"[cyan]Available namespaces: {', '.join(sorted(results.keys()))}[/cyan]")
            sys.exit(1)
        results = {namespace: results[namespace]}

    total_unused = 0
    total_defined = 0
    total_undefined_used = 0
    total_dynamic_patterns = 0

    total_cross_namespace = 0

    for ns in sorted(results.keys()):
        data = results[ns]
        unused = data["unused"]
        used = data["used"]
        undefined_used = data["undefined_used"]
        cross_namespace_used = data.get("cross_namespace_used", [])
        dynamic_patterns = data.get("dynamic_patterns", [])

        total_unused += len(unused)
        total_defined += len(unused) + len(used)
        total_undefined_used += len(undefined_used)
        total_cross_namespace += len(cross_namespace_used)
        total_dynamic_patterns += len(dynamic_patterns)

        console.print(Panel(f"[bold cyan]{ns}.json[/bold cyan]", style="blue"))

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Status", style="bold")
        table.add_column("Count", justify="right")
        table.add_column("Percentage", justify="right")

        total_keys = len(unused) + len(used)
        unused_pct = (len(unused) / total_keys * 100) if total_keys > 0 else 0
        used_pct = (len(used) / total_keys * 100) if total_keys > 0 else 0

        table.add_row("Total Defined", str(total_keys), "100.0%")
        table.add_row(
            "Used",
            f"[green]{len(used)}[/green]",
            f"[green]{used_pct:.1f}%[/green]",
        )
        table.add_row(
            "Unused",
            f"[red]{len(unused)}[/red]" if unused else "[green]0[/green]",
            f"[red]{unused_pct:.1f}%[/red]" if unused else "[green]0.0%[/green]",
        )
        if undefined_used:
            table.add_row(
                "Undefined (used but not defined)",
                f"[yellow]{len(undefined_used)}[/yellow]",
                "",
            )

        console.print(table)

        if unused:
            console.print(f"\n[red]Unused keys in {ns}.json:[/red]")
            for key in unused[:20]:  # Show first 20
                console.print(f"  [yellow]- {key}[/yellow]")
            if len(unused) > 20:
                console.print(f"  [dim]... and {len(unused) - 20} more[/dim]")

        if undefined_used:
            console.print(f"\n[yellow]Keys used in code but not defined in {ns}.json:[/yellow]")
            for key in undefined_used[:20]:
                console.print(f"  [yellow]- {key}[/yellow]")
            if len(undefined_used) > 20:
                console.print(f"  [dim]... and {len(undefined_used) - 20} more[/dim]")

        if cross_namespace_used and show_used:
            # Only show cross-namespace keys in verbose mode (--show-used)
            console.print(f"\n[cyan]Keys used in {ns} but defined in other namespaces:[/cyan]")
            console.print("[dim](These may be from helper functions without useTranslation)[/dim]")
            for key in cross_namespace_used[:20]:
                console.print(f"  [cyan]- {key}[/cyan]")
            if len(cross_namespace_used) > 20:
                console.print(f"  [dim]... and {len(cross_namespace_used) - 20} more[/dim]")

        if dynamic_patterns:
            console.print(f"\n[blue]Dynamic patterns detected in {ns}.json:[/blue]")
            for pattern in dynamic_patterns[:10]:
                # Remove [DYNAMIC] prefix for display
                clean_pattern = pattern.replace("[DYNAMIC] ", "")
                console.print(f"  [blue]- {clean_pattern}[/blue]")
            if len(dynamic_patterns) > 10:
                console.print(f"  [dim]... and {len(dynamic_patterns) - 10} more[/dim]")

        if show_used and used:
            console.print(f"\n[green]Used keys in {ns}.json:[/green]")
            for key in used[:20]:
                console.print(f"  [green]- {key}[/green]")
            if len(used) > 20:
                console.print(f"  [dim]... and {len(used) - 20} more[/dim]")

        console.print()

    # Summary
    console.print(Panel("[bold]Summary[/bold]", style="cyan"))
    summary_table = Table(show_header=True, header_style="bold magenta")
    summary_table.add_column("Metric", style="bold")
    summary_table.add_column("Count", justify="right")

    summary_table.add_row("Total Defined Keys", str(total_defined))
    summary_table.add_row("Total Used Keys", str(total_defined - total_unused))
    summary_table.add_row(
        "Total Unused Keys",
        f"[red]{total_unused}[/red]" if total_unused > 0 else "[green]0[/green]",
    )
    summary_table.add_row(
        "Keys Used But Not Defined",
        f"[yellow]{total_undefined_used}[/yellow]" if total_undefined_used > 0 else "[green]0[/green]",
    )
    summary_table.add_row(
        "Cross-Namespace Keys",
        f"[cyan]{total_cross_namespace}[/cyan]" if total_cross_namespace > 0 else "0",
    )
    summary_table.add_row(
        "Dynamic Patterns Detected",
        f"[blue]{total_dynamic_patterns}[/blue]" if total_dynamic_patterns > 0 else "0",
    )

    if total_defined > 0:
        usage_pct = ((total_defined - total_unused) / total_defined) * 100
        summary_table.add_row(
            "Usage Rate",
            f"[green]{usage_pct:.1f}%[/green]" if usage_pct >= 90 else f"[yellow]{usage_pct:.1f}%[/yellow]",
        )

    console.print(summary_table)

    if remove_unused and total_unused > 0:
        console.print("\n[yellow]Removing unused keys from all locale files...[/yellow]")

        locale_files = get_locale_files()
        for lf in locale_files:
            for ns in results.keys():
                unused_keys = set(results[ns]["unused"])
                if not unused_keys:
                    continue

                json_file = LOCALES_DIR / lf.locale / f"{ns}.json"
                if not json_file.exists():
                    continue

                try:
                    data = load_json(json_file)

                    # Remove unused keys
                    def remove_keys(dst, prefix=""):
                        keys_to_remove = []
                        for k, v in list(dst.items()):
                            full_key = f"{prefix}.{k}" if prefix else k
                            if full_key in unused_keys:
                                keys_to_remove.append(k)
                            elif isinstance(v, dict):
                                remove_keys(v, full_key)
                                if not v:
                                    keys_to_remove.append(k)
                        for k in keys_to_remove:
                            del dst[k]

                    remove_keys(data)

                    # Save the file
                    def sort_dict_keys(obj):
                        if isinstance(obj, dict):
                            return {
                                k: sort_dict_keys(obj[k]) for k in sorted(obj.keys(), key=natural_sort_key)
                            }
                        return obj

                    data = sort_dict_keys(data)
                    with open(json_file, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                        f.write("\n")

                    console.print(
                        f"[green]Removed {len(unused_keys)} unused keys from {lf.locale}/{ns}.json[/green]"
                    )

                except Exception as e:
                    console.print(f"[red]Error processing {json_file}: {e}[/red]")

        console.print("\n[green]Unused keys removed from all locale files![/green]")

    if total_unused == 0 and total_undefined_used == 0:
        console.print("\n[green]All translation keys are being used correctly![/green]")
        sys.exit(0)
    elif total_undefined_used > 0:
        console.print(
            f"\n[yellow]Warning: {total_undefined_used} keys are used in code but not defined in translation files![/yellow]"
        )
        sys.exit(1)
    else:
        console.print(f"\n[yellow]Found {total_unused} unused translation keys.[/yellow]")
        sys.exit(1)
