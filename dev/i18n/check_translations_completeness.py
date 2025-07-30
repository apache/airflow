#!/usr/bin/env python3
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

# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "rich",
#   "rich-click",
# ]
# ///
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, NamedTuple

import rich_click as click
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

click.rich_click.MAX_WIDTH = 120
click.rich_click.USE_RICH_MARKUP = True

LOCALES_DIR = (
    Path(__file__).parents[2] / "airflow-core" / "src" / "airflow" / "ui" / "public" / "i18n" / "locales"
)


class LocaleSummary(NamedTuple):
    """
    Summary of missing and extra translation keys for a file, per locale.

    Attributes:
        missing_keys: A dictionary mapping locale codes to lists of missing translation keys.
        extra: A dictionary mapping locale codes to lists of extra translation keys.
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
            if filename in lf.files:
                path = LOCALES_DIR / lf.locale / filename
                try:
                    data = load_json(path)
                    keys = set(flatten_keys(data))
                except Exception as e:
                    print(f"Error loading {path}: {e}")
                    keys = set()
            else:
                keys = None
            key_sets.append(LocaleKeySet(locale=lf.locale, keys=keys))
        keys_by_locale = {ks.locale: ks.keys for ks in key_sets}
        en_keys = keys_by_locale.get("en", set()) or set()
        missing_keys: dict[str, list[str]] = {}
        extra_keys: dict[str, list[str]] = {}
        missing_counts[filename] = {}
        for ks in key_sets:
            if ks.locale == "en":
                continue
            if ks.keys is None:
                missing_keys[ks.locale] = list(en_keys)
                extra_keys[ks.locale] = []
                missing_counts[filename][ks.locale] = len(en_keys)
            else:
                missing = list(en_keys - ks.keys)
                missing_keys[ks.locale] = missing
                extra_keys[ks.locale] = list(ks.keys - en_keys)
                missing_counts[filename][ks.locale] = len(missing)
        summary[filename] = LocaleSummary(missing_keys=missing_keys, extra_keys=extra_keys)
    return summary, missing_counts


def print_locale_file_table(
    locale_files: list[LocaleFiles], console: Console, language: str | None = None
) -> None:
    if language and language == "en":
        return
    console.print("[bold underline]Locales and their files:[/bold underline]", style="cyan")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Locale")
    table.add_column("Files")
    filtered = (
        locale_files if language is None else [lf for lf in locale_files if lf.locale in (language, "en")]
    )
    for lf in filtered:
        table.add_row(lf.locale, ", ".join(lf.files))
    console.print(table)


def print_file_set_differences(
    locale_files: list[LocaleFiles], console: Console, language: str | None = None
) -> bool:
    if language and language == "en":
        return False
    filtered = (
        locale_files if language is None else [lf for lf in locale_files if lf.locale in (language, "en")]
    )
    all_file_sets = {lf.locale: set(lf.files) for lf in filtered}
    file_sets: list[set[str]] = list(all_file_sets.values())
    found_difference = False
    if len(file_sets) > 1 and any(file_sets[0] != fs for fs in file_sets[1:]):
        found_difference = True
        console.print("[bold red]Error: Locales have different sets of translation files![/bold red]")
        for locale, files_set in all_file_sets.items():
            console.print(f"[yellow]{locale}[/yellow]: {sorted(list(files_set))}")
    return found_difference


def print_language_summary(
    locale_files: list[LocaleFiles], summary: dict[str, LocaleSummary], console: Console
) -> bool:
    found_difference = False
    missing_in_en: dict[str, dict[str, set[str]]] = {}
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
                if locale != "en":
                    for k in extra_keys:
                        missing_in_en.setdefault(filename, {}).setdefault(k, set()).add(locale)
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


def get_outdated_entries_for_language(
    summary: dict[str, LocaleSummary], language: str
) -> dict[str, list[str]]:
    """Return a dict of filename -> list of outdated/old keys for the given language (present in other languages, missing in the given language)."""
    outdated: dict[str, list[str]] = {}
    for filename, diff in summary.items():
        missing = diff.missing_keys.get(language, [])
        if missing:
            outdated[filename] = list(missing)
    return outdated


def count_todo_and_total(obj):
    """Recursively count TODO: translate entries and total entries in a dict or list."""
    todo = 0
    total = 0
    if isinstance(obj, dict):
        for v in obj.values():
            t, n = count_todo_and_total(v)
            todo += t
            total += n
    elif isinstance(obj, list):
        for v in obj:
            t, n = count_todo_and_total(v)
            todo += t
            total += n
    else:
        total = 1
        if isinstance(obj, str) and obj.strip().startswith("TODO: translate"):
            todo = 1
    return todo, total


def print_translation_progress(console, locale_files, missing_counts, summary):
    from rich.table import Table

    tables = defaultdict(lambda: Table(show_lines=True))
    all_files = set()
    for lf in locale_files:
        all_files.update(lf.files)
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
        table.add_column("Percent", style="bold")
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

                # Count TODOs
                def count_todos(obj):
                    if isinstance(obj, dict):
                        return sum(count_todos(v) for v in obj.values())
                    if isinstance(obj, list):
                        return sum(count_todos(v) for v in obj)
                    if isinstance(obj, str) and obj.strip().startswith("TODO: translate"):
                        return 1
                    return 0

                file_todos = count_todos(data)
                file_translated = file_total - file_missing
                percent = 100 * file_translated / file_total if file_total else 100
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
                percent = 0
                style = "red"
            table.add_row(
                f"[bold reverse]{filename}[/bold reverse]",
                str(file_missing),
                str(file_extra),
                str(file_todos),
                str(file_translated),
                str(file_total),
                f"{percent:.1f}%",
                style=style,
            )
            total_missing += file_missing
            total_extra += file_extra
            total_todos += file_todos
            total_translated += file_translated
            total_total += file_total
        percent = 100 * total_translated / total_total if total_total else 100
        table.add_row(
            "All files",
            str(total_missing),
            str(total_extra),
            str(total_todos),
            str(total_translated),
            str(total_total),
            f"{percent:.1f}%",
            style="red" if percent < 100 else "bold green",
        )
    for _lang, table in tables.items():
        console.print(table)


@click.command()
@click.option(
    "--language", "-l", default=None, help="Show summary for a single language (e.g. en, de, pl, etc.)"
)
@click.option(
    "--add-missing",
    is_flag=True,
    default=False,
    help="Add missing translations for the selected language, prefixed with 'TODO: translate:'.",
)
def cli(language: str | None = None, add_missing: bool = False):
    if add_missing:
        if not language:
            raise ValueError("--language is required when passing --add_missing")
        locale_path = LOCALES_DIR / language
        locale_path.mkdir(exist_ok=True)
    locale_files = get_locale_files()
    console = Console(force_terminal=True, color_system="auto")
    print_locale_file_table(locale_files, console, language)
    found_difference = print_file_set_differences(locale_files, console, language)
    summary, missing_counts = compare_keys(locale_files)
    console.print("\n[bold underline]Summary of differences by language:[/bold underline]", style="cyan")
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
                [lf for lf in locale_files if lf.locale == language], filtered_summary, console
            )
            found_difference = found_difference or lang_diff
            if add_missing:
                add_missing_translations(language, filtered_summary, console)
    else:
        lang_diff = print_language_summary(locale_files, summary, console)
        found_difference = found_difference or lang_diff
    print_translation_progress(
        console,
        [lf for lf in locale_files if language is None or lf.locale == language],
        missing_counts,
        summary,
    )
    if not found_difference:
        console.print("[green]All translations are complete and consistent![green]")
    if found_difference:
        sys.exit(1)


def add_missing_translations(language: str, summary: dict[str, LocaleSummary], console: Console):
    """
    Add missing translations for the selected language.

    It does it by copying them from English and prefixing with 'TODO: translate:'.
    """
    for filename, diff in summary.items():
        missing_keys = diff.missing_keys.get(language, [])
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
        except Exception as e:
            console.print(f"[yellow]Failed to load {language} file {language}: {e}[/yellow]")
            lang_data = {}  # Start with an empty dict if the file doesn't exist

        # Helper to recursively add missing keys
        def add_keys(src, dst, prefix=""):
            for k, v in src.items():
                full_key = f"{prefix}.{k}" if prefix else k
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
        # Write back to file, preserving order
        lang_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lang_path, "w", encoding="utf-8") as f:
            json.dump(lang_data, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")  # Ensure newline at the end of the file
        console.print(f"[green]Added missing translations to {lang_path}[/green]")


if __name__ == "__main__":
    cli()
