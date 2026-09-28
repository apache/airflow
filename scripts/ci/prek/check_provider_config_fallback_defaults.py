#!/usr/bin/env python
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
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "pyyaml",
#   "rich>=13.6.0",
# ]
# ///
"""
Check that ``provider_config_fallback_defaults.cfg`` only carries options providers still declare.

``airflow-core/src/airflow/config_templates/provider_config_fallback_defaults.cfg`` is a hand-maintained
subset of provider configuration: the defaults core may need while provider modules are being imported,
before the providers' own configuration has been loaded. Everything in it is merged into ``conf`` as if
the provider had declared it (``has_option``, ``as_dict``, ``airflow config list``), so an entry that no
``provider.yaml`` declares any more keeps a removed option alive indefinitely.

This check enforces the "cfg is a subset of provider.yaml" direction only: every ``(section, option)``
in the cfg must be declared in the ``config`` section of some ``providers/**/provider.yaml`` with the
same default.
"""

from __future__ import annotations

import sys
from configparser import ConfigParser
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import yaml
from common_prek_utils import (
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_ROOT_PATH,
    console,
    get_all_provider_yaml_files,
)
from rich.markup import escape

if TYPE_CHECKING:
    from collections.abc import Iterable

FALLBACK_CFG_PATH = (
    AIRFLOW_CORE_SOURCES_PATH / "airflow" / "config_templates" / "provider_config_fallback_defaults.cfg"
)

# Options whose cfg default intentionally differs from the provider.yaml default.
# (section, option, provider.yaml default, cfg default)
#
# Keep in sync with PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK in
# devel-common/src/tests_common/test_utils/config.py - the unit test in
# scripts/tests/ci/prek/test_check_provider_config_fallback_defaults.py fails when the two lists differ.
PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK: list[tuple[str, str, str, str]] = [
    (
        "celery",
        "celery_app_name",
        "airflow.providers.celery.executors.celery_executor",
        "airflow.executors.celery_executor",
    ),
]


class ProviderOption(NamedTuple):
    """Default declared for a config option in a provider.yaml (``None`` when declared as ``~``)."""

    default: str | None
    provider_yaml: Path


def load_fallback_cfg(cfg_path: Path) -> dict[str, dict[str, str]]:
    """Return ``{section: {option: value}}`` for the fallback cfg, lower-cased and without interpolation."""
    parser = ConfigParser(interpolation=None)
    parser.read_string(cfg_path.read_text(), source=str(cfg_path))
    return {
        section.lower(): {option.lower(): value for option, value in parser.items(section)}
        for section in parser.sections()
    }


def load_provider_config_options(
    provider_yaml_files: Iterable[Path],
) -> dict[tuple[str, str], ProviderOption]:
    """Return ``{(section, option): ProviderOption}`` for every config option the given provider.yaml files declare."""
    loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    options: dict[tuple[str, str], ProviderOption] = {}
    for provider_yaml in sorted(provider_yaml_files):
        provider_info = yaml.load(provider_yaml.read_text(), Loader=loader)
        for section, section_content in (provider_info.get("config") or {}).items():
            for option, option_content in ((section_content or {}).get("options") or {}).items():
                default = (option_content or {}).get("default")
                options[(section.lower(), option.lower())] = ProviderOption(
                    default=None if default is None else str(default),
                    provider_yaml=provider_yaml,
                )
    return options


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(AIRFLOW_ROOT_PATH).as_posix()
    except ValueError:
        return path.as_posix()


def find_drift(
    cfg: dict[str, dict[str, str]],
    provider_options: dict[tuple[str, str], ProviderOption],
    overrides: Iterable[tuple[str, str, str, str]] = PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK,
) -> list[str]:
    """
    Return one message per cfg entry that no provider.yaml declares, or declares with a different default.

    Sections that no provider.yaml declares at all are reported once, not once per option.
    """
    provider_sections = {section for section, _ in provider_options}
    intentional = {
        (section, option): (metadata_default, cfg_default)
        for section, option, metadata_default, cfg_default in overrides
    }
    errors: list[str] = []
    for section, options in cfg.items():
        if section not in provider_sections:
            errors.append(
                f"[{section}]: no provider.yaml declares this section any more - remove the whole section"
            )
            continue
        for option, cfg_value in options.items():
            declared = provider_options.get((section, option))
            if declared is None:
                errors.append(
                    f"[{section}] {option}: no provider.yaml declares this option any more - remove it"
                )
                continue
            expected = intentional.get((section, option))
            if expected is not None:
                metadata_default, cfg_default = expected
                if declared.default != metadata_default or cfg_value != cfg_default:
                    errors.append(
                        f"[{section}] {option}: intentional override is out of date - "
                        f"{_display_path(declared.provider_yaml)} declares {declared.default!r}, "
                        f"the cfg has {cfg_value!r}, but PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK "
                        f"expects ({metadata_default!r}, {cfg_default!r})"
                    )
                continue
            if declared.default is None:
                errors.append(
                    f"[{section}] {option} = {cfg_value!r}: "
                    f"{_display_path(declared.provider_yaml)} declares no default for it"
                )
            elif declared.default != cfg_value:
                errors.append(
                    f"[{section}] {option} = {cfg_value!r}: "
                    f"{_display_path(declared.provider_yaml)} declares default {declared.default!r}"
                )
    return errors


def main() -> int:
    cfg = load_fallback_cfg(FALLBACK_CFG_PATH)
    provider_options = load_provider_config_options(get_all_provider_yaml_files())
    errors = find_drift(cfg, provider_options)
    if errors:
        console.print(
            f"\n[red]{_display_path(FALLBACK_CFG_PATH)} is out of sync with provider.yaml files![/]\n"
        )
        for error in errors:
            # Section names look like Rich markup tags, so escape them before printing.
            console.print(f"  - {escape(error)}")
        console.print(
            "\n[yellow]Every option in the fallback cfg must be declared in some providers/**/provider.yaml "
            "with the same default. If a provider dropped or renamed an option, drop it from the cfg too. "
            "If a default has to differ on purpose, add it to PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK both "
            "in this script and in devel-common/src/tests_common/test_utils/config.py.[/]\n"
        )
        return 1
    checked = sum(len(options) for options in cfg.values())
    console.print(
        f"[green]All {checked} fallback defaults in {len(cfg)} sections are declared "
        "in provider.yaml files with matching defaults.[/]"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
