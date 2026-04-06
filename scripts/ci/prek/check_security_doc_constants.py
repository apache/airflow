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
#   "pyyaml>=6.0.3",
#   "rich>=13.6.0",
# ]
# ///
"""
Validate that security documentation references to config.yml options stay in sync.

Checks performed:
  1. Every ``[section] option`` reference in the security RST files corresponds to an
     actual option in config.yml.
  2. Default values quoted in the docs match the defaults in config.yml.
  3. The sensitive-variable list in security_model.rst includes all config options
     marked ``sensitive: true`` in config.yml that have an ``AIRFLOW__`` env-var form.
  4. ``[section] option`` references use correct section names.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_prek_utils import AIRFLOW_ROOT_PATH

console = Console(color_system="standard", width=200)

CONFIG_YML = AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "config_templates" / "config.yml"
PROVIDERS_ROOT = AIRFLOW_ROOT_PATH / "providers"

SECURITY_DOCS = [
    AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "security" / "jwt_token_authentication.rst",
    AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "security" / "security_model.rst",
]

# Pattern to match ``[section] option_name`` references in RST
SECTION_OPTION_RE = re.compile(r"``\[(\w+)\]\s+(\w+)``")

# Pattern to match default value claims like "default: VALUE" or "Default VALUE" or "(default VALUE)"
# in table rows like "- 86400 (24h)" preceded by a config option
DEFAULT_IN_TABLE_RE = re.compile(
    r"``\[(\w+)\]\s+(\w+)``.*?(?:default[:\s]+|Default[:\s]+)(\S+)", re.IGNORECASE
)

# Pattern to match AIRFLOW__SECTION__OPTION env var references
ENV_VAR_RE = re.compile(r"``(AIRFLOW__\w+)``")

# Map section+option to the AIRFLOW__ env var form
SECTION_OPT_TO_ENV = re.compile(r"AIRFLOW__([A-Z_]+)__([A-Z_]+)")


def option_to_env_var(section: str, option: str) -> str:
    """Convert a config section+option to its AIRFLOW__ env var form."""
    return f"AIRFLOW__{section.upper()}__{option.upper()}"


def load_config() -> dict:
    """Load config.yml and provider.yaml files to get all config sections/options."""
    with open(CONFIG_YML) as f:
        config = yaml.safe_load(f)

    # Also load provider.yaml files which define config sections under "config:" key
    # (e.g., [celery], [sentry], [workers])
    for provider_yaml in PROVIDERS_ROOT.glob("*/provider.yaml"):
        with open(provider_yaml) as f:
            provider_data = yaml.safe_load(f)
            if provider_data and "config" in provider_data:
                for section_name, section_data in provider_data["config"].items():
                    if isinstance(section_data, dict) and section_name not in config:
                        config[section_name] = section_data

    return config


def get_all_options(config: dict) -> dict[tuple[str, str], dict]:
    """Return a dict of (section, option) -> option_config for all config options."""
    result = {}
    for section_name, section_data in config.items():
        if not isinstance(section_data, dict) or "options" not in section_data:
            continue
        for option_name, option_config in section_data["options"].items():
            if isinstance(option_config, dict):
                result[(section_name, option_name)] = option_config
    return result


def get_sensitive_env_vars(all_options: dict[tuple[str, str], dict]) -> set[str]:
    """Return set of AIRFLOW__X__Y env vars for all sensitive config options."""
    result = set()
    for (section, option), config in all_options.items():
        if config.get("sensitive"):
            result.add(option_to_env_var(section, option))
    return result


def check_option_references(doc_path: Path, all_options: dict[tuple[str, str], dict]) -> list[str]:
    """Check that all [section] option references in the doc exist in config.yml."""
    errors = []
    content = doc_path.read_text()

    for line_num, line in enumerate(content.splitlines(), 1):
        for match in SECTION_OPTION_RE.finditer(line):
            section = match.group(1)
            option = match.group(2)
            if (section, option) not in all_options:
                # Check if the section exists at all
                section_exists = any(s == section for s, _ in all_options)
                if section_exists:
                    errors.append(
                        f"{doc_path.name}:{line_num}: Option ``[{section}] {option}`` not found in config.yml"
                    )
                else:
                    errors.append(
                        f"{doc_path.name}:{line_num}: Section ``[{section}]`` not found in config.yml"
                    )
    return errors


def check_env_var_references(doc_path: Path, all_options: dict[tuple[str, str], dict]) -> list[str]:
    """Check that AIRFLOW__X__Y env var references correspond to real config options."""
    errors = []
    content = doc_path.read_text()

    for line_num, line in enumerate(content.splitlines(), 1):
        for match in ENV_VAR_RE.finditer(line):
            env_var = match.group(1)
            m = SECTION_OPT_TO_ENV.match(env_var)
            if not m:
                continue
            section = m.group(1).lower()
            option = m.group(2).lower()
            if (section, option) not in all_options:
                # Check if the section exists
                section_exists = any(s == section for s, _ in all_options)
                if section_exists:
                    errors.append(
                        f"{doc_path.name}:{line_num}: Env var ``{env_var}`` references "
                        f"option [{section}] {option} which is not in config.yml"
                    )
                else:
                    errors.append(
                        f"{doc_path.name}:{line_num}: Env var ``{env_var}`` references "
                        f"section [{section}] which is not in config.yml"
                    )
    return errors


def check_sensitive_vars_listed(security_model_path: Path, sensitive_env_vars: set[str]) -> list[str]:
    """
    Check that security_model.rst lists all sensitive config vars (as env vars).

    Returns warnings (printed but not counted as errors) since the doc explicitly
    states the list is non-exhaustive.
    """
    content = security_model_path.read_text()

    # Find the env vars that ARE listed in the doc
    listed_env_vars = set()
    for match in ENV_VAR_RE.finditer(content):
        env_var = match.group(1)
        if env_var.startswith("AIRFLOW__"):
            listed_env_vars.add(env_var)

    # Print warnings for missing sensitive vars (not errors — the list is non-exhaustive)
    missing = sorted(sensitive_env_vars - listed_env_vars)
    if missing:
        console.print()
        console.print(
            f"  [yellow]⚠[/] {security_model_path.name}: The following sensitive config variables "
            f"are not mentioned in the deployment hardening section (the list is documented as "
            f"non-exhaustive, so these are warnings, not errors):"
        )
        for env_var in missing:
            console.print(f"    [yellow]- {env_var}[/]")

    # No errors returned — these are warnings only
    return []


def check_defaults_in_tables(doc_path: Path, all_options: dict[tuple[str, str], dict]) -> list[str]:
    """
    Check default values in RST table rows match config.yml.

    Looks for patterns like:
      * - ``[section] option``
        - 86400 (24h)
    in list-table blocks.
    """
    errors = []
    content = doc_path.read_text()
    lines = content.splitlines()

    # Simple heuristic: find table rows with a config reference followed by a default value row
    i = 0
    while i < len(lines):
        line = lines[i]
        match = SECTION_OPTION_RE.search(line)
        if match and "* -" in line:
            section = match.group(1)
            option = match.group(2)
            # Next non-empty line starting with "- " or "     -" is the value
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines) and lines[j].strip().startswith("-"):
                value_line = lines[j].strip().lstrip("- ").strip()
                key = (section, option)
                if key in all_options:
                    config_default = str(all_options[key].get("default", "~"))
                    # Extract numeric part from doc value like "86400 (24h)" -> "86400"
                    doc_value = value_line.split()[0] if value_line else ""
                    # Strip surrounding backticks
                    doc_value = doc_value.strip("`")
                    # Normalize: remove quotes from config default
                    config_default_clean = config_default.strip('"').strip("'")
                    if (
                        doc_value
                        and config_default_clean
                        and config_default_clean not in ("~", "None", "none", "")
                        and doc_value != config_default_clean
                        # Don't flag if the doc value is a human-readable form
                        and not doc_value.startswith("Auto")
                        and not doc_value.startswith("None")
                        and doc_value != "``GUESS``"
                    ):
                        errors.append(
                            f"{doc_path.name}:{j + 1}: Default for [{section}] {option} is "
                            f"'{doc_value}' in docs but '{config_default_clean}' in config.yml"
                        )
        i += 1

    return errors


def main() -> int:
    config = load_config()
    all_options = get_all_options(config)
    sensitive_env_vars = get_sensitive_env_vars(all_options)

    all_errors: list[str] = []

    for doc_path in SECURITY_DOCS:
        if not doc_path.exists():
            console.print(f"  [yellow]⚠[/] {doc_path.name} not found, skipping")
            continue

        all_errors.extend(check_option_references(doc_path, all_options))
        all_errors.extend(check_env_var_references(doc_path, all_options))
        all_errors.extend(check_defaults_in_tables(doc_path, all_options))

    # Check sensitive vars are listed in security_model.rst
    security_model = AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "security" / "security_model.rst"
    if security_model.exists():
        all_errors.extend(check_sensitive_vars_listed(security_model, sensitive_env_vars))

    if all_errors:
        console.print()
        for error in all_errors:
            console.print(f"  [red]✗[/] {error}")
        console.print()
        console.print(f"[red]Security doc constants check failed with {len(all_errors)} error(s).[/]")
        console.print(
            "[yellow]Fix the documentation to match config.yml, or update config.yml if the docs are correct.[/]"
        )
        return 1

    console.print("[green]Security doc constants check passed.[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
