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
"""
Airflow Registry Metadata Extractor

Extracts provider and module metadata from:
1. provider.yaml files - Rich metadata (integrations, logos, categories)
2. objects.inv files - Module listing and documentation URLs
3. provider_metadata.json - Version history and compatibility
4. PyPI API - Download statistics (optional, requires network)

Output: JSON files for the Astro static site generator
"""

from __future__ import annotations

import ast
import json
import re
import shutil
import urllib.request
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import tomllib
import yaml


def fetch_pypi_downloads(package_name: str) -> dict[str, int]:
    """Fetch download statistics from pypistats.org API."""
    try:
        url = f"https://pypistats.org/api/packages/{package_name}/recent"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            return {
                "weekly": data["data"].get("last_week", 0),
                "monthly": data["data"].get("last_month", 0),
                "total": 0,  # Total not available in recent endpoint
            }
    except Exception as e:
        print(f"    Warning: Could not fetch PyPI stats for {package_name}: {e}")
        return {"weekly": 0, "monthly": 0, "total": 0}


def fetch_pypi_dates(package_name: str) -> dict[str, str]:
    """Fetch first release and latest release dates from PyPI JSON API."""
    try:
        url = f"https://pypi.org/pypi/{package_name}/json"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())

        earliest = None
        latest = None
        for version_files in data.get("releases", {}).values():
            for file_info in version_files:
                upload = file_info.get("upload_time_iso_8601") or file_info.get("upload_time")
                if not upload:
                    continue
                ts = upload[:10]
                if earliest is None or ts < earliest:
                    earliest = ts
                if latest is None or ts > latest:
                    latest = ts

        return {
            "first_released": earliest or "",
            "last_updated": latest or "",
        }
    except Exception as e:
        print(f"    Warning: Could not fetch PyPI dates for {package_name}: {e}")
        return {"first_released": "", "last_updated": ""}


# Base paths
AIRFLOW_ROOT = Path(__file__).parent.parent.parent
SCRIPT_DIR = Path(__file__).parent
PROVIDERS_DIR = AIRFLOW_ROOT / "providers"
REGISTRY_DIR = AIRFLOW_ROOT / "registry"
OUTPUT_DIR = REGISTRY_DIR / "src" / "_data"


@dataclass
class Category:
    """Category within a provider."""

    id: str
    name: str
    module_count: int = 0


@dataclass
class Module:
    """A module (operator, hook, sensor, etc.)."""

    id: str
    name: str  # Class name (e.g., SnowflakeOperator)
    type: str  # operator, hook, sensor, trigger, transfer
    import_path: str  # Full import path to the class
    module_path: str  # Module file path
    short_description: str
    docs_url: str
    source_url: str
    category: str
    provider_id: str
    provider_name: str


def extract_classes_from_python_file(file_path: Path, base_classes: set[str]) -> list[dict]:
    """
    Parse a Python file and extract class definitions that inherit from specific base classes.
    Returns list of dicts with class_name, docstring, and line_number.
    """
    if not file_path.exists():
        return []

    try:
        with open(file_path, encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
    except (SyntaxError, UnicodeDecodeError):
        return []

    classes = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            # Check if class inherits from any of the base classes
            for base in node.bases:
                base_name = ""
                if isinstance(base, ast.Name):
                    base_name = base.id
                elif isinstance(base, ast.Attribute):
                    base_name = base.attr

                if base_name in base_classes or not base_classes:
                    docstring = ast.get_docstring(node) or ""
                    # Get first line of docstring
                    short_desc = docstring.split("\n")[0].strip() if docstring else ""
                    classes.append(
                        {
                            "name": node.name,
                            "docstring": short_desc,
                            "line": node.lineno,
                        }
                    )
                    break

    return classes


def get_module_type_base_classes() -> dict[str, set[str]]:
    """Return base class names for each module type."""
    return {
        "operator": {
            "BaseOperator",
            "BaseSensorOperator",
            "DecoratedOperator",
            "PythonOperator",
            "BranchPythonOperator",
            "ExternalPythonOperator",
        },
        "hook": {
            "BaseHook",
            "DbApiHook",
            "DiscoverableHook",
        },
        "sensor": {
            "BaseSensorOperator",
            "PokeSensorOperator",
        },
        "trigger": {
            "BaseTrigger",
            "TriggerEvent",
        },
        "transfer": {
            "BaseOperator",  # Transfers are operators
        },
        "bundle": {
            "BaseDagBundle",
        },
    }


@dataclass
class Provider:
    """Provider metadata."""

    id: str
    name: str
    package_name: str
    description: str
    lifecycle: str = "production"  # AIP-95: incubation, production, mature, deprecated
    logo: str | None = None
    version: str = ""
    versions: list[str] = field(default_factory=list)
    airflow_versions: list[str] = field(default_factory=list)
    pypi_downloads: dict[str, int] = field(default_factory=lambda: {"weekly": 0, "monthly": 0, "total": 0})
    module_counts: dict[str, int] = field(
        default_factory=lambda: {
            "operator": 0,
            "hook": 0,
            "sensor": 0,
            "trigger": 0,
            "transfer": 0,
            "notifier": 0,
            "secret": 0,
            "logging": 0,
            "executor": 0,
            "bundle": 0,
            "decorator": 0,
        }
    )
    categories: list[dict] = field(default_factory=list)
    connection_types: list[dict] = field(default_factory=list)  # {conn_type, hook_class, docs_url}
    requires_python: str = ""  # e.g., ">=3.10"
    dependencies: list[str] = field(default_factory=list)  # from pyproject.toml
    optional_extras: dict[str, list[str]] = field(default_factory=dict)  # {extra_name: [deps]}
    dependents: list[str] = field(default_factory=list)
    related_providers: list[str] = field(default_factory=list)
    docs_url: str = ""
    source_url: str = ""
    pypi_url: str = ""
    first_released: str = ""
    last_updated: str = ""


def parse_provider_yaml(yaml_path: Path) -> dict[str, Any]:
    """Parse a provider.yaml file."""
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def parse_pyproject_toml(pyproject_path: Path) -> dict[str, Any]:
    """Parse pyproject.toml and extract requires-python, dependencies, and optional extras."""
    result: dict[str, Any] = {"requires_python": "", "dependencies": [], "optional_extras": {}}

    if not pyproject_path.exists():
        return result

    try:
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)

        project = data.get("project", {})

        result["requires_python"] = project.get("requires-python", "")
        result["dependencies"] = [d.strip() for d in project.get("dependencies", [])][:20]

        optional_deps = project.get("optional-dependencies", {})
        for extra_name, extra_deps in optional_deps.items():
            clean = [d.strip() for d in extra_deps if d.strip()]
            if clean:
                result["optional_extras"][extra_name] = clean[:5]

    except Exception as e:
        print(f"    Warning: Could not parse {pyproject_path}: {e}")

    return result


def extract_integrations_as_categories(provider_yaml: dict[str, Any]) -> list[Category]:
    """Extract integrations from provider.yaml as categories."""
    categories: dict[str, Category] = {}

    for integration in provider_yaml.get("integrations", []):
        name = integration.get("integration-name", "")
        if not name:
            continue

        # Create a slug for the category ID
        cat_id = name.lower().replace(" ", "-").replace("(", "").replace(")", "")
        cat_id = re.sub(r"[^a-z0-9-]", "", cat_id)

        if cat_id not in categories:
            categories[cat_id] = Category(id=cat_id, name=name, module_count=0)

    return list(categories.values())


def count_modules_by_type(provider_yaml: dict[str, Any]) -> dict[str, int]:
    """Count modules by type from provider.yaml."""
    counts = {
        "operator": 0,
        "hook": 0,
        "sensor": 0,
        "trigger": 0,
        "transfer": 0,
        "notifier": 0,
        "secret": 0,
        "logging": 0,
        "executor": 0,
        "bundle": 0,
        "decorator": 0,
    }

    for op in provider_yaml.get("operators", []):
        counts["operator"] += len(op.get("python-modules", []))

    for hook in provider_yaml.get("hooks", []):
        counts["hook"] += len(hook.get("python-modules", []))

    for sensor in provider_yaml.get("sensors", []):
        counts["sensor"] += len(sensor.get("python-modules", []))

    for trigger in provider_yaml.get("triggers", []):
        counts["trigger"] += len(trigger.get("python-modules", []))

    counts["transfer"] = len(provider_yaml.get("transfers", []))

    # Count notifications (notifiers)
    counts["notifier"] = len(provider_yaml.get("notifications", []))

    # Count secrets backends
    counts["secret"] = len(provider_yaml.get("secrets-backends", []))

    # Count logging handlers
    counts["logging"] = len(provider_yaml.get("logging", []))

    # Count executors
    counts["executor"] = len(provider_yaml.get("executors", []))

    # Count bundle backends
    for bundle_group in provider_yaml.get("bundles", []):
        counts["bundle"] += len(bundle_group.get("python-modules", []))

    # Count task decorators
    counts["decorator"] = len(provider_yaml.get("task-decorators", []))

    return counts


def module_path_to_file_path(module_path: str, provider_path: Path) -> Path:
    """Convert a Python module path to an actual file path.

    provider_path is the actual filesystem directory (e.g., providers/microsoft/azure/),
    not the dash-joined provider_id.
    """
    # e.g., airflow.providers.amazon.operators.s3 -> providers/amazon/src/airflow/providers/amazon/operators/s3.py
    parts = module_path.split(".")
    file_path = provider_path / "src" / "/".join(parts)
    return file_path.with_suffix(".py")


def extract_modules_from_yaml(
    provider_yaml: dict[str, Any],
    provider_id: str,
    provider_name: str,
    provider_path: Path,
    version: str = "",
) -> list[Module]:
    """Extract module information from provider.yaml, including actual class names from source files."""
    modules: list[Module] = []
    tag = f"providers-{provider_id}/{version}" if version else "main"
    base_docs_url = f"https://airflow.apache.org/docs/apache-airflow-providers-{provider_id}/stable"
    # Use the actual directory structure (e.g., providers/microsoft/azure/src) not the dash-joined ID
    provider_rel_path = provider_path.relative_to(PROVIDERS_DIR)
    base_source_url = f"https://github.com/apache/airflow/blob/{tag}/providers/{provider_rel_path}/src"

    # Helper to extract integration name as category
    def get_category(integration_name: str) -> str:
        cat_id = integration_name.lower().replace(" ", "-").replace("(", "").replace(")", "")
        return re.sub(r"[^a-z0-9-]", "", cat_id)

    def extract_classes_for_module(
        module_path: str,
        module_type: str,
        integration: str,
        category: str,
        transfer_desc: str | None = None,
    ) -> list[Module]:
        """Extract classes from a Python module file."""
        file_path = module_path_to_file_path(module_path, provider_path)
        module_name = module_path.split(".")[-1]

        # Get expected base classes for this module type
        base_class_map = {
            "operator": {"BaseOperator"},
            "hook": {"BaseHook"},
            "sensor": {"BaseSensorOperator"},
            "trigger": {"BaseTrigger"},
            "transfer": {"BaseOperator"},
            "bundle": set(),
        }
        base_classes = base_class_map.get(module_type, set())

        # Try to parse the Python file for class names
        classes = extract_classes_from_python_file(file_path, base_classes)

        if not classes:
            # Fallback: create a single module entry based on the module name
            class_name = "".join(word.capitalize() for word in module_name.split("_"))
            # Add type suffix if not already present
            type_suffix = module_type.capitalize()
            if not class_name.endswith(type_suffix):
                class_name = f"{class_name}{type_suffix}"

            # Use API reference URL format
            api_ref_path = module_path.replace(".", "/")
            full_class_path = f"{module_path}.{class_name}"
            api_docs_url = f"{base_docs_url}/_api/{api_ref_path}/index.html#{full_class_path}"

            return [
                Module(
                    id=f"{provider_id}-{module_name}-{class_name}",
                    name=class_name,
                    type=module_type,
                    import_path=f"{module_path}.{class_name}",
                    module_path=module_path,
                    short_description=transfer_desc or f"{integration} {module_type}",
                    docs_url=api_docs_url,
                    source_url=f"{base_source_url}/{module_path.replace('.', '/')}.py",
                    category=category,
                    provider_id=provider_id,
                    provider_name=provider_name,
                )
            ]

        # Filter classes to only include those matching the expected type
        type_patterns = {
            "operator": ["Operator", "Command"],
            "hook": ["Hook"],
            "sensor": ["Sensor"],
            "trigger": ["Trigger"],
            "transfer": ["Operator", "Transfer"],
            "bundle": ["Bundle"],
        }
        patterns = type_patterns.get(module_type, [])

        result = []
        for cls in classes:
            class_name = cls["name"]
            # Include if matches type pattern, or all public classes when no pattern defined
            is_relevant = (
                any(class_name.endswith(p) for p in patterns) if patterns else not class_name.startswith("_")
            )
            # Skip base/abstract classes
            is_base = class_name.startswith("Base") or "Abstract" in class_name or "Mixin" in class_name

            if is_relevant and not is_base:
                # Use API reference URL format - more reliable than Guides
                # Format: {base_docs_url}/_api/{module_path}/index.html#{full_class_path}
                api_ref_path = module_path.replace(".", "/")
                full_class_path = f"{module_path}.{class_name}"
                api_docs_url = f"{base_docs_url}/_api/{api_ref_path}/index.html#{full_class_path}"

                result.append(
                    Module(
                        id=f"{provider_id}-{module_name}-{class_name}",
                        name=class_name,
                        type=module_type,
                        import_path=f"{module_path}.{class_name}",
                        module_path=module_path,
                        short_description=cls["docstring"] or transfer_desc or f"{integration} {module_type}",
                        docs_url=api_docs_url,
                        source_url=f"{base_source_url}/{module_path.replace('.', '/')}.py#L{cls['line']}",
                        category=category,
                        provider_id=provider_id,
                        provider_name=provider_name,
                    )
                )

        # If no matching classes found, fall back to module-level entry
        if not result:
            class_name = "".join(word.capitalize() for word in module_name.split("_"))
            type_suffix = module_type.capitalize()
            if not class_name.endswith(type_suffix):
                class_name = f"{class_name}{type_suffix}"

            # Use API reference URL format
            api_ref_path = module_path.replace(".", "/")
            full_class_path = f"{module_path}.{class_name}"
            api_docs_url = f"{base_docs_url}/_api/{api_ref_path}/index.html#{full_class_path}"

            result.append(
                Module(
                    id=f"{provider_id}-{module_name}-{class_name}",
                    name=class_name,
                    type=module_type,
                    import_path=f"{module_path}.{class_name}",
                    module_path=module_path,
                    short_description=transfer_desc or f"{integration} {module_type}",
                    docs_url=api_docs_url,
                    source_url=f"{base_source_url}/{module_path.replace('.', '/')}.py",
                    category=category,
                    provider_id=provider_id,
                    provider_name=provider_name,
                )
            )

        return result

    # Extract operators
    for op_group in provider_yaml.get("operators", []):
        integration = op_group.get("integration-name", "")
        category = get_category(integration)
        for module_path in op_group.get("python-modules", []):
            modules.extend(extract_classes_for_module(module_path, "operator", integration, category))

    # Extract hooks
    for hook_group in provider_yaml.get("hooks", []):
        integration = hook_group.get("integration-name", "")
        category = get_category(integration)
        for module_path in hook_group.get("python-modules", []):
            modules.extend(extract_classes_for_module(module_path, "hook", integration, category))

    # Extract sensors
    for sensor_group in provider_yaml.get("sensors", []):
        integration = sensor_group.get("integration-name", "")
        category = get_category(integration)
        for module_path in sensor_group.get("python-modules", []):
            modules.extend(extract_classes_for_module(module_path, "sensor", integration, category))

    # Extract triggers
    for trigger_group in provider_yaml.get("triggers", []):
        integration = trigger_group.get("integration-name", "")
        category = get_category(integration)
        for module_path in trigger_group.get("python-modules", []):
            modules.extend(extract_classes_for_module(module_path, "trigger", integration, category))

    # Extract transfers
    for transfer in provider_yaml.get("transfers", []):
        source = transfer.get("source-integration-name", "")
        target = transfer.get("target-integration-name", "")
        module_path = transfer.get("python-module", "")
        if module_path:
            transfer_desc = f"Transfer from {source} to {target}"
            modules.extend(
                extract_classes_for_module(
                    module_path, "transfer", source, get_category(source), transfer_desc
                )
            )

    # Extract bundle backends
    for bundle_group in provider_yaml.get("bundles", []):
        integration = bundle_group.get("integration-name", "")
        category = get_category(integration)
        for module_path in bundle_group.get("python-modules", []):
            modules.extend(extract_classes_for_module(module_path, "bundle", integration, category))

    # Extract notifiers (notifications)
    for notifier_path in provider_yaml.get("notifications", []):
        # notifier_path is a full class path like: airflow.providers.amazon.aws.notifications.chime.ChimeNotifier
        if notifier_path:
            parts = notifier_path.rsplit(".", 1)
            if len(parts) == 2:
                module_path, class_name = parts
                modules.append(
                    Module(
                        id=f"{provider_id}-notifier-{class_name}",
                        name=class_name,
                        type="notifier",
                        import_path=notifier_path,
                        module_path=module_path,
                        short_description=f"{class_name.replace('Notifier', '')} notifier",
                        docs_url=f"{base_docs_url}/_api/{module_path.replace('.', '/')}/index.html#{notifier_path}",
                        source_url=f"{base_source_url}/{module_path.replace('.', '/')}.py",
                        category="notifications",
                        provider_id=provider_id,
                        provider_name=provider_name,
                    )
                )

    # Extract secrets backends
    for secret_path in provider_yaml.get("secrets-backends", []):
        if secret_path:
            parts = secret_path.rsplit(".", 1)
            if len(parts) == 2:
                module_path, class_name = parts
                modules.append(
                    Module(
                        id=f"{provider_id}-secret-{class_name}",
                        name=class_name,
                        type="secret",
                        import_path=secret_path,
                        module_path=module_path,
                        short_description=f"{class_name.replace('Backend', '').replace('Secret', '')} secrets backend",
                        docs_url=f"{base_docs_url}/_api/{module_path.replace('.', '/')}/index.html#{secret_path}",
                        source_url=f"{base_source_url}/{module_path.replace('.', '/')}.py",
                        category="secrets",
                        provider_id=provider_id,
                        provider_name=provider_name,
                    )
                )

    # Extract logging handlers
    for logging_path in provider_yaml.get("logging", []):
        if logging_path:
            parts = logging_path.rsplit(".", 1)
            if len(parts) == 2:
                module_path, class_name = parts
                modules.append(
                    Module(
                        id=f"{provider_id}-logging-{class_name}",
                        name=class_name,
                        type="logging",
                        import_path=logging_path,
                        module_path=module_path,
                        short_description=f"{class_name.replace('TaskHandler', '').replace('Handler', '')} task log handler",
                        docs_url=f"{base_docs_url}/_api/{module_path.replace('.', '/')}/index.html#{logging_path}",
                        source_url=f"{base_source_url}/{module_path.replace('.', '/')}.py",
                        category="logging",
                        provider_id=provider_id,
                        provider_name=provider_name,
                    )
                )

    # Extract executors
    for executor_path in provider_yaml.get("executors", []):
        if executor_path:
            parts = executor_path.rsplit(".", 1)
            if len(parts) == 2:
                module_path, class_name = parts
                modules.append(
                    Module(
                        id=f"{provider_id}-executor-{class_name}",
                        name=class_name,
                        type="executor",
                        import_path=executor_path,
                        module_path=module_path,
                        short_description=f"{class_name.replace('Executor', '')} executor",
                        docs_url=f"{base_docs_url}/_api/{module_path.replace('.', '/')}/index.html#{executor_path}",
                        source_url=f"{base_source_url}/{module_path.replace('.', '/')}.py",
                        category="executors",
                        provider_id=provider_id,
                        provider_name=provider_name,
                    )
                )

    # Extract task decorators
    for decorator in provider_yaml.get("task-decorators", []):
        decorator_class = decorator.get("class-name", "")
        decorator_name = decorator.get("name", "")
        if decorator_class:
            parts = decorator_class.rsplit(".", 1)
            if len(parts) == 2:
                module_path, func_name = parts
                display_name = f"@task.{decorator_name}" if decorator_name else func_name
                modules.append(
                    Module(
                        id=f"{provider_id}-decorator-{decorator_name or func_name}",
                        name=display_name,
                        type="decorator",
                        import_path=decorator_class,
                        module_path=module_path,
                        short_description=f"Task decorator for {decorator_name or func_name}",
                        docs_url=f"{base_docs_url}/_api/{module_path.replace('.', '/')}/index.html#{decorator_class}",
                        source_url=f"{base_source_url}/{module_path.replace('.', '/')}.py",
                        category="decorators",
                        provider_id=provider_id,
                        provider_name=provider_name,
                    )
                )

    return modules


def determine_airflow_versions(dependencies: list[str]) -> list[str]:
    """Determine minimum Airflow version from pyproject.toml dependencies."""
    for dep in dependencies:
        if dep.startswith("apache-airflow>="):
            version_str = dep.split(">=")[1].split(",")[0].strip()
            parts = version_str.split(".")
            if len(parts) >= 2:
                return [f"{parts[0]}.{parts[1]}+"]
    return ["3.0+"]


def find_related_providers(provider_id: str, all_provider_yamls: dict[str, dict]) -> list[str]:
    """Find related providers based on shared integrations or dependencies."""
    current = all_provider_yamls.get(provider_id, {})
    current_integrations = {i.get("integration-name") for i in current.get("integrations", [])}

    related = []
    for other_id, other_yaml in all_provider_yamls.items():
        if other_id == provider_id:
            continue
        other_integrations = {i.get("integration-name") for i in other_yaml.get("integrations", [])}
        overlap = current_integrations & other_integrations
        if overlap:
            related.append(other_id)

    return related[:5]  # Limit to 5 related providers


def main():
    """Main extraction function."""
    print("Airflow Registry Metadata Extractor")
    print("=" * 50)

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_providers: list[Provider] = []
    all_modules: list[Module] = []
    all_provider_yamls: dict[str, dict] = {}

    # First pass: Load all provider.yaml files (including nested ones like dbt/cloud, microsoft/azure)
    provider_yaml_paths: dict[str, Path] = {}
    for yaml_path in PROVIDERS_DIR.rglob("provider.yaml"):
        # Calculate provider_id from path relative to PROVIDERS_DIR
        # e.g., providers/amazon/provider.yaml -> amazon
        # e.g., providers/microsoft/azure/provider.yaml -> microsoft-azure
        relative_path = yaml_path.relative_to(PROVIDERS_DIR)
        parts = relative_path.parts[:-1]  # Remove 'provider.yaml'
        provider_id = "-".join(parts)

        try:
            provider_yaml = parse_provider_yaml(yaml_path)
            all_provider_yamls[provider_id] = provider_yaml
            provider_yaml_paths[provider_id] = yaml_path.parent
        except Exception as e:
            print(f"  Error parsing {yaml_path}: {e}")

    print(f"Found {len(all_provider_yamls)} providers with provider.yaml")

    # Second pass: Extract full metadata
    for provider_id, provider_yaml in all_provider_yamls.items():
        provider_path = provider_yaml_paths[provider_id]

        package_name = provider_yaml.get("package-name", f"apache-airflow-providers-{provider_id}")
        name = provider_yaml.get("name", provider_id.replace("-", " ").title())
        description = provider_yaml.get("description", "")

        # Clean up RST formatting in description
        # Convert RST links like `Text <url>`__ to just "Text"
        description = re.sub(r"`([^<]+)\s*<[^>]+>`__", r"\1", description)
        # Remove any remaining backticks
        description = re.sub(r"`", "", description)
        # Remove bullet points (- at start of lines)
        description = re.sub(r"\n\s*-\s*", ", ", description)
        # Clean up extra whitespace and normalize
        description = re.sub(r"\s+", " ", description).strip()
        # Remove trailing commas
        description = re.sub(r",\s*$", "", description)
        # Fix extra spaces before punctuation
        description = re.sub(r"\s+([),.])", r"\1", description)
        # Remove "including:" followed by just commas
        description = re.sub(r"including:\s*,", "including", description)
        # Fix "(including X )" -> "(including X)"
        description = re.sub(r"\s+\)", ")", description)
        # Remove empty parentheses
        description = re.sub(r"\(\s*\)", "", description)
        # Truncate long descriptions
        if len(description) > 200:
            description = description[:197] + "..."

        # Get versions
        versions = provider_yaml.get("versions", [])
        version = versions[0] if versions else "0.0.0"

        # Count modules
        module_counts = count_modules_by_type(provider_yaml)

        # Extract categories from integrations
        categories = extract_integrations_as_categories(provider_yaml)

        # Find logo and copy to registry public folder
        logo = None
        logo_path = None

        # Try to find the most representative logo for the provider
        # Priority: Look for "main" provider logos that match the provider name
        integration_logos_dir = provider_path / "docs" / "integration-logos"

        # Common patterns for main provider logos
        main_logo_patterns = [
            f"{name.replace(' ', '-')}.png",  # e.g., "Google-Cloud.png"
            f"{name.replace(' ', '-')}.svg",
            f"{name}.png",
            f"{provider_id.replace('-', '_')}.png",
        ]

        # Special case mappings for well-known providers
        logo_priority_map = {
            "amazon": ["AWS-Cloud-alt_light-bg@4x.png", "Amazon-Web-Services.png"],
            "google": ["Google-Cloud.png", "Google.png"],
            "microsoft-azure": ["Microsoft-Azure.png"],
            "snowflake": ["Snowflake.png"],
            "databricks": ["Databricks.png"],
        }

        # Setup logos destination directory
        logos_dest_dir = REGISTRY_DIR / "public" / "logos"
        logos_dest_dir.mkdir(parents=True, exist_ok=True)

        if integration_logos_dir.exists():
            # First, check for priority logos for known providers
            if provider_id in logo_priority_map:
                for priority_logo in logo_priority_map[provider_id]:
                    potential_logo = integration_logos_dir / priority_logo
                    if potential_logo.exists():
                        logo_dest = logos_dest_dir / f"{provider_id}-{potential_logo.name}"
                        shutil.copy2(potential_logo, logo_dest)
                        logo = f"/logos/{provider_id}-{potential_logo.name}"
                        break

            # If no priority logo found, try general patterns
            if not logo:
                for pattern in main_logo_patterns:
                    potential_logo = integration_logos_dir / pattern
                    if potential_logo.exists():
                        logo_dest = logos_dest_dir / f"{provider_id}-{potential_logo.name}"
                        shutil.copy2(potential_logo, logo_dest)
                        logo = f"/logos/{provider_id}-{potential_logo.name}"
                        break

            # Still no logo? Use the one from provider.yaml integrations
            if not logo:
                for integration in provider_yaml.get("integrations", []):
                    if integration.get("logo"):
                        logo_path = integration["logo"]
                        logo_filename = logo_path.split("/")[-1]
                        logo_source = integration_logos_dir / logo_filename
                        if logo_source.exists():
                            logo_dest = logos_dest_dir / f"{provider_id}-{logo_filename}"
                            shutil.copy2(logo_source, logo_dest)
                            logo = f"/logos/{provider_id}-{logo_filename}"
                            break

            # Last resort: use the first available logo
            if not logo:
                logos = list(integration_logos_dir.glob("*.png")) + list(integration_logos_dir.glob("*.svg"))
                if logos:
                    logo_source = logos[0]
                    logo_dest = logos_dest_dir / f"{provider_id}-{logo_source.name}"
                    shutil.copy2(logo_source, logo_dest)
                    logo = f"/logos/{provider_id}-{logo_source.name}"

        # Extract connection types from provider.yaml
        # Link to the connections index page since individual connection pages might not exist
        connection_types = []
        connections_index_url = (
            f"https://airflow.apache.org/docs/{package_name}/stable/connections/index.html"
        )
        for conn in provider_yaml.get("connection-types", []):
            conn_type = conn.get("connection-type", "")
            hook_class = conn.get("hook-class-name", "")
            if conn_type:
                connection_types.append(
                    {
                        "conn_type": conn_type,
                        "hook_class": hook_class,
                        "docs_url": connections_index_url,
                    }
                )

        # Fetch PyPI download statistics and release dates
        pypi_downloads = fetch_pypi_downloads(package_name)
        pypi_dates = fetch_pypi_dates(package_name)

        # Parse pyproject.toml for requires-python and dependencies
        pyproject_path = provider_path / "pyproject.toml"
        pyproject_data = parse_pyproject_toml(pyproject_path)

        # Airflow version compatibility (from pyproject.toml dependencies)
        airflow_versions = determine_airflow_versions(pyproject_data["dependencies"])

        provider = Provider(
            id=provider_id,
            name=name,
            package_name=package_name,
            description=description,
            lifecycle=provider_yaml.get("lifecycle", "production"),
            logo=logo,
            version=version,
            versions=versions,
            airflow_versions=airflow_versions,
            pypi_downloads=pypi_downloads,
            module_counts=module_counts,
            categories=[asdict(c) for c in categories],
            connection_types=connection_types,
            requires_python=pyproject_data["requires_python"],
            dependencies=pyproject_data["dependencies"],
            optional_extras=pyproject_data.get("optional_extras", {}),
            docs_url=f"https://airflow.apache.org/docs/{package_name}/stable/",
            source_url=f"https://github.com/apache/airflow/tree/providers-{provider_id}/{version}/providers/{provider_path.relative_to(PROVIDERS_DIR)}",
            pypi_url=f"https://pypi.org/project/{package_name}/",
            first_released=pypi_dates["first_released"],
            last_updated=pypi_dates["last_updated"],
        )

        all_providers.append(provider)

        # Extract modules (now extracts actual class names from Python files)
        modules = extract_modules_from_yaml(provider_yaml, provider_id, name, provider_path, version)
        all_modules.extend(modules)

        print(f"  {provider_id}: {len(modules)} classes, {len(categories)} categories")

    # Deduplicate modules by ID (same class may appear in multiple integrations)
    seen_module_ids: set[str] = set()
    unique_modules: list[Module] = []
    for m in all_modules:
        if m.id not in seen_module_ids:
            seen_module_ids.add(m.id)
            unique_modules.append(m)
    all_modules = unique_modules
    print(f"\nDeduplicated to {len(all_modules)} unique modules")

    # Third pass: Recalculate module counts based on actual extracted classes
    provider_module_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "operator": 0,
            "hook": 0,
            "sensor": 0,
            "trigger": 0,
            "transfer": 0,
            "notifier": 0,
            "secret": 0,
            "logging": 0,
            "executor": 0,
            "bundle": 0,
            "decorator": 0,
        }
    )
    for m in all_modules:
        provider_module_counts[m.provider_id][m.type] += 1

    for provider in all_providers:
        provider.module_counts = dict(provider_module_counts[provider.id])

    # Fourth pass: Find related providers
    for provider in all_providers:
        provider.related_providers = find_related_providers(provider.id, all_provider_yamls)

    # Sort providers alphabetically by name
    all_providers.sort(key=lambda p: p.name.lower())

    # Sort modules by provider's last_updated date (newest first) so the
    # homepage "Recently Updated" section shows genuinely recent modules.
    date_by_provider = {p.id: p.last_updated for p in all_providers}
    all_modules.sort(key=lambda m: date_by_provider.get(m.provider_id, ""), reverse=True)

    # Convert to JSON-serializable format
    providers_json = {"providers": [asdict(p) for p in all_providers]}
    modules_json = {"modules": [asdict(m) for m in all_modules]}

    # Write output files to all output directories.
    # Inside breeze, registry/ is not mounted so OUTPUT_DIR writes are lost.
    # SCRIPT_DIR (dev/registry/) is always mounted, so the other extraction
    # scripts can pick up providers.json / modules.json from there.
    output_dirs = [OUTPUT_DIR, SCRIPT_DIR]

    for out_dir in output_dirs:
        if not out_dir.parent.exists():
            continue
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(out_dir / "providers.json", "w") as f:
            json.dump(providers_json, f, indent=2)
        with open(out_dir / "modules.json", "w") as f:
            json.dump(modules_json, f, indent=2)
        print(f"\nWrote {len(all_providers)} providers and {len(all_modules)} modules to {out_dir}")

    print("\nDone!")


if __name__ == "__main__":
    main()
