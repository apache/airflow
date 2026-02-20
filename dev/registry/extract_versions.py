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
Extract version-specific provider data from git tags.

Reads provider.yaml, pyproject.toml, and source files at specific git tags
to produce per-version JSON files for the registry.

Runs on the host (no breeze needed). Skips the latest version since that
data already exists in providers.json + modules.json.

Usage:
    python dev/registry/extract_versions.py                          # 1 older version per provider
    python dev/registry/extract_versions.py --provider amazon --versions 3
    python dev/registry/extract_versions.py --all-versions           # backfill everything
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML required. Install with: pip install pyyaml")
    sys.exit(1)


AIRFLOW_ROOT = Path(__file__).parent.parent.parent
PROVIDERS_DIR = AIRFLOW_ROOT / "providers"
REGISTRY_DIR = AIRFLOW_ROOT / "registry"
OUTPUT_DIR = REGISTRY_DIR / "src" / "_data" / "versions"


def build_provider_id_to_path_map() -> dict[str, str]:
    """Scan providers/ for provider.yaml to build provider_id -> directory_path mapping."""
    mapping = {}
    for yaml_path in sorted(PROVIDERS_DIR.rglob("provider.yaml")):
        rel = yaml_path.relative_to(PROVIDERS_DIR)
        parts = rel.parts[:-1]  # Remove 'provider.yaml'
        if "src" in parts:
            continue
        provider_id = "-".join(parts)
        dir_path = "/".join(parts)
        mapping[provider_id] = dir_path
    return mapping


def git_show(tag: str, path: str) -> str | None:
    """Read a file at a specific git tag. Returns None if the path doesn't exist."""
    try:
        result = subprocess.run(
            ["git", "show", f"{tag}:{path}"],
            capture_output=True,
            text=True,
            cwd=AIRFLOW_ROOT,
            check=True,
        )
        return result.stdout
    except subprocess.CalledProcessError:
        return None


def git_tag_exists(tag: str) -> bool:
    """Check if a git tag exists locally."""
    result = subprocess.run(
        ["git", "rev-parse", "--verify", f"refs/tags/{tag}"],
        capture_output=True,
        cwd=AIRFLOW_ROOT,
        check=False,
    )
    return result.returncode == 0


def detect_layout(tag: str, dir_path: str) -> str | None:
    """
    Detect which repo layout a tag uses.

    Returns:
        "new"  - providers/{dir_path}/provider.yaml (per-provider src/)
        "old"  - providers/src/airflow/providers/{dir_path}/provider.yaml (flat src/)
        None   - neither found
    """
    new_path = f"providers/{dir_path}/provider.yaml"
    if git_show(tag, new_path) is not None:
        return "new"

    old_path = f"providers/src/airflow/providers/{dir_path}/provider.yaml"
    if git_show(tag, old_path) is not None:
        return "old"

    return None


def get_provider_yaml_path(layout: str, dir_path: str) -> str:
    if layout == "new":
        return f"providers/{dir_path}/provider.yaml"
    return f"providers/src/airflow/providers/{dir_path}/provider.yaml"


def get_pyproject_path(layout: str, dir_path: str) -> str:
    if layout == "new":
        return f"providers/{dir_path}/pyproject.toml"
    return "providers/pyproject.toml"


def get_source_file_path(layout: str, dir_path: str, module_path: str) -> str:
    """Convert a Python module path to a file path at the given layout."""
    parts = module_path.split(".")
    rel_file = "/".join(parts) + ".py"
    if layout == "new":
        return f"providers/{dir_path}/src/{rel_file}"
    return f"providers/src/{rel_file}"


def parse_pyproject_toml_content(content: str, layout: str) -> dict[str, Any]:
    """Parse pyproject.toml content for dependencies, requires-python, and extras."""
    result: dict[str, Any] = {"requires_python": "", "dependencies": [], "optional_extras": {}}

    if layout == "old":
        # Old layout has a single providers/pyproject.toml that isn't per-provider
        return result

    requires_match = re.search(r'requires-python\s*=\s*"([^"]+)"', content)
    if requires_match:
        result["requires_python"] = requires_match.group(1)

    deps_match = re.search(r"dependencies\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if deps_match:
        deps = re.findall(r"""['"]([^'"]+)['"]""", deps_match.group(1))
        result["dependencies"] = [d.strip() for d in deps if d.strip() and not d.strip().startswith("#")]

    optional_match = re.search(r"\[project\.optional-dependencies\](.*?)(?=\n\[|\Z)", content, re.DOTALL)
    if optional_match:
        for match in re.finditer(r'"([^"]+)"\s*=\s*\[(.*?)\]', optional_match.group(1), re.DOTALL):
            extra_name = match.group(1)
            extra_deps = re.findall(r"""['"]([^'"]+)['"]""", match.group(2))
            clean = [d.strip() for d in extra_deps if d.strip() and not d.strip().startswith("#")]
            if clean:
                result["optional_extras"][extra_name] = clean

    return result


def extract_classes_from_source(source: str) -> list[dict[str, Any]]:
    """AST-parse Python source to extract class names, docstrings, and line numbers."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    classes = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            if node.name.startswith("_"):
                continue
            docstring = ast.get_docstring(node) or ""
            first_line = docstring.split("\n")[0].strip() if docstring else ""
            classes.append({"name": node.name, "docstring": first_line, "line": node.lineno})
    return classes


def extract_modules_from_yaml(
    provider_yaml: dict[str, Any],
    tag: str,
    layout: str,
    dir_path: str,
    provider_id: str,
    version: str,
) -> list[dict[str, Any]]:
    """Extract module information from provider.yaml + source files at a git tag."""
    modules: list[dict[str, Any]] = []
    seen_import_paths: set[str] = set()
    base_docs_url = f"https://airflow.apache.org/docs/apache-airflow-providers-{provider_id}/{version}"

    if layout == "new":
        base_source_url = f"https://github.com/apache/airflow/blob/{tag}/providers/{dir_path}/src"
    else:
        base_source_url = f"https://github.com/apache/airflow/blob/{tag}/providers/src"

    type_patterns = {
        "operator": ["Operator", "Command"],
        "hook": ["Hook"],
        "sensor": ["Sensor"],
        "trigger": ["Trigger"],
        "transfer": ["Operator", "Transfer"],
        "bundle": ["Bundle"],
    }

    def get_category(integration_name: str) -> str:
        cat_id = integration_name.lower().replace(" ", "-").replace("(", "").replace(")", "")
        return re.sub(r"[^a-z0-9-]", "", cat_id)

    def process_module(module_path: str, module_type: str, integration: str, category: str) -> None:
        file_path = get_source_file_path(layout, dir_path, module_path)
        source = git_show(tag, file_path)
        module_name = module_path.split(".")[-1]
        patterns = type_patterns.get(module_type, [])

        if source:
            classes = extract_classes_from_source(source)
            relevant = []
            for cls in classes:
                name = cls["name"]
                is_relevant = (
                    any(name.endswith(p) for p in patterns) if patterns else not name.startswith("_")
                )
                is_base = name.startswith("Base") or "Abstract" in name or "Mixin" in name
                if is_relevant and not is_base:
                    relevant.append(cls)

            for cls in relevant:
                api_ref = module_path.replace(".", "/")
                full_path = f"{module_path}.{cls['name']}"
                if full_path in seen_import_paths:
                    continue
                seen_import_paths.add(full_path)
                modules.append(
                    {
                        "name": cls["name"],
                        "type": module_type,
                        "import_path": full_path,
                        "short_description": cls["docstring"] or f"{integration} {module_type}",
                        "docs_url": f"{base_docs_url}/_api/{api_ref}/index.html#{full_path}",
                        "source_url": f"{base_source_url}/{module_path.replace('.', '/')}.py#L{cls['line']}",
                        "category": category,
                    }
                )
            if relevant:
                return

        # Fallback: synthetic entry from module name
        class_name = "".join(word.capitalize() for word in module_name.split("_"))
        type_suffix = module_type.capitalize()
        if not class_name.endswith(type_suffix):
            class_name = f"{class_name}{type_suffix}"
        api_ref = module_path.replace(".", "/")
        full_path = f"{module_path}.{class_name}"
        if full_path in seen_import_paths:
            return
        seen_import_paths.add(full_path)
        modules.append(
            {
                "name": class_name,
                "type": module_type,
                "import_path": full_path,
                "short_description": f"{integration} {module_type}",
                "docs_url": f"{base_docs_url}/_api/{api_ref}/index.html#{full_path}",
                "source_url": f"{base_source_url}/{module_path.replace('.', '/')}.py",
                "category": category,
            }
        )

    # Operators
    for group in provider_yaml.get("operators", []):
        integration = group.get("integration-name", "")
        category = get_category(integration)
        for mp in group.get("python-modules", []):
            process_module(mp, "operator", integration, category)

    # Hooks
    for group in provider_yaml.get("hooks", []):
        integration = group.get("integration-name", "")
        category = get_category(integration)
        for mp in group.get("python-modules", []):
            process_module(mp, "hook", integration, category)

    # Sensors
    for group in provider_yaml.get("sensors", []):
        integration = group.get("integration-name", "")
        category = get_category(integration)
        for mp in group.get("python-modules", []):
            process_module(mp, "sensor", integration, category)

    # Triggers
    for group in provider_yaml.get("triggers", []):
        integration = group.get("integration-name", "")
        category = get_category(integration)
        for mp in group.get("python-modules", []):
            process_module(mp, "trigger", integration, category)

    # Transfers
    for transfer in provider_yaml.get("transfers", []):
        source = transfer.get("source-integration-name", "")
        mp = transfer.get("python-module", "")
        if mp:
            process_module(mp, "transfer", source, get_category(source))

    # Bundles
    for group in provider_yaml.get("bundles", []):
        integration = group.get("integration-name", "")
        category = get_category(integration)
        for mp in group.get("python-modules", []):
            process_module(mp, "bundle", integration, category)

    # Notifiers - stored as full class paths
    for notifier_path in provider_yaml.get("notifications", []):
        if notifier_path:
            parts = notifier_path.rsplit(".", 1)
            if len(parts) == 2:
                mod_path, class_name = parts
                modules.append(
                    {
                        "name": class_name,
                        "type": "notifier",
                        "import_path": notifier_path,
                        "short_description": f"{class_name.replace('Notifier', '')} notifier",
                        "docs_url": f"{base_docs_url}/_api/{mod_path.replace('.', '/')}/index.html#{notifier_path}",
                        "source_url": f"{base_source_url}/{mod_path.replace('.', '/')}.py",
                        "category": "notifications",
                    }
                )

    # Secrets backends
    for secret_path in provider_yaml.get("secrets-backends", []):
        if secret_path:
            parts = secret_path.rsplit(".", 1)
            if len(parts) == 2:
                mod_path, class_name = parts
                modules.append(
                    {
                        "name": class_name,
                        "type": "secret",
                        "import_path": secret_path,
                        "short_description": f"{class_name} secrets backend",
                        "docs_url": f"{base_docs_url}/_api/{mod_path.replace('.', '/')}/index.html#{secret_path}",
                        "source_url": f"{base_source_url}/{mod_path.replace('.', '/')}.py",
                        "category": "secrets",
                    }
                )

    # Logging
    for log_path in provider_yaml.get("logging", []):
        if log_path:
            parts = log_path.rsplit(".", 1)
            if len(parts) == 2:
                mod_path, class_name = parts
                modules.append(
                    {
                        "name": class_name,
                        "type": "logging",
                        "import_path": log_path,
                        "short_description": f"{class_name} log handler",
                        "docs_url": f"{base_docs_url}/_api/{mod_path.replace('.', '/')}/index.html#{log_path}",
                        "source_url": f"{base_source_url}/{mod_path.replace('.', '/')}.py",
                        "category": "logging",
                    }
                )

    # Executors
    for exec_path in provider_yaml.get("executors", []):
        if exec_path:
            parts = exec_path.rsplit(".", 1)
            if len(parts) == 2:
                mod_path, class_name = parts
                modules.append(
                    {
                        "name": class_name,
                        "type": "executor",
                        "import_path": exec_path,
                        "short_description": f"{class_name} executor",
                        "docs_url": f"{base_docs_url}/_api/{mod_path.replace('.', '/')}/index.html#{exec_path}",
                        "source_url": f"{base_source_url}/{mod_path.replace('.', '/')}.py",
                        "category": "executors",
                    }
                )

    return modules


def count_modules(modules: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for m in modules:
        t = m["type"]
        counts[t] = counts.get(t, 0) + 1
    return counts


def extract_version_data(
    provider_id: str,
    version: str,
    dir_path: str,
) -> dict[str, Any] | None:
    """Extract all data for a single provider version from its git tag."""
    tag = f"providers-{provider_id}/{version}"

    if not git_tag_exists(tag):
        print(f"  SKIP {tag}: tag not found locally")
        return None

    layout = detect_layout(tag, dir_path)
    if layout is None:
        print(f"  SKIP {tag}: could not detect layout")
        return None

    # Read provider.yaml
    yaml_path = get_provider_yaml_path(layout, dir_path)
    yaml_content = git_show(tag, yaml_path)
    if yaml_content is None:
        print(f"  SKIP {tag}: provider.yaml not found")
        return None

    try:
        provider_yaml = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        print(f"  SKIP {tag}: invalid provider.yaml: {e}")
        return None

    # Read pyproject.toml
    pyproject_path = get_pyproject_path(layout, dir_path)
    pyproject_content = git_show(tag, pyproject_path)
    pyproject_data: dict[str, Any] = {"requires_python": "", "dependencies": [], "optional_extras": {}}
    if pyproject_content:
        pyproject_data = parse_pyproject_toml_content(pyproject_content, layout)

    # For old layout, dependencies are in provider.yaml
    if layout == "old" and not pyproject_data["dependencies"]:
        pyproject_data["dependencies"] = provider_yaml.get("dependencies", [])

    # Connection types
    connection_types = []
    for ct in provider_yaml.get("connection-types", []):
        connection_types.append(
            {
                "conn_type": ct.get("connection-type", ""),
                "hook_class": ct.get("hook-class-name", ""),
            }
        )

    # Extract modules from source files
    modules = extract_modules_from_yaml(provider_yaml, tag, layout, dir_path, provider_id, version)
    module_counts = count_modules(modules)

    return {
        "provider_id": provider_id,
        "version": version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "requires_python": pyproject_data["requires_python"],
        "dependencies": pyproject_data["dependencies"],
        "optional_extras": pyproject_data["optional_extras"],
        "connection_types": connection_types,
        "module_counts": module_counts,
        "modules": modules,
    }


def main():
    parser = argparse.ArgumentParser(description="Extract version-specific provider data from git tags")
    parser.add_argument("--provider", help="Extract for a specific provider ID only")
    parser.add_argument(
        "--versions",
        type=int,
        default=1,
        help="Number of older versions to extract per provider (default: 1)",
    )
    parser.add_argument("--all-versions", action="store_true", help="Extract all versions")
    args = parser.parse_args()

    # Load providers.json
    providers_path = REGISTRY_DIR / "src" / "_data" / "providers.json"
    if not providers_path.exists():
        print(f"ERROR: {providers_path} not found. Run extract_metadata.py first.")
        sys.exit(1)

    with open(providers_path) as f:
        providers_data = json.load(f)

    id_to_path = build_provider_id_to_path_map()

    providers_list = providers_data["providers"]
    if args.provider:
        providers_list = [p for p in providers_list if p["id"] == args.provider]
        if not providers_list:
            print(f"ERROR: Provider '{args.provider}' not found")
            sys.exit(1)

    total_extracted = 0
    total_skipped = 0

    for provider in providers_list:
        pid = provider["id"]
        latest_version = provider["version"]
        all_versions = provider.get("versions", [latest_version])

        dir_path = id_to_path.get(pid)
        if dir_path is None:
            print(f"WARN: No directory mapping for provider '{pid}', skipping")
            total_skipped += 1
            continue

        # Determine which versions to extract (skip latest, it's in providers.json)
        non_latest = [v for v in all_versions if v != latest_version]
        if args.all_versions:
            versions_to_extract = non_latest
        else:
            versions_to_extract = non_latest[: args.versions]

        if not versions_to_extract:
            continue

        print(f"\n{pid}: extracting {len(versions_to_extract)} version(s)")

        for version in versions_to_extract:
            print(f"  {version}...", end=" ", flush=True)
            data = extract_version_data(pid, version, dir_path)
            if data is None:
                total_skipped += 1
                continue

            version_dir = OUTPUT_DIR / pid / version
            version_dir.mkdir(parents=True, exist_ok=True)
            with open(version_dir / "metadata.json", "w") as f:
                json.dump(data, f, separators=(",", ":"))

            n_modules = len(data["modules"])
            print(f"OK ({n_modules} modules, {len(data['dependencies'])} deps)")
            total_extracted += 1

    print(f"\nDone: {total_extracted} versions extracted, {total_skipped} skipped")


if __name__ == "__main__":
    main()
