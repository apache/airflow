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
Airflow Registry Parameter & Module Extractor

Discovers provider modules (operators, hooks, sensors, triggers, etc.) at runtime
and extracts constructor/function parameters via MRO or signature inspection.
Produces both modules.json (the full module catalog) and per-provider
parameters.json files.

Must be run inside breeze where all providers are installed.

Usage:
    breeze run python dev/registry/extract_parameters.py

Output:
    - dev/registry/modules.json (+ registry/src/_data/modules.json on host)
    - dev/registry/output/versions/{provider_id}/{version}/parameters.json
    - registry/src/_data/versions/{provider_id}/{version}/parameters.json
    - dev/registry/runtime_modules.json (debug stats)
"""

from __future__ import annotations

import argparse
import concurrent.futures
import importlib
import inspect
import json
import logging
import re
import sys
import typing
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml
from extract_metadata import fetch_provider_inventory, read_inventory
from registry_tools.types import BASE_CLASS_IMPORTS, CLASS_LEVEL_SECTIONS, MODULE_LEVEL_SECTIONS

AIRFLOW_ROOT = Path(__file__).parent.parent.parent
SCRIPT_DIR = Path(__file__).parent
PROVIDERS_DIR = AIRFLOW_ROOT / "providers"

PROVIDERS_JSON_CANDIDATES = [
    SCRIPT_DIR / "providers.json",
    AIRFLOW_ROOT / "registry" / "src" / "_data" / "providers.json",
]

# Inside breeze, write to dev/registry/output/ (mounted).
# On host, also write to the registry data directory.
OUTPUT_DIRS = [
    SCRIPT_DIR / "output",
    AIRFLOW_ROOT / "registry" / "src" / "_data",
]


@dataclass
class Module:
    """A discovered provider module (operator, hook, sensor, etc.)."""

    id: str
    name: str  # Class name (e.g., SnowflakeOperator)
    type: str  # operator, hook, sensor, trigger, transfer, etc.
    import_path: str  # Full import path to the class
    module_path: str  # Module file path
    short_description: str
    docs_url: str
    source_url: str
    category: str
    provider_id: str
    provider_name: str


def get_category(integration_name: str) -> str:
    """Slugify an integration name into a category ID."""
    cat_id = integration_name.lower().replace(" ", "-").replace("(", "").replace(")", "")
    return re.sub(r"[^a-z0-9-]", "", cat_id)


def format_annotation(annotation: type, _depth: int = 0) -> str | None:
    """Convert a type annotation to a human-readable string."""
    if _depth > 5:
        return str(annotation)

    if annotation is inspect.Parameter.empty:
        return None

    if annotation is type(None):
        return "None"

    origin = getattr(annotation, "__origin__", None)

    # typing.Union (includes X | Y on 3.10+)
    if origin is typing.Union:
        args = typing.get_args(annotation)
        parts = [format_annotation(a, _depth + 1) for a in args]
        return " | ".join(p for p in parts if p)

    if origin is not None:
        args = typing.get_args(annotation)
        origin_name = getattr(origin, "__name__", str(origin))
        if args:
            arg_strs = [format_annotation(a, _depth + 1) or "Any" for a in args]
            return f"{origin_name}[{', '.join(arg_strs)}]"
        return origin_name

    if hasattr(annotation, "__name__"):
        return annotation.__name__

    s = str(annotation)
    s = re.sub(r"\btyping\.", "", s)
    s = re.sub(r"\bcollections\.abc\.", "", s)
    return s


def format_default(default: object) -> object:
    """Convert a default value to a JSON-serializable representation."""
    if default is inspect.Parameter.empty:
        return None

    if default is None:
        return None

    if isinstance(default, (str, int, float, bool)):
        return default

    if isinstance(default, (list, tuple, dict)):
        try:
            json.dumps(default)
            return default
        except (TypeError, ValueError):
            pass

    try:
        return str(default)
    except Exception:
        return repr(default)


def get_params_from_signature(sig: inspect.Signature, qualified_origin: str) -> dict[str, dict]:
    """Convert an inspect.Signature into parameter metadata."""
    params: dict[str, dict] = {}

    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        if name.startswith("_"):
            continue
        if param.kind in (param.VAR_KEYWORD, param.VAR_POSITIONAL):
            continue

        params[name] = {
            "name": name,
            "type": format_annotation(param.annotation),
            "default": format_default(param.default),
            "required": param.default is inspect.Parameter.empty,
            "origin": qualified_origin,
        }

    return params


def get_params_from_class(cls: type) -> dict[str, dict]:
    """
    Extract all __init__ parameters by walking the MRO in reverse
    so child class overrides parent for the same parameter name.
    Records the full qualified origin (module.ClassName) for each param.
    """
    params: dict[str, dict] = {}

    for klass in reversed(cls.__mro__):
        if klass is object:
            continue

        init = klass.__dict__.get("__init__")
        if init is None:
            continue

        try:
            sig = inspect.signature(init)
        except (TypeError, ValueError):
            continue

        qualified_origin = f"{klass.__module__}.{klass.__qualname__}"
        params.update(get_params_from_signature(sig, qualified_origin))

    return params


def get_mro_chain(cls: type) -> list[str]:
    """Return the full MRO as a list of qualified class names."""
    return [f"{k.__module__}.{k.__qualname__}" for k in cls.__mro__ if k is not object]


def parse_param_descriptions(doc: str) -> dict[str, str]:
    """Parse ``:param name: description`` entries from a docstring."""
    descriptions: dict[str, str] = {}
    if not doc:
        return descriptions

    for match in re.finditer(
        r":param\s+(\w+):\s*(.+?)(?=\n\s*:\w|\n\s*\.\.|$)",
        doc,
        re.DOTALL,
    ):
        name = match.group(1)
        desc = match.group(2).strip()
        desc = re.sub(r"\s+", " ", desc)
        if name not in descriptions:
            descriptions[name] = desc

    return descriptions


def parse_docstring_params(cls: type) -> dict[str, str]:
    """
    Parse :param name: description from class and ancestor docstrings.
    Child class descriptions take priority.
    """
    descriptions: dict[str, str] = {}

    for klass in cls.__mro__:
        if klass is object:
            continue
        for name, desc in parse_param_descriptions(getattr(klass, "__doc__", None) or "").items():
            if name not in descriptions:
                descriptions[name] = desc

    return descriptions


def extract_class_params(cls: type) -> tuple[list[str], list[dict]]:
    """
    Extract parameter list for a class, merging signature + docstrings.
    Only includes params originating from provider classes (airflow.providers.*).
    Returns (mro_chain, filtered_params).
    """
    params = get_params_from_class(cls)
    descriptions = parse_docstring_params(cls)

    for name, param in params.items():
        if name in descriptions:
            param["description"] = descriptions[name]
        else:
            param["description"] = None

    provider_params = [p for p in params.values() if p["origin"].startswith("airflow.providers.")]
    mro = get_mro_chain(cls)

    return mro, provider_params


def extract_callable_params(func: typing.Callable[..., typing.Any]) -> tuple[list[str], list[dict]]:
    """Extract parameter metadata from a callable signature and docstring."""
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError) as e:
        raise TypeError(f"Could not inspect callable signature: {e}") from e

    qualified_origin = f"{func.__module__}.{func.__qualname__}"
    params = get_params_from_signature(sig, qualified_origin)
    descriptions = parse_param_descriptions(getattr(func, "__doc__", None) or "")

    for name, param in params.items():
        param["description"] = descriptions.get(name)

    provider_params = [p for p in params.values() if p["origin"].startswith("airflow.providers.")]
    return [], provider_params


def extract_params(obj: object) -> tuple[list[str], list[dict]]:
    """Extract parameter metadata from either a class or a callable."""
    if inspect.isclass(obj):
        return extract_class_params(obj)
    if callable(obj):
        return extract_callable_params(typing.cast("typing.Callable[..., typing.Any]", obj))
    raise TypeError(f"Unsupported import type: {type(obj)!r}")


def import_symbol(import_path: str) -> object | None:
    """Import a symbol from its full dotted path."""
    parts = import_path.rsplit(".", 1)
    if len(parts) != 2:
        return None

    module_path, symbol_name = parts
    try:
        module = importlib.import_module(module_path)
        return getattr(module, symbol_name, None)
    except Exception as e:
        print(f"  WARN failed to import {import_path}: {e}")
        return None


def find_json(candidates: list[Path], name: str) -> Path:
    """Find first existing JSON file from candidates list."""
    for candidate in candidates:
        if candidate.exists():
            return candidate
    print(f"ERROR: {name} not found. Searched:")
    for c in candidates:
        print(f"  - {c}")
    print(f"\nCopy {name} to dev/registry/ or run extract_metadata.py first.")
    sys.exit(1)


log = logging.getLogger(__name__)


def load_base_classes() -> dict[str, type]:
    """Import base classes for issubclass checks.

    Returns a mapping of type name -> base class (e.g. "sensor" -> BaseSensorOperator).
    """
    base_classes: dict[str, type] = {}
    for type_name, import_path in BASE_CLASS_IMPORTS:
        module_path, class_name = import_path.rsplit(".", 1)
        try:
            mod = importlib.import_module(module_path)
            base_classes[type_name] = getattr(mod, class_name)
        except Exception:
            log.warning("Could not import base class %s", import_path)
    return base_classes


def _should_skip_class(name: str) -> bool:
    """Return True if a class name should be excluded from discovery."""
    if name.startswith("_"):
        return True
    if name.startswith("Base"):
        return True
    if "Abstract" in name or "Mixin" in name:
        return True
    return False


def _get_first_docstring_line(cls: type) -> str | None:
    """Return the first non-empty line of a class docstring, or None."""
    doc = getattr(cls, "__doc__", None)
    if not doc:
        return None
    for line in doc.strip().splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def _get_source_line(cls: type) -> int | None:
    """Return the source line number for a class, or None if unavailable."""
    try:
        return inspect.getsourcelines(cls)[1]
    except (OSError, TypeError):
        return None


def discover_classes_from_provider(
    provider_yaml_path: Path,
    base_classes: dict[str, type],
    inventory: dict[str, str] | None = None,
    version: str = "",
) -> list[dict]:
    """Discover classes from a single provider by importing its modules at runtime.

    Reads the provider.yaml to find which modules/classes to inspect, imports them,
    and returns metadata for each discovered class with all 11 Module fields.
    """
    with open(provider_yaml_path) as f:
        provider_yaml = yaml.safe_load(f)

    provider_id = provider_yaml.get("package-name", "").replace("apache-airflow-providers-", "")
    if not provider_id:
        return []

    provider_name = provider_yaml.get("name", provider_id.replace("-", " ").title())
    provider_rel_path = provider_yaml_path.parent.relative_to(PROVIDERS_DIR)
    tag = f"providers-{provider_id}/{version}" if version else "main"
    base_docs_url = f"https://airflow.apache.org/docs/apache-airflow-providers-{provider_id}/stable"
    base_source_url = f"https://github.com/apache/airflow/blob/{tag}/providers/{provider_rel_path}/src"

    # Build integration-name lookup for module-level sections
    # Maps (section_name, module_path) -> integration_name
    integration_by_module: dict[tuple[str, str], str] = {}
    for section_name in list(MODULE_LEVEL_SECTIONS) + ["bundles"]:
        for group in provider_yaml.get(section_name, []):
            integration = group.get("integration-name", "")
            for mp in group.get("python-modules", []):
                integration_by_module[(section_name, mp)] = integration

    def resolve_docs_url(full_class_path: str, module_path: str) -> str:
        """Look up docs URL from inventory, falling back to manual construction."""
        if inventory and full_class_path in inventory:
            return f"{base_docs_url}/{inventory[full_class_path]}"
        api_ref_path = module_path.replace(".", "/")
        return f"{base_docs_url}/_api/{api_ref_path}/index.html#{full_class_path}"

    def make_source_url(cls: type, module_path: str) -> str:
        """Construct a GitHub source URL with line number when available."""
        url = f"{base_source_url}/{module_path.replace('.', '/')}.py"
        line = _get_source_line(cls)
        if line:
            url += f"#L{line}"
        return url

    def make_entry(
        cls_or_obj: type,
        name: str,
        module_type: str,
        import_path: str,
        module_path: str,
        integration: str = "",
        category: str = "",
        transfer_desc: str | None = None,
    ) -> dict:
        """Build a full module entry dict with all 11 fields."""
        module_name = module_path.split(".")[-1]
        docstring = _get_first_docstring_line(cls_or_obj)
        short_desc = docstring or transfer_desc or f"{integration} {module_type}".strip()

        return {
            "id": f"{provider_id}-{module_name}-{name}",
            "name": name,
            "type": module_type,
            "import_path": import_path,
            "module_path": module_path,
            "short_description": short_desc,
            "docs_url": resolve_docs_url(import_path, module_path),
            "source_url": make_source_url(cls_or_obj, module_path),
            "category": category or get_category(integration),
            "provider_id": provider_id,
            "provider_name": provider_name,
        }

    discovered: list[dict] = []

    # --- Module-level sections (operators, hooks, sensors, triggers, bundles) ---
    for section_name, module_type in MODULE_LEVEL_SECTIONS.items():
        expected_base = base_classes.get(module_type)
        for group in provider_yaml.get(section_name, []):
            integration = group.get("integration-name", "")
            category = get_category(integration)
            for module_path in group.get("python-modules", []):
                try:
                    mod = importlib.import_module(module_path)
                except Exception:
                    log.warning("Could not import module %s", module_path)
                    continue

                for name, cls in inspect.getmembers(mod, inspect.isclass):
                    if cls.__module__ != mod.__name__:
                        continue
                    if _should_skip_class(name):
                        continue
                    if expected_base and not issubclass(cls, expected_base):
                        continue

                    discovered.append(
                        make_entry(
                            cls,
                            name,
                            module_type,
                            f"{module_path}.{name}",
                            module_path,
                            integration,
                            category,
                        )
                    )

    # --- Transfers (module-level, singular python-module key) ---
    transfer_base = base_classes.get("operator")
    for transfer in provider_yaml.get("transfers", []):
        module_path = transfer.get("python-module", "")
        if not module_path:
            continue
        source = transfer.get("source-integration-name", "")
        target = transfer.get("target-integration-name", "")
        transfer_desc = f"Transfer from {source} to {target}" if source and target else None
        category = get_category(source) if source else ""

        try:
            mod = importlib.import_module(module_path)
        except Exception:
            log.warning("Could not import module %s", module_path)
            continue

        for name, cls in inspect.getmembers(mod, inspect.isclass):
            if cls.__module__ != mod.__name__:
                continue
            if _should_skip_class(name):
                continue
            if transfer_base and not issubclass(cls, transfer_base):
                continue

            discovered.append(
                make_entry(
                    cls,
                    name,
                    "transfer",
                    f"{module_path}.{name}",
                    module_path,
                    source,
                    category,
                    transfer_desc,
                )
            )

    # --- Class-level sections (notifications, secrets-backends, logging, executors) ---
    for section_name, module_type in CLASS_LEVEL_SECTIONS.items():
        for class_path in provider_yaml.get(section_name, []):
            if not class_path or not isinstance(class_path, str):
                continue
            parts = class_path.rsplit(".", 1)
            if len(parts) != 2:
                continue
            module_path, class_name = parts
            try:
                mod = importlib.import_module(module_path)
                candidate = getattr(mod, class_name, None)
            except Exception:
                log.warning("Could not import %s", class_path)
                continue
            if candidate is None or not inspect.isclass(candidate):
                log.warning("%s is not a class", class_path)
                continue
            cls = typing.cast("type[typing.Any]", candidate)

            # Use section name as category for class-level entries
            category_map = {
                "notifications": "notifications",
                "secrets-backends": "secrets",
                "logging": "logging",
                "executors": "executors",
            }

            discovered.append(
                make_entry(
                    cls,
                    class_name,
                    module_type,
                    class_path,
                    module_path,
                    category=category_map.get(section_name, section_name),
                )
            )

    # --- Task decorators (class-name key in each entry) ---
    for decorator in provider_yaml.get("task-decorators", []):
        class_path = decorator.get("class-name", "")
        decorator_name = decorator.get("name", "")
        if not class_path:
            continue
        parts = class_path.rsplit(".", 1)
        if len(parts) != 2:
            continue
        module_path, func_name = parts
        try:
            mod = importlib.import_module(module_path)
            obj = getattr(mod, func_name, None)
        except Exception:
            log.warning("Could not import %s", class_path)
            continue
        if obj is None:
            continue

        display_name = f"@task.{decorator_name}" if decorator_name else func_name
        docstring = _get_first_docstring_line(obj) if hasattr(obj, "__doc__") else None
        short_desc = docstring or f"Task decorator for {decorator_name or func_name}"

        discovered.append(
            {
                "id": f"{provider_id}-decorator-{decorator_name or func_name}",
                "name": display_name,
                "type": "decorator",
                "import_path": class_path,
                "module_path": module_path,
                "short_description": short_desc,
                "docs_url": resolve_docs_url(class_path, module_path),
                "source_url": f"{base_source_url}/{module_path.replace('.', '/')}.py",
                "category": "decorators",
                "provider_id": provider_id,
                "provider_name": provider_name,
            }
        )

    return discovered


def compare_with_ast(
    runtime_classes: list[dict],
    modules_json_path: Path,
) -> dict:
    """Compare runtime-discovered classes against AST-produced modules.json.

    Returns a stats dict with counts of phantoms, misses, and type mismatches.
    """
    with open(modules_json_path) as f:
        ast_data = json.load(f)

    ast_modules = ast_data.get("modules", [])

    ast_by_path: dict[str, dict] = {}
    for m in ast_modules:
        path = m.get("import_path", "")
        if path:
            ast_by_path[path] = m

    runtime_by_path: dict[str, dict] = {}
    for r in runtime_classes:
        path = r.get("import_path", "")
        if path:
            runtime_by_path[path] = r

    ast_paths = set(ast_by_path)
    runtime_paths = set(runtime_by_path)

    phantoms = sorted(ast_paths - runtime_paths)
    misses = sorted(runtime_paths - ast_paths)
    common = ast_paths & runtime_paths

    type_mismatches = []
    for path in sorted(common):
        ast_type = ast_by_path[path].get("type", "")
        runtime_type = runtime_by_path[path].get("type", "")
        if ast_type != runtime_type:
            type_mismatches.append(
                {
                    "import_path": path,
                    "ast_type": ast_type,
                    "runtime_type": runtime_type,
                }
            )

    # Print comparison table
    print("\n" + "=" * 60)
    print("Runtime vs AST Comparison")
    print("=" * 60)
    print(f"  AST classes:      {len(ast_paths)}")
    print(f"  Runtime classes:  {len(runtime_paths)}")
    print(f"  In common:        {len(common)}")
    print(f"  AST phantoms:     {len(phantoms)} (in AST, not runtime)")
    print(f"  AST misses:       {len(misses)} (in runtime, not AST)")
    print(f"  Type mismatches:  {len(type_mismatches)}")

    if phantoms:
        print(f"\nAST Phantoms ({len(phantoms)}):")
        for p in phantoms[:20]:
            ast_type = ast_by_path[p].get("type", "?")
            print(f"  [{ast_type}] {p}")
        if len(phantoms) > 20:
            print(f"  ... and {len(phantoms) - 20} more")

    if misses:
        print(f"\nAST Misses ({len(misses)}):")
        for p in misses[:20]:
            rt_type = runtime_by_path[p].get("type", "?")
            print(f"  [{rt_type}] {p}")
        if len(misses) > 20:
            print(f"  ... and {len(misses) - 20} more")

    if type_mismatches:
        print(f"\nType Mismatches ({len(type_mismatches)}):")
        for m in type_mismatches[:20]:
            print(f"  {m['import_path']}: AST={m['ast_type']} Runtime={m['runtime_type']}")
        if len(type_mismatches) > 20:
            print(f"  ... and {len(type_mismatches) - 20} more")

    print("=" * 60)

    return {
        "ast_phantoms": len(phantoms),
        "ast_misses": len(misses),
        "type_mismatches": len(type_mismatches),
        "phantom_paths": phantoms,
        "miss_paths": misses,
        "mismatch_details": type_mismatches,
    }


def _extract_params_from_modules(
    modules: list[dict],
) -> tuple[dict[str, dict[str, dict]], dict[str, str], int, int, int]:
    """Extract parameters from a list of module dicts.

    Returns (provider_classes, provider_names, total_processed, total_failed, total_params).
    """
    provider_classes: dict[str, dict[str, dict]] = defaultdict(dict)
    provider_names: dict[str, str] = {}
    total_processed = 0
    total_failed = 0
    total_params = 0

    for i, module in enumerate(modules, 1):
        import_path = module.get("import_path", "")
        provider_id = module.get("provider_id", "")
        provider_name = module.get("provider_name", module.get("provider_id", ""))
        module_type = module.get("type", "")
        class_name = module.get("name", "")

        if not import_path or not provider_id:
            continue

        provider_names[provider_id] = provider_name

        obj = import_symbol(import_path)
        if obj is None:
            total_failed += 1
            continue

        try:
            mro, params = extract_params(obj)
        except Exception as e:
            print(f"  ERROR extracting params for {import_path}: {e}")
            total_failed += 1
            continue

        provider_classes[provider_id][import_path] = {
            "name": class_name,
            "type": module_type,
            "mro": mro,
            "parameters": params,
        }

        total_processed += 1
        total_params += len(params)

        if i % 100 == 0:
            print(f"  Processed {i}/{len(modules)} modules...")

    return provider_classes, provider_names, total_processed, total_failed, total_params


def _write_parameter_files(
    provider_classes: dict[str, dict[str, dict]],
    provider_names: dict[str, str],
    provider_versions: dict[str, str],
    generated_at: str,
) -> None:
    """Write per-provider parameter JSON files."""
    for output_dir in OUTPUT_DIRS:
        if not output_dir.parent.exists():
            continue

        written = 0
        for pid, classes in provider_classes.items():
            version = provider_versions.get(pid)
            if not version:
                print(f"  WARN: no version found for {pid}, skipping")
                continue

            version_dir = output_dir / "versions" / pid / version
            version_dir.mkdir(parents=True, exist_ok=True)

            provider_data = {
                "provider_id": pid,
                "provider_name": provider_names.get(pid, pid),
                "version": version,
                "generated_at": generated_at,
                "classes": classes,
            }
            with open(version_dir / "parameters.json", "w") as f:
                json.dump(provider_data, f, separators=(",", ":"))
            written += 1

        print(f"Wrote {written} provider parameter files to {output_dir}/versions/")


def _fetch_inventories(
    provider_ids: set[str],
    provider_yamls: dict[str, dict],
) -> dict[str, dict[str, str]]:
    """Fetch Sphinx inventory files in parallel for all providers."""
    package_names: dict[str, str] = {}
    for pid in provider_ids:
        py = provider_yamls.get(pid, {})
        package_names[pid] = py.get("package-name", f"apache-airflow-providers-{pid}")

    def _fetch_and_parse(pid: str) -> tuple[str, dict[str, str] | None]:
        inv_path = fetch_provider_inventory(package_names[pid])
        if inv_path:
            try:
                return pid, read_inventory(inv_path)
            except Exception as e:
                print(f"    Warning: Could not parse inventory for {pid}: {e}")
                return pid, None
        return pid, None

    inventories: dict[str, dict[str, str]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_and_parse, pid): pid for pid in provider_ids}
        for future in concurrent.futures.as_completed(futures):
            pid, inv = future.result()
            if inv:
                inventories[pid] = inv

    return inventories


def main():
    parser = argparse.ArgumentParser(description="Extract provider parameters and modules")
    parser.add_argument(
        "--provider",
        default=None,
        help="Only process this provider ID (e.g. 'amazon'). Skips modules.json write.",
    )
    parser.add_argument(
        "--providers-json",
        default=None,
        help="Path to providers.json (overrides default search paths).",
    )
    args = parser.parse_args()

    print("Airflow Registry Parameter & Module Extractor")
    print("=" * 50)

    if args.providers_json:
        providers_json_path = Path(args.providers_json)
    else:
        providers_json_path = find_json(PROVIDERS_JSON_CANDIDATES, "providers.json")
    with open(providers_json_path) as f:
        providers_data = json.load(f)

    provider_versions: dict[str, str] = {}
    for p in providers_data.get("providers", []):
        provider_versions[p["id"]] = p["version"]

    generated_at = datetime.now(timezone.utc).isoformat()
    _main_discover(provider_versions, generated_at, only_provider=args.provider)

    print("\nDone!")


def _main_discover(
    provider_versions: dict[str, str],
    generated_at: str,
    only_provider: str | None = None,
) -> None:
    """Runtime discovery: find classes from provider.yaml files, produce modules.json and parameters.

    When only_provider is set, only that provider is scanned and modules.json is NOT written
    (it would be incomplete). This enables parallel backfills since the only output is
    the per-provider parameters.json file.
    """
    provider_yaml_paths = sorted(PROVIDERS_DIR.rglob("provider.yaml"))
    print(f"Found {len(provider_yaml_paths)} provider.yaml files")

    base_classes = load_base_classes()
    print(f"Loaded {len(base_classes)} base classes: {', '.join(sorted(base_classes))}")

    # Load all provider.yaml data and map provider_id -> yaml dict / path
    provider_yamls_by_id: dict[str, dict] = {}
    provider_paths_by_id: dict[str, Path] = {}
    for yaml_path in provider_yaml_paths:
        with open(yaml_path) as f:
            py = yaml.safe_load(f)
        pid = py.get("package-name", "").replace("apache-airflow-providers-", "")
        if pid:
            provider_yamls_by_id[pid] = py
            provider_paths_by_id[pid] = yaml_path

    # Filter to single provider if requested
    if only_provider:
        if only_provider not in provider_paths_by_id:
            print(f"ERROR: provider '{only_provider}' not found in provider.yaml files")
            sys.exit(1)
        provider_paths_by_id = {only_provider: provider_paths_by_id[only_provider]}
        provider_yamls_by_id = {only_provider: provider_yamls_by_id[only_provider]}
        print(f"Filtering to provider: {only_provider}")

    # Fetch Sphinx inventories in parallel
    print("Fetching Sphinx inventory files ...")
    inventories = _fetch_inventories(set(provider_yamls_by_id), provider_yamls_by_id)
    print(f"  {len(inventories)}/{len(provider_yamls_by_id)} inventories loaded")

    all_discovered: list[dict] = []
    providers_seen: set[str] = set()

    for pid, yaml_path in sorted(provider_paths_by_id.items()):
        version = provider_versions.get(pid, "")
        discovered = discover_classes_from_provider(
            yaml_path,
            base_classes,
            inventory=inventories.get(pid),
            version=version,
        )
        all_discovered.extend(discovered)
        for d in discovered:
            providers_seen.add(d["provider_id"])

    print(f"\nDiscovered {len(all_discovered)} classes from {len(providers_seen)} providers")

    # Deduplicate by ID
    seen_ids: set[str] = set()
    unique_modules: list[dict] = []
    for m in all_discovered:
        mid = m["id"]
        if mid not in seen_ids:
            seen_ids.add(mid)
            unique_modules.append(m)
    all_discovered = unique_modules
    print(f"Deduplicated to {len(all_discovered)} unique modules")

    # Write modules.json only when doing a full build (no --provider filter).
    # With --provider, the output would be incomplete and would clobber the
    # full modules.json from a previous build.
    if not only_provider:
        modules_json = {"modules": all_discovered}
        output_dirs = [SCRIPT_DIR, AIRFLOW_ROOT / "registry" / "src" / "_data"]
        for out_dir in output_dirs:
            if not out_dir.parent.exists():
                continue
            out_dir.mkdir(parents=True, exist_ok=True)
            with open(out_dir / "modules.json", "w") as f:
                json.dump(modules_json, f, indent=2)
            print(f"Wrote {len(all_discovered)} modules to {out_dir / 'modules.json'}")

        # Write runtime_modules.json (debug/stats file)
        runtime_output = {
            "generated_at": generated_at,
            "discovery_method": "runtime",
            "stats": {
                "total_classes": len(all_discovered),
                "total_providers": len(providers_seen),
            },
            "classes": all_discovered,
        }
        runtime_json_path = SCRIPT_DIR / "runtime_modules.json"
        with open(runtime_json_path, "w") as f:
            json.dump(runtime_output, f, indent=2)
        print(f"Wrote {runtime_json_path}")
    else:
        print("Skipping modules.json write (--provider mode)")

    # Extract parameters
    print("\nExtracting parameters from runtime-discovered classes...")
    provider_classes, provider_names, total_processed, total_failed, total_params = (
        _extract_params_from_modules(all_discovered)
    )

    print(f"\nProcessed {total_processed} classes, {total_failed} failed imports")
    print(f"Extracted {total_params} total parameters")
    print(f"Across {len(provider_classes)} providers")

    _write_parameter_files(provider_classes, provider_names, provider_versions, generated_at)


if __name__ == "__main__":
    sys.exit(main() or 0)
