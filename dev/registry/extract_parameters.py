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
Airflow Registry Parameter Extractor

Extracts constructor parameters for provider modules using runtime inspection.
Only includes parameters defined in provider classes (airflow.providers.*),
not inherited SDK/core params like those from BaseOperator. The full MRO chain
is stored for reference. Must be run inside breeze where all providers are installed.

Usage:
    breeze run python dev/registry/extract_parameters.py

Output:
    - registry/src/_data/versions/{provider_id}/{version}/parameters.json
    - dev/registry/output/versions/{provider_id}/{version}/parameters.json
"""

from __future__ import annotations

import importlib
import inspect
import json
import re
import sys
import typing
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

AIRFLOW_ROOT = Path(__file__).parent.parent.parent
SCRIPT_DIR = Path(__file__).parent

# When running inside breeze, registry/ is not mounted.
# The script reads modules.json from dev/registry/ (which IS mounted)
# and writes output there too. A post-step copies to registry/_data/.
MODULES_JSON_CANDIDATES = [
    SCRIPT_DIR / "modules.json",
    AIRFLOW_ROOT / "registry" / "src" / "_data" / "modules.json",
]

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


def get_mro_chain(cls: type) -> list[str]:
    """Return the full MRO as a list of qualified class names."""
    return [f"{k.__module__}.{k.__qualname__}" for k in cls.__mro__ if k is not object]


def parse_docstring_params(cls: type) -> dict[str, str]:
    """
    Parse :param name: description from class and ancestor docstrings.
    Child class descriptions take priority.
    """
    descriptions: dict[str, str] = {}

    for klass in cls.__mro__:
        if klass is object:
            continue
        doc = getattr(klass, "__doc__", None) or ""
        if not doc:
            continue

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


def import_class(import_path: str) -> type | None:
    """Import a class from its full dotted path."""
    parts = import_path.rsplit(".", 1)
    if len(parts) != 2:
        return None

    module_path, class_name = parts
    try:
        module = importlib.import_module(module_path)
        return getattr(module, class_name, None)
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


def main():
    print("Airflow Registry Parameter Extractor")
    print("=" * 50)

    modules_json_path = find_json(MODULES_JSON_CANDIDATES, "modules.json")
    providers_json_path = find_json(PROVIDERS_JSON_CANDIDATES, "providers.json")

    print(f"Reading modules from {modules_json_path}")
    with open(modules_json_path) as f:
        modules_data = json.load(f)

    with open(providers_json_path) as f:
        providers_data = json.load(f)

    # Build provider_id -> latest version mapping
    provider_versions: dict[str, str] = {}
    for p in providers_data.get("providers", []):
        provider_versions[p["id"]] = p["version"]

    modules = modules_data.get("modules", [])
    print(f"Found {len(modules)} modules to process")

    generated_at = datetime.now(timezone.utc).isoformat()

    provider_classes: dict[str, dict[str, dict]] = defaultdict(dict)
    provider_names: dict[str, str] = {}

    total_processed = 0
    total_failed = 0
    total_params = 0

    for i, module in enumerate(modules, 1):
        import_path = module.get("import_path", "")
        provider_id = module.get("provider_id", "")
        provider_name = module.get("provider_name", "")
        module_type = module.get("type", "")
        class_name = module.get("name", "")

        if not import_path or not provider_id:
            continue

        provider_names[provider_id] = provider_name

        cls = import_class(import_path)
        if cls is None:
            total_failed += 1
            continue

        try:
            mro, params = extract_class_params(cls)
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

    print(f"\nProcessed {total_processed} classes, {total_failed} failed imports")
    print(f"Extracted {total_params} total parameters")
    print(f"Across {len(provider_classes)} providers")

    # Write per-provider files to versions/{pid}/{version}/parameters.json
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

    print("\nDone!")


if __name__ == "__main__":
    sys.exit(main() or 0)
