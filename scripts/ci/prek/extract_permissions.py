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
"""
Extract permission requirements from FastAPI routes in Airflow REST API.

This script statically parses FastAPI route files under airflow-core's public REST API
routes to extract required permissions for each endpoint. It generates a reference
RST documentation file for security/api_permissions_ref.rst.

It runs completely statically using Python's built-in AST parser, requiring no runtime
Airflow imports or active execution environment, making it suitable for CI checks.
"""

from __future__ import annotations

import ast
import pathlib
import sys
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Paths (all relative to the repo root, resolved from this file's location)
# ---------------------------------------------------------------------------
REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
PUBLIC_ROUTES_DIR = REPO_ROOT / "airflow-core/src/airflow/api_fastapi/core_api/routes/public"
OUTPUT_RST = REPO_ROOT / "airflow-core/docs/security/api_permissions_ref.rst"

# The global /api/v2 prefix comes from public_router in __init__.py
API_PREFIX = "/api/v2"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass(frozen=True, order=True)
class PermissionEntry:
    """One HTTP operation's permission requirement."""

    full_path: str  # full route path, e.g. /api/v2/dags/{dag_id}
    http_method: str  # GET / POST / PATCH / PUT / DELETE
    tag: str  # OpenAPI tag, e.g. "DAG", "Variable"
    resource: str  # e.g. "DAG", "DAG.RUN", "Variable", "View"
    required_permission: str  # e.g. "GET", "POST", "DELETE", "multi", "PLUGINS"
    source_file: str  # route file basename for traceability


# ---------------------------------------------------------------------------
# Per-file AST helpers
# ---------------------------------------------------------------------------


def _resolve_string_node(node: ast.expr, module_consts: dict[str, str]) -> str:
    """
    Convert an AST expression to a string.

    Handles:
      - ast.Constant  → direct string
      - ast.BinOp(+)  → resolve left and right recursively (string concat)
      - ast.Name      → look up in module_consts if available
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _resolve_string_node(node.left, module_consts)
        right = _resolve_string_node(node.right, module_consts)
        return left + right
    if isinstance(node, ast.Name) and node.id in module_consts:
        return module_consts[node.id]
    # Give up — return an unresolvable marker (will surface in tests)
    return f"<unresolved:{ast.unparse(node)}>"


def _extract_module_string_constants(tree: ast.Module) -> dict[str, str]:
    """
    Walk top-level assignments and collect simple string assignments.

    e.g.  task_instances_prefix = "/dagRuns/{dag_run_id}/taskInstances"
    → {"task_instances_prefix": "/dagRuns/{dag_run_id}/taskInstances"}
    """
    consts: dict[str, str] = {}
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            consts[node.targets[0].id] = node.value.value
    return consts


def _extract_routers(tree: ast.Module) -> dict[str, str]:
    """
    Find all assignments like some_router = AirflowRouter(...) at module level.

    Returns a mapping of router variable name to its prefix.
    """
    routers: dict[str, str] = {}
    for node in tree.body:
        if not (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Call)
        ):
            continue
        call = node.value
        call_name = (
            call.func.id
            if isinstance(call.func, ast.Name)
            else call.func.attr
            if isinstance(call.func, ast.Attribute)
            else ""
        )
        if call_name != "AirflowRouter":
            continue

        target_name = node.targets[0].id
        prefix = ""
        for kw in call.keywords:
            if kw.arg == "prefix" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                prefix = kw.value.value
            elif kw.arg == "dependencies" and isinstance(kw.value, ast.List):
                for dep_item in kw.value.elts:
                    for subnode in ast.walk(dep_item):
                        if isinstance(subnode, ast.Call):
                            fn_name = _get_requires_access_call_name(subnode)
                            if fn_name is not None:
                                raise ValueError(
                                    f"Unsupported extraction semantics: Router-level permission dependency '{fn_name}' "
                                    f"on router '{target_name}' is not supported by the static permission extractor."
                                )
        routers[target_name] = prefix
    return routers


def _get_requires_access_call_name(call_node: ast.Call) -> str | None:
    """Extract the function name from a requires_access_*() call node."""
    fn = call_node.func
    if isinstance(fn, ast.Name) and fn.id.startswith("requires_access"):
        return fn.id
    if isinstance(fn, ast.Attribute) and fn.attr.startswith("requires_access"):
        return fn.attr
    return None


def _extract_method_arg(call_node: ast.Call) -> str:
    """
    Extract the HTTP method from a requires_access_*(...) call.

    Two calling conventions exist in the codebase:
      requires_access_dag("GET", ...)            ← positional
      requires_access_dag(method="GET", ...)     ← keyword

    Returns the method string (GET/POST/PUT/DELETE) or "multi"
    for bulk functions that carry no method.
    """
    # Positional first arg
    if call_node.args:
        first = call_node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            return first.value.upper()
        return ast.unparse(first).strip("\"'").upper()

    # Keyword method=
    for kw in call_node.keywords:
        if kw.arg == "method":
            val = kw.value
            if isinstance(val, ast.Constant) and isinstance(val.value, str):
                return val.value.upper()
            return ast.unparse(val).strip("\"'").upper()

    # bulk functions: no method arg
    return "multi"


def _extract_entity_arg(call_node: ast.Call) -> str | None:
    """
    Extract the access_entity or first positional (for requires_access_view).

    Returns e.g. "TASK_INSTANCE", "PLUGINS", or None.
    """
    fn_name = _get_requires_access_call_name(call_node) or ""

    # For requires_access_view the entity IS the first positional arg
    if fn_name == "requires_access_view":
        for kw in call_node.keywords:
            if kw.arg == "access_view":
                return ast.unparse(kw.value).split(".")[-1]  # AccessView.PLUGINS → "PLUGINS"
        if call_node.args:
            return ast.unparse(call_node.args[0]).split(".")[-1]
        return None

    # For requires_access_dag the entity is the access_entity keyword
    # or second positional argument
    if fn_name == "requires_access_dag":
        for kw in call_node.keywords:
            if kw.arg == "access_entity":
                return ast.unparse(kw.value).split(".")[-1]  # DagAccessEntity.RUN → "RUN"

        if len(call_node.args) >= 2:
            return ast.unparse(call_node.args[1]).split(".")[-1]

        return None

    return None


# Map from requires_access_* function name → (resource base name, forced entity or None)
_FN_TO_RESOURCE_INFO: dict[str, tuple[str, str | None]] = {
    "requires_access_dag": ("DAG", None),
    "requires_access_dag_from_file_token": ("DAG", None),  # reparse authorizes the file_token's Dags
    "requires_access_backfill": ("DAG", "RUN"),  # backfill is a DAG.RUN alias
    "requires_access_dag_run_bulk": ("DAG", "RUN"),  # dag_run bulk is a DAG.RUN alias
    "requires_access_dag_run_clear_bulk": ("DAG", "RUN"),  # dag_run clear bulk is a DAG.RUN alias
    "requires_access_event_log": ("DAG", "AUDIT_LOG"),  # event log is a DAG.AUDIT_LOG alias
    "requires_access_pool": ("Pool", None),
    "requires_access_pool_bulk": ("Pool", None),
    "requires_access_connection": ("Connection", None),
    "requires_access_connection_bulk": ("Connection", None),
    "requires_access_configuration": ("Configuration", None),
    "requires_access_variable": ("Variable", None),
    "requires_access_variable_bulk": ("Variable", None),
    "requires_access_asset": ("Asset", None),
    "requires_access_asset_alias": ("AssetAlias", None),
    "requires_access_view": ("View", None),
}


def _build_resource_label(fn_name: str, entity: str | None) -> str:
    """Convert fn_name + entity into a human-readable resource label."""
    if fn_name in _FN_TO_RESOURCE_INFO:
        base, forced_entity = _FN_TO_RESOURCE_INFO[fn_name]
        entity_to_use = forced_entity or entity
        if entity_to_use:
            return f"{base}.{entity_to_use}"
        return base
    return fn_name


def _extract_tag_from_decorator(decorator: ast.Call) -> str:
    """Get the OpenAPI tag from  @router.get(tags=["Tag"]) if present."""
    for kw in decorator.keywords:
        if kw.arg == "tags" and isinstance(kw.value, ast.List):
            for elt in kw.value.elts:
                if isinstance(elt, ast.Constant):
                    return str(elt.value)
    return "?"


# ---------------------------------------------------------------------------
# Core extraction per file
# ---------------------------------------------------------------------------


def extract_from_file(path: pathlib.Path) -> list[PermissionEntry]:
    """Parse one route file and return all PermissionEntry objects."""
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (OSError, SyntaxError) as exc:
        print(f"[WARN] Could not parse {path.name}: {exc}", file=sys.stderr)
        return []

    # Build lookup tables for this file
    module_consts = _extract_module_string_constants(tree)
    routers = _extract_routers(tree)

    results: list[PermissionEntry] = []

    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue

            # Determine HTTP method from decorator attribute: @router.GET / .get / .post …
            if not isinstance(decorator.func, ast.Attribute):
                continue
            http_verb = decorator.func.attr.upper()
            if http_verb not in {"GET", "POST", "PATCH", "PUT", "DELETE", "HEAD"}:
                continue

            # Resolve the route path
            route_suffix = ""
            if decorator.args:
                route_suffix = _resolve_string_node(decorator.args[0], module_consts)

            # Resolve the prefix based on the router variable used in the decorator
            router_prefix = ""
            if isinstance(decorator.func.value, ast.Name):
                router_var = decorator.func.value.id
                router_prefix = routers.get(router_var, "")

            full_path = API_PREFIX + router_prefix + route_suffix

            # Extract tag (for grouping in the RST table)
            tag = _extract_tag_from_decorator(decorator)

            # Find dependencies=[...] kwarg
            deps_kwarg = next(
                (kw for kw in decorator.keywords if kw.arg == "dependencies"),
                None,
            )

            has_permission_dependency = False
            if deps_kwarg is not None and isinstance(deps_kwarg.value, ast.List):
                # Walk the dependency list
                for dep_item in deps_kwarg.value.elts:
                    if not isinstance(dep_item, ast.Call):
                        continue
                    # Must be Depends(...)
                    dep_name = (
                        dep_item.func.id
                        if isinstance(dep_item.func, ast.Name)
                        else getattr(dep_item.func, "attr", "")
                    )
                    if dep_name != "Depends" or not dep_item.args:
                        continue

                    inner = dep_item.args[0]
                    if not isinstance(inner, ast.Call):
                        continue

                    fn_name = _get_requires_access_call_name(inner)
                    if fn_name is None:
                        continue

                    method = _extract_method_arg(inner)
                    entity = _extract_entity_arg(inner)
                    resource = _build_resource_label(fn_name, entity)
                    permission = entity if fn_name == "requires_access_view" else method
                    if not isinstance(permission, str):
                        raise ValueError(
                            f"Could not resolve required permission for {fn_name} in {path.name}"
                        )

                    results.append(
                        PermissionEntry(
                            http_method=http_verb,
                            full_path=full_path,
                            tag=tag,
                            resource=resource,
                            required_permission=permission,
                            source_file=path.name,
                        )
                    )
                    has_permission_dependency = True

            if not has_permission_dependency:
                results.append(
                    PermissionEntry(
                        http_method=http_verb,
                        full_path=full_path,
                        tag=tag,
                        resource="Public",
                        required_permission="No Airflow permission required",
                        source_file=path.name,
                    )
                )

    return results


# ---------------------------------------------------------------------------
# Main extraction entry point
# ---------------------------------------------------------------------------


def extract_all_permissions(routes_dir: pathlib.Path) -> list[PermissionEntry]:
    """
    Walk all public route files and return a sorted, deduplicated list
    of PermissionEntry objects.
    """
    all_entries: list[PermissionEntry] = []
    for route_file in sorted(routes_dir.glob("*.py")):
        if route_file.name == "__init__.py":
            continue
        all_entries.extend(extract_from_file(route_file))

    # Deduplicate (same path+method+resource can appear from multiple deps)
    seen: set[PermissionEntry] = set()
    deduped: list[PermissionEntry] = []
    sorted_entries = sorted(
        all_entries,
        key=lambda e: (
            e.full_path,
            e.http_method,
            e.resource,
            e.required_permission,
        ),
    )
    for entry in sorted_entries:
        if entry not in seen:
            seen.add(entry)
            deduped.append(entry)

    return deduped


# ---------------------------------------------------------------------------
# RST generation
# ---------------------------------------------------------------------------

RST_HEADER = """\
 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY.
   Regenerate with:  python scripts/ci/prek/extract_permissions.py
   Trigger:          prek run generate-api-permissions-doc --all-files

API Endpoint Permission Reference
==================================

This page lists the required permission for every endpoint in the stable
Airflow REST API (``/api/v2``).  It is generated automatically from the
source code so it stays up to date as endpoints are added or changed.

.. seealso::

    :doc:`/security/api` — for authentication instructions (JWT tokens).

.. note::

    Permissions are enforced by the configured **auth manager**.  The
    :class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager`
    interface defines the contract; individual auth manager implementations
    (e.g. the Simple Auth Manager, or the FAB provider) translate these
    resource/method tuples into their own role/permission models.

"""

RST_TABLE_HEADER = """\
.. list-table:: Stable REST API endpoint permissions
   :header-rows: 1
   :widths: 7 50 20 13

   * - Method
     - Endpoint path
     - Resource
     - Required permission
"""


def _rst_table_row(entry: PermissionEntry) -> str:
    return (
        f"   * - ``{entry.http_method}``\n"
        f"     - ``{entry.full_path}``\n"
        f"     - ``{entry.resource}``\n"
        f"     - ``{entry.required_permission}``\n"
    )


def render_rst(entries: list[PermissionEntry]) -> str:
    """Render the full RST document from the list of PermissionEntry objects."""
    rows = "".join(_rst_table_row(e) for e in entries)
    return RST_HEADER + RST_TABLE_HEADER + rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Extract API permissions and write RST reference doc.")
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Check mode: exit 1 if the generated content differs from "
            f"what is on disk at {OUTPUT_RST}. "
            "Use in CI to detect stale documentation."
        ),
    )
    parser.add_argument(
        "--print",
        dest="print_only",
        action="store_true",
        help="Print the generated RST to stdout instead of writing to disk.",
    )
    args = parser.parse_args(argv)

    entries = extract_all_permissions(PUBLIC_ROUTES_DIR)
    content = render_rst(entries)

    if args.print_only:
        print(content)
        return 0

    if args.check:
        if not OUTPUT_RST.exists():
            print(
                f"[FAIL] {OUTPUT_RST} does not exist. Run: python scripts/ci/prek/extract_permissions.py",
                file=sys.stderr,
            )
            return 1
        existing = OUTPUT_RST.read_text(encoding="utf-8")
        if existing != content:
            print(
                f"[FAIL] {OUTPUT_RST} is stale. Run: python scripts/ci/prek/extract_permissions.py",
                file=sys.stderr,
            )
            return 1
        print(f"[OK] {OUTPUT_RST} is up to date.")
        return 0

    # Write mode (default)
    OUTPUT_RST.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_RST.write_text(content, encoding="utf-8")
    print(f"[OK] Written {len(entries)} entries to {OUTPUT_RST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
