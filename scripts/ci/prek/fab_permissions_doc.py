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
Generate the FAB auth manager's REST API permission table.

``extract_permissions.py`` produces the auth-manager-agnostic reference table
(``Resource`` + ``Required permission``) for airflow-core. The FAB provider
documents the same endpoints in its own vocabulary -- concrete permission names
such as ``DAGs.can_read`` plus the minimum built-in role that grants them --
and that table is still maintained by hand, so it drifts.

This module reuses ``extract_permissions.py``'s parser and renders the FAB view
from the same entries, so both tables come from one source of truth.

Like ``extract_permissions.py`` this runs entirely statically: route files, the
FAB resource maps and the FAB role definitions are all read with Python's AST
parser, so no Airflow or FAB import is required.
"""

from __future__ import annotations

import ast
import pathlib
import sys

from extract_permissions import (
    PUBLIC_ROUTES_DIR,
    PermissionEntry,
    extract_all_permissions,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
FAB_SRC = REPO_ROOT / "providers/fab/src/airflow/providers/fab"
PERMISSIONS_PY = FAB_SRC / "www/security/permissions.py"
AUTH_MANAGER_PY = FAB_SRC / "auth_manager/fab_auth_manager.py"
SECURITY_MANAGER_PY = FAB_SRC / "auth_manager/security_manager/override.py"
OUTPUT_RST = REPO_ROOT / "providers/fab/docs/auth-manager/_api_permissions_table.rst"

# FAB serves its own user and role management endpoints from a separate router, using
# ``requires_fab_custom_view`` rather than the core ``requires_access_*`` helpers. They
# are real endpoints and belong in this table, so they are parsed here as well.
FAB_ROUTES_DIR = FAB_SRC / "auth_manager/api_fastapi/routes"
FAB_ROUTER_PY = FAB_ROUTES_DIR / "router.py"

# HTTP verb -> FAB action. PATCH and PUT are both edits.
_METHOD_TO_ACTION = {
    "GET": "can_read",
    "POST": "can_create",
    "PUT": "can_edit",
    "PATCH": "can_edit",
    "DELETE": "can_delete",
}

# Built-in roles, ordered from least to most privileged. ``ROLE_CONFIGS`` builds
# each one as a superset of the previous, so the minimum role that grants a set
# of permissions is the first entry here whose permissions cover them all.
_ROLE_ORDER = ("Public", "Viewer", "User", "Op", "Admin")


def _resolve_str_constants(tree: ast.Module) -> dict[str, str]:
    """Collect module-level ``NAME = "value"`` string assignments."""
    out: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        out[target.id] = node.value.value
    return out


def _parse(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _resource_display_names() -> dict[str, str]:
    """``RESOURCE_DAG`` -> ``DAGs``, straight from ``permissions.py``."""
    return _resolve_str_constants(_parse(PERMISSIONS_PY))


def _dict_literal(tree: ast.Module, name: str) -> ast.Dict | None:
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if isinstance(node.value, ast.Dict):
                        return node.value
        if isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == name:
                if isinstance(node.value, ast.Dict):
                    return node.value
    return None


def _enum_member(node: ast.expr) -> str | None:
    """``DagAccessEntity.TASK_INSTANCE`` -> ``TASK_INSTANCE``."""
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _const_names(node: ast.expr) -> tuple[str, ...]:
    """A ``RESOURCE_X`` name, or a tuple of them, as their identifier strings."""
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, ast.Tuple):
        return tuple(e.id for e in node.elts if isinstance(e, ast.Name))
    return ()


def _entity_maps() -> tuple[dict[str, tuple[str, ...]], dict[str, str]]:
    """Read FAB's two mapping dicts rather than restating them here.

    Restating them would create exactly the second source of truth this
    generator exists to remove.
    """
    tree = _parse(AUTH_MANAGER_PY)

    dag_map: dict[str, tuple[str, ...]] = {}
    node = _dict_literal(tree, "_MAP_DAG_ACCESS_ENTITY_TO_FAB_RESOURCE_TYPE")
    if node is not None:
        for key, value in zip(node.keys, node.values):
            member = _enum_member(key) if key is not None else None
            if member:
                dag_map[member] = _const_names(value)

    view_map: dict[str, str] = {}
    node = _dict_literal(tree, "_MAP_ACCESS_VIEW_TO_FAB_RESOURCE_TYPE")
    if node is not None:
        for key, value in zip(node.keys, node.values):
            member = _enum_member(key) if key is not None else None
            names = _const_names(value)
            if member and names:
                view_map[member] = names[0]

    return dag_map, view_map


def _role_permissions() -> dict[str, set[tuple[str, str]]]:
    """Build ``{role: {(action, resource), ...}}`` from ``override.py``.

    ``VIEWER_PERMISSIONS`` and friends are class-level list literals of
    ``(ACTION_*, RESOURCE_*)`` tuples; ``ROLE_CONFIGS`` then concatenates them.
    Both are parsed statically.
    """
    tree = _parse(SECURITY_MANAGER_PY)
    consts = _resolve_str_constants(tree)

    groups: dict[str, set[tuple[str, str]]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.List):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or not target.id.endswith("_PERMISSIONS"):
                continue
            pairs: set[tuple[str, str]] = set()
            for elt in node.value.elts:
                if not isinstance(elt, ast.Tuple) or len(elt.elts) != 2:
                    continue
                action = _name_of(elt.elts[0])
                resource = _name_of(elt.elts[1])
                if action and resource:
                    pairs.add((action, resource))
            groups[target.id] = pairs

    # ROLE_CONFIGS is cumulative; rebuild that stacking explicitly.
    roles: dict[str, set[tuple[str, str]]] = {"Public": set()}
    roles["Viewer"] = set(groups.get("VIEWER_PERMISSIONS", set()))
    roles["User"] = roles["Viewer"] | groups.get("USER_PERMISSIONS", set())
    roles["Op"] = roles["User"] | groups.get("OP_PERMISSIONS", set())
    roles["Admin"] = roles["Op"] | groups.get("ADMIN_PERMISSIONS", set())

    # Resolve ACTION_*/RESOURCE_* identifiers to their string values.
    perm_consts = _resolve_str_constants(_parse(PERMISSIONS_PY))
    perm_consts.update(consts)
    resolved: dict[str, set[tuple[str, str]]] = {}
    for role, pairs in roles.items():
        resolved[role] = {(perm_consts.get(a, a), perm_consts.get(r, r)) for a, r in pairs}
    return resolved


def _name_of(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def fab_permissions_for(entry: PermissionEntry) -> list[str]:
    """Translate one generic entry into FAB permission strings."""
    display = _resource_display_names()
    dag_map, view_map = _entity_maps()

    if entry.resource == "Public":
        return []

    if entry.resource == "View" or entry.resource.startswith("View."):
        # ``_build_resource_label`` renders these as ``View.PLUGINS``; the view name is
        # also carried in ``required_permission``.
        view_name = entry.resource.split(".", 1)[1] if "." in entry.resource else entry.required_permission
        const = view_map.get(view_name)
        if const is None:
            return []
        return [f"{display.get(const, const)}.can_read"]

    # ``required_permission`` is the method passed to ``requires_access_*``, which is
    # not always the route's HTTP verb: ``/clearTaskInstances`` is a POST route that
    # authorizes with PUT. The authorization method is the one that decides the action.
    auth_method = entry.required_permission
    action = _METHOD_TO_ACTION.get(auth_method, "can_read")

    if entry.resource.startswith("DAG."):
        entity = entry.resource.split(".", 1)[1]
        consts = dag_map.get(entity, ())
        perms = [f"{display.get(c, c)}.{action}" for c in consts]
        # A sub-entity check always runs the base Dag check first: ``is_authorized_dag``
        # uses GET for a read and PUT for anything else, so the base requirement is
        # can_read on GET and can_edit otherwise -- it is present on GET routes too.
        base = "can_read" if auth_method == "GET" else "can_edit"
        dag_name = display.get("RESOURCE_DAG", "DAGs")
        return [f"{dag_name}.{base}", *perms]

    const_name = f"RESOURCE_{_RESOURCE_TO_CONST.get(entry.resource, entry.resource.upper())}"
    return [f"{display.get(const_name, entry.resource)}.{action}"]


# Generic resource label -> the RESOURCE_* suffix in permissions.py.
_RESOURCE_TO_CONST = {
    "DAG": "DAG",
    "Pool": "POOL",
    "Connection": "CONNECTION",
    "Configuration": "CONFIG",
    "Variable": "VARIABLE",
    "Asset": "ASSET",
    "AssetAlias": "ASSET_ALIAS",
}


def minimum_role(perms: list[str]) -> str:
    """First built-in role whose permissions cover every required permission."""
    if not perms:
        return "Public"
    roles = _role_permissions()
    required = set()
    for perm in perms:
        resource, _, action = perm.rpartition(".")
        required.add((action, resource))
    for role in _ROLE_ORDER:
        if required <= roles.get(role, set()):
            return role
    return "Admin"


def _fab_router_prefix() -> str:
    """Read ``FAB_AUTH_PREFIX`` from router.py rather than hardcoding it."""
    return _resolve_str_constants(_parse(FAB_ROUTER_PY)).get("FAB_AUTH_PREFIX", "")


def _fab_route_entries() -> list[tuple[str, str, str, str]]:
    """Parse FAB's own routes into ``(path, http_method, permissions, role)`` rows.

    ``requires_fab_custom_view(method, resource)`` names its resource directly, so no
    entity expansion or base-Dag rule applies here -- unlike the core helpers.
    """
    display = _resource_display_names()
    prefix = _fab_router_prefix()
    rows: list[tuple[str, str, str, str]] = []

    for route_file in sorted(FAB_ROUTES_DIR.glob("*.py")):
        if route_file.name in {"__init__.py", "router.py"}:
            continue
        for node in ast.walk(_parse(route_file)):
            if not isinstance(node, ast.FunctionDef):
                continue
            for deco in node.decorator_list:
                if not isinstance(deco, ast.Call):
                    continue
                verb = _name_of(deco.func)
                if verb is None or verb.upper() not in _METHOD_TO_ACTION:
                    continue
                if not deco.args or not isinstance(deco.args[0], ast.Constant):
                    continue
                path = deco.args[0].value
                if not isinstance(path, str):
                    continue

                perms: list[str] = []
                for kw in deco.keywords:
                    if kw.arg != "dependencies" or not isinstance(kw.value, ast.List):
                        continue
                    for dep in kw.value.elts:
                        if not isinstance(dep, ast.Call) or not dep.args:
                            continue
                        inner = dep.args[0]
                        if not isinstance(inner, ast.Call):
                            continue
                        if _name_of(inner.func) != "requires_fab_custom_view":
                            continue
                        if len(inner.args) < 2 or not isinstance(inner.args[0], ast.Constant):
                            continue
                        auth_method = inner.args[0].value
                        const = _name_of(inner.args[1])
                        if not const or not isinstance(auth_method, str):
                            continue
                        action = _METHOD_TO_ACTION.get(auth_method, "can_read")
                        perms.append(f"{display.get(const, const)}.{action}")

                if perms:
                    rows.append((f"{prefix}{path}", verb.upper(), ", ".join(perms), minimum_role(perms)))
    return rows


# The generated file must carry the ASF header itself; otherwise the ``insert-license``
# hook adds it and this generator strips it back out on the next run.
RST_LICENSE_HEADER = """\
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
"""


def render_rst(entries: list[PermissionEntry]) -> str:
    rows = []
    for entry in sorted(entries):
        perms = fab_permissions_for(entry)
        rows.append(
            (
                entry.full_path,
                entry.http_method,
                ", ".join(perms) if perms else "None",
                minimum_role(perms),
            )
        )

    rows.extend(_fab_route_entries())
    rows.sort()

    head = ("Endpoint", "Method", "Permissions", "Minimum role")
    lines = [
        *RST_LICENSE_HEADER.split("\n"),
        ".. THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY.",
        "   Regenerate with:  python scripts/ci/prek/fab_permissions_doc.py",
        "   Trigger:          prek run generate-fab-permissions-doc --all-files",
        "",
        ".. list-table:: Stable REST API permissions (FAB auth manager)",
        "   :header-rows: 1",
        "   :widths: 45 8 32 15",
        "",
        f"   * - {head[0]}",
        f"     - {head[1]}",
        f"     - {head[2]}",
        f"     - {head[3]}",
    ]
    for row_path, row_method, row_perms, row_role in rows:
        lines += [
            f"   * - ``{row_path}``",
            f"     - {row_method}",
            f"     - {row_perms}",
            f"     - {row_role}",
        ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Generate the FAB API permission table.")
    parser.add_argument("--check", action="store_true", help="Exit 1 if the file on disk is stale.")
    parser.add_argument("--print", dest="print_only", action="store_true", help="Print instead of write.")
    args = parser.parse_args(argv)

    entries = extract_all_permissions(PUBLIC_ROUTES_DIR)
    content = render_rst(entries)

    if args.print_only:
        print(content)
        return 0

    if args.check:
        if not OUTPUT_RST.exists() or OUTPUT_RST.read_text(encoding="utf-8") != content:
            print(f"[FAIL] {OUTPUT_RST} is stale. Run: python {pathlib.Path(__file__).name}", file=sys.stderr)
            return 1
        print(f"[OK] {OUTPUT_RST} is up to date.")
        return 0

    OUTPUT_RST.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_RST.write_text(content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
