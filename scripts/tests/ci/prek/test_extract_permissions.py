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
Tests for scripts/ci/prek/extract_permissions.py.

Test strategy:
  - Unit tests parse small synthetic code strings, never real route files.
    This makes tests fast, self-contained, and immune to unrelated route changes.
  - Integration tests call extract_all_permissions() against the real
    routes/public directory to guard against regressions when routes change.
  - No snapshot tests: we assert on invariants (no unresolved markers,
    no duplicates, count ≥ known_minimum) rather than exact string equality.

Run with (no Airflow env needed — extractor is stdlib-only):
  uv run --project scripts pytest scripts/tests/ci/prek/test_extract_permissions.py -xvs
"""

from __future__ import annotations

import ast
import sys
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Add scripts/ci/prek to sys.path so we can import the extractor directly.
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[4]  # scripts/tests/ci/prek → repo root
PREK_DIR = REPO_ROOT / "scripts/ci/prek"

if str(PREK_DIR) not in sys.path:
    sys.path.insert(0, str(PREK_DIR))

from extract_permissions import (  # noqa: E402
    _FN_TO_RESOURCE_INFO,
    PermissionEntry,
    _build_resource_label,
    _extract_method_arg,
    _extract_module_string_constants,
    _extract_routers,
    _resolve_string_node,
    extract_all_permissions,
    extract_from_file,
    render_rst,
)

PUBLIC_ROUTES_DIR = REPO_ROOT / "airflow-core/src/airflow/api_fastapi/core_api/routes/public"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_expr(code: str) -> ast.expr:
    """Parse a single expression string into an AST node."""
    return ast.parse(code, mode="eval").body


def parse_module(code: str) -> ast.Module:
    """Parse a dedented code block into a module AST."""
    return ast.parse(textwrap.dedent(code))


def _make_route_file(tmp_path: Path, code: str) -> Path:
    """Write synthetic route code to a temp .py file."""
    f = tmp_path / "test_route.py"
    f.write_text(textwrap.dedent(code))
    return f


# ===========================================================================
# Unit tests: _resolve_string_node
# ===========================================================================


class TestResolveStringNode:
    def test_plain_string_constant(self):
        node = parse_expr("'/dags'")
        assert _resolve_string_node(node, {}) == "/dags"

    def test_name_lookup_in_module_consts(self):
        node = parse_expr("my_prefix")
        assert _resolve_string_node(node, {"my_prefix": "/taskInstances"}) == "/taskInstances"

    def test_binop_concatenation(self):
        # Mirrors task_instances.py:  task_instances_prefix + "/{task_id}"
        node = parse_expr("task_instances_prefix + '/{task_id}'")
        consts = {"task_instances_prefix": "/dagRuns/{dag_run_id}/taskInstances"}
        result = _resolve_string_node(node, consts)
        assert result == "/dagRuns/{dag_run_id}/taskInstances/{task_id}"

    def test_nested_binop(self):
        # a + b + c  →  (a + b) + c  (left-associative)
        node = parse_expr("a + b + c")
        consts = {"a": "/dags", "b": "/{dag_id}", "c": "/runs"}
        assert _resolve_string_node(node, consts) == "/dags/{dag_id}/runs"

    def test_unresolvable_name_returns_marker(self):
        node = parse_expr("unknown_var")
        result = _resolve_string_node(node, {})
        assert result.startswith("<unresolved:")

    def test_unresolvable_node_type_returns_marker(self):
        # A function call is not a resolvable string expression
        node = parse_expr("get_prefix()")
        result = _resolve_string_node(node, {})
        assert result.startswith("<unresolved:")


# ===========================================================================
# Unit tests: _extract_module_string_constants
# ===========================================================================


class TestExtractModuleStringConstants:
    def test_simple_string_assignment(self):
        tree = parse_module("""
            prefix = "/dagRuns/{dag_run_id}/taskInstances"
        """)
        consts = _extract_module_string_constants(tree)
        assert consts == {"prefix": "/dagRuns/{dag_run_id}/taskInstances"}

    def test_ignores_non_string_assignments(self):
        tree = parse_module("""
            count = 42
            flag = True
            name = "/dags"
        """)
        consts = _extract_module_string_constants(tree)
        # Only the string assignment is collected
        assert consts == {"name": "/dags"}
        assert "count" not in consts
        assert "flag" not in consts

    def test_ignores_augmented_assignments(self):
        tree = parse_module("""
            prefix = "/base"
            prefix += "/extra"
        """)
        # += is an AugAssign, not Assign — should not be picked up
        consts = _extract_module_string_constants(tree)
        assert consts == {"prefix": "/base"}

    def test_multiple_string_constants(self):
        tree = parse_module("""
            a_prefix = "/a"
            b_prefix = "/b"
        """)
        consts = _extract_module_string_constants(tree)
        assert consts == {"a_prefix": "/a", "b_prefix": "/b"}

    def test_empty_module(self):
        tree = parse_module("")
        assert _extract_module_string_constants(tree) == {}


# ===========================================================================
# Unit tests: _extract_router_prefix
# ===========================================================================


class TestExtractRouters:
    def test_extracts_prefixes_from_airflow_routers(self):
        tree = parse_module("""
            router_a = AirflowRouter(tags=["DAG"], prefix="/first")
            router_b = AirflowRouter(prefix="/second")
            router_c = AirflowRouter()
        """)
        assert _extract_routers(tree) == {
            "router_a": "/first",
            "router_b": "/second",
            "router_c": "",
        }

    def test_returns_empty_dict_when_no_routers(self):
        tree = parse_module("x = 1")
        assert _extract_routers(tree) == {}

    def test_prefix_with_path_parameter(self):
        tree = parse_module("""
            ti_router = AirflowRouter(tags=["Task Instance"], prefix="/dags/{dag_id}")
        """)
        assert _extract_routers(tree) == {"ti_router": "/dags/{dag_id}"}

    def test_router_level_permission_dependency_raises_value_error(self):
        tree = parse_module("""
            from fastapi import Depends
            from airflow.api_fastapi.core_api.security import requires_access_dag

            dags_router = AirflowRouter(
                tags=["DAG"],
                prefix="/dags",
                dependencies=[Depends(requires_access_dag("GET"))],
            )
        """)
        with pytest.raises(
            ValueError, match="Unsupported extraction semantics: Router-level permission dependency"
        ):
            _extract_routers(tree)


# ===========================================================================
# Unit tests: _extract_method_arg
# ===========================================================================


class TestExtractMethodArg:
    def _call(self, code: str) -> ast.Call:
        return ast.parse(code, mode="eval").body  # type: ignore[return-value]

    def test_keyword_method(self):
        call = self._call("requires_access_dag(method='GET', access_entity=DagAccessEntity.RUN)")
        assert _extract_method_arg(call) == "GET"

    def test_positional_method(self):
        # e.g. requires_access_variable('DELETE')
        call = self._call("requires_access_variable('DELETE')")
        assert _extract_method_arg(call) == "DELETE"

    def test_positional_method_is_uppercased(self):
        call = self._call("requires_access_pool('get')")
        assert _extract_method_arg(call) == "GET"

    def test_bulk_function_returns_multi(self):
        # requires_access_pool_bulk() has no method argument
        call = self._call("requires_access_pool_bulk()")
        assert _extract_method_arg(call) == "multi"

    def test_keyword_method_is_uppercased(self):
        call = self._call("requires_access_dag(method='put')")
        assert _extract_method_arg(call) == "PUT"

    def test_post_keyword(self):
        call = self._call("requires_access_connection(method='POST')")
        assert _extract_method_arg(call) == "POST"


# ===========================================================================
# Unit tests: _build_resource_label
# ===========================================================================


class TestBuildResourceLabel:
    def test_simple_resource_no_entity(self):
        assert _build_resource_label("requires_access_pool", None) == "Pool"

    def test_dag_with_entity(self):
        assert _build_resource_label("requires_access_dag", "RUN") == "DAG.RUN"

    def test_dag_with_task_instance_entity(self):
        assert _build_resource_label("requires_access_dag", "TASK_INSTANCE") == "DAG.TASK_INSTANCE"

    def test_alias_backfill_forces_run_entity(self):
        # requires_access_backfill → DAG.RUN regardless of entity arg
        assert _build_resource_label("requires_access_backfill", None) == "DAG.RUN"

    def test_alias_dag_run_bulk_forces_run_entity(self):
        assert _build_resource_label("requires_access_dag_run_bulk", None) == "DAG.RUN"

    def test_alias_event_log_forces_audit_log(self):
        assert _build_resource_label("requires_access_event_log", None) == "DAG.AUDIT_LOG"

    def test_view_with_no_entity_returns_base(self):
        assert _build_resource_label("requires_access_view", None) == "View"

    def test_view_with_entity(self):
        assert _build_resource_label("requires_access_view", "PLUGINS") == "View.PLUGINS"

    def test_unknown_function_falls_back_to_fn_name(self):
        # If a new requires_access_* is added but not yet in the map, the
        # function name is used.  Tests will catch it via the coverage test.
        assert _build_resource_label("requires_access_new_thing", None) == "requires_access_new_thing"


# ===========================================================================
# Unit tests: extract_from_file (synthetic route files)
# ===========================================================================


class TestExtractFromFile:
    def test_basic_get_with_keyword_method(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            dags_router = AirflowRouter(tags=["DAG"], prefix="/dags")

            @dags_router.get(
                "/{dag_id}",
                dependencies=[Depends(requires_access_dag(method="GET"))],
            )
            def get_dag(dag_id: str): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        e = entries[0]
        assert e.http_method == "GET"
        assert e.full_path == "/api/v2/dags/{dag_id}"
        assert e.resource == "DAG"
        assert e.required_permission == "GET"

    def test_multiple_routers_in_same_file(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            dag_run_router = AirflowRouter(prefix="/dags/{dag_id}/dagRuns")
            dag_run_at_dag_router = AirflowRouter(prefix="/dags/{dag_id}")

            @dag_run_router.post(
                "/clear",
                dependencies=[Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.RUN))],
            )
            def clear_dag_runs(dag_id: str): ...

            @dag_run_at_dag_router.post(
                "/clearDagRuns",
                dependencies=[Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.RUN))],
            )
            def clear_dag_runs_at_dag(dag_id: str): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 2
        # Sort by full_path to be deterministic in assertions
        entries_sorted = sorted(entries, key=lambda e: e.full_path)

        # /api/v2/dags/{dag_id}/clearDagRuns (from dag_run_at_dag_router)
        assert entries_sorted[0].full_path == "/api/v2/dags/{dag_id}/clearDagRuns"
        assert entries_sorted[0].http_method == "POST"

        # /api/v2/dags/{dag_id}/dagRuns/clear (from dag_run_router)
        assert entries_sorted[1].full_path == "/api/v2/dags/{dag_id}/dagRuns/clear"
        assert entries_sorted[1].http_method == "POST"

    def test_positional_method_arg(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            variables_router = AirflowRouter(prefix="/variables")

            @variables_router.delete(
                "/{variable_key:path}",
                dependencies=[Depends(requires_access_variable("DELETE"))],
            )
            def delete_variable(variable_key: str): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        assert entries[0].http_method == "DELETE"
        assert entries[0].required_permission == "DELETE"
        assert entries[0].resource == "Variable"

    def test_dag_access_entity_positional(self, tmp_path):
        """The second positional arg to requires_access_dag is the access_entity."""
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            dag_router = AirflowRouter(tags=["DAG"], prefix="/dags/{dag_id}")

            @dag_router.get(
                "/taskLogs",
                dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_LOGS))],
            )
            def get_task_logs(dag_id: str): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        e = entries[0]
        assert e.resource == "DAG.TASK_LOGS"
        assert e.required_permission == "GET"

    def test_binop_path_resolved(self, tmp_path):
        # Mirrors the task_instances.py pattern exactly
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            task_instances_router = AirflowRouter(prefix="/dags/{dag_id}")
            task_instances_prefix = "/dagRuns/{dag_run_id}/taskInstances"

            @task_instances_router.get(
                task_instances_prefix + "/{task_id}",
                dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
            )
            def get_task_instance(dag_id: str, dag_run_id: str, task_id: str): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        e = entries[0]
        assert e.full_path == "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        assert e.resource == "DAG.TASK_INSTANCE"
        assert e.http_method == "GET"

    def test_bulk_function_no_method_arg(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            pools_router = AirflowRouter(prefix="/pools")

            @pools_router.patch(
                "",
                dependencies=[Depends(requires_access_pool_bulk())],
            )
            def bulk_pools(): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        assert entries[0].required_permission == "multi"
        assert entries[0].resource == "Pool"

    def test_view_access_positional(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            plugins_router = AirflowRouter(tags=["Plugin"], prefix="/plugins")

            @plugins_router.get(
                "",
                dependencies=[Depends(requires_access_view(AccessView.PLUGINS))],
            )
            def get_plugins(): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        e = entries[0]
        assert e.resource == "View.PLUGINS"
        assert e.required_permission == "PLUGINS"

    def test_no_dependencies_kwarg_extracted_as_public(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            router = AirflowRouter(prefix="/version")

            @router.get("")
            def get_version(): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        e = entries[0]
        assert e.http_method == "GET"
        assert e.full_path == "/api/v2/version"
        assert e.resource == "Public"
        assert e.required_permission == "No Airflow permission required"

    def test_only_unrelated_dependencies_extracted_as_public(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            router = AirflowRouter(prefix="/version")

            @router.get("", dependencies=[Depends(action_logging())])
            def get_version(): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        e = entries[0]
        assert e.http_method == "GET"
        assert e.full_path == "/api/v2/version"
        assert e.resource == "Public"
        assert e.required_permission == "No Airflow permission required"

    def test_multiple_deps_on_same_route_produces_multiple_entries(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            dag_run_router = AirflowRouter(prefix="/dags/{dag_id}/dagRuns")

            @dag_run_router.get(
                "/{dag_run_id}/upstreamAssetEvents",
                dependencies=[
                    Depends(requires_access_asset(method="GET")),
                    Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN)),
                ],
            )
            def get_upstream_events(): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 2
        resources = {e.resource for e in entries}
        assert "Asset" in resources
        assert "DAG.RUN" in resources

    def test_non_requires_access_dep_is_ignored(self, tmp_path):
        # action_logging() is a dep that should not produce a permission entry
        f = _make_route_file(
            tmp_path,
            """
            from fastapi import Depends
            router = AirflowRouter(prefix="/dags")

            @router.post(
                "",
                dependencies=[Depends(action_logging()), Depends(requires_access_dag(method="POST"))],
            )
            def post_dag(): ...
            """,
        )
        entries = extract_from_file(f)
        assert len(entries) == 1
        assert entries[0].resource == "DAG"

    def test_syntax_error_returns_empty_list(self, tmp_path, capsys):
        f = tmp_path / "broken.py"
        f.write_text("def broken(:\n")
        entries = extract_from_file(f)
        assert entries == []
        # Should print a warning, not raise
        captured = capsys.readouterr()
        assert "WARN" in captured.err

    def test_source_file_is_basename(self, tmp_path):
        f = _make_route_file(
            tmp_path,
            """
            router = AirflowRouter(prefix="/pools")

            @router.get("", dependencies=[Depends(requires_access_pool(method="GET"))])
            def get_pools(): ...
            """,
        )
        entries = extract_from_file(f)
        assert entries[0].source_file == "test_route.py"


# ===========================================================================
# Unit tests: _FN_TO_RESOURCE coverage invariant
# ===========================================================================


class TestResourceMapCoverage:
    """Guard against _FN_TO_RESOURCE_INFO going stale as new requires_access_* functions are added."""

    def _get_all_imported_security_fns(self) -> set[str]:
        """
        Find all requires_access_* names imported in public route files.
        This is the ground truth of what the extractor must know about.
        """
        imported: set[str] = set()
        for route_file in PUBLIC_ROUTES_DIR.glob("*.py"):
            if route_file.name == "__init__.py":
                continue
            try:
                tree = ast.parse(route_file.read_text())
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module and "security" in node.module:
                    for alias in node.names:
                        if alias.name.startswith("requires_access"):
                            imported.add(alias.name)
        return imported

    def test_all_imported_functions_are_in_resource_map(self):
        """
        If a new requires_access_* function is added to security.py and used in
        a route, it must also be added to _FN_TO_RESOURCE_INFO in the extractor.

        Failure here means: a new endpoint has no documented permission.
        """
        imported = self._get_all_imported_security_fns()
        unmapped = imported - set(_FN_TO_RESOURCE_INFO.keys())
        assert not unmapped, (
            "These requires_access_* functions are used in route files but are not "
            "in _FN_TO_RESOURCE_INFO in extract_permissions.py:\n"
            + "\n".join(f"  - {fn}" for fn in sorted(unmapped))
        )

    def test_no_dead_entries_in_resource_map(self):
        """
        Every entry in _FN_TO_RESOURCE_INFO should correspond to a function actually
        used in route files.  Dead entries suggest the function was removed or renamed.

        This is a WARNING-level test: it identifies map entries that should be cleaned up.
        """
        imported = self._get_all_imported_security_fns()
        dead = set(_FN_TO_RESOURCE_INFO.keys()) - imported
        assert not dead, (
            "These entries in _FN_TO_RESOURCE_INFO are not used by any route file "
            "and should be removed:\n" + "\n".join(f"  - {fn}" for fn in sorted(dead))
        )


# ===========================================================================
# Integration tests: extract_all_permissions against real routes
# ===========================================================================


class TestExtractAllPermissions:
    """
    Integration tests against the real route files.
    Uses invariants, not snapshots, so they survive unrelated route additions.
    """

    @pytest.fixture(scope="class")
    def all_entries(self) -> list[PermissionEntry]:
        return extract_all_permissions(PUBLIC_ROUTES_DIR)

    def test_extracts_non_empty_result(self, all_entries):
        assert len(all_entries) > 0

    def test_minimum_known_entry_count(self, all_entries):
        """
        Guard against the extractor silently returning fewer results.
        The exact number will grow; 100 is a floor well below current 123.
        """
        assert len(all_entries) >= 100, f"Expected ≥100 entries, got {len(all_entries)}"

    def test_output_is_sorted(self, all_entries):
        expected = sorted(
            all_entries,
            key=lambda e: (
                e.full_path,
                e.http_method,
                e.resource,
                e.required_permission,
            ),
        )
        assert all_entries == expected

    def test_public_endpoints_coverage(self, all_entries):
        """Verify that known public endpoints are extracted as Public."""
        public_paths = {
            "/api/v2/monitor/health": "GET",
            "/api/v2/version": "GET",
            "/api/v2/auth/login": "GET",
            "/api/v2/auth/logout": "GET",
        }
        for path, method in public_paths.items():
            matches = [e for e in all_entries if e.full_path == path and e.http_method == method]
            assert len(matches) == 1, f"Expected exactly one match for public endpoint {method} {path}"
            e = matches[0]
            assert e.resource == "Public"
            assert e.required_permission == "No Airflow permission required"

    def test_no_duplicate_entries(self, all_entries):
        seen: set[PermissionEntry] = set()
        for e in all_entries:
            assert e not in seen, f"Duplicate entry: {e}"
            seen.add(e)

    def test_no_unresolved_path_markers(self, all_entries):
        unresolved = [e for e in all_entries if "<unresolved:" in e.full_path]
        assert not unresolved, "Some route paths could not be resolved statically:\n" + "\n".join(
            f"  {e.source_file}: {e.full_path}" for e in unresolved
        )

    def test_all_paths_start_with_api_v2(self, all_entries):
        bad = [e for e in all_entries if not e.full_path.startswith("/api/v2")]
        assert not bad, f"Paths not starting with /api/v2: {[e.full_path for e in bad]}"

    def test_all_http_methods_are_valid(self, all_entries):
        valid = {"GET", "POST", "PATCH", "PUT", "DELETE", "HEAD"}
        bad = [e for e in all_entries if e.http_method not in valid]
        assert not bad, f"Invalid HTTP methods: {[(e.http_method, e.full_path) for e in bad]}"

    def test_all_resources_are_mapped(self, all_entries):
        """
        No resource should fall back to a raw function name.
        If _FN_TO_RESOURCE_INFO is missing an entry, the resource will equal
        the function name (e.g. "requires_access_new_thing").
        """
        # A resource that matches a raw requires_access_* name is unmapped
        raw_fn_resources = [e for e in all_entries if e.resource.startswith("requires_access")]
        assert not raw_fn_resources, (
            "Some entries have unmapped resources (function name used as fallback):\n"
            + "\n".join(f"  {e.source_file}: {e.resource}" for e in raw_fn_resources)
        )

    # --- Specific known endpoints ---

    def test_dags_get_list_permission(self, all_entries):
        matches = [e for e in all_entries if e.full_path == "/api/v2/dags" and e.http_method == "GET"]
        assert len(matches) >= 1
        assert any(e.resource == "DAG" and e.required_permission == "GET" for e in matches)

    def test_variable_delete_permission(self, all_entries):
        matches = [
            e for e in all_entries if "/api/v2/variables/" in e.full_path and e.http_method == "DELETE"
        ]
        assert len(matches) >= 1
        assert all(e.resource == "Variable" and e.required_permission == "DELETE" for e in matches)

    def test_task_instance_path_resolved(self, all_entries):
        """The BinOp path in task_instances.py must be fully resolved."""
        ti_entries = [e for e in all_entries if "/taskInstances/" in e.full_path]
        assert len(ti_entries) > 0
        for e in ti_entries:
            assert "<unresolved:" not in e.full_path

    def test_dag_run_bulk_mapped_to_dag_run(self, all_entries):
        bulk = [e for e in all_entries if e.source_file == "dag_run.py" and e.required_permission == "multi"]
        assert len(bulk) >= 1
        assert all(e.resource == "DAG.RUN" for e in bulk)

    def test_view_permissions_use_entity_as_permission(self, all_entries):
        view_entries = [e for e in all_entries if e.resource.startswith("View.")]
        assert len(view_entries) > 0
        for e in view_entries:
            # For view permissions, required_permission == the view name (e.g. "PLUGINS")
            assert e.required_permission == e.resource.split(".")[-1]

    def test_event_log_mapped_to_dag_audit_log(self, all_entries):
        el_entries = [e for e in all_entries if e.source_file == "event_logs.py"]
        assert len(el_entries) > 0
        assert all(e.resource == "DAG.AUDIT_LOG" for e in el_entries)

    def test_backfill_mapped_to_dag_run(self, all_entries):
        bf_entries = [e for e in all_entries if e.source_file == "backfills.py"]
        assert len(bf_entries) > 0
        assert all(e.resource == "DAG.RUN" for e in bf_entries)

    def test_clear_dag_runs_endpoint_prefix(self, all_entries):
        """Verify that the clearDagRuns endpoint resolves to the correct path prefix."""
        matches = [e for e in all_entries if e.full_path == "/api/v2/dags/{dag_id}/clearDagRuns"]
        assert len(matches) == 1
        e = matches[0]
        assert e.http_method == "POST"
        assert e.resource == "DAG.RUN"
        assert e.required_permission == "multi"

        # Also verify that other dag runs endpoints still resolve with the longer prefix /api/v2/dags/{dag_id}/dagRuns
        dag_runs_list = [
            e
            for e in all_entries
            if e.full_path == "/api/v2/dags/{dag_id}/dagRuns" and e.http_method == "GET"
        ]
        assert len(dag_runs_list) >= 1


# ===========================================================================
# Integration tests: render_rst
# ===========================================================================


class TestRenderRst:
    @pytest.fixture(scope="class")
    def rst_content(self) -> str:
        entries = extract_all_permissions(PUBLIC_ROUTES_DIR)
        return render_rst(entries)

    def test_rst_contains_auto_generated_marker(self, rst_content):
        assert "AUTO-GENERATED" in rst_content

    def test_rst_contains_list_table_directive(self, rst_content):
        assert ".. list-table::" in rst_content

    def test_rst_contains_api_v2_paths(self, rst_content):
        assert "/api/v2/" in rst_content

    def test_rst_contains_no_unresolved_markers(self, rst_content):
        assert "<unresolved:" not in rst_content

    def test_rst_output_is_deterministic(self):
        """Two successive calls must produce identical output."""
        entries1 = extract_all_permissions(PUBLIC_ROUTES_DIR)
        entries2 = extract_all_permissions(PUBLIC_ROUTES_DIR)
        assert render_rst(entries1) == render_rst(entries2)


# ===========================================================================
# Integration test: --check mode (CI guard)
# ===========================================================================


class TestCheckMode:
    def test_check_mode_passes_when_rst_is_up_to_date(self, tmp_path, monkeypatch):
        """The --check CLI flag should return 0 when the on-disk RST matches."""
        import extract_permissions as ep

        entries = extract_all_permissions(PUBLIC_ROUTES_DIR)
        content = render_rst(entries)

        rst_file = tmp_path / "api_permissions_ref.rst"
        rst_file.write_text(content)

        monkeypatch.setattr(ep, "OUTPUT_RST", rst_file)
        result = ep.main(["--check"])
        assert result == 0

    def test_check_mode_fails_when_rst_is_stale(self, tmp_path, monkeypatch):
        """The --check CLI flag should return 1 when the on-disk RST is stale."""
        import extract_permissions as ep

        rst_file = tmp_path / "api_permissions_ref.rst"
        rst_file.write_text("# stale content")

        monkeypatch.setattr(ep, "OUTPUT_RST", rst_file)
        result = ep.main(["--check"])
        assert result == 1

    def test_check_mode_fails_when_rst_missing(self, tmp_path, monkeypatch):
        """The --check CLI flag should return 1 when the RST file doesn't exist."""
        import extract_permissions as ep

        monkeypatch.setattr(ep, "OUTPUT_RST", tmp_path / "does_not_exist.rst")
        result = ep.main(["--check"])
        assert result == 1
