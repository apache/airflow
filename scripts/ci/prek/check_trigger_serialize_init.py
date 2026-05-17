#!/usr/bin/env python
#
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
#   "rich>=13.6.0",
# ]
# ///
"""
Check that provider ``Trigger`` classes keep ``__init__`` and ``serialize()`` in sync.

``__init__`` and ``serialize()`` are written as a pair: a trigger is instantiated once when the
operator defers, then serialized and re-instantiated on whichever triggerer process runs it. Any
``__init__`` parameter that ``serialize()`` does not return is silently dropped on that round-trip
-- the reconstructed trigger falls back to the parameter's default. See
https://github.com/apache/airflow/blob/main/airflow-core/docs/authoring-and-scheduling/deferring.rst

This check parses each provider trigger module with ``ast`` and, for every trigger class whose
``__init__`` signature *and* ``serialize()`` return dict can both be resolved statically (including
through in-file base classes), flags any ``__init__`` parameter missing from the ``serialize()``
return dict.

Classes whose ``serialize()`` is built dynamically (``**spread`` of a non-``super()`` value,
``.update()``, returning a variable, ...) or that inherit ``__init__``/``serialize()`` from a base
class defined in another file cannot be resolved statically and are skipped -- the check never
guesses, so it produces no false positives.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_PROVIDERS_ROOT_PATH, console

DEFERRING_DOC = (
    "https://github.com/apache/airflow/blob/main/airflow-core/docs/authoring-and-scheduling/deferring.rst"
)

# Key format for both sets below: "<path relative to the providers/ directory>::<ClassName>".

# Trigger classes that genuinely violate the __init__/serialize() contract today. They predate
# the check and are excluded so it can be enabled without a tree-wide fix; each is tracked for a
# follow-up fix in a separate PR. Do NOT add new entries here -- fix the trigger instead.
KNOWN_VIOLATIONS: set[str] = {
    # `caller` is passed straight to DatabricksHook(caller=...) and never stored/serialized, so it
    # falls back to the class-name default on a triggerer round-trip.
    "databricks/src/airflow/providers/databricks/triggers/databricks.py::DatabricksExecutionTrigger",
    "databricks/src/airflow/providers/databricks/triggers/databricks.py::DatabricksSQLStatementExecutionTrigger",
    # `dataset_id`, `table_id`, `poll_interval` are forwarded to the parent __init__ and used, but
    # the overridden serialize() omits them.
    "google/src/airflow/providers/google/cloud/triggers/bigquery.py::BigQueryIntervalCheckTrigger",
    # `poll_interval` and `impersonation_chain` are stored and used but missing from serialize().
    "google/src/airflow/providers/google/cloud/triggers/datafusion.py::DataFusionStartPipelineTrigger",
    # `endpoint_prefix` is stored as self._endpoint_prefix and used in run() but missing from serialize().
    "apache/livy/src/airflow/providers/apache/livy/triggers/livy.py::LivyTrigger",
}

# Trigger classes that the check flags but which are correct *by design*: an old/aliased parameter
# is folded into its replacement at construction time, so it does not need to round-trip. These are
# permanent exclusions, not tech debt.
BY_DESIGN_EXCLUSIONS: set[str] = {
    # `delta` is converted to an absolute `moment` in __init__ and the class deliberately serializes
    # as a DateTimeTrigger (see its docstring) -- there is no `delta` to reconstruct.
    "standard/src/airflow/providers/standard/triggers/temporal.py::TimeDeltaTrigger",
    # `pod_name` is a deprecated alias folded into `pod_names` in __init__; serializing only
    # `pod_names` preserves the value and avoids re-triggering the deprecation path on restart.
    "google/src/airflow/providers/google/cloud/triggers/kubernetes_engine.py::GKEJobTrigger",
    "cncf/kubernetes/src/airflow/providers/cncf/kubernetes/triggers/job.py::KubernetesJobTrigger",
}

_EXCLUDED = KNOWN_VIOLATIONS | BY_DESIGN_EXCLUSIONS


def _get_init_param_names(func: ast.FunctionDef) -> set[str]:
    """Return the names of reconstructable __init__ parameters (``*args``/``**kwargs`` excluded)."""
    args = func.args
    names = {a.arg for a in (*args.posonlyargs, *args.args, *args.kwonlyargs)}
    names.discard("self")
    names.discard("cls")
    return names


def _get_base_simple_names(cls: ast.ClassDef) -> list[str]:
    """Return the simple (last-component) names of a class's bases."""
    out: list[str] = []
    for base in cls.bases:
        if isinstance(base, ast.Name):
            out.append(base.id)
        elif isinstance(base, ast.Attribute):
            out.append(base.attr)
    return out


def _get_method(cls: ast.ClassDef, name: str) -> ast.FunctionDef | None:
    for node in cls.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node if isinstance(node, ast.FunctionDef) else None
    return None


class ModuleAnalyzer:
    """Resolves trigger __init__/serialize() pairs within a single module, following in-file bases."""

    def __init__(self, path: Path) -> None:
        self.path = path
        tree = ast.parse(path.read_text("utf-8"), str(path))
        self.classes: dict[str, ast.ClassDef] = {
            node.name: node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        }

    def _in_file_base(self, cls: ast.ClassDef) -> ast.ClassDef | None:
        for name in _get_base_simple_names(cls):
            if name in self.classes:
                return self.classes[name]
        return None

    def is_trigger(self, cls: ast.ClassDef, _seen: set[str] | None = None) -> bool:
        if cls.name.endswith("Trigger"):
            return True
        _seen = _seen or set()
        if cls.name in _seen:
            return False
        _seen.add(cls.name)
        for name in _get_base_simple_names(cls):
            if "Trigger" in name:
                return True
            base = self.classes.get(name)
            if base is not None and self.is_trigger(base, _seen):
                return True
        return False

    def _resolve_method(self, cls: ast.ClassDef, name: str) -> tuple[ast.FunctionDef, ast.ClassDef] | None:
        """Walk in-file bases until *name* is found; return (method, defining class)."""
        current: ast.ClassDef | None = cls
        seen: set[str] = set()
        while current is not None and current.name not in seen:
            seen.add(current.name)
            method = _get_method(current, name)
            if method is not None:
                return method, current
            current = self._in_file_base(current)
        return None

    def _get_serialize_keys(self, cls: ast.ClassDef, _seen: set[str] | None = None) -> set[str] | None:
        """
        Statically resolve the keys of the dict returned by *cls*'s effective ``serialize()``.

        Returns ``None`` when the dict cannot be resolved statically (dynamic construction,
        external base class, ``**`` spread of a non-``super()`` value, ...).
        """
        _seen = _seen or set()
        if cls.name in _seen:
            return None
        _seen.add(cls.name)

        resolved = self._resolve_method(cls, "serialize")
        if resolved is None:
            return None
        serialize, defining_cls = resolved

        returns = [n for n in ast.walk(serialize) if isinstance(n, ast.Return)]
        if len(returns) != 1 or returns[0].value is None:
            return None
        ret = returns[0].value
        if not isinstance(ret, (ast.Tuple, ast.List)) or len(ret.elts) != 2:
            return None
        payload = ret.elts[1]
        if not isinstance(payload, ast.Dict):
            return None

        keys: set[str] = set()
        for key, value in zip(payload.keys, payload.values):
            if key is None:
                # ``**spread`` entry -- only resolvable when it spreads super().serialize().
                spread = self._get_super_serialize_keys(value, defining_cls, _seen)
                if spread is None:
                    return None
                keys |= spread
                continue
            if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                return None
            keys.add(key.value)
        return keys

    def _get_super_serialize_keys(
        self, value: ast.expr, defining_cls: ast.ClassDef, _seen: set[str]
    ) -> set[str] | None:
        """Resolve keys for a ``**super().serialize()[1]`` / ``**super().serialize()`` spread."""
        node = value
        if isinstance(node, ast.Subscript):  # super().serialize()[1]
            node = node.value
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "serialize"
            and isinstance(node.func.value, ast.Call)
            and isinstance(node.func.value.func, ast.Name)
            and node.func.value.func.id == "super"
        ):
            return None
        base = self._in_file_base(defining_cls)
        if base is None:
            return None
        return self._get_serialize_keys(base, _seen)

    def get_violations(self) -> list[tuple[str, list[str]]]:
        """Return ``(class_name, [missing params])`` for every resolvable trigger that violates."""
        results: list[tuple[str, list[str]]] = []
        rel = self.path.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH).as_posix()
        for name, cls in self.classes.items():
            if f"{rel}::{name}" in _EXCLUDED:
                continue
            if not self.is_trigger(cls):
                continue
            init_resolved = self._resolve_method(cls, "__init__")
            if init_resolved is None:
                continue  # __init__ inherited from an out-of-file base -- cannot resolve.
            params = _get_init_param_names(init_resolved[0])
            serialize_keys = self._get_serialize_keys(cls)
            if serialize_keys is None:
                continue  # serialize() dynamic or inherited from an out-of-file base.
            missing = sorted(params - serialize_keys)
            if missing:
                results.append((name, missing))
        return results


def _iter_files(argv: list[str]) -> list[Path]:
    if argv:
        return [Path(a).resolve() for a in argv]
    return sorted(AIRFLOW_PROVIDERS_ROOT_PATH.glob("*/src/airflow/providers/**/triggers/*.py")) + sorted(
        AIRFLOW_PROVIDERS_ROOT_PATH.glob("*/*/src/airflow/providers/**/triggers/*.py")
    )


def main(argv: list[str]) -> int:
    error_count = 0
    for path in _iter_files(argv):
        if path.name == "__init__.py":
            continue
        try:
            analyzer = ModuleAnalyzer(path)
        except SyntaxError as exc:
            console.print(f"[red]Could not parse {path}: {exc}")
            error_count += 1
            continue
        rel = path.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH).as_posix()
        for class_name, missing in analyzer.get_violations():
            error_count += 1
            console.print(
                f"[red]{rel}::{class_name}[/] -- __init__ parameter(s) "
                f"{', '.join(repr(m) for m in missing)} missing from serialize() return dict"
            )
    if error_count:
        console.print(
            f"\n[red]Found {error_count} trigger(s) whose serialize() drops __init__ parameters.[/]\n"
            "Every __init__ parameter must appear in the serialize() return dict, otherwise it is "
            "silently lost when the triggerer re-instantiates the trigger.\n"
            f"See: {DEFERRING_DOC}\n"
        )
    return 1 if error_count else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
