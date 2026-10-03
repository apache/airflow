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
Check that deferrable Amazon tasks hand their hook configuration to the trigger they defer to.

A deferrable task builds its hook twice: once in the worker, once in the triggerer. The triggerer
half is built from the arguments the operator passed at the ``self.defer()`` call site, so anything
not passed there is lost, and the deferred half silently reaches AWS with boto3 defaults: a
different region, different TLS verification, different timeouts.

Two shapes are checked, both by parsing the provider with ``ast``:

1. Every ``self.defer(trigger=SomeTrigger(...))`` passes ``region_name``, ``verify`` and
   ``botocore_config``.
2. Every hook a trigger module builds by hand, alongside the one ``aws_hook_class`` gives it,
   carries the same configuration. A trigger whose job client verifies TLS while its log client
   does not is the same bug in miniature.

A ``trigger=`` expression that cannot be resolved statically is never skipped silently: it has to
be acknowledged in ``UNREADABLE_DEFER_SITES`` instead. The allowlists below are checked in both
directions, so an entry that is no longer needed fails the check and has to be removed.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Iterator
from pathlib import Path

from common_prek_utils import AIRFLOW_PROVIDERS_ROOT_PATH, console

AWS_ROOT = AIRFLOW_PROVIDERS_ROOT_PATH / "amazon" / "src" / "airflow" / "providers" / "amazon" / "aws"

HOOK_CONFIGURATION = ("region_name", "verify", "botocore_config")

# Triggers that take no boto3 client configuration at all, so there is nothing to hand them.
UNCONFIGURABLE_TRIGGERS = frozenset(
    {
        # Not an AwsBaseWaiterTrigger: its hook is addressed by execution name.
        "SageMakerNotebookJobTrigger",
        # A KubernetesPodTrigger; it reaches the pod through a kubeconfig, not a boto3 client.
        "EksPodTrigger",
    }
)

# Sites whose trigger is built elsewhere and only referenced at the call, so the class cannot be
# read off it.
UNREADABLE_DEFER_SITES = frozenset({("operators/eks.py", "trigger")})

# Services carved out as Contributors Workshop tasks, so their triggers are still unmigrated. Each
# entry is one self-contained contribution: widen the trigger's __init__, set aws_hook_class, pass
# the parameters at the call site, then delete the entry here.
PENDING_MIGRATION = frozenset(
    {
        ("sensors/batch.py", "BatchJobTrigger"),
    }
)

# Hand-built hooks that take no connection parameters, keyed by path relative to the aws package so
# that two trigger modules sharing a basename cannot share an entry.
HAND_BUILT_HOOK_EXCEPTIONS = frozenset(
    {
        # Addressed by execution name; takes no connection parameters at all.
        ("triggers/sagemaker_unified_studio.py", "SageMakerNotebookHook"),
        # EksPodOperator is a KubernetesPodOperator: it carries no verify or botocore_config to pass.
        ("triggers/eks.py", "EksHook"),
    }
)


def read_trigger_name(call: ast.Call) -> str | None:
    """Return the trigger class a construction names, or ``None`` when the callee cannot be read."""
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def resolve_trigger_constructions(expr: ast.expr) -> list[tuple[ast.Call, str]] | None:
    """
    Resolve a ``trigger=`` expression to the constructions it can evaluate to, each with its name.

    ``None`` means the expression cannot be read statically. Returning that rather than an empty
    list is what keeps a site from disappearing: a bare reference, a subscript, or a conditional
    with one unreadable branch all have to be acknowledged in ``UNREADABLE_DEFER_SITES`` instead of
    quietly contributing nothing to the sweep.
    """
    if isinstance(expr, ast.Call):
        # A construction whose callee cannot be named is no more readable than a bare reference:
        # the allowlists key on the class name, so an unnamed one could never match them.
        name = read_trigger_name(expr)
        return [(expr, name)] if name is not None else None
    if isinstance(expr, ast.IfExp):
        constructions: list[tuple[ast.Call, str]] = []
        for branch in (expr.body, expr.orelse):
            resolved = resolve_trigger_constructions(branch)
            if resolved is None:
                return None
            constructions.extend(resolved)
        return constructions
    return None


def read_module(path: Path) -> ast.Module | None:
    """Parse *path*, or return ``None`` when it cannot be parsed."""
    try:
        return ast.parse(path.read_text())
    except SyntaxError:
        return None


def find_unparseable_modules(root: Path) -> list[str]:
    """Return the modules the sweep could not parse, so they are reported rather than skipped."""
    return [
        path.relative_to(root).as_posix() for path in sorted(root.rglob("*.py")) if read_module(path) is None
    ]


def walk_defer_sites(root: Path) -> Iterator[tuple[Path, ast.expr]]:
    """Yield the ``trigger=`` expression of every ``self.defer(...)`` under *root*."""
    # Every file, not just operators/ and sensors/: ``defer`` is a BaseOperator method, so a site
    # can appear anywhere, and a directory filter would drop a nested subpackage without saying so.
    for path in sorted(root.rglob("*.py")):
        tree = read_module(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (
                isinstance(func, ast.Attribute)
                and func.attr == "defer"
                and isinstance(func.value, ast.Name)
                and func.value.id == "self"
            ):
                continue
            trigger = next((kw.value for kw in node.keywords if kw.arg == "trigger"), None)
            if trigger is not None:
                yield path, trigger


def find_defer_sites(root: Path) -> list[tuple[str, int, str, list[str]]]:
    """Collect every ``self.defer(trigger=SomeTrigger(...))`` under *root*."""
    sites: list[tuple[str, int, str, list[str]]] = []
    for path, trigger in walk_defer_sites(root):
        # The trigger may be built inline, or picked between in a conditional expression, so take
        # every construction the expression can yield rather than assuming a single call.
        for call, name in resolve_trigger_constructions(trigger) or ():
            if name in UNCONFIGURABLE_TRIGGERS:
                continue
            passed = {kw.arg for kw in call.keywords if kw.arg}
            sites.append(
                (
                    path.relative_to(root).as_posix(),
                    call.lineno,
                    name,
                    [parameter for parameter in HOOK_CONFIGURATION if parameter not in passed],
                )
            )
    return sites


def find_unreadable_defer_sites(root: Path) -> set[tuple[str, str]]:
    """Return defer sites whose trigger expression cannot be resolved to the constructions it yields."""
    return {
        (path.relative_to(root).as_posix(), ast.unparse(trigger))
        for path, trigger in walk_defer_sites(root)
        if resolve_trigger_constructions(trigger) is None
    }


def find_hand_built_hooks(root: Path) -> list[tuple[str, int, str, list[str]]]:
    """Collect every hook constructed directly inside a trigger module under *root*."""
    sites: list[tuple[str, int, str, list[str]]] = []
    for path in sorted((root / "triggers").rglob("*.py")):
        tree = read_module(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            # Match a module-qualified ``module.SomeHook(...)`` as well as a bare name.
            hook = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
            if not hook.endswith("Hook"):
                continue
            passed = {keyword.arg for keyword in node.keywords if keyword.arg}
            # AwsGenericHook names the botocore config ``config``.
            if "config" in passed:
                passed.add("botocore_config")
            sites.append(
                (
                    path.relative_to(root).as_posix(),
                    node.lineno,
                    hook,
                    [parameter for parameter in HOOK_CONFIGURATION if parameter not in passed],
                )
            )
    return sites


def collect_errors(root: Path) -> list[str]:
    """Return one message per violation, empty when the provider is clean."""
    errors: list[str] = []

    for source in find_unparseable_modules(root):
        errors.append(f"{source} could not be parsed, so the check cannot see what it defers to.")

    defer_sites = find_defer_sites(root)
    if not defer_sites:
        errors.append(f"No self.defer(trigger=...) calls found under {root}; the check is not looking at it.")

    unreadable = find_unreadable_defer_sites(root)
    for source, expression in sorted(unreadable - UNREADABLE_DEFER_SITES):
        errors.append(
            f"{source} defers to '{expression}', which cannot be read statically. "
            f"Add it to UNREADABLE_DEFER_SITES so the site is acknowledged rather than skipped."
        )
    for source, expression in sorted(UNREADABLE_DEFER_SITES - unreadable):
        errors.append(
            f"{source} no longer defers to the unreadable '{expression}'. "
            f"Drop it from UNREADABLE_DEFER_SITES."
        )

    for source, line, trigger, missing in defer_sites:
        if (source, trigger) in PENDING_MIGRATION:
            if not missing:
                errors.append(
                    f"{source}:{line} now passes its hook configuration to {trigger}. "
                    f"Drop it from PENDING_MIGRATION so the site stays covered."
                )
            continue
        if missing:
            errors.append(
                f"{source}:{line} defers to {trigger} without passing {', '.join(missing)}. "
                f"The triggerer builds its own hook, so anything not passed here is lost."
            )

    for source, line, hook, missing in find_hand_built_hooks(root):
        if (source, hook) in HAND_BUILT_HOOK_EXCEPTIONS:
            if not missing:
                errors.append(
                    f"{source}:{line} now configures {hook}. "
                    f"Drop it from HAND_BUILT_HOOK_EXCEPTIONS so the site stays covered."
                )
            continue
        if missing:
            errors.append(
                f"{source}:{line} builds {hook} without {', '.join(missing)}. "
                f"It reaches AWS with boto3 defaults while the trigger's own hook does not."
            )

    return errors


def main() -> int:
    errors = collect_errors(AWS_ROOT)
    for error in errors:
        console.print(f"[red]{error}[/]")
    if errors:
        console.print(
            f"\n[red]Found {len(errors)} deferred hook configuration problem(s).[/]\n"
            "A deferrable task builds its hook again in the triggerer, from what the operator "
            "passed to the trigger. Anything not passed there is lost.\n"
        )
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
