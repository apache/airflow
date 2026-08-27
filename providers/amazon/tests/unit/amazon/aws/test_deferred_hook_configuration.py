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
from __future__ import annotations

import ast
import importlib
import inspect
import pkgutil
from pathlib import Path

import pytest

import airflow.providers.amazon.aws as aws_module
import airflow.providers.amazon.aws.triggers as triggers_module
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

AWS_ROOT = Path(inspect.getfile(aws_module)).parent
HOOK_CONFIGURATION = ("region_name", "verify", "botocore_config")

# A deferrable task builds its hook twice: once in the worker, once in the triggerer. Unless the
# operator hands its hook configuration to the trigger, the triggerer silently falls back to boto3
# defaults -- a different region, different SSL verification, different timeouts.
UNCONFIGURABLE_TRIGGERS = frozenset(
    {
        # Not an AwsBaseWaiterTrigger: its hook is addressed by execution name, and takes no
        # connection parameters at all.
        "SageMakerNotebookJobTrigger",
        # A KubernetesPodTrigger; it reaches the pod through a kubeconfig, not a boto3 client.
        "EksPodTrigger",
    }
)

# Sites whose trigger is built elsewhere and only referenced here, so the class cannot be read off
# the call. Kept explicit so that a new unreadable site fails the suite instead of being skipped.
UNREADABLE_DEFER_SITES = frozenset({("operators/eks.py", "trigger")})

# Services carved out as Contributors Workshop tasks, so their triggers are still unmigrated. Each
# entry is one self-contained contribution: widen the trigger's __init__, set aws_hook_class, pass
# the parameters at the call site, then delete the entry here. The test asserts an entry is still
# needed, so the allowlist cannot outlive the work it tracks.
PENDING_MIGRATION = frozenset(
    {
        ("sensors/batch.py", "BatchJobTrigger"),
        ("sensors/opensearch_serverless.py", "OpenSearchServerlessCollectionActiveTrigger"),
    }
)


def trigger_constructions(expr: ast.expr) -> list[ast.Call]:
    """Resolve a ``trigger=`` expression to the constructions it can evaluate to."""
    if isinstance(expr, ast.Call):
        return [expr]
    if isinstance(expr, ast.IfExp):
        return trigger_constructions(expr.body) + trigger_constructions(expr.orelse)
    return []


def find_defer_sites() -> list[tuple[str, int, str, list[str]]]:
    """Collect every ``self.defer(trigger=SomeTrigger(...))`` in the provider."""
    sites = []
    for path in sorted(AWS_ROOT.rglob("*.py")):
        if path.parent.name not in ("operators", "sensors"):
            continue
        for node in ast.walk(ast.parse(path.read_text())):
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
            if trigger is None:
                continue
            # The trigger may be built inline, or picked between in a conditional expression, so
            # take every construction the expression can yield rather than assuming a single call.
            for call in trigger_constructions(trigger):
                name = (
                    call.func.attr if isinstance(call.func, ast.Attribute) else getattr(call.func, "id", "")
                )
                if name in UNCONFIGURABLE_TRIGGERS:
                    continue
                passed = {kw.arg for kw in call.keywords if kw.arg}
                sites.append(
                    (
                        str(path.relative_to(AWS_ROOT)),
                        call.lineno,
                        name,
                        [p for p in HOOK_CONFIGURATION if p not in passed],
                    )
                )
    return sites


def find_unreadable_defer_sites() -> set[tuple[str, str]]:
    """Defer sites whose trigger is a bare reference, so its class cannot be read statically."""
    unreadable = set()
    for path in sorted(AWS_ROOT.rglob("*.py")):
        if path.parent.name not in ("operators", "sensors"):
            continue
        for node in ast.walk(ast.parse(path.read_text())):
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
            if isinstance(trigger, ast.Name):
                unreadable.add((str(path.relative_to(AWS_ROOT)), trigger.id))
    return unreadable


DEFER_SITES = find_defer_sites()


def test_defer_sites_are_discovered():
    assert DEFER_SITES, f"no self.defer(trigger=...) calls found under {AWS_ROOT}"


def test_no_defer_site_escapes_the_check():
    """A defer site whose trigger cannot be read statically must be acknowledged, not skipped."""
    assert find_unreadable_defer_sites() == UNREADABLE_DEFER_SITES


@pytest.mark.parametrize(
    ("source", "line", "trigger", "missing"),
    DEFER_SITES,
    ids=[f"{source}:{line}" for source, line, _, _ in DEFER_SITES],
)
def test_deferred_trigger_receives_hook_configuration(source, line, trigger, missing):
    if (source, trigger) in PENDING_MIGRATION:
        assert missing, (
            f"{source}:{line} now passes its hook configuration to {trigger}. "
            f"Drop it from PENDING_MIGRATION so the site stays covered."
        )
        pytest.skip(f"{source} is a Contributors Workshop task; see PENDING_MIGRATION")

    assert not missing, (
        f"{source}:{line} defers to {trigger} without passing {', '.join(missing)}. "
        f"The triggerer builds its own hook, so anything not passed here is lost."
    )


def find_waiter_triggers() -> list[type[AwsBaseWaiterTrigger]]:
    """Import every trigger module, then walk the subclass tree."""
    for module in pkgutil.iter_modules(triggers_module.__path__):
        importlib.import_module(f"{triggers_module.__name__}.{module.name}")

    found: set[type[AwsBaseWaiterTrigger]] = set()
    pending = [AwsBaseWaiterTrigger]
    while pending:
        for subclass in pending.pop().__subclasses__():
            if subclass not in found:
                found.add(subclass)
                pending.append(subclass)
    return sorted(found, key=lambda cls: cls.__name__)


@pytest.mark.parametrize(
    "trigger_class",
    find_waiter_triggers(),
    ids=lambda cls: cls.__name__,
)
def test_waiter_trigger_can_build_a_hook(trigger_class):
    """Every waiter trigger must declare ``aws_hook_class`` or provide its own ``hook()``."""
    assert hasattr(trigger_class, "aws_hook_class") or "hook" in vars(trigger_class), (
        f"{trigger_class.__name__} sets neither aws_hook_class nor hook(); "
        f"building its hook would fail at runtime."
    )


# A trigger may build a second hook by hand for a side channel -- streaming CloudWatch logs, most
# often -- alongside the one ``aws_hook_class`` gives it. That hook talks to AWS too, so it needs
# the same configuration; a trigger whose job client verifies TLS while its log client does not is
# the same bug in miniature.
HAND_BUILT_HOOK_EXCEPTIONS = frozenset(
    {
        # Addressed by execution name; takes no connection parameters at all.
        ("sagemaker_unified_studio.py", "SageMakerNotebookHook"),
        # EksPodOperator is a KubernetesPodOperator: it carries no verify or botocore_config to pass.
        ("eks.py", "EksHook"),
        # Contributors Workshop task; see PENDING_MIGRATION.
        ("opensearch_serverless.py", "OpenSearchServerlessHook"),
    }
)


def find_hand_built_hooks() -> list[tuple[str, int, str, list[str]]]:
    """Collect every hook constructed directly inside a trigger module."""
    sites = []
    for path in sorted((AWS_ROOT / "triggers").rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id.endswith("Hook")
            ):
                continue
            passed = {keyword.arg for keyword in node.keywords if keyword.arg}
            # AwsGenericHook names the botocore config ``config``.
            if "config" in passed:
                passed.add("botocore_config")
            missing = [name for name in HOOK_CONFIGURATION if name not in passed]
            sites.append((path.name, node.lineno, node.func.id, missing))
    return sites


HAND_BUILT_HOOKS = find_hand_built_hooks()


@pytest.mark.parametrize(
    ("source", "line", "hook", "missing"),
    HAND_BUILT_HOOKS,
    ids=[f"{source}:{line}" for source, line, _, _ in HAND_BUILT_HOOKS],
)
def test_hand_built_trigger_hook_receives_configuration(source, line, hook, missing):
    """A hook a trigger builds itself must carry the same configuration as its main hook."""
    if (source, hook) in HAND_BUILT_HOOK_EXCEPTIONS:
        assert missing, (
            f"{source}:{line} now configures {hook}. "
            f"Drop it from HAND_BUILT_HOOK_EXCEPTIONS so the site stays covered."
        )
        pytest.skip(f"{source} builds {hook} with nothing to configure")

    assert not missing, (
        f"{source}:{line} builds {hook} without {', '.join(missing)}. "
        f"It reaches AWS with boto3 defaults while the trigger's own hook does not."
    )
