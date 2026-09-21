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
from collections.abc import Iterator
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


def read_trigger_name(call: ast.Call) -> str | None:
    """The trigger class a construction names, or ``None`` when the callee cannot be read."""
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def trigger_constructions(expr: ast.expr) -> list[tuple[ast.Call, str]] | None:
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
            resolved = trigger_constructions(branch)
            if resolved is None:
                return None
            constructions.extend(resolved)
        return constructions
    return None


def walk_defer_sites() -> Iterator[tuple[Path, ast.expr]]:
    """Yield the ``trigger=`` expression of every ``self.defer(...)`` in the provider."""
    # Every file, not just operators/ and sensors/: ``defer`` is a BaseOperator method, so a site
    # can appear anywhere, and a directory filter would drop a nested subpackage without saying so.
    for path in sorted(AWS_ROOT.rglob("*.py")):
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
            if trigger is not None:
                yield path, trigger


def find_defer_sites() -> list[tuple[str, int, str, list[str]]]:
    """Collect every ``self.defer(trigger=SomeTrigger(...))`` in the provider."""
    sites: list[tuple[str, int, str, list[str]]] = []
    for path, trigger in walk_defer_sites():
        # The trigger may be built inline, or picked between in a conditional expression, so take
        # every construction the expression can yield rather than assuming a single call.
        for call, name in trigger_constructions(trigger) or ():
            if name in UNCONFIGURABLE_TRIGGERS:
                continue
            passed = {kw.arg for kw in call.keywords if kw.arg}
            sites.append(
                (
                    path.relative_to(AWS_ROOT).as_posix(),
                    call.lineno,
                    name,
                    [p for p in HOOK_CONFIGURATION if p not in passed],
                )
            )
    return sites


def find_unreadable_defer_sites() -> set[tuple[str, str]]:
    """Defer sites whose trigger expression cannot be resolved to the constructions it yields."""
    return {
        (path.relative_to(AWS_ROOT).as_posix(), ast.unparse(trigger))
        for path, trigger in walk_defer_sites()
        if trigger_constructions(trigger) is None
    }


DEFER_SITES = find_defer_sites()


def test_defer_sites_are_discovered():
    assert DEFER_SITES, f"no self.defer(trigger=...) calls found under {AWS_ROOT}"


def test_no_defer_site_escapes_the_check():
    """A defer site whose trigger cannot be read statically must be acknowledged, not skipped."""
    assert find_unreadable_defer_sites() == UNREADABLE_DEFER_SITES


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        pytest.param("SomeTrigger(x=1)", 1, id="call"),
        pytest.param("A() if flag else B()", 2, id="conditional-both-readable"),
        pytest.param("trigger", None, id="bare-name"),
        pytest.param("self._trigger", None, id="attribute"),
        pytest.param("triggers[kind]", None, id="subscript"),
        pytest.param("A() if flag else self._trigger", None, id="conditional-one-unreadable"),
        pytest.param("TRIGGERS[kind](x=1)", None, id="unnameable-callee"),
        pytest.param("module.SomeTrigger(x=1)", 1, id="module-qualified-callee"),
    ],
)
def test_unreadable_trigger_expressions_resolve_to_none(expression, expected):
    """Anything the sweep cannot resolve must report None so the site is forced onto the allowlist."""
    constructions = trigger_constructions(ast.parse(expression, mode="eval").body)

    assert (constructions if constructions is None else len(constructions)) == expected


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
    """The hook a trigger names must accept what ``_hook_parameters`` will pass it."""
    if trigger_class.hook is not AwsBaseWaiterTrigger.hook:
        pytest.skip(f"{trigger_class.__name__} builds its hook by hand")
    inspect.signature(trigger_class.aws_hook_class).bind_partial(
        aws_conn_id=None, region_name=None, verify=None, config=None
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
            missing = [name for name in HOOK_CONFIGURATION if name not in passed]
            sites.append((path.name, node.lineno, hook, missing))
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
