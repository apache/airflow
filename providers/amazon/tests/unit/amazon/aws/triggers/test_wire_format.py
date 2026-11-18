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
A task deferred on amazon provider 9.x must resume after the provider is upgraded or rolled back.

``wire_format_9_x.py`` holds what 9.x stores for a deferred task, and the rules for changing it.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import pkgutil
from collections import defaultdict
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import airflow.providers.amazon
from airflow.providers.amazon.aws import triggers
from airflow.triggers.base import BaseTrigger

from unit.amazon.aws.triggers.wire_format_9_x import (
    BROKEN_RESUME_METHODS,
    RESUME_METHODS,
    TRIGGER_ROWS,
    TRIGGERS_WITHOUT_ROWS,
    UNSERIALIZED_TRIGGER_PARAMS,
)


def load_class(classpath: str) -> type:
    module_name, _, class_name = classpath.rpartition(".")
    return getattr(importlib.import_module(module_name), class_name)


def find_trigger_classes() -> dict[str, type[BaseTrigger]]:
    found: dict[str, type[BaseTrigger]] = {}
    for module_info in pkgutil.iter_modules(triggers.__path__, f"{triggers.__name__}."):
        module = importlib.import_module(module_info.name)
        for name, member in inspect.getmembers(module, inspect.isclass):
            if issubclass(member, BaseTrigger) and member.__module__ == module.__name__:
                found[f"{module.__name__}.{name}"] = member
    return found


def find_deferrals() -> set[tuple[str, str, tuple[str, ...]]]:
    """Find every ``self.defer()`` call in the provider as (class path, method name, ``next_kwargs`` keys)."""
    package_root = Path(airflow.providers.amazon.__file__).parent
    found: set[tuple[str, str, tuple[str, ...]]] = set()
    for path in package_root.rglob("*.py"):
        module_parts = path.relative_to(package_root).with_suffix("").parts
        module_name = ".".join((airflow.providers.amazon.__name__, *module_parts))
        class_nodes = [node for node in ast.parse(path.read_text()).body if isinstance(node, ast.ClassDef)]
        for class_node in class_nodes:
            for node in ast.walk(class_node):
                if not (isinstance(node, ast.Call) and get_self_attribute(node.func) == "defer"):
                    continue
                arguments = {keyword.arg: keyword.value for keyword in node.keywords}
                where = f"{path}:{node.lineno}"
                next_kwargs = get_next_kwargs_keys(arguments.get("kwargs"), where)
                method_name = arguments["method_name"]
                if isinstance(method_name, ast.Constant) and isinstance(method_name.value, str):
                    found.add((f"{module_name}.{class_node.name}", method_name.value, next_kwargs))
                elif attribute := get_self_attribute(method_name):
                    # The method name is a class attribute, so every subclass may resume somewhere else.
                    module = importlib.import_module(module_name)
                    owner = getattr(module, class_node.name)
                    for name in (candidate.name for candidate in class_nodes):
                        if issubclass(subclass := getattr(module, name), owner):
                            found.add((f"{module_name}.{name}", getattr(subclass, attribute), next_kwargs))
                else:
                    raise NotImplementedError(f"{where}: cannot tell which method this defers to")
    return found


def get_self_attribute(node: ast.AST) -> str | None:
    """Return ``name`` when the node is ``self.name``."""
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "self":
        return node.attr
    return None


def get_next_kwargs_keys(node: ast.expr | None, where: str) -> tuple[str, ...]:
    if node is None or (isinstance(node, ast.Constant) and node.value is None):
        return ()
    if isinstance(node, ast.Dict):
        keys = [
            key.value for key in node.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)
        ]
        if len(keys) == len(node.keys):
            return tuple(sorted(keys))
    raise NotImplementedError(f"{where}: cannot tell which next_kwargs this defers with")


@pytest.mark.parametrize("row", TRIGGER_ROWS, ids=lambda row: row["classpath"].rpartition(".")[2])
def test_trigger_rebuilds_from_its_9_x_row(row: dict[str, Any]):
    trigger = load_class(row["classpath"])(**row["kwargs"])

    # Comparing the values, not only loading the trigger, catches a kwarg that ``**kwargs`` swallows.
    assert trigger.serialize() == (row["classpath"], {**row["kwargs"], **row.get("reserialized", {})})


def test_every_trigger_has_a_row():
    with_row = {row["classpath"] for row in TRIGGER_ROWS}

    assert set(find_trigger_classes()) - with_row == TRIGGERS_WITHOUT_ROWS


def test_rows_hold_every_trigger_parameter():
    serialized: defaultdict[str, set[str]] = defaultdict(set)
    for row in TRIGGER_ROWS:
        serialized[row["classpath"]].update(row["kwargs"])

    unserialized: dict[str, set[str]] = {}
    for classpath, trigger_class in find_trigger_classes().items():
        if classpath in TRIGGERS_WITHOUT_ROWS:
            continue
        named_parameters = {
            name
            for name, parameter in inspect.signature(trigger_class).parameters.items()
            if parameter.kind not in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
        }
        if missing := named_parameters - serialized[classpath]:
            unserialized[classpath] = missing

    assert unserialized == UNSERIALIZED_TRIGGER_PARAMS


@pytest.mark.parametrize(
    ("classpath", "method_name", "next_kwargs"),
    [
        pytest.param(
            classpath,
            method_name,
            next_kwargs,
            id=f"{classpath.rpartition('.')[2]}.{method_name}",
            marks=[pytest.mark.xfail(raises=AttributeError, strict=True)]
            if (classpath, method_name) in BROKEN_RESUME_METHODS
            else [],
        )
        for classpath, method_name, next_kwargs in RESUME_METHODS
    ],
)
def test_task_resumes_into_its_9_x_method(classpath: str, method_name: str, next_kwargs: tuple[str, ...]):
    method = getattr(load_class(classpath), method_name)

    # The task runner resumes a deferred task with ``method(context, event=..., **next_kwargs)``.
    inspect.signature(method).bind(
        mock.sentinel.operator,
        mock.sentinel.context,
        event=mock.sentinel.event,
        **dict.fromkeys(next_kwargs),
    )


def test_every_deferral_has_a_resume_method_row():
    assert find_deferrals() - set(RESUME_METHODS) == set()
