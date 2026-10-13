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
# requires-python = ">=3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""Check configured task decorator stubs against their operator and factory parameter names.

Currently covers kubernetes and kubernetes_cmd. Only explicitly configured signatures are read;
inherited BaseOperator arguments, annotations, and defaults are outside this check's scope.
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path

from common_prek_utils import (
    AIRFLOW_PROVIDERS_ROOT_PATH,
    AIRFLOW_ROOT_PATH,
    AIRFLOW_TASK_SDK_SOURCES_PATH,
    console,
)


@dataclass(frozen=True)
class FunctionReference:
    """Identify a source function or method using a repository-relative path."""

    path: Path
    name: str
    class_name: str | None = None


@dataclass(frozen=True)
class DecoratorSpec:
    """Define a decorator's signature sources and internally supplied operator arguments."""

    name: str
    operator: FunctionReference
    decorated_operator: FunctionReference
    factory: FunctionReference
    internally_supplied: frozenset[str]


SDK_PACKAGE = AIRFLOW_TASK_SDK_SOURCES_PATH.relative_to(AIRFLOW_ROOT_PATH) / "airflow/sdk"
STUB_PATH = SDK_PACKAGE / "definitions/decorators/__init__.pyi"
SHARED_FACTORY = FunctionReference(SDK_PACKAGE / "bases/decorator.py", "task_decorator_factory")
KUBERNETES_PACKAGE = (
    AIRFLOW_PROVIDERS_ROOT_PATH.relative_to(AIRFLOW_ROOT_PATH)
    / "cncf/kubernetes/src/airflow/providers/cncf/kubernetes"
)
POD_OPERATOR = FunctionReference(KUBERNETES_PACKAGE / "operators/pod.py", "__init__", "KubernetesPodOperator")
DECORATORS = (
    DecoratorSpec(
        name="kubernetes",
        operator=POD_OPERATOR,
        decorated_operator=FunctionReference(
            KUBERNETES_PACKAGE / "decorators/kubernetes.py", "__init__", "_KubernetesDecoratedOperator"
        ),
        factory=FunctionReference(KUBERNETES_PACKAGE / "decorators/kubernetes.py", "kubernetes_task"),
        internally_supplied=frozenset({"cmds"}),
    ),
    DecoratorSpec(
        name="kubernetes_cmd",
        operator=POD_OPERATOR,
        decorated_operator=FunctionReference(
            KUBERNETES_PACKAGE / "decorators/kubernetes_cmd.py", "__init__", "_KubernetesCmdDecoratedOperator"
        ),
        factory=FunctionReference(KUBERNETES_PACKAGE / "decorators/kubernetes_cmd.py", "kubernetes_cmd_task"),
        internally_supplied=frozenset({"cmds", "arguments"}),
    ),
)
FRAMEWORK_PARAMETERS = frozenset(
    {"python_callable", "op_args", "op_kwargs", "kwargs_to_upstream", "decorated_operator_class"}
)


def extract_parameter_names(function: ast.FunctionDef) -> set[str]:
    """Extract explicit parameter names, excluding self, cls, and variadic arguments."""
    args = function.args
    return {arg.arg for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs)} - {"self", "cls"}


def get_function(
    root: Path, reference: FunctionReference, *, configuration_overload: bool = False
) -> ast.FunctionDef:
    """Find a unique function or method by parsing its source without importing it.

    For stub configuration overloads, ignore signatures accepting ``python_callable``.
    Raise ValueError for missing or ambiguous definitions.
    """
    path = root / reference.path
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    body = tree.body
    if reference.class_name is not None:
        classes = [n for n in body if isinstance(n, ast.ClassDef) and n.name == reference.class_name]
        if len(classes) != 1:
            raise ValueError(
                f"{reference.path}: expected one class {reference.class_name}, found {len(classes)}"
            )
        body = classes[0].body

    functions = [n for n in body if isinstance(n, ast.FunctionDef) and n.name == reference.name]
    if configuration_overload:
        functions = [n for n in functions if "python_callable" not in extract_parameter_names(n)]
    if len(functions) != 1:
        owner = f"{reference.class_name}." if reference.class_name else ""
        kind = "configuration overload" if configuration_overload else "function"
        raise ValueError(
            f"{reference.path}: expected one {kind} {owner}{reference.name}, found {len(functions)}"
        )
    return functions[0]


def collect_errors(
    root: Path = AIRFLOW_ROOT_PATH, specs: tuple[DecoratorSpec, ...] = DECORATORS
) -> list[str]:
    """Report parameter mismatches between each stub and its configured signatures.

    Combine operator, wrapper, and factory parameters after applying framework and
    internally supplied exclusions. Include unreadable or ambiguous inputs as errors.
    """
    errors = []
    for spec in specs:
        stub_reference = FunctionReference(STUB_PATH, spec.name, "TaskDecoratorCollection")
        try:
            references = (spec.operator, spec.decorated_operator, spec.factory, SHARED_FACTORY)
            functions = [get_function(root, reference) for reference in references]
            operator_parameters = extract_parameter_names(functions[0])
            obsolete_exclusions = spec.internally_supplied - operator_parameters
            if obsolete_exclusions:
                raise ValueError(
                    f"{spec.operator.path}:{functions[0].lineno}: internally supplied parameters "
                    f"no longer exist on {spec.operator.class_name}: {', '.join(sorted(obsolete_exclusions))}"
                )

            expected = {}
            for reference, function in zip(references, functions):
                names = extract_parameter_names(function) - FRAMEWORK_PARAMETERS
                if reference == spec.operator:
                    names -= spec.internally_supplied
                for name in names:
                    expected[name] = f"{reference.path}:{function.lineno}"

            stub = get_function(root, stub_reference, configuration_overload=True)
            declared = extract_parameter_names(stub)
        except (OSError, SyntaxError, ValueError) as error:
            errors.append(f"@task.{spec.name}: {error}")
            continue

        location = f"{stub_reference.path}:{stub.lineno}: @task.{spec.name}"
        missing = expected.keys() - declared
        if missing:
            sources = ", ".join(sorted({expected[name] for name in missing}))
            errors.append(
                f"{location} is missing parameters: {', '.join(sorted(missing))}. "
                f"Declared in {sources}. Update {STUB_PATH}."
            )
        unexpected = declared - expected.keys()
        if unexpected:
            errors.append(
                f"{location} declares unexpected parameters: {', '.join(sorted(unexpected))}. "
                f"Update {STUB_PATH} to match {spec.operator.path} and its decorator/factories."
            )
    return errors


def main(root: Path = AIRFLOW_ROOT_PATH) -> int:
    """Print check failures and return 1 on failure or 0 on success."""
    errors = collect_errors(root)
    for error in errors:
        console.print(error, markup=False)
    return int(bool(errors))


if __name__ == "__main__":
    sys.exit(main())
