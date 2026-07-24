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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Any

from common_prek_utils import AIRFLOW_ROOT_PATH
from rich.console import Console
from rich.markup import escape

console = Console(color_system="standard", width=200)
# Pre-existing violations exempted from the checks; burn-down tracked at
# https://github.com/apache/airflow/issues/70296
EXEMPTIONS_FILE = "generated/validate_operators_init_exemptions.txt"
EXEMPTIONS_PATH = AIRFLOW_ROOT_PATH / EXEMPTIONS_FILE
BASE_CLASS_NAME_SUFFIXES = ("BaseOperator", "BaseSensorOperator")
# Helper callables used as template_fields values, mapped to the fields the helper injects
# on top of the explicit arguments. Injected fields are owned and assigned by the base class,
# so only the explicit arguments are fields the defining class must assign itself.
TEMPLATE_FIELD_HELPERS: dict[str, frozenset[str]] = {
    "aws_template_fields": frozenset({"aws_conn_id", "region_name", "verify"}),
}


def _resolve_base_name(base: ast.expr) -> str:
    """
    Resolve a base-class expression to its plain name.

    Unwraps subscripted generics (``AwsBaseOperator[EmrHook]``) and attribute access
    (``module.BaseOperator``).

    :param base: The base-class expression node.
    :return: The resolved name, or an empty string when it cannot be resolved.
    """
    if isinstance(base, ast.Subscript):
        base = base.value
    if isinstance(base, ast.Attribute):
        return base.attr
    if isinstance(base, ast.Name):
        return base.id
    return ""


def _is_operator(class_node: ast.ClassDef) -> bool:
    """
    Check if a given class node is an operator or sensor, based on the string suffix of the
    base IDs (ends with "BaseOperator" or "BaseSensorOperator").
    TODO: Enhance this function to work with nested inheritance trees through dynamic imports.

    :param class_node: The class node to check.
    :return: True if the class definition is of an operator, False otherwise.
    """
    return any(_resolve_base_name(base).endswith(BASE_CLASS_NAME_SUFFIXES) for base in class_node.bases)


def _extract_field_names(value: ast.expr | None) -> list[str] | None:
    """
    Extract template-field names from a ``template_fields`` value expression.

    Supports a tuple of constants and known helper calls with constant arguments
    (e.g. ``aws_template_fields("s3_bucket", "s3_key")``). For helper calls, fields the
    helper injects on behalf of the base class are excluded — see ``TEMPLATE_FIELD_HELPERS``.

    :param value: The value expression assigned to ``template_fields``.
    :return: The extracted field names, or None if the expression shape is not supported.
    """
    if isinstance(value, ast.Tuple):
        return [str(elt.value) for elt in value.elts if isinstance(elt, ast.Constant)]
    if isinstance(value, ast.Call):
        injected = TEMPLATE_FIELD_HELPERS.get(_resolve_base_name(value.func))
        if injected is not None:
            args = [str(arg.value) for arg in value.args if isinstance(arg, ast.Constant)]
            return [arg for arg in args if arg not in injected]
    return None


def _extract_template_fields(class_node: ast.ClassDef) -> list[str]:
    """
    This method takes a class node as input and extracts the template fields from it.
    Template fields are identified by an assignment statement where the target is a variable
    named "template_fields" and the value is a tuple of constants or a known helper call.

    :param class_node: The class node representing the class for which template fields need to be extracted.
    :return: A list of template fields extracted from the class node.
    """
    for class_item in class_node.body:
        if isinstance(class_item, ast.Assign):
            for target in class_item.targets:
                if isinstance(target, ast.Name) and target.id == "template_fields":
                    fields = _extract_field_names(class_item.value)
                    if fields is not None:
                        return fields
        elif isinstance(class_item, ast.AnnAssign):
            if isinstance(class_item.target, ast.Name) and class_item.target.id == "template_fields":
                fields = _extract_field_names(class_item.value)
                if fields is not None:
                    return fields
    return []


def _handle_parent_constructor_kwargs(
    template_fields: list[str],
    ctor_stmt: ast.stmt,
    missing_assignments: list[str],
    invalid_assignments: list[str],
) -> list[str]:
    """
    This method checks if template fields are correctly assigned in a call to class parent's
    constructor call.
    It handles both the detection of missing assignments and invalid assignments.
    It assumes that if the call is valid - the parent class will correctly assign the template
    field.
    TODO: Enhance this function to work with nested inheritance trees through dynamic imports.

    :param missing_assignments: list[str] - List of template fields that have not been assigned a value.
    :param ctor_stmt: ast.Expr - AST node representing the constructor statement.
    :param invalid_assignments: list[str] - List of template fields that have been assigned incorrectly.
    :param template_fields: list[str] - List of template fields to be assigned.

    :return: list[str] - List of template fields that are still missing assignments.
    """
    if isinstance(ctor_stmt, ast.Expr):
        if (
            isinstance(ctor_stmt.value, ast.Call)
            and isinstance(ctor_stmt.value.func, ast.Attribute)
            and isinstance(ctor_stmt.value.func.value, ast.Call)
            and isinstance(ctor_stmt.value.func.value.func, ast.Name)
            and ctor_stmt.value.func.value.func.id == "super"
        ):
            for arg in ctor_stmt.value.keywords:
                if arg.arg is not None and arg.arg in template_fields:
                    if not isinstance(arg.value, ast.Name) or arg.arg != arg.value.id:
                        invalid_assignments.append(arg.arg)
            assigned_targets = [arg.arg for arg in ctor_stmt.value.keywords if arg.arg is not None]
            return list(set(missing_assignments) - set(assigned_targets))
    return missing_assignments


def _handle_constructor_statement(
    template_fields: list[str],
    ctor_stmt: ast.stmt,
    missing_assignments: list[str],
    invalid_assignments: list[str],
) -> list[str]:
    """
    This method handles a single constructor statement by doing the following actions:
        1. Removing assigned fields of template_fields from missing_assignments.
        2. Detecting invalid assignments of template fields and adding them to invalid_assignments.

    :param template_fields: Tuple of template fields.
    :param ctor_stmt: Constructor statement (for example, self.field_name = param_name)
    :param missing_assignments: List of missing assignments.
    :param invalid_assignments: List of invalid assignments.
    :return: List of missing assignments after handling the assigned targets.
    """
    assigned_template_fields: list[str] = []
    if isinstance(ctor_stmt, ast.Assign):
        if isinstance(ctor_stmt.targets[0], ast.Attribute):
            for target in ctor_stmt.targets:
                if isinstance(target, ast.Attribute) and target.attr in template_fields:
                    if isinstance(ctor_stmt.value, ast.IfExp) and _is_value_preserving_ternary(
                        ctor_stmt.value, target.attr
                    ):
                        _handle_assigned_field(
                            assigned_template_fields, invalid_assignments, target, ctor_stmt.value.body
                        )
                    elif isinstance(ctor_stmt.value, ast.BoolOp) and isinstance(ctor_stmt.value.op, ast.Or):
                        _handle_assigned_field(
                            assigned_template_fields, invalid_assignments, target, ctor_stmt.value.values[0]
                        )
                    else:
                        _handle_assigned_field(
                            assigned_template_fields, invalid_assignments, target, ctor_stmt.value
                        )
        elif isinstance(ctor_stmt.targets[0], ast.Tuple) and isinstance(ctor_stmt.value, ast.Tuple):
            for target, value in zip(ctor_stmt.targets[0].elts, ctor_stmt.value.elts):
                if isinstance(target, ast.Attribute):
                    _handle_assigned_field(assigned_template_fields, invalid_assignments, target, value)
    elif isinstance(ctor_stmt, ast.AnnAssign):
        if isinstance(ctor_stmt.target, ast.Attribute) and ctor_stmt.target.attr in template_fields:
            _handle_assigned_field(
                assigned_template_fields, invalid_assignments, ctor_stmt.target, ctor_stmt.value
            )
    return list(set(missing_assignments) - set(assigned_template_fields))


def _handle_assigned_field(
    assigned_template_fields: list[str], invalid_assignments: list[str], target: ast.Attribute, value: Any
) -> None:
    """
    Handle an assigned field by its value.

    :param assigned_template_fields: A list to store the valid assigned fields.
    :param invalid_assignments: A list to store the invalid assignments.
    :param target: The target field.
    :param value: The value of the field.
    """
    if not isinstance(value, ast.Name) or target.attr != value.id:
        invalid_assignments.append(target.attr)
    else:
        assigned_template_fields.append(target.attr)


def _target_name(target: ast.expr) -> str | None:
    """
    Resolve an assignment target to the field name it binds.

    :param target: The assignment target node.
    :return: The attribute name for ``self.<name>`` targets, the identifier for bare-name
        targets, or None for anything else.
    """
    if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
        return target.attr
    if isinstance(target, ast.Name):
        return target.id
    return None


def _is_super_init_call(node: ast.Call) -> bool:
    """
    Check whether a call node is ``super().__init__(...)``.

    :param node: The call node to check.
    :return: True if the node calls ``__init__`` on a ``super()`` call.
    """
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "__init__"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Name)
        and node.func.value.func.id == "super"
    )


def _is_value_preserving_ternary(value: ast.IfExp, field: str) -> bool:
    """
    Check whether a ternary keeps the field value intact when it is set.

    Matches ``field if field else <default>`` and ``field if field is not None else <default>``
    — both equivalent to the sanctioned ``field or <default>`` defaulting idiom.

    :param value: The ternary expression node.
    :param field: The template field name.
    :return: True if the ternary only substitutes a default for an unset value.
    """
    if not (isinstance(value.body, ast.Name) and value.body.id == field):
        return False
    test = value.test
    if isinstance(test, ast.Name) and test.id == field:
        return True
    return (
        isinstance(test, ast.Compare)
        and isinstance(test.left, ast.Name)
        and test.left.id == field
        and len(test.ops) == 1
        and isinstance(test.ops[0], (ast.Is, ast.IsNot))
        and len(test.comparators) == 1
        and isinstance(test.comparators[0], ast.Constant)
        and test.comparators[0].value is None
    )


def _collect_sanctioned_uses(ctor: ast.FunctionDef, template_fields: list[str]) -> set[int]:
    """
    Collect the AST node ids of template-field reads that belong to sanctioned patterns.

    Sanctioned patterns are the ones the project documents as safe in a constructor:
    ``self.field = field``, ``self.field = field or <default>``, the equivalent
    value-preserving ternaries, the local rebind ``field = field or <default>``,
    tuple assignments pairing names one-to-one, and forwarding via
    ``super().__init__(field=field)``.

    :param ctor: The constructor function node.
    :param template_fields: The template fields of the class.
    :return: Set of ``id()``s of Name nodes participating in sanctioned patterns.
    """
    sanctioned: set[int] = set()

    def mark(value: ast.expr | None, field: str) -> None:
        if isinstance(value, ast.BoolOp) and isinstance(value.op, ast.Or):
            value = value.values[0]
        elif isinstance(value, ast.IfExp) and _is_value_preserving_ternary(value, field):
            for name_node in ast.walk(value.test):
                if isinstance(name_node, ast.Name) and name_node.id == field:
                    sanctioned.add(id(name_node))
            value = value.body
        if isinstance(value, ast.Name) and value.id == field:
            sanctioned.add(id(value))

    for node in ast.walk(ctor):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            pairs: list[tuple[ast.expr, ast.expr | None]]
            if len(targets) == 1 and isinstance(targets[0], ast.Tuple) and isinstance(node.value, ast.Tuple):
                pairs = list(zip(targets[0].elts, node.value.elts))
            else:
                pairs = [(target, node.value) for target in targets]
            for target, value in pairs:
                name = _target_name(target)
                if name is not None and name in template_fields:
                    mark(value, name)
        elif isinstance(node, ast.Call) and _is_super_init_call(node):
            for keyword in node.keywords:
                if keyword.arg is not None and keyword.arg in template_fields:
                    mark(keyword.value, keyword.arg)
    return sanctioned


def _check_constructor_field_logic(
    class_node: ast.ClassDef, template_fields: list[str], source_lines: list[str]
) -> int:
    """
    Check a class's constructor for logic applied to template fields.

    Template fields are rendered after the constructor runs, so any read of a template-field
    parameter (or ``self.<field>``) outside the sanctioned assignment/forwarding patterns —
    validation calls, conditionals, transformations, string interpolation — operates on the
    un-rendered Jinja expression and must move to ``execute()``.

    :param class_node: The AST node representing the class definition.
    :param template_fields: The template fields of the class.
    :param source_lines: The source lines of the file, for reporting.
    :return: The number of offending source lines found.
    """
    ctor = next(
        (item for item in class_node.body if isinstance(item, ast.FunctionDef) and item.name == "__init__"),
        None,
    )
    if ctor is None or not template_fields:
        return 0
    sanctioned = _collect_sanctioned_uses(ctor, template_fields)
    args = ctor.args
    # Only names bound in the constructor scope can refer to a template field; without this,
    # a field named e.g. "json" would false-positive on uses of the stdlib module.
    bound_names = {arg.arg for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs]}
    bound_names |= {
        node.id for node in ast.walk(ctor) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store)
    }
    # Parameter defaults evaluate at class-definition scope, where a name that matches a
    # template field (e.g. a field named "conf" vs. the configuration module) is not the field.
    in_defaults = {
        id(node)
        for default in [*args.defaults, *args.kw_defaults]
        if default is not None
        for node in ast.walk(default)
    }

    findings: dict[int, set[str]] = {}
    for node in ast.walk(ctor):
        if id(node) in in_defaults:
            continue
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            if node.id in template_fields and node.id in bound_names and id(node) not in sanctioned:
                findings.setdefault(node.lineno, set()).add(node.id)
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
            if isinstance(node.value, ast.Name) and node.value.id == "self" and node.attr in template_fields:
                findings.setdefault(node.lineno, set()).add(f"self.{node.attr}")

    if findings:
        console.print(
            f"{class_node.name}'s constructor applies logic to template fields. Template fields "
            f"are rendered after the constructor runs, so validation or transformation here acts "
            f"on the un-rendered Jinja expression and should move to execute():"
        )
        for lineno in sorted(findings):
            source = source_lines[lineno - 1].strip() if lineno <= len(source_lines) else ""
            console.print(
                f"[red]  line {lineno}: {escape(source)}  ({', '.join(sorted(findings[lineno]))})[/red]"
            )
    return len(findings)


def _check_constructor_template_fields(class_node: ast.ClassDef, template_fields: list[str]) -> int:
    """
    This method checks a class's constructor for missing or invalid assignments of template fields.
    When there isn't a constructor - it assumes that the template fields are defined in the parent's
    constructor correctly.
    TODO: Enhance this function to work with nested inheritance trees through dynamic imports.

    :param class_node: the AST node representing the class definition
    :param template_fields: a tuple of template fields
    :return: the number of invalid template fields found
    """
    count = 0
    class_name = class_node.name
    missing_assignments = template_fields.copy()
    invalid_assignments: list[str] = []
    init_flag: bool = False
    for class_item in class_node.body:
        if isinstance(class_item, ast.FunctionDef) and class_item.name == "__init__":
            init_flag = True
            for ctor_stmt in class_item.body:
                missing_assignments = _handle_parent_constructor_kwargs(
                    template_fields, ctor_stmt, missing_assignments, invalid_assignments
                )
                missing_assignments = _handle_constructor_statement(
                    template_fields, ctor_stmt, missing_assignments, invalid_assignments
                )

    if init_flag and missing_assignments:
        count += len(missing_assignments)
        console.print(
            f"{class_name}'s constructor lacks direct assignments for "
            f"instance members corresponding to the following template fields "
            f"(i.e., self.field_name = field_name or super.__init__(field_name=field_name, ...) ):"
        )
        console.print(f"[red]{missing_assignments}[/red]")

    if invalid_assignments:
        count += len(invalid_assignments)
        console.print(
            f"{class_name}'s constructor contains invalid assignments to the following instance "
            f"members that should be corresponding to template fields "
            f"(i.e., self.field_name = field_name):"
        )
        console.print(f"[red]{[f'self.{entry}' for entry in invalid_assignments]}[/red]")
    return count


def _load_exemptions() -> dict[str, set[str]]:
    """
    Load the exemption list for known violations that predate the constructor-logic check.

    Each non-comment line has the form ``<repo-relative-path>::<ClassName>``. Exempted classes
    are skipped by all checks; an exempted class with no findings fails as stale so the entry
    is removed in the same PR that fixes the class, until the list is empty.

    :return: Mapping of repo-relative file path to the exempted class names in that file.
    """
    exemptions: dict[str, set[str]] = {}
    if not EXEMPTIONS_PATH.exists():
        return exemptions
    for raw_line in EXEMPTIONS_PATH.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        entry_path, sep, class_name = line.partition("::")
        if sep and class_name:
            exemptions.setdefault(entry_path, set()).add(class_name)
    return exemptions


def _get_exempted_classes(path: str, exemptions: dict[str, set[str]]) -> set[str]:
    """
    Find the exempted class names for a checked file.

    Exemption entries store repo-relative paths while the hook may receive paths relative
    to another working directory, so entries are matched as path suffixes.

    :param path: The file path as passed to the script.
    :param exemptions: The loaded exemption mapping.
    :return: The exempted class names for this file, or an empty set.
    """
    resolved = Path(path).resolve().as_posix()
    for entry_path, classes in exemptions.items():
        if resolved == entry_path or resolved.endswith(f"/{entry_path}"):
            return classes
    return set()


def main():
    """
    Check missing or invalid template fields in constructors of providers' operators.

    :return: The total number of errors found.
    """
    err = 0
    exemptions = _load_exemptions()
    for path in sys.argv[1:]:
        console.print(f"[yellow]{path}[/yellow]")
        source = open(path).read()
        source_lines = source.splitlines()
        tree = ast.parse(source)
        exempted_classes = _get_exempted_classes(path, exemptions)
        exempted_finding_counts: dict[str, int] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and _is_operator(class_node=node):
                template_fields = _extract_template_fields(node) or []
                if node.name in exempted_classes:
                    with console.capture():
                        count = _check_constructor_template_fields(node, template_fields)
                        count += _check_constructor_field_logic(node, template_fields, source_lines)
                    exempted_finding_counts[node.name] = count
                    continue
                err += _check_constructor_template_fields(node, template_fields)
                err += _check_constructor_field_logic(node, template_fields, source_lines)
        for class_name in sorted(exempted_classes):
            if not exempted_finding_counts.get(class_name):
                err += 1
                console.print(
                    f"[red]Stale exemption for {class_name} — the class has no findings anymore "
                    f"(or is not detected as an operator); remove its entry from "
                    f"{EXEMPTIONS_FILE}[/red]"
                )
    return err


if __name__ == "__main__":
    # A raw error count wraps at 256 (e.g. 256 findings -> exit code 0), so clamp to 0/1.
    sys.exit(1 if main() else 0)
