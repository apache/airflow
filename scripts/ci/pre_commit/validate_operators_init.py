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
from __future__ import annotations

import ast
import sys
from typing import Any

from rich.console import Console

console = Console(color_system="standard", width=200)
BASE_OPERATOR_CLASS_NAME = "BaseOperator"


def _is_operator(class_node: ast.ClassDef) -> bool:
    """
    Check if a given class node is an operator, based of the string suffix of the base IDs
    (ends with "BaseOperator").
    TODO: Enhance this function to work with nested inheritance trees through dynamic imports.

    :param class_node: The class node to check.
    :return: True if the class definition is of an operator, False otherwise.
    """
    for base in class_node.bases:
        if isinstance(base, ast.Name) and base.id.endswith(BASE_OPERATOR_CLASS_NAME):
            return True
    return False


def _extract_template_fields(class_node: ast.ClassDef) -> list[str]:
    """
    This method takes a class node as input and extracts the template fields from it.
    Template fields are identified by an assignment statement where the target is a variable
    named "template_fields" and the value is a tuple of constants.

    :param class_node: The class node representing the class for which template fields need to be extracted.
    :return: A list of template fields extracted from the class node.
    """
    for class_item in class_node.body:
        if isinstance(class_item, ast.Assign):
            for target in class_item.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id == "template_fields"
                    and isinstance(class_item.value, ast.Tuple)
                ):
                    return [
                        elt.value
                        for elt in class_item.value.elts
                        if isinstance(elt, ast.Constant)
                    ]
        elif isinstance(class_item, ast.AnnAssign):
            if (
                isinstance(class_item.target, ast.Name)
                and class_item.target.id == "template_fields"
                and isinstance(class_item.value, ast.Tuple)
            ):
                return [
                    elt.value
                    for elt in class_item.value.elts
                    if isinstance(elt, ast.Constant)
                ]
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

    :param missing_assignments: List[str] - List of template fields that have not been assigned a value.
    :param ctor_stmt: ast.Expr - AST node representing the constructor statement.
    :param invalid_assignments: List[str] - List of template fields that have been assigned incorrectly.
    :param template_fields: List[str] - List of template fields to be assigned.

    :return: List[str] - List of template fields that are still missing assignments.
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
            assigned_targets = [
                arg.arg for arg in ctor_stmt.value.keywords if arg.arg is not None
            ]
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
                    if isinstance(ctor_stmt.value, ast.BoolOp) and isinstance(
                        ctor_stmt.value.op, ast.Or
                    ):
                        _handle_assigned_field(
                            assigned_template_fields,
                            invalid_assignments,
                            target,
                            ctor_stmt.value.values[0],
                        )
                    else:
                        _handle_assigned_field(
                            assigned_template_fields,
                            invalid_assignments,
                            target,
                            ctor_stmt.value,
                        )
        elif isinstance(ctor_stmt.targets[0], ast.Tuple) and isinstance(
            ctor_stmt.value, ast.Tuple
        ):
            for target, value in zip(ctor_stmt.targets[0].elts, ctor_stmt.value.elts):
                if isinstance(target, ast.Attribute):
                    _handle_assigned_field(
                        assigned_template_fields, invalid_assignments, target, value
                    )
    elif isinstance(ctor_stmt, ast.AnnAssign):
        if (
            isinstance(ctor_stmt.target, ast.Attribute)
            and ctor_stmt.target.attr in template_fields
        ):
            _handle_assigned_field(
                assigned_template_fields,
                invalid_assignments,
                ctor_stmt.target,
                ctor_stmt.value,
            )
    return list(set(missing_assignments) - set(assigned_template_fields))


def _handle_assigned_field(
    assigned_template_fields: list[str],
    invalid_assignments: list[str],
    target: ast.Attribute,
    value: Any,
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


def _check_constructor_template_fields(
    class_node: ast.ClassDef, template_fields: list[str]
) -> int:
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


def main():
    """
    Check missing or invalid template fields in constructors of providers' operators.

    :return: The total number of errors found.
    """
    err = 0
    for path in sys.argv[1:]:
        console.print(f"[yellow]{path}[/yellow]")
        tree = ast.parse(open(path).read())
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and _is_operator(class_node=node):
                template_fields = _extract_template_fields(node) or []
                err += _check_constructor_template_fields(node, template_fields)
    return err


if __name__ == "__main__":
    sys.exit(main())
