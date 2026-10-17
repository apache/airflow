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
"""Check that example DAGs do not build shell commands out of runtime values.

Example DAGs are copied into real deployments, so the pattern they show is the pattern users
ship. A shell command assembled with an f-string, ``str.format`` or ``%`` from a value that is
only known at run time -- a task's output, a DAG param, ``dag_run.conf`` -- lets that value
be interpreted by the shell. The value should reach the command through ``env`` instead, and the
command text should stay a constant.

The check is static and deliberately conservative:

* ``bash_command=`` on any operator call must be a string literal, a name bound to a module-level
  constant, or an f-string that only interpolates module-level ``UPPER_CASE`` constants. Anything
  else -- a subscript or attribute of a task result, ``.format(...)``, ``%``, ``+`` -- is flagged.
* A function decorated with ``@task.bash`` may not return an f-string, ``.format(...)``, ``%`` or
  ``+`` expression that interpolates a parameter which some call site feeds with another task's
  output or a ``params`` / ``conf`` template. Literals and context values are the documented
  ``@task.bash`` idiom and are not flagged.

* Command text -- a literal, a name bound to one, or the literal parts of an f-string -- may not
  use a Jinja expression that renders ``xcom_pull``, ``params`` or ``dag_run.conf`` into the
  command. Airflow renders the template before the shell runs, so the rendered value is parsed as
  command text just as an f-string value would be. Context values such as ``{{ ds }}`` are fine.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from rich.console import Console

console = Console(color_system="standard", width=200)

COMMAND_KEYWORDS = ("bash_command",)


def _is_literal_text(node: ast.expr | None) -> bool:
    """Return ``True`` for a string literal, optionally wrapped in ``textwrap.dedent``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return True
    if isinstance(node, ast.Call) and len(node.args) == 1 and not node.keywords:
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        return name == "dedent" and _is_literal_text(node.args[0])
    return False


def _literal_text(node: ast.expr | None) -> str:
    """Return the text of a literal accepted by ``_is_literal_text``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call) and node.args:
        return _literal_text(node.args[0])
    return ""


def _literal_bindings(tree: ast.Module) -> tuple[set[str], dict[str, str]]:
    """
    Return the names that only ever hold constant command text, and the literal text they hold.

    Constant names are ``UPPER_CASE`` names and names that are only ever bound to a literal.
    """
    literal_text: dict[str, str] = {}
    other_names: set[str] = set()
    for node in ast.walk(tree):
        targets: list[ast.expr] = []
        value: ast.expr | None = None
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            if _is_literal_text(value):
                literal_text[target.id] = literal_text.get(target.id, "") + _literal_text(value)
            else:
                other_names.add(target.id)
    upper_names = {name for name in literal_text.keys() | other_names if name.isupper()}
    return upper_names | (literal_text.keys() - other_names), literal_text


JINJA_EXTERNAL_MARKERS = ("xcom_pull", "params", "dag_run.conf")


def _jinja_external(text: str) -> str | None:
    """Return the external value a Jinja expression in ``text`` renders into the command, if any."""
    for opening in text.split("{{")[1:]:
        expression = opening.split("}}", 1)[0]
        for marker in JINJA_EXTERNAL_MARKERS:
            if marker in expression:
                return f"Jinja template renders {marker} into the command: {{{{{expression}}}}}"
    return None


def _is_constant_reference(node: ast.expr, constants: set[str]) -> bool:
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.Name):
        return node.id in constants or node.id.isupper()
    if isinstance(node, ast.Attribute):
        return node.attr.isupper()
    return False


def _unsafe_command(node: ast.expr, constants: set[str], literal_text: dict[str, str]) -> str | None:
    """Return why a ``bash_command`` value is built from a runtime value, or ``None`` if it is not."""
    if isinstance(node, ast.Constant):
        return _jinja_external(node.value) if isinstance(node.value, str) else None
    if _is_literal_text(node):
        return _jinja_external(_literal_text(node))
    if isinstance(node, ast.JoinedStr):
        for part in node.values:
            if isinstance(part, ast.FormattedValue) and not _is_constant_reference(part.value, constants):
                return f"f-string interpolates {ast.unparse(part.value)!r}"
            if isinstance(part, ast.Constant) and (reason := _jinja_external(str(part.value))):
                return reason
        return None
    if isinstance(node, ast.Name) and node.id in literal_text:
        if reason := _jinja_external(literal_text[node.id]):
            return reason
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        return "command is built with str.format()"
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Mod, ast.Add)):
        if _is_constant_reference(node.left, constants) and _is_constant_reference(node.right, constants):
            return None
        return "command is built with string concatenation or % formatting"
    if isinstance(node, ast.Name):
        return None if node.id in constants or node.id.isupper() else f"command comes from {node.id!r}"
    if isinstance(node, (ast.Subscript, ast.Attribute, ast.Call)):
        return f"command comes from {ast.unparse(node)!r}"
    return None


def _is_task_bash(decorator: ast.expr) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return isinstance(target, ast.Attribute) and target.attr == "bash"


EXTERNAL_TEMPLATE_MARKERS = ("params", "conf")


def _is_external_argument(node: ast.expr) -> bool:
    """Return ``True`` when a call argument carries a value the DAG author does not control.

    That is another task's output (``task.output``, ``result["key"]``, ``some_task()``) or a
    template reading DAG params or ``dag_run.conf``. Literals, loop variables and context
    templates such as ``{{ ds }}`` are not external.
    """
    if isinstance(node, (ast.Subscript, ast.Attribute, ast.Call)):
        return True
    if isinstance(node, ast.Constant) and isinstance(node.value, str) and "{{" in node.value:
        return any(marker in node.value for marker in EXTERNAL_TEMPLATE_MARKERS)
    return False


def _called_name(call: ast.Call) -> str | None:
    """Return the task function a call invokes, seeing through ``fn.override(...)(...)``."""
    func = call.func
    if isinstance(func, ast.Call) and isinstance(func.func, ast.Attribute) and func.func.attr == "override":
        func = func.func.value
    return func.id if isinstance(func, ast.Name) else None


def _externally_fed_parameters(
    tree: ast.Module, function: ast.FunctionDef | ast.AsyncFunctionDef
) -> set[str]:
    """Return the parameters of ``function`` that some call site feeds with an external value."""
    positional = [a.arg for a in [*function.args.posonlyargs, *function.args.args]]
    fed: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _called_name(node) != function.name:
            continue
        for index, argument in enumerate(node.args):
            if index < len(positional) and _is_external_argument(argument):
                fed.add(positional[index])
        for keyword in node.keywords:
            if keyword.arg and _is_external_argument(keyword.value):
                fed.add(keyword.arg)
    return fed


def _interpolates_parameter(node: ast.expr, parameters: set[str]) -> str | None:
    names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
    if isinstance(node, (ast.JoinedStr, ast.BinOp)) or (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format"
    ):
        used = sorted(names & parameters)
        if used:
            return f"returned command interpolates {', '.join(used)}, fed from another task, params or conf"
    return None


def check_file(file: Path) -> list[str]:
    """Return the shell-command errors found in a single example DAG file."""
    try:
        tree = ast.parse(file.read_text(), filename=str(file))
    except SyntaxError as exc:
        return [f"[red]{file}: could not be parsed: {exc}[/]\n"]
    constants, literal_text = _literal_bindings(tree)
    errors: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg in COMMAND_KEYWORDS:
                    reason = _unsafe_command(keyword.value, constants, literal_text)
                    if reason:
                        errors.append(f"[red]{file}:{keyword.value.lineno}: {keyword.arg}: {reason}.[/]\n")
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
            _is_task_bash(d) for d in node.decorator_list
        ):
            parameters = _externally_fed_parameters(tree, node)
            for returned in ast.walk(node):
                if isinstance(returned, ast.Return) and returned.value is not None:
                    reason = _interpolates_parameter(returned.value, parameters)
                    if not reason and _is_literal_text(returned.value):
                        reason = _jinja_external(_literal_text(returned.value))
                    if reason:
                        errors.append(
                            f"[red]{file}:{returned.lineno}: @task.bash {node.name}: {reason}.[/]\n"
                        )
    return errors


def main(argv: list[str]) -> int:
    errors: list[str] = []
    for argument in argv:
        errors.extend(check_file(Path(argument)))
    if not errors:
        return 0
    console.print("[red]Found example DAGs that build shell commands from runtime values:[/]\n")
    for error in errors:
        console.print(error)
    console.print(
        "Keep the command text constant and pass runtime values through [yellow]env[/], e.g.\n"
        '[yellow]BashOperator(bash_command=\'echo "$VALUE"\', env={"VALUE": value}, append_env=True)[/]\n'
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
