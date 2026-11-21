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
# requires-python = ">=3.11,<3.12"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Check CLI commands, arguments, and flags for consistency and documentation.

Enforces:
1. Long flags start with '--' and strictly conform to POSIX kebab-case (^--[a-z0-9]+(-[a-z0-9]+)*$).
2. Short flags start with '-' and consist of a single alphanumeric character (^-([a-zA-Z0-9])$).
3. Every Arg has a non-empty help docstring.
4. ActionCommand and GroupCommand names follow kebab-case and provide non-empty help docstrings.
5. ActionCommand arguments have no duplicate flags within the same command scope.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
import textwrap
from collections import Counter
from pathlib import Path

# Add script directory to sys.path to resolve common_prek_utils
sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_CORE_ROOT_PATH, AIRFLOW_PROVIDERS_ROOT_PATH, console

KEBAB_CASE_FLAG_PATTERN = re.compile(r"^--[a-z0-9]+(-[a-z0-9]+)*$")
SHORT_FLAG_PATTERN = re.compile(r"^-[a-zA-Z0-9]$")
COMMAND_NAME_PATTERN = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
POSITIONAL_ARG_PATTERN = re.compile(r"^[a-z0-9_]+$")


def validate_flag(flag: str) -> str | None:
    """Validate a single CLI flag string against naming standards."""
    if flag.startswith("--"):
        if not KEBAB_CASE_FLAG_PATTERN.match(flag):
            return f"Long flag '{flag}' must use strict kebab-case (e.g. '--my-arg')"
    elif flag.startswith("-"):
        if not SHORT_FLAG_PATTERN.match(flag):
            return f"Short flag '{flag}' must be a single alphanumeric character (e.g. '-v')"
    else:
        if not POSITIONAL_ARG_PATTERN.match(flag):
            return f"Positional argument '{flag}' must be a valid identifier (e.g. 'dag_id')"
    return None


def extract_string_constant(node: ast.expr) -> str | None:
    """Extract string value from an AST expression if it is a constant string."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def extract_flags_from_arg_call(call: ast.Call) -> list[tuple[str, int]]:
    """Extract flag strings and their line numbers from an Arg call."""
    flags: list[tuple[str, int]] = []

    # Check 1st positional argument: Arg(("flag1", "flag2"), ...) or Arg("flag", ...)
    if call.args:
        first_arg = call.args[0]
        if isinstance(first_arg, (ast.Tuple, ast.List)):
            for elt in first_arg.elts:
                val = extract_string_constant(elt)
                if val is not None:
                    flags.append((val, getattr(elt, "lineno", call.lineno)))
        else:
            val = extract_string_constant(first_arg)
            if val is not None:
                flags.append((val, getattr(first_arg, "lineno", call.lineno)))

    # Check keyword argument: Arg(flags=("flag1", "flag2"), ...)
    for kw in call.keywords:
        if kw.arg == "flags":
            if isinstance(kw.value, (ast.Tuple, ast.List)):
                for elt in kw.value.elts:
                    val = extract_string_constant(elt)
                    if val is not None:
                        flags.append((val, getattr(elt, "lineno", kw.value.lineno)))
            else:
                val = extract_string_constant(kw.value)
                if val is not None:
                    flags.append((val, getattr(kw.value, "lineno", kw.value.lineno)))

    return flags


def check_help_docstring_present(call: ast.Call) -> tuple[bool, str | None]:
    """Check whether an Arg call contains a valid, non-empty help docstring."""
    # Positional help: Arg(flags, "help text", ...)
    if len(call.args) >= 2:
        val = extract_string_constant(call.args[1])
        if val is not None:
            if not val.strip():
                return False, "Help docstring cannot be empty or whitespace-only"
            return True, None
        # Non-constant / dynamic expression (e.g. textwrap.dedent or variable)
        return True, None

    # Keyword help: Arg(flags, help="help text", ...)
    for kw in call.keywords:
        if kw.arg == "help":
            val = extract_string_constant(kw.value)
            if val is not None:
                if not val.strip():
                    return False, "Help docstring cannot be empty or whitespace-only"
                return True, None
            # Non-constant / dynamic expression
            return True, None

    return False, "Arg is missing a required help docstring"


class CliConsistencyVisitor(ast.NodeVisitor):
    """AST visitor to audit CLI argument definitions, command names, and help texts."""

    def __init__(self, filename: str):
        self.filename = filename
        self.violations: list[str] = []
        self.arg_definitions: dict[str, list[str]] = {}

    def visit_Assign(self, node: ast.Assign) -> None:
        """Track variable assignments of Arg objects to validate command args later."""
        if isinstance(node.value, ast.Call):
            func_name = getattr(node.value.func, "id", getattr(node.value.func, "attr", None))
            if func_name == "Arg":
                flags = [f[0] for f in extract_flags_from_arg_call(node.value)]
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        self.arg_definitions[target.id] = flags
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Inspect calls to Arg, ActionCommand, and GroupCommand."""
        func_name = getattr(node.func, "id", getattr(node.func, "attr", None))

        if func_name == "Arg":
            self._check_arg_call(node)
        elif func_name in ("ActionCommand", "GroupCommand"):
            self._check_command_call(node, func_name)

        self.generic_visit(node)

    def _check_arg_call(self, node: ast.Call) -> None:
        """Validate Arg flags and help documentation."""
        flags = extract_flags_from_arg_call(node)
        for flag, lineno in flags:
            err = validate_flag(flag)
            if err:
                self.violations.append(f"{self.filename}:{lineno}: {err}")

        has_help, help_err = check_help_docstring_present(node)
        if not has_help:
            flag_repr = ", ".join(repr(f[0]) for f in flags) if flags else "<unknown>"
            self.violations.append(f"{self.filename}:{node.lineno}: Arg({flag_repr}): {help_err}")

    def _check_command_call(self, node: ast.Call, command_type: str) -> None:
        """Validate command/group names, help docstrings, and duplicate flags."""
        cmd_name: str | None = None
        has_help = False
        empty_help = False
        args_nodes: list[ast.expr] = []

        for kw in node.keywords:
            if kw.arg == "name":
                cmd_name = extract_string_constant(kw.value)
            elif kw.arg == "help":
                has_help = True
                val = extract_string_constant(kw.value)
                if val is not None and not val.strip():
                    empty_help = True
            elif kw.arg == "args" and isinstance(kw.value, (ast.Tuple, ast.List)):
                args_nodes.extend(kw.value.elts)

        # Name check
        if cmd_name is not None:
            if not COMMAND_NAME_PATTERN.match(cmd_name):
                self.violations.append(
                    f"{self.filename}:{node.lineno}: {command_type} name '{cmd_name}' "
                    f"must use kebab-case (e.g. 'clear' or 'list-runs')"
                )

        # Help check (skip unpacking helpers like `ActionCommand(**cmd)`)
        if node.keywords and any(kw.arg == "name" for kw in node.keywords):
            if not has_help:
                self.violations.append(
                    f"{self.filename}:{node.lineno}: {command_type} '{cmd_name or 'unnamed'}' "
                    f"is missing a required help docstring"
                )
            elif empty_help:
                self.violations.append(
                    f"{self.filename}:{node.lineno}: {command_type} '{cmd_name or 'unnamed'}' "
                    f"has an empty help docstring"
                )

        # Duplicate flags check for ActionCommand
        if command_type == "ActionCommand" and args_nodes:
            command_flags: list[str] = []
            for arg_node in args_nodes:
                if isinstance(arg_node, ast.Name) and arg_node.id in self.arg_definitions:
                    command_flags.extend(
                        flag for flag in self.arg_definitions[arg_node.id] if flag.startswith("-")
                    )
                elif isinstance(arg_node, ast.Call):
                    # Inline Arg call
                    inline_flags = extract_flags_from_arg_call(arg_node)
                    command_flags.extend(flag[0] for flag in inline_flags if flag[0].startswith("-"))

            counts = Counter(command_flags)
            for flag, count in counts.items():
                if count > 1:
                    self.violations.append(
                        f"{self.filename}:{node.lineno}: ActionCommand '{cmd_name or 'unnamed'}' "
                        f"has duplicate flag '{flag}' defined {count} times"
                    )


def check_code(code: str, filename: str = "<string>") -> list[str]:
    """Audit Python source code directly and return a list of violation messages."""
    dedented_code = textwrap.dedent(code)
    try:
        tree = ast.parse(dedented_code, filename=filename)
    except SyntaxError as e:
        return [f"{filename}:{e.lineno or 1}: Syntax error: {e.msg}"]

    visitor = CliConsistencyVisitor(filename=filename)
    visitor.visit(tree)
    return visitor.violations


def check_file(path: Path) -> list[str]:
    """Audit a single Python file on disk and return a list of violation messages."""
    try:
        code = path.read_text(encoding="utf-8")
    except OSError as e:
        return [f"{path}: Failed to read file: {e}"]
    return check_code(code, filename=str(path))


def is_cli_definition_file(path: Path) -> bool:
    """Determine whether a path points to a CLI definition file."""
    path_posix = path.as_posix()
    if path_posix.endswith("airflow-core/src/airflow/cli/cli_config.py"):
        return True
    if "/cli/definition.py" in path_posix:
        return True
    return False


def find_cli_definition_files(paths: list[Path] | None = None) -> list[Path]:
    """Find CLI definition files matching the given paths or discover all default targets."""
    if paths:
        return [p for p in paths if is_cli_definition_file(p)]

    targets: list[Path] = []
    core_config = AIRFLOW_CORE_ROOT_PATH / "src" / "airflow" / "cli" / "cli_config.py"
    if core_config.is_file():
        targets.append(core_config)

    if AIRFLOW_PROVIDERS_ROOT_PATH.is_dir():
        for def_path in sorted(AIRFLOW_PROVIDERS_ROOT_PATH.glob("**/cli/definition.py")):
            if def_path.is_file():
                targets.append(def_path)

    return targets


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Audit Airflow CLI definitions for flag naming, help docstrings, and flag collisions."
    )
    parser.add_argument(
        "files",
        nargs="*",
        type=Path,
        help="Optional list of specific files to audit.",
    )
    return parser.parse_args()


def main() -> int:
    """Run CLI consistency audit across targets."""
    args = parse_args()
    files_to_check = find_cli_definition_files(args.files)

    if not files_to_check:
        if args.files:
            console.print("[yellow]No relevant CLI definition files found among provided files.[/]")
        else:
            console.print("[yellow]No CLI definition files found.[/]")
        return 0

    console.print(f"[blue]Auditing {len(files_to_check)} CLI definition file(s)...[/]")
    total_violations: list[str] = []

    for file_path in files_to_check:
        violations = check_file(file_path)
        if violations:
            total_violations.extend(violations)

    if total_violations:
        console.print(f"\n[red]Found {len(total_violations)} CLI consistency violation(s):[/]\n")
        for err in total_violations:
            console.print(f"  [red]- {err}[/]")
        return 1

    console.print(f"[green]All {len(files_to_check)} CLI definition files conform to standards.[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
