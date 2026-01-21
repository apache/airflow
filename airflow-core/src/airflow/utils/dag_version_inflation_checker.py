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
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

RUNTIME_VARYING_CALLS = [
    ("datetime", "now"),
    ("datetime", "today"),
    ("datetime", "utcnow"),
    ("date", "today"),
    ("time", "time"),
    ("time", "localtime"),
    ("random", "random"),
    ("random", "randint"),
    ("random", "choice"),
    ("random", "uniform"),
    ("uuid", "uuid4"),
    ("uuid", "uuid1"),
    ("pendulum", "now"),
    ("pendulum", "today"),
    ("pendulum", "yesterday"),
    ("pendulum", "tomorrow"),
]


class DagVersionInflationCheckLevel(Enum):
    """enum class for Dag version inflation check level."""

    off = "off"
    warning = "warning"
    error = "error"


class DagVersionInflationCheckResult:
    """
    Represents the result of stability analysis on a Dag file.

    Stores detected warnings and formats them appropriately based on the configured check level
    (warning or error).
    """

    def __init__(self, check_level: DagVersionInflationCheckLevel):
        self.check_level: DagVersionInflationCheckLevel = check_level
        self.warnings: list[RuntimeVaryingValueWarning] = []
        self.runtime_varying_values: dict = {}

    def format_warnings(self) -> str | None:
        """Return formatted string of warning list."""
        if not self.warnings:
            return None

        lines = [
            "This Dag uses runtime-variable values in Dag construction.",
            "It causes the Dag version to increase as values change on every Dag parse.",
            "",
        ]
        for w in self.warnings:
            lines.extend(
                [
                    f"Line {w.line}, Col {w.col}",
                    f"Code: {w.code}",
                    f"Issue: {w.message}",
                    "",
                ]
            )

        if self.runtime_varying_values:
            lines.extend(
                [
                    "️Don't use the variables as arguments in Dag/Task constructors:",
                    *(
                        f"  Line {line}: '{var_name}' related '{source}'"
                        for var_name, (line, source) in sorted(
                            self.runtime_varying_values.items(),
                            key=lambda x: x[1][0],
                        )
                    ),
                    "",
                ]
            )

        return "\n".join(lines)

    def get_formatted_warnings(self, dag_ids: list[str]) -> list[dict[str, str | None]]:
        """Convert warning statement to Dag warning format."""
        from airflow.models.dagwarning import DagWarningType

        if not self.warnings or self.check_level != DagVersionInflationCheckLevel.warning:
            return []
        return [
            {
                "dag_id": dag_id,
                "warning_type": DagWarningType.RUNTIME_VARYING_VALUE.value,
                "message": self.format_warnings(),
            }
            for dag_id in dag_ids
        ]

    def get_error_format_dict(self, file_path, bundle_path):
        if not self.warnings or self.check_level != DagVersionInflationCheckLevel.error:
            return None

        relative_file_path = str(Path(file_path).relative_to(bundle_path)) if bundle_path else file_path
        return {relative_file_path: self.format_warnings()}


@dataclass
class RuntimeVaryingValueWarning:
    """Warning information for runtime-varying value detection."""

    line: int
    col: int
    code: str
    message: str


class WarningContext(str, Enum):
    """Context types for warnings."""

    TASK_CONSTRUCTOR = "Task constructor"
    DAG_CONSTRUCTOR = "Dag constructor"


class RuntimeVaryingValueAnalyzer:
    """
    Analyzer dedicated to tracking and detecting runtime-varying values.

    This analyzer is responsible for identifying if a given AST node
    contains values that change on every execution (datetime.now(), random(), etc.).
    """

    def __init__(
        self,
        varying_vars: dict[str, tuple[int, str]],
        imports: dict[str, str],
        from_imports: dict[str, tuple[str, str]],
    ):
        self.varying_vars = varying_vars
        self.imports = imports
        self.from_imports = from_imports

    def get_varying_source(self, node: ast.expr) -> str | None:
        """
        Check if an AST node contains runtime-varying values and return the source.

        Checks:
        - Runtime-varying function calls (datetime.now(), etc.)
        - Runtime-varying variable references
        - Runtime-varying values in f-strings
        - Runtime-varying values in expressions/collections
        """
        if isinstance(node, ast.Call):
            # 1. Direct runtime-varying call
            if self.is_runtime_varying_call(node):
                return ast.unparse(node)

            # 2. Method call chain
            if isinstance(node.func, ast.Attribute):
                return self.get_varying_source(node.func.value)

        # 3. Runtime-varying variable reference
        if isinstance(node, ast.Name) and node.id in self.varying_vars:
            _, source = self.varying_vars[node.id]
            return source

        # 4. f-string
        if isinstance(node, ast.JoinedStr):
            return self.get_varying_fstring(node)

        # 5. Binary operation
        if isinstance(node, ast.BinOp):
            return self.get_varying_source(node.left) or self.get_varying_source(node.right)

        # 6. Collections (list/tuple/set)
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return self.get_varying_collection(node.elts)

        # 7. List comprehension
        if isinstance(node, ast.ListComp):
            return self.get_varying_source(node.elt)

        # 8. Dictionary
        if isinstance(node, ast.Dict):
            return self.get_varying_dict(node)

        return None

    def get_varying_fstring(self, node: ast.JoinedStr) -> str | None:
        """Check for runtime-varying values inside f-strings."""
        for value in node.values:
            if isinstance(value, ast.FormattedValue) and (source := self.get_varying_source(value.value)):
                return source
        return None

    def get_varying_collection(self, elements: list) -> str | None:
        """Check for runtime-varying values in collection elements."""
        for elt in elements:
            if source := self.get_varying_source(elt):
                return source
        return None

    def get_varying_dict(self, node: ast.Dict) -> str | None:
        """Check for runtime-varying values in dictionary keys/values."""
        for key, value in zip(node.keys, node.values):
            if key and (source := self.get_varying_source(key)):
                return source
            if value and (source := self.get_varying_source(value)):
                return source
        return None

    def is_runtime_varying_call(self, node: ast.Call) -> bool:
        """
        Check if a call is runtime-varying.

        1. Is the function itself runtime-varying?
        2. Do the arguments contain runtime-varying values?
        """
        # Check if the function itself is runtime-varying
        if isinstance(node.func, ast.Attribute) and self.is_runtime_varying_attribute_call(node.func):
            return True

        if isinstance(node.func, ast.Name) and self.is_runtime_varying_name_call(node.func):
            return True

        # Check if arguments contain runtime-varying values
        return self.has_varying_arguments(node)

    def has_varying_arguments(self, node: ast.Call) -> bool:
        """Check if function arguments contain runtime-varying values."""
        for arg in node.args:
            if self.get_varying_source(arg):
                return True

        for kw in node.keywords:
            if self.get_varying_source(kw.value):
                return True

        return False

    def is_runtime_varying_attribute_call(self, attr: ast.Attribute) -> bool:
        """Check for runtime-varying calls like datetime.now()."""
        method_name = attr.attr

        if isinstance(attr.value, ast.Name):
            module_or_alias = attr.value.id
            actual_module = self.imports.get(module_or_alias, module_or_alias)

            # If imported via "from import"
            if module_or_alias in self.from_imports:
                _, original_name = self.from_imports[module_or_alias]
                actual_module = original_name

            return (actual_module, method_name) in RUNTIME_VARYING_CALLS

        # Nested attribute (e.g., datetime.datetime.now)
        if isinstance(attr.value, ast.Attribute):
            inner_attr = attr.value
            if isinstance(inner_attr.value, ast.Name):
                return (inner_attr.attr, method_name) in RUNTIME_VARYING_CALLS

        return False

    def is_runtime_varying_name_call(self, func: ast.Name) -> bool:
        """Check for runtime-varying calls like now() (when imported via 'from import')."""
        func_name = func.id

        if func_name in self.from_imports:
            module, original_name = self.from_imports[func_name]
            module_parts = module.split(".")

            for part in module_parts:
                if (part, original_name) in RUNTIME_VARYING_CALLS:
                    return True

        return False


class DagTaskDetector:
    """
    Detector dedicated to identifying Dag and Task constructors.

    This detector identifies when code is creating Dag or Task objects
    in Airflow. It needs to handle both traditional class instantiation and decorator styles.
    """

    def __init__(self, from_imports: dict[str, tuple[str, str]]):
        self.from_imports: dict[str, tuple[str, str]] = from_imports
        self.dag_instances: set[str] = set()
        self.is_in_dag_context: bool = False

    def is_dag_constructor(self, node: ast.Call) -> bool:
        """Check if a call is a Dag constructor."""
        if not isinstance(node.func, ast.Name):
            return False

        func_name = node.func.id

        # "from airflow import DAG" form or "from airflow.decorator import dag"
        if func_name in self.from_imports:
            module, original = self.from_imports[func_name]
            if (module == "airflow" or module.startswith("airflow.")) and original in ("DAG", "dag"):
                return True

        return False

    def is_task_constructor(self, node: ast.Call) -> bool:
        """
        Check if a call is a Task constructor.

        Criteria:
        1. All calls within a Dag with block
        2. Calls that receive a Dag instance as an argument (dag=...)
        """
        # Inside Dag with block
        if self.is_in_dag_context:
            return True

        # Passing Dag instance as argument
        for arg in node.args:
            if isinstance(arg, ast.Name) and arg.id in self.dag_instances:
                return True

        for keyword in node.keywords:
            if keyword.value and isinstance(keyword.value, ast.Name):
                if keyword.value.id in self.dag_instances:
                    return True

        return False

    def register_dag_instance(self, var_name: str):
        """Register a Dag instance variable name."""
        self.dag_instances.add(var_name)

    def enter_dag_context(self):
        """Enter a Dag with block."""
        self.is_in_dag_context = True

    def exit_dag_context(self):
        """Exit a Dag with block."""
        self.is_in_dag_context = False


class AirflowRuntimeVaryingValueChecker(ast.NodeVisitor):
    """
    Main visitor class to detect runtime-varying value usage in Airflow Dag/Task.

    Main responsibilities:
    - Traverse AST and visit nodes
    - Detect Dag/Task creation
    - Track runtime-varying values and generate warnings
    """

    def __init__(self, check_level: DagVersionInflationCheckLevel = DagVersionInflationCheckLevel.warning):
        self.static_check_result: DagVersionInflationCheckResult = DagVersionInflationCheckResult(
            check_level=check_level
        )
        self.imports: dict[str, str] = {}
        self.from_imports: dict[str, tuple[str, str]] = {}
        self.varying_vars: dict[str, tuple[int, str]] = {}
        self.check_level = check_level

        # Helper objects
        self.value_analyzer = RuntimeVaryingValueAnalyzer(self.varying_vars, self.imports, self.from_imports)
        self.dag_detector = DagTaskDetector(self.from_imports)

    def visit_Import(self, node: ast.Import):
        """Process import statements."""
        for alias in node.names:
            name = alias.asname or alias.name
            self.imports[name] = alias.name

    def visit_ImportFrom(self, node: ast.ImportFrom):
        """Process from ... import statements."""
        if node.module:
            for alias in node.names:
                name = alias.asname or alias.name
                self.from_imports[name] = (node.module, alias.name)

    def visit_Assign(self, node: ast.Assign):
        """
        Process variable assignments.

        Checks:
        1. Dag instance assignment
        2. Task instance assignment
        3. Runtime-varying value assignment
        """
        value = node.value

        # Dag constructor
        if isinstance(value, ast.Call) and self.dag_detector.is_dag_constructor(value):
            self._register_dag_instances(node.targets)
            self._check_and_warn(value, WarningContext.DAG_CONSTRUCTOR)

        # Task constructor
        elif isinstance(value, ast.Call) and self.dag_detector.is_task_constructor(value):
            self._check_and_warn(value, WarningContext.TASK_CONSTRUCTOR)

        # Track runtime-varying values
        else:
            self._track_varying_assignment(node)

    def visit_Call(self, node: ast.Call):
        """
        Process function calls.

        Check not assign but just call the function or Dag definition via decorator.
        """
        if self.dag_detector.is_dag_constructor(node):
            self._check_and_warn(node, WarningContext.DAG_CONSTRUCTOR)

        elif self.dag_detector.is_task_constructor(node):
            self._check_and_warn(node, WarningContext.TASK_CONSTRUCTOR)

    def visit_For(self, node: ast.For):
        """
        Process for statements.

        Check if iteration target contains runtime-varying values.
        """
        # check the iterator value is runtime-varying
        # iter is runtime-varying : for iter in [datetime.now(), 3]
        if varying_source := self.value_analyzer.get_varying_source(node.iter):
            if isinstance(node.target, ast.Name):
                self.varying_vars[node.target.id] = (node.lineno, varying_source)

        for body in node.body:
            self.visit(body)

        if varying_source:
            if isinstance(node.target, ast.Name):
                self.varying_vars.pop(node.target.id)

    def visit_With(self, node: ast.With):
        """
        Process with statements.

        Detect Dag context manager.
        """
        is_with_dag_context = False
        for item in node.items:
            # check if the Dag instance exists in with context
            self.visit(item)
            if isinstance(item.context_expr, ast.Call):
                if self.dag_detector.is_dag_constructor(item.context_expr):
                    # check the value defined in with statement to detect entering Dag with block
                    is_with_dag_context = True

        if is_with_dag_context:
            self.dag_detector.enter_dag_context()

        for body in node.body:
            self.visit(body)

        # Exit Dag with block
        self.dag_detector.exit_dag_context()

    def _register_dag_instances(self, targets: list):
        """Register Dag instance variable names."""
        for target in targets:
            if isinstance(target, ast.Name):
                self.dag_detector.register_dag_instance(target.id)

    def _track_varying_assignment(self, node: ast.Assign):
        """Track variable assignments with runtime-varying values."""
        if varying_source := self.value_analyzer.get_varying_source(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.varying_vars[target.id] = (node.lineno, varying_source)

    def _check_and_warn(self, call: ast.Call, context: WarningContext):
        """Check function call arguments and generate warnings."""
        if self.value_analyzer.get_varying_source(call):
            self.static_check_result.warnings.append(
                RuntimeVaryingValueWarning(
                    line=call.lineno,
                    col=call.col_offset,
                    code=ast.unparse(call),
                    message=self._get_warning_message(context),
                )
            )

    def _get_warning_message(self, context: WarningContext) -> str:
        """Get appropriate warning message based on context."""
        if self.dag_detector.is_in_dag_context and context == WarningContext.TASK_CONSTRUCTOR:
            return "Don't use runtime-varying values as function arguments within with Dag block"
        return f"Don't use runtime-varying value as argument in {context.value}"


def check_dag_file_stability(file_path) -> DagVersionInflationCheckResult:
    from airflow.configuration import conf

    try:
        check_level = DagVersionInflationCheckLevel(
            conf.get("dag_processor", "dag_version_inflation_check_level")
        )
    except ValueError:
        check_level = DagVersionInflationCheckLevel.warning

    if check_level == DagVersionInflationCheckLevel.off:
        return DagVersionInflationCheckResult(check_level=check_level)

    try:
        parsed = ast.parse(Path(file_path).read_bytes())
    except (SyntaxError, ValueError, TypeError, FileNotFoundError):
        return DagVersionInflationCheckResult(check_level=check_level)

    checker = AirflowRuntimeVaryingValueChecker(check_level)
    checker.visit(parsed)
    checker.static_check_result.runtime_varying_values = checker.varying_vars
    return checker.static_check_result
