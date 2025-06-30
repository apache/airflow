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

import inspect
import sys

import libcst as cst
from in_container_utils import AIRFLOW_ROOT_PATH, console

DECORATOR_OPERATOR_MAP = {
    "kubernetes": "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator",
    "sensor": "airflow.sdk.bases.sensor.BaseSensorOperator",
    "virtualenv": "airflow.providers.standard.operators.python.PythonVirtualenvOperator",
    "branch_virtualenv": "airflow.providers.standard.operators.python.PythonVirtualenvOperator",
    "bash": "airflow.providers.standard.operators.bash.BashOperator",
    "short_circuit": "airflow.providers.standard.operators.python.ShortCircuitOperator",
    "python": "airflow.providers.standard.operators.python.PythonOperator",
    "external_python": "airflow.providers.standard.operators.python.ExternalPythonOperator",
    # Add more here...
}
DECORATOR_PYI_PATH = (
    AIRFLOW_ROOT_PATH / "task-sdk" / "src" / "airflow" / "sdk" / "definitions" / "decorators" / "__init__.pyi"
)
decorator_pyi_file_content = DECORATOR_PYI_PATH.read_text()
STOP_CLASSES = {"airflow.models.baseoperator.BaseOperator", "airflow.sdk.bases.operator.BaseOperator"}


def extract_function_params(code, function_name, return_type):
    """Extracts parameters from a specific function definition in the given code.

    Args:
        code (str): The Python code to parse.
        function_name (str): The name of the function to extract parameters from.
        return_type (str): As the pyi file has multiple @overload decorator, extract function param based on return type.

    Returns:
        list: A list of parameter names, or None if the function is not found.
    """
    module = cst.parse_module(code)

    class FunctionParamExtractor(cst.CSTVisitor):
        def __init__(self, target_function_name, target_return_type):
            self.target_function_name = target_function_name
            self.target_return_type = target_return_type
            self.params: list[str] = []

        def visit_FunctionDef(self, node):
            # Match function name
            if node.name.value == self.target_function_name:
                if node.returns:
                    annotation = node.returns.annotation
                    if isinstance(annotation, cst.Name) and annotation.value == self.target_return_type:
                        parameters_node = node.params
                        self.params.extend(param.name.value for param in parameters_node.params)
                        self.params.extend(param.name.value for param in parameters_node.kwonly_params)
                        self.params.extend(param.name.value for param in parameters_node.posonly_params)
                        if parameters_node.star_kwarg:
                            self.params.append(parameters_node.star_kwarg.name.value)
                            return False  # Stop traversing after finding the real function
            return True  # Keep traversing

    extractor = FunctionParamExtractor(function_name, return_type)
    module.visit(extractor)
    return extractor.params


def get_decorator_params(decorator_name: str):
    params = extract_function_params(decorator_pyi_file_content, decorator_name, "TaskDecorator")
    return set(params)


def get_operator_params(operator_path: str) -> set[str]:
    module_path, class_name = operator_path.rsplit(".", 1)
    module = __import__(module_path, fromlist=[class_name])
    operator_cls = getattr(module, class_name)
    console.print(f"operator_cls: {operator_cls}")
    all_params: set[str] = set()
    for cls in inspect.getmro(operator_cls):
        full_class_path = f"{cls.__module__}.{cls.__qualname__}"
        if full_class_path in STOP_CLASSES:
            break
        if cls is object:
            continue  # Skip base object
        try:
            sig = inspect.signature(cls)
        except (ValueError, TypeError):
            console.print(f"[red]Could not inspect: {cls}[/]")
            continue
        all_params.update(p for p in sig.parameters.keys() if p not in ("self", "args", "kwargs"))
    console.print("[green]Extracted params:[/] ", all_params)
    return all_params


def verify_signature_consistency():
    failure = False
    console.print("Verify signature consistency")
    for decorator, operator_path in DECORATOR_OPERATOR_MAP.items():
        decorator_params = get_decorator_params(decorator)
        operator_params = get_operator_params(operator_path)
        missing_in_decorator = operator_params - decorator_params

        ignored = {"kwargs", "args", "self", "python_callable", "op_args", "op_kwargs"}
        missing_in_decorator -= ignored
        if missing_in_decorator:
            failure = True
            console.print(
                f"[yellow]Missing params in[/] [bold]__init__.py[/] for {decorator}: {missing_in_decorator}"
            )
    if failure:
        console.print("[red]Some of the decorator signatures are missing in __init__.py[/]")
        sys.exit(1)
    console.print("[green]All decorator signature matches[/]")
    sys.exit(0)


if __name__ == "__main__":
    verify_signature_consistency()
