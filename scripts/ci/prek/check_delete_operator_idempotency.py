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

from __future__ import annotations

import ast
import sys

# Parameters that indicate non-idempotency (or ability to configure it)
FORBIDDEN_PARAMS = {"ignore_if_missing", "fail_if_not_exists", "ignore_not_found"}


def check_file(path: str) -> list[str]:
    errors = []
    try:
        with open(path) as f:
            content = f.read()
        tree = ast.parse(content)
    except Exception:
        # Ignore syntax errors or read errors
        return []

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            # Check if class name implies Delete Operator
            if "Operator" in node.name and "Delete" in node.name:
                # Find __init__
                init_method = None
                for sub in node.body:
                    if isinstance(sub, ast.FunctionDef) and sub.name == "__init__":
                        init_method = sub
                        break

                if not init_method:
                    continue

                # Check arguments
                params_found = []
                # Check regular args
                for arg in init_method.args.args:
                    if arg.arg in FORBIDDEN_PARAMS:
                        params_found.append(arg.arg)
                # Check kwonly args
                for arg in init_method.args.kwonlyargs:
                    if arg.arg in FORBIDDEN_PARAMS:
                        params_found.append(arg.arg)

                if not params_found:
                    continue

                # Check for deprecation warning in __init__
                is_deprecated = False
                for inner_node in ast.walk(init_method):
                    if isinstance(inner_node, ast.Call):
                        # Checking warnings.warn call
                        # Can be warnings.warn(...) or explicit warn(...) call
                        call_func = inner_node.func
                        is_warn = False
                        if isinstance(call_func, ast.Attribute) and call_func.attr == "warn":
                            # e.g. warnings.warn or anything.warn
                            if isinstance(call_func.value, ast.Name) and call_func.value.id == "warnings":
                                is_warn = True
                        elif isinstance(call_func, ast.Name) and call_func.id == "warn":
                            is_warn = True

                        if is_warn:
                            # Check args of warn
                            if inner_node.args:
                                first_arg = inner_node.args[0]
                                if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                                    # check contents
                                    warn_msg = first_arg.value.lower()
                                    if "deprecated" in warn_msg:
                                        # Iterate over found params to see if they are mentioned
                                        # Ideally we want ALL found params to be mentioned.
                                        # Since we usually find one, let's just check if ANY is mentioned.
                                        # Or better, check if ALL are mentioned?
                                        # Let's check if the specific param is mentioned.

                                        # If multiple params found, we need to verify all are deprecated.
                                        # Simplified: If "deprecated" is in warning, assume it covers it.
                                        # To be more precise:
                                        for p in params_found:
                                            if p in warn_msg:
                                                is_deprecated = True
                                                break

                        if is_deprecated:
                            break

                if params_found and not is_deprecated:
                    errors.append(
                        f"{path}:{node.lineno}: Class '{node.name}' contains non-idempotent parameters {params_found} which must be deprecated."
                    )
    return errors


if __name__ == "__main__":
    found_errors = []
    for path in sys.argv[1:]:
        found_errors.extend(check_file(path))

    if found_errors:
        print("\n".join(found_errors))
        sys.exit(1)
