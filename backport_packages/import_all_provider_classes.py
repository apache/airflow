#!/usr/bin/env python3
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

import importlib
import os
import sys
import traceback
from inspect import isclass
from typing import List


def import_all_provider_classes(source_path: str,
                                provider_ids: List[str] = None,
                                print_imports: bool = False) -> List[str]:
    """
    Imports all classes in providers packages. This method loads and imports
    all the classes found in providers, so that we can find all the subclasses
    of operators/sensors etc.

    :param provider_ids - provider ids that should be loaded.
    :param print_imports - if imported class should also be printed in output
    :param source_path: path to look for sources - might be None to look for all packages in all source paths
    :return: list of all imported classes
    """
    if provider_ids:
        prefixed_provider_paths = [source_path + "/airflow/providers/" + provider_id.replace(".", "/")
                                   for provider_id in provider_ids]
    else:
        prefixed_provider_paths = [source_path + "/airflow/providers/"]

    imported_classes = []
    tracebacks = []
    for root, _, files in os.walk(source_path):
        if all([not root.startswith(prefix_provider_path)
                for prefix_provider_path in prefixed_provider_paths]) or root.endswith("__pycache__"):
            # Skip loading module if it is not in the list of providers that we are looking for
            continue
        package_name = root[len(source_path) + 1:].replace("/", ".")
        for file in files:
            if file.endswith(".py"):
                module_name = package_name + "." + file[:-3] if file != "__init__.py" else package_name
                if print_imports:
                    print(f"Importing module: {module_name}")
                # noinspection PyBroadException
                try:
                    _module = importlib.import_module(module_name)
                    for attribute_name in dir(_module):
                        class_name = module_name + "." + attribute_name
                        attribute = getattr(_module, attribute_name)
                        if isclass(attribute):
                            if print_imports:
                                print(f"Imported {class_name}")
                            imported_classes.append(class_name)
                except Exception:
                    exception_str = traceback.format_exc()
                    tracebacks.append(exception_str)
    if tracebacks:
        print("""
ERROR: There were some import errors
""", file=sys.stderr)
        for trace in tracebacks:
            print("----------------------------------------", file=sys.stderr)
            print(trace, file=sys.stderr)
            print("----------------------------------------", file=sys.stderr)
        sys.exit(1)
    else:
        return imported_classes


if __name__ == '__main__':
    install_source_path = None
    for python_path_candidate in sys.path:
        providers_path_candidate = os.path.join(python_path_candidate, "airflow", "providers")
        if os.path.isdir(providers_path_candidate):
            install_source_path = python_path_candidate
    print()
    print(f"Walking all paths in {install_source_path}")
    print()
    import_all_provider_classes(print_imports=True, source_path=install_source_path)
    print()
    print("SUCCESS: All backport packages are importable!")
    print()
