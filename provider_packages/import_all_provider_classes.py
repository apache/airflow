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
import pkgutil
import sys
import traceback
from inspect import isclass
from typing import List


def import_all_provider_classes(
    source_paths: str, provider_ids: List[str] = None, print_imports: bool = False
) -> List[str]:
    """
    Imports all classes in providers packages. This method loads and imports
    all the classes found in providers, so that we can find all the subclasses
    of operators/sensors etc.

    :param source_paths: list of paths to look for sources - might be None to look for all packages in all
        source paths
    :param provider_ids - provider ids that should be loaded.
    :param print_imports - if imported class should also be printed in output
    :return: list of all imported classes
    """
    imported_classes = []
    tracebacks = []

    def onerror(_):
        nonlocal tracebacks
        exception_str = traceback.format_exc()
        tracebacks.append(exception_str)

    def mk_path(provider_id):
        provider_path = provider_id.replace('.', '/')
        return (os.path.join(path + provider_path) for path in source_paths)

    def mk_prefix(provider_id):
        if not provider_id:
            return 'airflow.providers.'
        else:
            return f'airflow.providers.{provider_id}.'

    if not provider_ids:
        provider_ids = ['']

    for provider_id in provider_ids:
        for modinfo in pkgutil.walk_packages(
            mk_path(provider_id), prefix=mk_prefix(provider_id), onerror=onerror
        ):
            if print_imports:
                print(f"Importing module: {modinfo.name}")
            try:
                _module = importlib.import_module(modinfo.name)
                for attribute_name in dir(_module):
                    class_name = modinfo.name + "." + attribute_name
                    attribute = getattr(_module, attribute_name)
                    if isclass(attribute):
                        if print_imports:
                            print(f"Imported {class_name}")
                        imported_classes.append(class_name)
            except Exception:  # noqa
                exception_str = traceback.format_exc()
                tracebacks.append(exception_str)
    if tracebacks:
        print(
            """
ERROR: There were some import errors
""",
            file=sys.stderr,
        )
        for trace in tracebacks:
            print("----------------------------------------", file=sys.stderr)
            print(trace, file=sys.stderr)
            print("----------------------------------------", file=sys.stderr)
        sys.exit(1)
    else:
        return imported_classes


if __name__ == '__main__':
    try:
        import airflow.providers

        install_source_path = list(iter(airflow.providers.__path__))
    except ImportError as e:
        print("----------------------------------------", file=sys.stderr)
        print(e, file=sys.stderr)
        print("----------------------------------------", file=sys.stderr)
        sys.exit(1)

    print()
    print(f"Walking all paths in {install_source_path}")
    print()
    import_all_provider_classes(print_imports=True, source_paths=install_source_path)
    print()
    print("SUCCESS: All provider packages are importable!")
    print()
