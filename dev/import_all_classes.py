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
import argparse
import importlib
import pkgutil
import sys
import traceback
from inspect import isclass
from typing import List


def import_all_classes(
    paths: List[str],
    prefix: str,
    provider_ids: List[str] = None,
    print_imports: bool = False,
    print_skips: bool = False,
) -> List[str]:
    """
    Imports all classes in providers packages. This method loads and imports
    all the classes found in providers, so that we can find all the subclasses
    of operators/sensors etc.

    :param paths: list of paths to look the provider packages in
    :param prefix: prefix to add
    :param provider_ids - provider ids that should be loaded.
    :param print_imports - if imported class should also be printed in output
    :param print_skips - if skipped classes should also be printed in output
    :return: list of all imported classes
    """
    imported_classes = []
    tracebacks = []

    def mk_prefix(provider_id):
        return f'{prefix}{provider_id}'

    if provider_ids:
        provider_prefixes = [mk_prefix(provider_id) for provider_id in provider_ids]
    else:
        provider_prefixes = [prefix]

    def onerror(_):
        nonlocal tracebacks
        exception_string = traceback.format_exc()
        if any([provider_prefix in exception_string for provider_prefix in provider_prefixes]):
            tracebacks.append(exception_string)

    for modinfo in pkgutil.walk_packages(path=paths, prefix=prefix, onerror=onerror):
        if not any(modinfo.name.startswith(provider_prefix) for provider_prefix in provider_prefixes):
            if print_skips:
                print(f"Skipping module: {modinfo.name}")
            continue
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

    parser = argparse.ArgumentParser(description='Perform import of all provider classes.')
    parser.add_argument('--path', action='append', help='paths to search providers in')
    parser.add_argument('--prefix', help='prefix to add in front of the class', default='airflow.providers.')

    args = parser.parse_args()
    print()
    print(f"Walking all packages in {args.path} with prefix {args.prefix}")
    print()
    classes = import_all_classes(print_imports=True, print_skips=True, paths=args.path, prefix=args.prefix)
    if len(classes) == 0:
        raise Exception("Something is seriously wrong - no classes imported")
    print()
    print(f"SUCCESS: All provider packages are importable! Imported {len(classes)} classes.")
    print()
