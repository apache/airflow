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

import argparse
import inspect
import os
import pkgutil
import sys
from importlib import import_module
from pathlib import Path

import airflow
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.secrets import BaseSecretsBackend
from airflow.sensors.base import BaseSensorOperator

program = f"./{__file__}" if not __file__.startswith("./") else __file__

if __name__ != "__main__":
    raise SystemExit(
        "This file is intended to be used as an executable program. You cannot use it as a module."
        f"To execute this script, run the '{program}' command"
    )

AIRFLOW_ROOT = Path(airflow.__file__).resolve().parents[1]


def _find_clazzes(directory, base_class):
    found_classes = set()
    for module_finder, name, ispkg in pkgutil.iter_modules([directory]):
        if ispkg:
            continue

        relative_path = os.path.relpath(module_finder.path, AIRFLOW_ROOT)
        package_name = relative_path.replace("/", ".")
        full_module_name = package_name + "." + name
        try:
            mod = import_module(full_module_name)
        except ModuleNotFoundError:
            print(f"Module {full_module_name} can not be loaded.", file=sys.stderr)
            continue

        clazzes = inspect.getmembers(mod, inspect.isclass)
        integration_clazzes = [
            clazz
            for name, clazz in clazzes
            if issubclass(clazz, base_class) and clazz.__module__.startswith(package_name)
        ]

        for found_clazz in integration_clazzes:
            found_classes.add(f"{found_clazz.__module__}.{found_clazz.__name__}")

    return found_classes


HELP = """\
List operators, hooks, sensors, secrets backend in the installed Airflow.

You can combine this script with other tools e.g. awk, grep, cut, uniq, sort.
"""

EPILOG = f"""
Examples:

If you want to display only sensors, you can execute the following command.

    {program} | grep sensors

If you want to display only secrets backend, you can execute the following command.

    {program} | grep secrets

If you want to count the operators/sensors in each providers package, you can use the following command.

    {program} | \\
        grep providers | \\
        grep 'sensors\\|operators' | \\
        cut -d "." -f 3 | \\
        uniq -c | \\
        sort -n -r
"""

parser = argparse.ArgumentParser(
    prog=program,
    description=HELP,
    formatter_class=argparse.RawTextHelpFormatter,
    epilog=EPILOG,
)
# argparse handle `-h/--help/` internally
parser.parse_args()

RESOURCE_TYPES = {
    "secrets": BaseSecretsBackend,
    "operators": BaseOperator,
    "sensors": BaseSensorOperator,
    "hooks": BaseHook,
}

for integration_base_directory, integration_class in RESOURCE_TYPES.items():
    for integration_directory in (AIRFLOW_ROOT / "airflow").rglob(
        integration_base_directory
    ):
        if "contrib" not in integration_directory.parts:
            for clazz_to_print in sorted(
                _find_clazzes(integration_directory, integration_class)
            ):
                print(clazz_to_print)
