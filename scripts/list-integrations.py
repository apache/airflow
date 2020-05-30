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
import argparse
import inspect
import os
import pkgutil
import sys
from glob import glob
from importlib import import_module

import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.secrets import BaseSecretsBackend
from airflow.sensors.base_sensor_operator import BaseSensorOperator

if __name__ != "__main__":
    raise Exception(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        "To run this script, run the './list-integrations.py' command"
    )

AIRFLOW_ROOT = os.path.abspath(os.path.join(os.path.dirname(airflow.__file__), os.pardir))


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

        for clazz in integration_clazzes:
            found_classes.add("{}.{}".format(clazz.__module__, clazz.__name__))

    return found_classes


prog = "./" + os.path.basename(sys.argv[0])

HELP = """\
List operators, hooks, sensors, secrets backend in the installed Airflow.

You can combine this script with other tools e.g. awk, grep, cut, uniq, sort.
"""

EPILOG = f"""
Examples:

If you want to display only sensors, you can execute the following command.

    {prog} | grep sensors

If you want to display only secrets backend, you can execute the following command.

    {prog} | grep secrets

If you want to count the operators/sensors in each providers package, you can use the following command.

    {prog} | \\
        grep providers | \\
        grep 'sensors\\|operators' | \\
        cut -d "." -f 3 | \\
        uniq -c | \\
        sort -n -r
"""

parser = argparse.ArgumentParser(
    description=HELP, formatter_class=argparse.RawTextHelpFormatter, epilog=EPILOG
)
# argparse handle `-h/--help/` internally
parser.parse_args()

RESOURCE_TYPES = {
    "secrets": BaseSecretsBackend,
    "operators": BaseOperator,
    "sensors": BaseSensorOperator,
    "hooks": BaseHook,
}

for integration_directory, integration_class in RESOURCE_TYPES.items():
    for integration_directory in glob(f"{AIRFLOW_ROOT}/airflow/**/{integration_directory}", recursive=True):
        if "contrib" in integration_directory:
            continue

        for clazz in sorted(_find_clazzes(integration_directory, integration_class)):
            print(clazz)
