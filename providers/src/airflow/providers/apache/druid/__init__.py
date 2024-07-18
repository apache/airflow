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
#
# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE
# OVERWRITTEN WHEN PREPARING DOCUMENTATION FOR THE PACKAGES.
#
# IF YOU WANT TO MODIFY THIS FILE, YOU SHOULD MODIFY THE TEMPLATE
# `PROVIDER__INIT__PY_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze/templates` DIRECTORY
#
from __future__ import annotations

import packaging.version

from airflow import __version__ as airflow_version

__all__ = ["__version__"]

__version__ = "4.0.0"

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse(
    "2.9.0"
):
    raise RuntimeError(
        f"The package `apache-airflow-providers-apache-druid:{__version__}` needs Apache Airflow 2.9.0+"
    )


def airflow_dependency_version():
    import re
    import yaml

    from os.path import join, dirname

    with open(join(dirname(__file__), "provider.yaml"), encoding="utf-8") as file:
        for dependency in yaml.safe_load(file)["dependencies"]:
            if dependency.startswith('apache-airflow'):
                match = re.search(r'>=([\d\.]+)', dependency)
                if match:
                    return packaging.version.parse(match.group(1))
