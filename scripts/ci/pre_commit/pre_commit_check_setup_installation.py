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
"""
Checks if all the libraries in setup.py are listed in installation.rst file
"""

import os
import re
import sys
from os.path import dirname
from typing import Dict, List

AIRFLOW_SOURCES_DIR = os.path.join(dirname(__file__), os.pardir, os.pardir, os.pardir)
SETUP_PY_FILE = 'setup.py'
DOCS_FILE = 'installation.rst'
PY_IDENTIFIER = r'[a-zA-Z_][a-zA-Z0-9_\.]*'


def get_file_content(*path_elements: str) -> str:
    file_path = os.path.join(AIRFLOW_SOURCES_DIR, *path_elements)
    with open(file_path) as file_to_read:
        return file_to_read.read()


def get_extras_from_setup() -> Dict[str, List[str]]:
    """
    Returns an array EXTRAS_REQUIREMENTS with aliases from setup.py file in format:
    {'package name': ['alias1', 'alias2], ...}
    """
    setup_content = get_file_content(SETUP_PY_FILE)

    extras_section_regex = re.compile(r'^EXTRAS_REQUIREMENTS: Dict[^{]+{([^}]+)}', re.MULTILINE)
    extras_section = extras_section_regex.findall(setup_content)[0]

    extras_regex = re.compile(
        rf'^\s+[\"\']({PY_IDENTIFIER})[\"\']:\s*({PY_IDENTIFIER})[^#\n]*(#\s*TODO.*)?$', re.MULTILINE
    )

    extras_dict: Dict[str, List[str]] = {}
    for extras in extras_regex.findall(extras_section):
        package = extras[1]
        alias = extras[0]
        if not extras_dict.get(package):
            extras_dict[package] = []
        extras_dict[package].append(alias)
    return extras_dict


def get_extras_from_docs() -> List[str]:
    """
    Returns an array of install packages names from installation.rst.
    """
    docs_content = get_file_content('docs', DOCS_FILE)

    extras_section_regex = re.compile(
        rf'^\|[^|]+\|.*pip install .apache-airflow\[({PY_IDENTIFIER})\].', re.MULTILINE
    )
    extras = extras_section_regex.findall(docs_content)

    extras = list(filter(lambda entry: entry != 'all', extras))
    return extras


if __name__ == '__main__':
    setup_packages = get_extras_from_setup()
    docs_packages = get_extras_from_docs()

    output_table = ""

    for extras in sorted(setup_packages.keys()):
        if not set(setup_packages[extras]).intersection(docs_packages):
            output_table += "| {:20} | {:^10} | {:^10} |\n".format(extras, "V", "")

    setup_packages_str = str(setup_packages)
    for extras in sorted(docs_packages):
        if f"'{extras}'" not in setup_packages_str:
            output_table += "| {:20} | {:^10} | {:^10} |\n".format(extras, "", "V")

    if output_table == "":
        sys.exit(0)

    print(
        f"""
ERROR

"EXTRAS_REQUIREMENTS" section in {SETUP_PY_FILE} should be synchronized
with "Extra Packages" section in documentation file doc/{DOCS_FILE}.

here is a list of packages that are used but are not documented, or
documented although not used.
    """
    )
    print(".{:_^22}.{:_^12}.{:_^12}.".format("NAME", "SETUP", "INSTALLATION"))
    print(output_table)

    sys.exit(1)
