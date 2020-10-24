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

AIRFLOW_SOURCES_DIR = os.path.join(dirname(__file__), os.pardir, os.pardir, os.pardir)
SETUP_PY_FILE = 'setup.py'
DOCS_FILE = 'installation.rst'
PY_IDENTIFIER = '[a-zA-Z_][a-zA-Z0-9_\.]*'

def get_file_content(*file_path: str) -> str:
    file_path = os.path.join(AIRFLOW_SOURCES_DIR, *file_path)
    with open(file_path) as file_to_read:
        return file_to_read.read()

def get_extras_from_setup() -> {str: bool}:
    """
    Returns an array of extras from setup.py file in format:
    {'package name': is_deprecated, ...]
    """
    setup_content = get_file_content(SETUP_PY_FILE)
    
    extras_section_regex = re.compile(
        '^EXTRAS_REQUIREMENTS: Dict[^{]+{([^}]+)}', re.MULTILINE)
    extras_section = extras_section_regex.findall(setup_content)[0]

    extras_regex = re.compile(
        f'^\s+[\"\']([a-zA-Z_][a-zA-Z0-9_\.]*)[\"\']:\s*([a-zA-Z_][a-zA-Z0-9_\.]*)[^#\n]*(#\s*TODO.*)?$', re.MULTILINE)
    extras = extras_regex.findall(extras_section)

    extras = list(filter(lambda entry: entry[0] == entry[1], extras))

    return {x[0]: ' remove ' in x[2] for x in extras}

def get_extras_from_docs() -> {str: bool}:
    """
    Returns an array of extras from installation.rst file in format:
    {'package name': is_deprecated, ...]
    """
    docs_content = get_file_content('docs', DOCS_FILE)

    extras_section_regex = re.compile(f'^\|([^|]+)\|.*pip install .apache-airflow\[({PY_IDENTIFIER})\].', re.MULTILINE)
    extras = extras_section_regex.findall(docs_content)

    return {x[1]: 'deprecated' in x[0] for x in extras}

if __name__ == '__main__':
    setup_packages = get_extras_from_setup()
    docs_packages = get_extras_from_docs()
    all_packages = {**setup_packages, **docs_packages}

    def format_extras(extras):
        return 

    differences = {item: (setup_packages.get(item, ''), docs_packages.get(item, '')) for item in all_packages}

    if len(differences) == 0:
        exit(0)
    
    print(".{:_^22}.{:_^12}.{:_^12}.".format("EXTRAS", "SETUP", "INSTALLATION"))
    for extras in sorted(differences.keys()):
        if(differences[extras][0] != differences[extras][1]):
            setup_label = differences[extras][0]
            setup_label  = "remove" if setup_label else ("V" if setup_label == False else "!")
            docs_label = differences[extras][1]
            docs_label  = "deprecated" if docs_label else ("V" if docs_label == False else "!")
            print(f"| {extras:20} | {setup_label:^10} | {docs_label:^10} |")

    exit(1)
