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
Checks for an order of dependencies in setup.py
"""

import os
import re
import sys
from os.path import abspath, dirname
from typing import List

SETUP_PY_FILE = 'setup.py'
INSTALLATION_RST_FILE = 'installation.rst'

def get_setup_py_content() -> str:
    setup_py_file_path = abspath(os.path.join(dirname(__file__), os.pardir,
        os.pardir, os.pardir, SETUP_PY_FILE))
    with open(setup_py_file_path) as setup_file:
        setup_content = setup_file.read()
    return setup_content

def get_installation_rst_content() -> str:
    installation_rst_file_path = abspath(os.path.join(dirname(__file__), os.pardir,
        os.pardir, os.pardir, 'docs', INSTALLATION_RST_FILE))
    with open(installation_rst_file_path) as installation_file:
        installation_rst_content = installation_file.read()
    return installation_rst_content

def get_main_dependent_group(setup_content: str) -> []:
    """
    Test for an order of dependencies groups between mark
    '# Start dependencies group' and '# End dependencies group' in setup.py
    """
    pattern_main_dependent_group = re.compile(
        '# Start dependencies group\n(.*)# End dependencies group', re.DOTALL)
    main_dependent_group = pattern_main_dependent_group.findall(setup_content)[0]

    pattern_sub_dependent = re.compile('^([a-zA-Z_][a-zA-Z0-9_]*)\s+=',
            re.MULTILINE)

    return pattern_sub_dependent.findall(main_dependent_group)

def get_extra_packages(installation_rst_content: str) -> []:
    pattern_sub_dependent = re.compile('^\|\s([a-zA-Z_][a-zA-Z0-9_]*)', re.MULTILINE)
    doc_entries = pattern_sub_dependent.findall(installation_rst_content)
    return list(filter(lambda entry: entry != 'subpackage', doc_entries))

if __name__ == '__main__':
    setup_context_main = get_setup_py_content()
    libraries = get_main_dependent_group(setup_context_main)

    installation_rst_context = get_installation_rst_content()
    doc_entries = get_extra_packages(installation_rst_context)

    print(f"These libraries are not present in {SETUP_PY_FILE} but missing "
            "in {INSTALLATION_RST_FILE}:")
    print([x for x in libraries if x not in set(doc_entries)])
    print([x for x in doc_entries if x not in set(libraries)])


