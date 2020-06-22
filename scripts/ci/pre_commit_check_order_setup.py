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
Test for an order of dependencies in setup.py
"""

import os
import re
import sys
from os.path import abspath, dirname
from typing import List

errors = []


def _check_list_sorted(the_list: List[str], message: str) -> None:
    sorted_list = sorted(the_list)
    if the_list == sorted_list:
        print(f"{message} is ok")
        return
    i = 0
    while sorted_list[i] == the_list[i]:
        i += 1
    print(f"{message} NOK")
    errors.append(f"ERROR in {message}. First wrongly sorted element"
                  f" {the_list[i]}. Should be {sorted_list[i]}")


def setup() -> str:
    setup_py_file_path = abspath(os.path.join(dirname(__file__), os.pardir, os.pardir, 'setup.py'))
    with open(setup_py_file_path) as setup_file:
        setup_context = setup_file.read()
    return setup_context


def check_main_dependent_group(setup_context: str) -> None:
    """
    Test for an order of dependencies groups between mark
    '# Start dependencies group' and '# End dependencies group' in setup.py
    """
    pattern_main_dependent_group = re.compile(
        '# Start dependencies group\n(.*)# End dependencies group', re.DOTALL)
    main_dependent_group = pattern_main_dependent_group.findall(setup_context)[0]

    pattern_sub_dependent = re.compile(' = \\[.*?\\]\n', re.DOTALL)
    main_dependent = pattern_sub_dependent.sub(',', main_dependent_group)

    src = main_dependent.strip(',').split(',')
    _check_list_sorted(src, "Order of dependencies")


def check_sub_dependent_group(setup_context: str) -> None:
    r"""
    Test for an order of each dependencies groups declare like
    `^dependent_group_name = [.*?]\n` in setup.py
    """
    pattern_dependent_group_name = re.compile('^(\\w+) = \\[', re.MULTILINE)
    dependent_group_names = pattern_dependent_group_name.findall(setup_context)

    pattern_dependent_version = re.compile('[~|><=;].*')

    for group_name in dependent_group_names:
        pattern_sub_dependent = re.compile(
            '{group_name} = \\[(.*?)\\]'.format(group_name=group_name), re.DOTALL)
        sub_dependent = pattern_sub_dependent.findall(setup_context)[0]
        pattern_dependent = re.compile('\'(.*?)\'')
        dependent = pattern_dependent.findall(sub_dependent)

        src = [pattern_dependent_version.sub('', p) for p in dependent]
        _check_list_sorted(src, f"Order of sub-dependencies group: {group_name}")


def check_alias_dependent_group(setup_context: str) -> None:
    """
    Test for an order of each dependencies groups declare like
    `alias_dependent_group = dependent_group_1 + ... + dependent_group_n` in setup.py
    """
    pattern = re.compile('^\\w+ = (\\w+ \\+.*)', re.MULTILINE)
    dependents = pattern.findall(setup_context)

    for dependent in dependents:
        src = dependent.split(' + ')
        _check_list_sorted(src, f"Order of alias dependencies group: {dependent}")


def check_install_and_setup_requires(setup_context: str) -> None:
    """
    Test for an order of dependencies in function do_setup section
    install_requires and setup_requires in setup.py
    """
    pattern_install_and_setup_requires = re.compile(
        '(setup_requires) ?= ?\\[(.*?)\\]', re.DOTALL)
    install_and_setup_requires = pattern_install_and_setup_requires.findall(setup_context)

    for dependent_requires in install_and_setup_requires:
        pattern_dependent = re.compile('\'(.*?)\'')
        dependent = pattern_dependent.findall(dependent_requires[1])
        pattern_dependent_version = re.compile('[~|><=;].*')

        src = [pattern_dependent_version.sub('', p) for p in dependent]
        _check_list_sorted(src, f"Order of dependencies in do_setup section: {dependent_requires[0]}")


def check_extras_require(setup_context: str) -> None:
    """
    Test for an order of dependencies in function do_setup section
    extras_require in setup.py
    """
    pattern_extras_requires = re.compile(
        r'EXTRAS_REQUIREMENTS: Dict\[str, Iterable\[str\]] = {(.*?)}', re.DOTALL)
    extras_requires = pattern_extras_requires.findall(setup_context)[0]

    pattern_dependent = re.compile('\'(.*?)\'')
    src = pattern_dependent.findall(extras_requires)
    _check_list_sorted(src, "Order of dependencies in: extras_require")


def check_provider_requirements(setup_context: str) -> None:
    """
    Test for an order of dependencies in function do_setup section
    providers_require in setup.py
    """
    pattern_extras_requires = re.compile(
        r'PROVIDERS_REQUIREMENTS: Dict\[str, Iterable\[str\]\] = {(.*?)}', re.DOTALL)
    extras_requires = pattern_extras_requires.findall(setup_context)[0]

    pattern_dependent = re.compile('"(.*?)"')
    src = pattern_dependent.findall(extras_requires)
    _check_list_sorted(src, "Order of dependencies in: providers_require")


if __name__ == '__main__':
    setup_context_main = setup()
    check_main_dependent_group(setup_context_main)
    check_alias_dependent_group(setup_context_main)
    check_sub_dependent_group(setup_context_main)
    check_install_and_setup_requires(setup_context_main)
    check_extras_require(setup_context_main)
    check_provider_requirements(setup_context_main)

    print()
    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
