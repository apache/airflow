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
Test for an order of dependencies in setup.py, setup.cfg, and pyproject.toml
"""
import os
import re
import sys
from typing import List

import toml
from rich import print

errors = []

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir, os.pardir, os.pardir))
sys.path.insert(0, SOURCE_DIR_PATH)


def _check_list_sorted(the_list: List[str], message: str) -> None:
    print(the_list)
    sorted_list = sorted(the_list)
    if the_list == sorted_list:
        print(f"{message} is [green]ok[/]")
        print()
        return
    i = 0
    while sorted_list[i] == the_list[i]:
        i += 1
    print(f"{message} [red]NOK[/]")
    print()
    errors.append(
        f"ERROR in {message}. First wrongly sorted element {repr(the_list[i])}. Should "
        f"be {repr(sorted_list[i])}"
    )


def check_main_dependent_group(setup_contents: str) -> None:
    """
    Test for an order of dependencies groups between mark
    '# Start dependencies group' and '# End dependencies group' in setup.py
    """
    print("[blue]Checking main dependency group[/]")
    pattern_main_dependent_group = re.compile(
        '# Start dependencies group\n(.*)# End dependencies group', re.DOTALL
    )
    main_dependent_group = pattern_main_dependent_group.findall(setup_contents)[0]

    pattern_sub_dependent = re.compile(r' = \[.*?]\n', re.DOTALL)
    main_dependent = pattern_sub_dependent.sub(',', main_dependent_group)

    src = main_dependent.strip(',').split(',')
    _check_list_sorted(src, "Order of dependencies")

    for group in src:
        check_sub_dependent_group(group)


def check_sub_dependent_group(group_name: str) -> None:
    r"""
    Test for an order of each dependencies groups declare like
    `^dependent_group_name = [.*?]\n` in setup.py
    """
    print(f"[blue]Checking dependency group {group_name}[/]")
    _check_list_sorted(getattr(setup, group_name), f"Order of dependency group: {group_name}")


def check_alias_dependent_group(setup_context: str) -> None:
    """
    Test for an order of each dependencies groups declare like
    `alias_dependent_group = dependent_group_1 + ... + dependent_group_n` in setup.py
    """
    pattern = re.compile('^\\w+ = (\\w+ \\+.*)', re.MULTILINE)
    dependents = pattern.findall(setup_context)

    for dependent in dependents:
        print(f"[blue]Checking alias-dependent group {dependent}[/]")
        src = dependent.split(' + ')
        _check_list_sorted(src, f"Order of alias dependencies group: {dependent}")


def check_variable_order(var_name: str) -> None:
    print(f"[blue]Checking {var_name}[/]")

    var = getattr(setup, var_name)

    if isinstance(var, dict):
        _check_list_sorted(list(var.keys()), f"Order of dependencies in: {var_name}")
    else:
        _check_list_sorted(var, f"Order of dependencies in: {var_name}")


def check_install_requires() -> None:
    """
    Test for an order of dependencies in section install_requires in setup.cfg
    """

    from setuptools.config import read_configuration

    path = os.path.join(SOURCE_DIR_PATH, 'setup.cfg')
    config = read_configuration(path)

    pattern_dependent_version = re.compile('[~|><=;].*')

    key = 'install_requires'
    print(f"[blue]Checking setup.cfg group {key}[/]")
    deps = config['options'][key]
    dists = [pattern_dependent_version.sub('', p) for p in deps]
    _check_list_sorted(dists, f"Order of dependencies in setup.cfg section: {key}")


def check_build_system_requires() -> None:
    """
    Test for an order of dependencies in [build-system] requires in pyproject.toml
    """
    with open(os.path.join(SOURCE_DIR_PATH, "pyproject.toml")) as f:
        pyproject = toml.load(f)
    key = "[build-system] requires"

    pattern_dependent_version = re.compile('[~|><=;].*')
    print(f"[blue]Checking pyproject.toml key {key} [/]")
    deps = pyproject["build-system"]["requires"]
    dists = [pattern_dependent_version.sub('', p) for p in deps]
    _check_list_sorted(dists, f"Order of dependencies in pyproject.toml section: {key}")


if __name__ == '__main__':
    import setup

    with open(setup.__file__) as setup_file:
        file_contents = setup_file.read()
    check_main_dependent_group(file_contents)
    check_alias_dependent_group(file_contents)
    check_variable_order("PROVIDERS_REQUIREMENTS")
    check_variable_order("CORE_EXTRAS_REQUIREMENTS")
    check_variable_order("ADDITIONAL_EXTRAS_REQUIREMENTS")
    check_variable_order("EXTRAS_DEPRECATED_ALIASES")
    check_variable_order("PREINSTALLED_PROVIDERS")
    check_install_requires()
    check_build_system_requires()

    print()
    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
