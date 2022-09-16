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
from __future__ import annotations

import difflib
import os
import re
import sys
import textwrap
from os.path import abspath, dirname

from rich import print

errors = []

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir, os.pardir, os.pardir))
sys.path.insert(0, SOURCE_DIR_PATH)


class ConsoleDiff(difflib.Differ):
    def _dump(self, tag, x, lo, hi):
        """Generate comparison results for a same-tagged range."""
        for i in range(lo, hi):
            if tag == "+":
                yield f'[green]{tag} {x[i]}[/]'
            elif tag == "-":
                yield f'[red]{tag} {x[i]}[/]'
            else:
                yield f'{tag} {x[i]}'


def _check_list_sorted(the_list: list[str], message: str) -> None:
    sorted_list = sorted(the_list)
    if the_list == sorted_list:
        print(f"{message} is [green]ok[/]")
        print(the_list)
        print()
        return
    print(textwrap.indent("\n".join(ConsoleDiff().compare(the_list, sorted_list)), " " * 4))
    print()
    errors.append(f"ERROR in {message}. The elements are not sorted.")


def check_main_dependent_group(setup_contents: str) -> None:
    """
    Test for an order of dependencies groups between mark
    '# Start dependencies group' and '# End dependencies group' in setup.py
    """
    print("[info]Checking main dependency group[/]")
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
    print(f"[info]Checking dependency group {group_name}[/]")
    _check_list_sorted(getattr(setup, group_name), f"Order of dependency group: {group_name}")


def check_alias_dependent_group(setup_context: str) -> None:
    """
    Test for an order of each dependencies groups declare like
    `alias_dependent_group = dependent_group_1 + ... + dependent_group_n` in setup.py
    """
    pattern = re.compile('^\\w+ = (\\w+ \\+.*)', re.MULTILINE)
    dependents = pattern.findall(setup_context)

    for dependent in dependents:
        print(f"[info]Checking alias-dependent group {dependent}[/]")
        src = dependent.split(' + ')
        _check_list_sorted(src, f"Order of alias dependencies group: {dependent}")


def check_variable_order(var_name: str) -> None:
    print(f"[info]Checking {var_name}[/]")

    var = getattr(setup, var_name)

    if isinstance(var, dict):
        _check_list_sorted(list(var.keys()), f"Order of dependencies in: {var_name}")
    else:
        _check_list_sorted(var, f"Order of dependencies in: {var_name}")


def check_install_and_setup_requires() -> None:
    """
    Test for an order of dependencies in function do_setup section
    install_requires and setup_requires in setup.cfg
    """

    from setuptools.config import read_configuration

    path = abspath(os.path.join(dirname(__file__), os.pardir, os.pardir, os.pardir, 'setup.cfg'))
    config = read_configuration(path)

    pattern_dependent_version = re.compile('[~|><=;].*')

    for key in ('install_requires', 'setup_requires'):
        print(f"[info]Checking setup.cfg group {key}[/]")
        deps = config['options'][key]
        dists = [pattern_dependent_version.sub('', p) for p in deps]
        _check_list_sorted(dists, f"Order of dependencies in do_setup section: {key}")


if __name__ == '__main__':
    import setup

    with open(setup.__file__) as setup_file:
        file_contents = setup_file.read()
    check_main_dependent_group(file_contents)
    check_alias_dependent_group(file_contents)
    check_variable_order("CORE_EXTRAS_DEPENDENCIES")
    check_variable_order("ADDITIONAL_EXTRAS_DEPENDENCIES")
    check_variable_order("EXTRAS_DEPRECATED_ALIASES")
    check_variable_order("PREINSTALLED_PROVIDERS")
    check_install_and_setup_requires()

    print()
    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
