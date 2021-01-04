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
        f"ERROR in {message}. First wrongly sorted element" f" {the_list[i]}. Should be {sorted_list[i]}"
    )


def setup() -> str:
    setup_py_file_path = abspath(os.path.join(dirname(__file__), os.pardir, os.pardir, os.pardir, 'setup.py'))
    with open(setup_py_file_path) as setup_file:
        setup_context = setup_file.read()
    return setup_context


def check_main_dependent_group(setup_context: str) -> None:
    """
    Test for an order of dependencies groups between mark
    '# Start dependencies group' and '# End dependencies group' in setup.py
    """
    print("[blue]Checking main dependency group[/]")
    pattern_main_dependent_group = re.compile(
        '# Start dependencies group\n(.*)# End dependencies group', re.DOTALL
    )
    main_dependent_group = pattern_main_dependent_group.findall(setup_context)[0]

    pattern_sub_dependent = re.compile(r' = \[.*?]\n', re.DOTALL)
    main_dependent = pattern_sub_dependent.sub(',', main_dependent_group)

    src = main_dependent.strip(',').split(',')
    _check_list_sorted(src, "Order of dependencies")


def check_sub_dependent_group(setup_context: str) -> None:
    r"""
    Test for an order of each dependencies groups declare like
    `^dependent_group_name = [.*?]\n` in setup.py
    """
    pattern_dependent_group_name = re.compile(r'^(\w+) = \[', re.MULTILINE)
    dependent_group_names = pattern_dependent_group_name.findall(setup_context)

    pattern_dependent_version = re.compile(r'[~|><=;].*')

    for group_name in dependent_group_names:
        print(f"[blue]Checking dependency group {group_name}[/]")
        pattern_sub_dependent = re.compile(fr'{group_name} = \[(.*?)]\n', re.DOTALL)
        sub_dependent = pattern_sub_dependent.findall(setup_context)[0]
        pattern_dependent = re.compile(r"'(.*?)'")
        dependent = pattern_dependent.findall(sub_dependent)

        src = [pattern_dependent_version.sub('', p) for p in dependent]
        _check_list_sorted(src, f"Order of dependency group: {group_name}")


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


def check_provider_requirements(setup_context: str) -> None:
    """
    Test for an order of dependencies in PROVIDERS_REQUIREMENTS in setup.py
    """
    print("[blue]Checking providers_requirements[/]")
    pattern_providers_requirements = re.compile(r'PROVIDERS_REQUIREMENTS: [^{]*\{(.*?)}\n', re.DOTALL)
    providers_requirements = pattern_providers_requirements.findall(setup_context)[0]
    pattern_dependent = re.compile("'(.*?)'")
    src = pattern_dependent.findall(providers_requirements)
    _check_list_sorted(src, "Order of dependencies in: providers_require")


def check_extras_require(setup_context: str) -> None:
    """
    Test for an order of dependencies in EXTRAS_REQUIREMENTS in setup.py
    """
    print("[blue]Checking extras_requirements[/]")
    pattern_extras_requires = re.compile(r'EXTRAS_REQUIREMENTS: [^{]*{(.*?)}\n', re.DOTALL)
    extras_requires = pattern_extras_requires.findall(setup_context)[0]
    pattern_dependent = re.compile(r"'(.*?)'")
    src = pattern_dependent.findall(extras_requires)
    _check_list_sorted(src, "Order of dependencies in: extras_require")


def check_extras_deprecated_aliases(setup_context: str) -> None:
    """
    Test for an order of dependencies in EXTRAS_DEPRECATED_ALIASES in setup.py
    """
    print("[blue]Checking extras deprecated aliases[/]")
    pattern_extras_deprecated_aliases = re.compile(r'EXTRAS_DEPRECATED_ALIASES: [^{]*{(.*?)}\n', re.DOTALL)
    extras_deprecated_aliases = pattern_extras_deprecated_aliases.findall(setup_context)[0]
    pattern_dependent = re.compile("'(.*?)',")
    src = pattern_dependent.findall(extras_deprecated_aliases)
    _check_list_sorted(src, "Order of dependencies in: extras_deprecated_aliases")


def check_preinstalled_providers(setup_context: str) -> None:
    """
    Test for an order of providers in PREINSTALLED_PROVIDERS in setup.py
    """
    print("[blue]Checking preinstalled providers[/]")
    pattern_preinstalled_providers = re.compile(r'PREINSTALLED_PROVIDERS = \[(.*?)]\n', re.DOTALL)
    preinstalled_providers = pattern_preinstalled_providers.findall(setup_context)[0]
    pattern_dependent = re.compile("'(.*?)',")
    src = pattern_dependent.findall(preinstalled_providers)
    _check_list_sorted(src, "Order of dependencies in: preinstalled_providers")


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
        print(f"[blue]Checking setup.cfg group {key}[/]")
        deps = config['options'][key]
        dists = [pattern_dependent_version.sub('', p) for p in deps]
        _check_list_sorted(dists, f"Order of dependencies in do_setup section: {key}")


if __name__ == '__main__':
    setup_context_main = setup()
    check_main_dependent_group(setup_context_main)
    check_alias_dependent_group(setup_context_main)
    check_sub_dependent_group(setup_context_main)
    check_provider_requirements(setup_context_main)
    check_extras_require(setup_context_main)
    check_extras_deprecated_aliases(setup_context_main)
    check_preinstalled_providers(setup_context_main)
    check_install_and_setup_requires()

    print()
    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
