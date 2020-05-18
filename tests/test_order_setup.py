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
import unittest


class TestOrderSetup(unittest.TestCase):

    def setUp(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        self.setup_file = open('{parent_dir}/setup.py'.format(parent_dir=parent_dir))
        self.setup_context = self.setup_file.read()

    def tearDown(self):
        self.setup_file.close()

    def test_main_dependent_group(self):
        """
        Test for an order of dependencies groups between mark
        '# Start dependencies group' and '# End dependencies group' in setup.py
        """
        pattern_main_dependent_group = re.compile(
            '# Start dependencies group\n(.*)# End dependencies group', re.DOTALL)
        main_dependent_group = pattern_main_dependent_group.findall(self.setup_context)[0]

        pattern_sub_dependent = re.compile(' = \\[.*?\\]\n', re.DOTALL)
        main_dependent = pattern_sub_dependent.sub(',', main_dependent_group)

        src = main_dependent.strip(',').split(',')
        alphabetical = sorted(src)
        self.assertListEqual(alphabetical, src)

    def test_sub_dependent_group(self):
        """
        Test for an order of each dependencies groups declare like
        `^dependent_group_name = [.*?]\n` in setup.py
        """
        pattern_dependent_group_name = re.compile('^(\\w+) = \\[', re.MULTILINE)
        dependent_group_names = pattern_dependent_group_name.findall(self.setup_context)

        pattern_dependent_version = re.compile('[~|>|<|=|;].*')
        for group_name in dependent_group_names:
            pattern_sub_dependent = re.compile(
                '{group_name} = \\[(.*?)\\]'.format(group_name=group_name), re.DOTALL)
            sub_dependent = pattern_sub_dependent.findall(self.setup_context)[0]
            pattern_dependent = re.compile('\'(.*?)\'')
            dependent = pattern_dependent.findall(sub_dependent)

            src = [pattern_dependent_version.sub('', p) for p in dependent]
            alphabetical = sorted(src)
            self.assertListEqual(alphabetical, src)

    def test_alias_dependent_group(self):
        """
        Test for an order of each dependencies groups declare like
        `alias_dependent_group = dependent_group_1 + ... + dependent_group_n` in setup.py
        """
        pattern = re.compile('^\\w+ = (\\w+ \\+.*)', re.MULTILINE)
        dependents = pattern.findall(self.setup_context)
        for dependent in dependents:
            src = dependent.split(' + ')
            alphabetical = sorted(src)
            self.assertListEqual(alphabetical, src)

    def test_install_and_setup_requires(self):
        """
        Test for an order of dependencies in function do_setup section
        install_requires and setup_requires in setup.py
        """
        pattern_install_and_setup_requires = re.compile(
            '(INSTALL_REQUIREMENTS|setup_requires) ?= ?\\[(.*?)\\]', re.DOTALL)
        install_and_setup_requires = pattern_install_and_setup_requires.findall(self.setup_context)

        for dependent_requires in install_and_setup_requires:
            pattern_dependent = re.compile('\'(.*?)\'')
            dependent = pattern_dependent.findall(dependent_requires[1])
            pattern_dependent_version = re.compile('[~|>|<|=|;].*')

            src = [pattern_dependent_version.sub('', p) for p in dependent]
            alphabetical = sorted(src)
            self.assertListEqual(alphabetical, src)

    def test_extras_require(self):
        """
        Test for an order of dependencies in function do_setup section
        extras_require in setup.py
        """
        pattern_extras_requires = re.compile(
            r'EXTRAS_REQUIREMENTS: Dict\[str, Iterable\[str\]] = {(.*?)}', re.DOTALL)
        extras_requires = pattern_extras_requires.findall(self.setup_context)[0]

        pattern_dependent = re.compile('\'(.*?)\'')
        src = pattern_dependent.findall(extras_requires)
        alphabetical = sorted(src)
        self.assertListEqual(alphabetical, src)

    def test_provider_requirements(self):
        """
        Test for an order of dependencies in function do_setup section
        extras_require in setup.py
        """
        pattern_extras_requires = re.compile(
            r'PROVIDERS_REQUIREMENTS: Dict\[str, Iterable\[str\]\] = {(.*?)}', re.DOTALL)
        extras_requires = pattern_extras_requires.findall(self.setup_context)[0]

        pattern_dependent = re.compile('\'(.*?)\'')
        src = pattern_dependent.findall(extras_requires)
        alphabetical = sorted(src)
        self.assertListEqual(alphabetical, src)
