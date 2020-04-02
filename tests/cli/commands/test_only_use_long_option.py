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

import os
import re
import unittest


class TestOnlyUseLongOption(unittest.TestCase):

    def test_command_only_long_option(self):
        """
        Make sure all cli.commands test use long option for more clearer intent
        """
        pattern_1 = re.compile("\"-[a-zA-Z]\"")
        pattern_2 = re.compile("'-[a-zA-Z]'")
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ignore = ["__init__.py", "__pycache__", os.path.basename(__file__)]
        for test_file in os.listdir(current_dir):
            if test_file in ignore:
                continue
            match = []
            with open(os.path.join(current_dir, test_file), "r") as f:
                content = f.read()
                match.extend(pattern_1.findall(content))
                match.extend(pattern_2.findall(content))
                self.assertListEqual(
                    [],
                    match,
                    "Should use long option in test for more clearer intent, "
                    f"but get {match} in {test_file}"
                )
