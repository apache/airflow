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
Gets all tests cases from xunit file.
"""
from __future__ import annotations

import sys
from xml.etree import ElementTree


def last_replace(s, old, new, number_of_occurrences):
    """
    Replaces last n occurrences of the old string with the new one within the string provided

    :param s: string to replace occurrences with
    :param old: old string
    :param new: new string
    :param number_of_occurrences: how many occurrences should be replaced
    :return: string with last n occurrences replaced
    """
    list_of_components = s.rsplit(old, number_of_occurrences)
    return new.join(list_of_components)


def print_all_cases(xunit_test_file_path):
    """
    Prints all test cases read from the xunit test file

    :param xunit_test_file_path: path of the xunit file
    :return: None
    """
    with open(xunit_test_file_path) as file:
        text = file.read()

    root = ElementTree.fromstring(text)

    test_cases = root.findall(".//testcase")
    classes = set()
    modules = set()

    for test_case in test_cases:
        the_module = test_case["classname"].rpartition(".")[0]
        the_class = last_replace(test_case.get("classname"), ".", ":", 1)
        test_method = test_case.get("name")
        modules.add(the_module)
        classes.add(the_class)
        print(the_class + "." + test_method)

    for the_class in classes:
        print(the_class)

    for the_module in modules:
        print(the_module)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide name of xml unit file as first parameter")
        sys.exit(1)
    file_name = sys.argv[1]
    print_all_cases(file_name)
