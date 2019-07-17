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
Prints summary of test cases from xunit file.
"""
import argparse
from xml.etree import ElementTree


def print_cases(xmlunit_test_file, only_failed=False):
    """Prints tests cases."""
    with open(xmlunit_test_file, "r") as file:
        text = file.read()

    root = ElementTree.fromstring(text)

    test_cases = root.findall('.//testcase')
    for test_case in test_cases:
        errors = test_case.findall('error')
        failures = test_case.findall('failure')
        error_string = "".join([" Error:" + error.get('type') for error in errors])
        failure_string = "".join([" Failure:" + failure.get('type') for failure in failures])
        if only_failed and error_string == "" and failure_string == "":
            continue
        print(test_case.get('classname') + "." + test_case.get('name') + error_string + failure_string)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get test cases from xml file ')
    parser.add_argument('--xunit-file', dest='xunit_file', action='store',
                        help="XML Unit file where results of tests are stored")
    parser.add_argument('--only-failed', dest='only_failed', action='store_true',
                        help="Only display tests which have errors and failures")
    arguments = parser.parse_args()
    print_cases(arguments.xunit_file, only_failed=arguments.only_failed)
