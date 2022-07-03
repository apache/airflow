#!/usr/bin/env python
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
from __future__ import annotations

import os
import re
import sys
import xml.etree.ElementTree as ET

# Do not use anything outsdie of standard library in this file!
from functools import lru_cache
from pathlib import Path

TEXT_RED = '\033[31m'
TEXT_YELLOW = '\033[33m'
TEXT_RESET = '\033[0m'


@lru_cache(maxsize=None)
def translate_classname(classname):
    # The pytest xunit output has "classname" in the python sense as 'dir.subdir.package.Class' -- we want to
    # convert that back in to a pytest "selector" of dir/subdir/package.py::Class

    if not classname:
        return None

    context = Path.cwd()

    parts = classname.split('.')

    for i, component in enumerate(parts):
        candidate = context / component

        if candidate.is_dir():
            context = candidate
            continue
        candidate = context / (component + ".py")
        if candidate.is_file():
            context = candidate
            i += 1
            break
        break
    parts = parts[i:]

    val = str(context.relative_to(Path.cwd()))

    if parts:
        val += "::" + '.'.join(parts)
    return val


def translate_name(testcase):
    classname = translate_classname(testcase.get("classname"))
    name = testcase.get("name")

    if not classname:
        # Some times (i.e. collect error) the classname is empty and we only have a name
        return translate_classname(name)

    return f'{classname}::{name}'


def summarize_file(input, test_type, backend):
    root = ET.parse(input)

    # <testsuite errors="0" failures="0" hostname="1dea0db81c88" name="pytest" skipped="2" tests="640"
    # time="98.678">

    testsuite = root.find('.//testsuite')

    fail_message = ''

    num = int(testsuite.get('failures'))
    if num:
        fail_message = f"{num} failure"
        if num != 1:
            fail_message += "s"

    num = int(testsuite.get('errors'))
    if num:
        if fail_message:
            fail_message += ', '
        fail_message += f"{num} error"
        if num != 1:
            fail_message += "s"

    if not fail_message:
        return
    print(f'\n{TEXT_RED}==== {test_type} {backend}: {fail_message} ===={TEXT_RESET}\n')

    for testcase in testsuite.findall('.//testcase[error]'):
        case_name = translate_name(testcase)
        for err in testcase.iterfind('error'):
            print(f'{case_name}: {TEXT_YELLOW}{err.get("message")}{TEXT_RESET}')
    for testcase in testsuite.findall('.//testcase[failure]'):
        case_name = translate_name(testcase)
        for failure in testcase.iterfind('failure'):
            print(f'{case_name}: {TEXT_YELLOW}{failure.get("message")}{TEXT_RESET}')


if __name__ == '__main__':
    fname_pattern = re.compile('^test_result-(?P<test_type>.*?)-(?P<backend>.*).xml$')
    for fname in sys.argv[1:]:

        match = fname_pattern.match(os.path.basename(fname))

        if not match:
            exit(f"I cannot understand the name format of {fname!r}")

        with open(fname) as fh:
            summarize_file(fh, **match.groupdict())
