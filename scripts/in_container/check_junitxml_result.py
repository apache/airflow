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

import sys
import xml.etree.ElementTree as ET

TEXT_RED = "\033[31m"
TEXT_GREEN = "\033[32m"
TEXT_RESET = "\033[0m"

if __name__ == "__main__":
    fname = sys.argv[1]
    try:
        with open(fname) as fh:
            root = ET.parse(fh)
        testsuite = root.find(".//testsuite")
        if testsuite:
            num_failures = testsuite.get("failures")
            num_errors = testsuite.get("errors")
            if num_failures == "0" and num_errors == "0":
                print(f"\n{TEXT_GREEN}==== No errors, no failures. Good to go! ===={TEXT_RESET}\n")
                sys.exit(0)
            else:
                print(
                    f"\n{TEXT_RED}==== Errors: {num_errors}, Failures: {num_failures}. "
                    f"Failing the test! ===={TEXT_RESET}\n"
                )
                sys.exit(1)
        else:
            print(
                f"\n{TEXT_RED}==== The testsuite element does not exist in file {fname!r}. "
                f"Cannot evaluate status of the test! ===={TEXT_RESET}\n"
            )
            sys.exit(1)
    except Exception as e:
        print(
            f"\n{TEXT_RED}==== There was an error when parsing the junitxml file."
            f" Likely the file was corrupted ===={TEXT_RESET}\n"
        )
        print(f"\n{TEXT_RED}==== Error: {e} {TEXT_RESET}\n")
        sys.exit(2)
