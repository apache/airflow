#!/usr/bin/env python3
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

import subprocess
import sys


def get_staged_files():
    """Return a list of files that are staged for commit."""
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("Error: Unable to retrieve staged files.", file=sys.stderr)
        sys.exit(1)
    return result.stdout.splitlines()


def main():
    staged_files = get_staged_files()
    # Look for any file ending with .zip (case-insensitive)
    zip_files = [f for f in staged_files if f.lower().endswith(".zip")]
    if zip_files:
        print("Error: Committing .zip files is not allowed.")
        print("The following .zip file(s) were found in your staged changes:")
        for file in zip_files:
            print("  -", file)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
