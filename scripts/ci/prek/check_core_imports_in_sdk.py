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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from common_prek_utils import find_import_violations, report_import_violations

NOCHECK_CODE = "SDK002"


def check_file_for_core_imports(file_path: Path) -> list[tuple[int, str]]:
    """Check file for airflow-core imports (anything except airflow.sdk). Returns list of (line_num, import_statement)."""
    return find_import_violations(
        file_path,
        is_violating_module=lambda module: (
            module.startswith("airflow.") and not module.startswith("airflow.sdk")
        ),
        nocheck_code=NOCHECK_CODE,
        check_plain_imports=True,
    )


def main():
    parser = argparse.ArgumentParser(description="Check for core imports in task-sdk files")
    parser.add_argument("files", nargs="*", help="Files to check")
    args = parser.parse_args()

    if not args.files:
        return

    report_import_violations(
        args.files,
        check_func=check_file_for_core_imports,
        violation_label="core import(s) in task-sdk files",
        nocheck_code=NOCHECK_CODE,
    )


if __name__ == "__main__":
    main()
    sys.exit(0)
