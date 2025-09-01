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

import argparse
import sys
from datetime import datetime, timezone

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check for language freeze period and prevent changes to specified files."
    )
    parser.add_argument("--freeze-start-date", required=True, help="Start date of the freeze (YYYY-MM-DD)")
    parser.add_argument("--freeze-end-date", required=True, help="End date of the freeze (YYYY-MM-DD)")
    parser.add_argument("files", nargs="*", help="Files to check.")
    args = parser.parse_args()

    freeze_start = None
    freeze_end = None
    try:
        freeze_start = datetime.strptime(args.freeze_start_date, "%Y-%m-%d").date()
        freeze_end = datetime.strptime(args.freeze_end_date, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"Error: Invalid date format in pre-commit config. {e}", file=sys.stderr)
        sys.exit(1)

    today = datetime.now(timezone.utc).date()

    if freeze_start <= today <= freeze_end:
        if args.files:
            print(
                f"Error: English language freeze is active from {args.freeze_start_date} to "
                f"{args.freeze_end_date}.",
                file=sys.stderr,
            )
            print("Changes to English translation files are not allowed during this period.", file=sys.stderr)
            print("The following files are affected:", file=sys.stderr)
            for file_path in args.files:
                print(f"  - {file_path}", file=sys.stderr)
            sys.exit(1)

    sys.exit(0)
