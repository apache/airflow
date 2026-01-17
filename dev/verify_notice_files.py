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
"""
Verify NOTICE files in Airflow packages.

This script validates NOTICE files to ensure they:
- Include the current year in copyright statements
- Reference the Apache Software Foundation

Usage: 
    # Check all source NOTICE files
    python3 dev/verify_notice_files.py

    # Check specific NOTICE files
    python3 dev/verify_notice_files.py path/to/NOTICE [path/to/another/NOTICE ...]
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

CURRENT_YEAR = str(datetime.now().year)
EXPECTED_COPYRIGHT = f"Copyright 2016-{CURRENT_YEAR} The Apache Software Foundation"

errors = 0

# Get NOTICE files to check
notice_files = []
if len(sys.argv) > 1:
    # Check files passed as arguments
    notice_files = [Path(f) for f in sys.argv[1:]]
else:
    # Find all NOTICE files in the repository
    root = Path(".")
    notice_files = [
        root / "NOTICE",
        *root.glob("providers/**/NOTICE"),
        *(root / component / "NOTICE" for component in 
          ["airflow-core", "airflow-ctl", "chart", "clients/python", "go-sdk"]),
    ]
    notice_files = [f for f in notice_files if f.exists()]

# Check each NOTICE file
for notice_file in notice_files:
    content = notice_file.read_text()
    
    # Check for Apache Software Foundation reference
    if "Apache Software Foundation" not in content:
        print(f"❌ {notice_file}: Missing 'Apache Software Foundation' reference")
        errors += 1
        continue
    
    # Check copyright year
    if "Copyright" in content and EXPECTED_COPYRIGHT not in content:
        print(f"❌ {notice_file}: Missing expected string: {EXPECTED_COPYRIGHT!r}")
        errors += 1

sys.exit(1 if errors else 0)
