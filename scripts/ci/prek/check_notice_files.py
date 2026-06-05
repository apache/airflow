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
# /// script
# requires-python = ">=3.10, <3.11"
# dependencies = []
# ///
"""
Check that NOTICE files contain the Apache Software Foundation reference.

This script validates NOTICE files to ensure they reference the Apache Software Foundation.
The copyright year is intentionally not checked here to avoid CI failures at the start of
a new year. Year updates should be performed manually by the release manager using
``prek run update-notice-year --all-files`` before the first release of each new year.

Usage: check_notice_files.py <notice_file_paths...>
"""

from __future__ import annotations

import sys
from pathlib import Path

ASF_DECLARATION = "The Apache Software Foundation"

errors = 0

for notice_file in sys.argv[1:]:
    content = Path(notice_file).read_text()

    if ASF_DECLARATION not in content:
        print(f"❌ {notice_file}: Missing expected string: {ASF_DECLARATION!r}")
        errors += 1

sys.exit(1 if errors else 0)
