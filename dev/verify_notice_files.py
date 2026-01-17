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
Verify NOTICE files in Airflow packages for release.

Finds all NOTICE files (sources + distribution packages) and validates them
using the pre-commit hook.

Usage: python3 dev/verify_notice_files.py
"""

from __future__ import annotations

import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

# Collect all NOTICE files to check
notice_files: list[Path] = []

# Source NOTICE files
root = Path(".")
notice_files.extend(f for f in [
    root / "NOTICE",
    *root.glob("providers/**/NOTICE"),
    *(root / c / "NOTICE" for c in ["airflow-core", "airflow-ctl", "chart", "clients/python", "go-sdk"]),
] if f.exists())

# Extract NOTICE from distribution packages
dist_dir = root / "dist"
if dist_dir.exists():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        
        for whl in dist_dir.glob("*.whl"):
            try:
                with zipfile.ZipFile(whl) as zf:
                    for name in zf.namelist():
                        if name.endswith("NOTICE"):
                            zf.extract(name, tmppath)
            except Exception:
                pass
        
        for tgz in dist_dir.glob("*.tar.gz"):
            try:
                with tarfile.open(tgz) as tf:
                    for member in tf.getmembers():
                        if member.name.endswith("NOTICE"):
                            tf.extract(member, tmppath)
            except Exception:
                pass
        
        notice_files.extend(tmppath.rglob("NOTICE"))

# Run the pre-commit hook to check all files
if notice_files:
    result = subprocess.run(
        ["python3", "./scripts/ci/prek/check_notice_files.py", *map(str, notice_files)]
    )
    sys.exit(result.returncode)
