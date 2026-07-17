#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
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
#
#
# Usage:
#   uv run prune_old_svn_versions.py [--path PATH] [--execute]
#
# Defaults to current directory. Without --execute it only prints the svn remove commands.
#
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
#   "packaging>=23.0",
# ]
# ///
from __future__ import annotations

import argparse
import os
import subprocess
import sys

from packaging.version import InvalidVersion, Version
from rich.console import Console

console = Console()

# command-line args
parser = argparse.ArgumentParser()
parser.add_argument("--path", "-p", default=".")
parser.add_argument("--execute", "-x", action="store_true", help="Execute svn remove (otherwise dry-run)")
args = parser.parse_args()


def parse_version(v):
    try:
        return Version(v)
    except InvalidVersion:
        return None


root = os.path.abspath(args.path)
entries = [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
parsed = []
for name in entries:
    pv = parse_version(name)
    if pv is not None:
        parsed.append((pv, name))

if not parsed:
    msg = f"No version-like directories found in {root}"
    console.print(f"[bold yellow]{msg}[/]")
    sys.exit(0)

# sort by parsed version (stable)
parsed.sort(key=lambda x: x[0])
kept = parsed[-1][1]
to_remove = [name for _, name in parsed[:-1]]

msg = f"Keeping: {kept}"
console.print(msg)

if not to_remove:
    msg = "No older versions to remove."
    console.print(msg)
    sys.exit(0)

for name in to_remove:
    path = os.path.join(root, name)
    cmd = ["svn", "rm", path]
    if args.execute:
        msg = "Executing: " + " ".join(cmd)
        console.print(f"[cyan]{msg}[/]")
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.returncode != 0:
            msg = f"svn rm failed for {path}: {res.stderr.strip()}"
            console.print(f"[bold red]{msg}[/]")
            sys.exit(res.returncode)
        else:
            msg = f"Removed: {path}"
            console.print(f"[bold green]{msg}[/]")
    else:
        msg = "Dry-run: would run: " + " ".join(cmd)
        console.print(f"[cyan]{msg}[/]")
