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

import subprocess
import sys
from pathlib import Path

import yaml
from termcolor import colored
from wcmatch import glob

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

CONFIG_KEY = "labelPRBasedOnFilePath"

current_files = subprocess.check_output(["git", "ls-files"]).decode().splitlines()
git_root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).decode().strip())
cyborg_config_path = git_root / ".github" / "boring-cyborg.yml"
cyborg_config = yaml.safe_load(cyborg_config_path.read_text())
if CONFIG_KEY not in cyborg_config:
    raise SystemExit(f"Missing section {CONFIG_KEY}")

errors = []
for label, patterns in cyborg_config[CONFIG_KEY].items():
    for pattern in patterns:
        if glob.globfilter(current_files, pattern, flags=glob.G | glob.E):
            continue
        yaml_path = f"{CONFIG_KEY}.{label}"
        errors.append(
            f"Unused pattern [{colored(pattern, 'cyan')}] in [{colored(yaml_path, 'cyan')}] section."
        )

if errors:
    print(f"Found {colored(str(len(errors)), 'red')} problems:")
    print("\n".join(errors))
    sys.exit(1)
else:
    print("No found problems. Have a good day!")
