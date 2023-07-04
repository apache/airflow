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

import re
from pathlib import Path

import yaml

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()


if __name__ == "__main__":
    PRE_COMMIT_CONFIG_FILE = AIRFLOW_SOURCES / ".pre-commit-config.yaml"
    pre_commit_content = yaml.safe_load(PRE_COMMIT_CONFIG_FILE.read_text())
    for repo in pre_commit_content["repos"]:
        if repo["repo"] == "https://github.com/psf/black":
            black_version = repo["rev"]
            pre_commit_text = PRE_COMMIT_CONFIG_FILE.read_text()
            pre_commit_text = re.sub(r"black==[0-9\.]*", f"black=={black_version}", pre_commit_text)
            PRE_COMMIT_CONFIG_FILE.write_text(pre_commit_text)
            break
