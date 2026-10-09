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

from docs.utils.conf_constants import AIRFLOW_REPO_ROOT_PATH


def example_source_ref(tag: str) -> str:
    """Return HEAD if a remote-tracking ref or tag contains it; otherwise return the release tag."""

    def git(*args: str) -> str:
        try:
            result = subprocess.run(
                ["git", *args], cwd=AIRFLOW_REPO_ROOT_PATH, capture_output=True, text=True, check=False
            )
        except OSError:  # Git may be unavailable when building from an sdist.
            return ""
        return result.stdout.strip()

    head = git("rev-parse", "--verify", "--quiet", "HEAD^{commit}")
    if not head or head == git("rev-parse", "--verify", "--quiet", f"{tag}^{{commit}}"):
        return tag

    # Main builds need current examples; unpublished release-fix commits fall back to the tag.
    pushed = git("for-each-ref", "--contains", head, "--count=1", "refs/remotes", "refs/tags")
    return head if pushed else tag
