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

# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S101

from __future__ import annotations

import filecmp
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.generate_agent_skills import main as generate_main


def test_docs_and_committed_skills_are_in_sync(tmp_path) -> None:
    """Fail fast in CI when docs and committed standard skill folders drift apart."""
    project_root = Path(__file__).resolve().parents[1]
    docs_path = project_root / "docs" / "CONTRIBUTING_POC.rst"
    committed_skills_dir = project_root / "skills"
    generated_skills_dir = tmp_path / "skills"

    exit_code = generate_main([str(docs_path), str(generated_skills_dir)])
    assert exit_code == 0

    comparison = filecmp.dircmp(committed_skills_dir, generated_skills_dir)
    assert comparison.left_only == []
    assert comparison.right_only == []
    assert comparison.funny_files == []

    for common_file in comparison.common_files:
        left = committed_skills_dir / common_file
        right = generated_skills_dir / common_file
        assert left.read_text(encoding="utf-8") == right.read_text(encoding="utf-8")

    for subdir in comparison.common_dirs:
        left_subdir = committed_skills_dir / subdir
        right_subdir = generated_skills_dir / subdir
        sub_cmp = filecmp.dircmp(left_subdir, right_subdir)
        assert sub_cmp.left_only == []
        assert sub_cmp.right_only == []
        for file_name in sub_cmp.common_files:
            left_file = left_subdir / file_name
            right_file = right_subdir / file_name
            assert left_file.read_text(encoding="utf-8") == right_file.read_text(encoding="utf-8")
