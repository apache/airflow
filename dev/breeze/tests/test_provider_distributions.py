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

from pathlib import Path
from unittest.mock import patch

import pytest

from airflow_breeze.prepare_providers import provider_distributions
from airflow_breeze.prepare_providers.provider_distributions import (
    check_flit_worktree_compatibility,
)


@pytest.fixture
def worktree_root(tmp_path: Path) -> Path:
    (tmp_path / ".git").write_text("gitdir: /elsewhere/.git/worktrees/foo\n")
    return tmp_path


@pytest.fixture
def plain_checkout_root(tmp_path: Path) -> Path:
    (tmp_path / ".git").mkdir()
    return tmp_path


def test_worktree_blocks_sdist_build(worktree_root: Path) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", worktree_root):
        with pytest.raises(SystemExit) as exc_info:
            check_flit_worktree_compatibility("sdist")
        assert exc_info.value.code == 1


def test_worktree_blocks_both_format_build(worktree_root: Path) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", worktree_root):
        with pytest.raises(SystemExit) as exc_info:
            check_flit_worktree_compatibility("both")
        assert exc_info.value.code == 1


def test_worktree_allows_wheel_build(worktree_root: Path) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", worktree_root):
        check_flit_worktree_compatibility("wheel")


def test_plain_checkout_allows_sdist_build(plain_checkout_root: Path) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", plain_checkout_root):
        check_flit_worktree_compatibility("sdist")
        check_flit_worktree_compatibility("both")
        check_flit_worktree_compatibility("wheel")
