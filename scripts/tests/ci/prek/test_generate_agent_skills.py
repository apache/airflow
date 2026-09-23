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

from ci.prek.generate_agent_skills import (
    collect_blocks,
    render_lines,
)


def test_collect_blocks_finds_all_markers():
    blocks = collect_blocks()

    assert len(blocks) >= 3


def test_collect_blocks_sorted_by_order():
    blocks = collect_blocks()

    orders = [b.order for b in blocks]
    assert orders == sorted(orders)


def test_render_lines_produces_bullet_list():
    blocks = collect_blocks()
    lines = render_lines(blocks)

    assert len(lines) > 0
    assert all(line.startswith("- **") for line in lines)
    assert any("uv run --project <PROJECT> pytest" in line for line in lines)
    assert any("breeze testing helm-tests" in line for line in lines)
    assert any("prek run mypy-<project>" in line for line in lines)
    assert any("breeze ci selective-check --commit-ref" in line for line in lines)
