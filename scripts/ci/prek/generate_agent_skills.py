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
"""Sync the AGENTS.md Commands section from contributing docs.

Contributing docs contain ``.. AGENT-SKILL-START`` / ``.. AGENT-SKILL-END``
RST comment blocks with pre-formatted AGENTS.md bullet lines. This script
collects those blocks, sorts them by ``order``, and updates the
generated-commands section in AGENTS.md via ``insert_documentation()``.

The prek hook runs this automatically when contributing docs or AGENTS.md
change.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml
from common_prek_utils import AIRFLOW_ROOT_PATH, console, insert_documentation

CONTRIBUTING_DOCS_ROOT = AIRFLOW_ROOT_PATH / "contributing-docs"
AGENTS_MD_PATH = AIRFLOW_ROOT_PATH / "AGENTS.md"
AGENTS_MD_START = "<!-- START generated-commands, please keep comment here to allow auto update -->"
AGENTS_MD_END = "<!-- END generated-commands, please keep comment here to allow auto update -->"
BLOCK_RE = re.compile(r"\.\. AGENT-SKILL-START\n(.*?)\.\. AGENT-SKILL-END", re.DOTALL)


@dataclass(frozen=True)
class CommandBlock:
    order: int
    lines: tuple[str, ...]


def _dedent_rst_comment_body(raw: str) -> str:
    """Remove the 3-space RST comment indentation from each line."""
    lines = raw.splitlines()
    return "\n".join(line[3:] if line.startswith("   ") else line for line in lines).strip()


def _parse_blocks_from_rst(rst_path: Path) -> list[CommandBlock]:
    """Extract all AGENT-SKILL command blocks from a single RST file."""
    text = rst_path.read_text()
    blocks: list[CommandBlock] = []
    for match in BLOCK_RE.finditer(text):
        block = yaml.safe_load(_dedent_rst_comment_body(match.group(1)))
        if not isinstance(block, dict) or block.get("type") != "agents-md-commands":
            continue
        order = int(block.get("order", 100))
        raw_lines = block.get("lines", [])
        lines = tuple(str(line).rstrip() for line in raw_lines)
        blocks.append(CommandBlock(order=order, lines=lines))
    return blocks


def collect_blocks(
    contributing_docs_root: Path = CONTRIBUTING_DOCS_ROOT,
) -> list[CommandBlock]:
    """Scan all RST files and return command blocks sorted by order."""
    blocks: list[CommandBlock] = []
    for rst_path in sorted(contributing_docs_root.rglob("*.rst")):
        blocks.extend(_parse_blocks_from_rst(rst_path))
    blocks.sort(key=lambda b: b.order)
    return blocks


def render_lines(blocks: list[CommandBlock]) -> list[str]:
    """Flatten all blocks into a list of lines for insert_documentation()."""
    result: list[str] = []
    for block in blocks:
        for line in block.lines:
            result.append(line + "\n")
    return result


def main() -> int:
    blocks = collect_blocks()
    if not blocks:
        console.print("[red]No AGENT-SKILL blocks found in contributing docs.[/red]")
        return 1
    content = render_lines(blocks)
    insert_documentation(
        file_path=AGENTS_MD_PATH,
        content=content,
        header=AGENTS_MD_START,
        footer=AGENTS_MD_END,
        extra_information="AGENTS.md commands from contributing docs",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
