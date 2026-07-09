#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""Check that every common.ai example Dag is reachable from docs/examples.rst.

``examples.rst`` does not always link an example file directly -- for scenarios that
already have a dedicated guide (an operator/hook page under ``docs/operators`` or
``docs/hooks``), it links the guide via ``:doc:``/``:ref:`` and the guide embeds the
example with a ``literalinclude``/``exampleinclude`` directive instead. So "referenced"
here means reachable by following ``:doc:``/``:ref:`` links out of ``examples.rst``,
not necessarily mentioned by filename on that page itself.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_PROVIDERS_ROOT_PATH, console

COMMON_AI_ROOT_PATH = AIRFLOW_PROVIDERS_ROOT_PATH / "common" / "ai"
DOCS_PATH = COMMON_AI_ROOT_PATH / "docs"
EXAMPLES_RST_PATH = DOCS_PATH / "examples.rst"
EXAMPLE_DAGS_PATH = COMMON_AI_ROOT_PATH / "src" / "airflow" / "providers" / "common" / "ai" / "example_dags"

EXAMPLE_FILENAME_RE = re.compile(r"\bexample_[A-Za-z0-9_]+\.py\b")
DOC_ROLE_RE = re.compile(r":doc:`(?:[^`<]+<)?([^`<>]+)>?`")
REF_ROLE_RE = re.compile(r":ref:`(?:[^`<]+<)?([^`<>]+)>?`")


def get_example_dag_filenames() -> set[str]:
    return {path.name for path in EXAMPLE_DAGS_PATH.glob("example_*.py")}


def resolve_doc_target(target: str, referring_file: Path) -> Path | None:
    """Resolve a ``:doc:`` role target to an ``.rst`` file, Sphinx-style."""
    if target.startswith("/"):
        rst_path = DOCS_PATH / target.lstrip("/")
    else:
        rst_path = referring_file.parent / target
    rst_path = rst_path.with_suffix(".rst")
    return rst_path if rst_path.is_file() else None


def resolve_ref_target(label: str) -> Path | None:
    """Resolve a ``:ref:`` role target by finding its ``.. _label:`` definition."""
    label_marker = f".. _{label}:"
    for rst_path in DOCS_PATH.rglob("*.rst"):
        if label_marker in rst_path.read_text(encoding="utf-8"):
            return rst_path
    return None


def find_referenced_example_filenames() -> set[str]:
    """Walk :doc:/:ref: links out of examples.rst, collecting example filenames seen."""
    referenced: set[str] = set()
    visited: set[Path] = set()
    queue = [EXAMPLES_RST_PATH]
    while queue:
        rst_path = queue.pop()
        if rst_path in visited or not rst_path.is_file():
            continue
        visited.add(rst_path)
        text = rst_path.read_text(encoding="utf-8")
        referenced.update(EXAMPLE_FILENAME_RE.findall(text))
        for target in DOC_ROLE_RE.findall(text):
            resolved = resolve_doc_target(target, rst_path)
            if resolved is not None:
                queue.append(resolved)
        for label in REF_ROLE_RE.findall(text):
            resolved = resolve_ref_target(label)
            if resolved is not None:
                queue.append(resolved)
    return referenced


def main() -> int:
    if not EXAMPLES_RST_PATH.is_file():
        console.print(f"[red]Cannot find {EXAMPLES_RST_PATH}.[/]")
        return 1

    all_examples = get_example_dag_filenames()
    referenced_examples = find_referenced_example_filenames()
    missing = sorted(all_examples - referenced_examples)

    if missing:
        console.print(
            f"[red]The following example Dags are not reachable from {EXAMPLES_RST_PATH}[/] "
            "[red](directly, or via a :doc:/:ref: link to a guide that embeds them):[/]"
        )
        for filename in missing:
            console.print(f"[red]  - {filename}[/]")
        console.print(
            "[yellow]Add each new example Dag to examples.rst, either directly or through a "
            "guide page it links to.[/]"
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
