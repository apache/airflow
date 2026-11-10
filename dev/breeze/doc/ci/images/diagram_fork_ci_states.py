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
# requires-python = ">=3.11"
# dependencies = [
#    "rich>=13.6.0",
#    "graphviz>=0.20.1",
# ]
# ///
"""The fork CI reconciler's states for a gated pull request. Used in dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md and on the ADIP-12 wiki page."""

from __future__ import annotations

from pathlib import Path

import graphviz
from rich.console import Console

MY_DIR = Path(__file__).parent
MY_FILENAME = Path(__file__).with_suffix("").name

console = Console(width=400, color_system="standard")

DOT = r"""
digraph states {
  graph [rankdir=TB, fontname="Helvetica", fontsize=11, nodesep=0.45, ranksep=0.5, compound=true, bgcolor="white"];
  node  [shape=box, style="rounded,filled", fillcolor="#f2f4f7", color="#5b6b80", fontname="Helvetica", fontsize=11];
  edge  [fontname="Helvetica", fontsize=9, color="#5b6b80", fontcolor="#33404d"];

  start [shape=circle, label="", width=0.22, fillcolor="#33404d", color="#33404d"];

  subgraph cluster_gated {
    label="gated (label: fork ci required)"; fontname="Helvetica"; fontsize=12; style="rounded"; color="#8a96a3";
    setup_required [label="setup_required\nworkflow not enabled in the fork\n(draft)"];
    awaiting_push  [label="awaiting_push\nworkflow enabled, no run\nfor the head commit (draft)"];
    running        [label="running\nfork run in progress"];
    failed         [label="failed\nrun failed, cancelled, timed out\nor could not start", fillcolor="#fde8e8", color="#b54848"];
    green          [label="green\nno CI-defining change\nready for maintainer review", fillcolor="#e6f4ea", color="#3d8b57"];
    green_unverified [label="green_unverified\nCI-defining change\nneeds project ci", fillcolor="#fff6db", color="#b38a1d"];
  }

  closed        [label="closed\nsetup_required or awaiting_push\nfor 30 days", fillcolor="#eceff3", color="#8a96a3"];
  released      [label="released\nmaintainer added use project ci"];
  ungated       [label="ungated\nbase branch without fork CI"];
  gate_modified [label="gate_modified\nPR changes the gate (closed)", fillcolor="#fde8e8", color="#b54848"];
  gate_bypassed [label="gate_bypassed\nproject CI ran without\nuse project ci (closed)", fillcolor="#fde8e8", color="#b54848"];

  start -> setup_required [label="discovery adds\nfork ci required", lhead=cluster_gated];
  setup_required -> awaiting_push [label="author enables\nthe workflow"];
  awaiting_push -> running [label="author pushes"];
  running -> failed [label="run fails"];
  running -> green [label="run passes"];
  running -> green_unverified [label="run passes,\nCI files changed"];
  failed -> running [label="author pushes", style=dashed];
  green -> running [label="author pushes", style=dashed];
  green_unverified -> running [label="author pushes", style=dashed];
  running -> setup_required [label="workflow disabled", style=dotted];
  awaiting_push -> setup_required [label="workflow disabled", style=dotted];

  awaiting_push -> closed [label="30 days", ltail=cluster_gated];
  closed -> awaiting_push [label="reopened", style=dashed, lhead=cluster_gated];
  green_unverified -> released [label="maintainer adds\nuse project ci", ltail=cluster_gated];
  green -> ungated [label="base without fork CI", ltail=cluster_gated];
  failed -> gate_modified [label="PR changes the gate", ltail=cluster_gated];
  running -> gate_bypassed [label="gate tampering", ltail=cluster_gated];
}
"""


def generate_diagram() -> None:
    image_file = MY_DIR / f"{MY_FILENAME}.svg"
    console.print(f"[bright_blue]Generating {image_file}")
    graphviz.Source(DOT).render(outfile=image_file, format="svg", cleanup=True)
    console.print(f"[green]Generated {image_file}")


if __name__ == "__main__":
    generate_diagram()
